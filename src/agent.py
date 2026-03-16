"""
AI Data Quality Agent — powered by Claude.

Claude acts as the intelligence layer:
- Analyzes check failures and determines root cause
- Recommends whether to auto-heal or escalate
- Generates human-readable explanations for Slack alerts
- Learns patterns across multiple failing checks
"""

import json
import os
from dataclasses import asdict, dataclass
from typing import Any

import anthropic
import structlog
import yaml
from dotenv import load_dotenv

from src.checks import AnomalyChecker, CompletenessChecker, FreshnessChecker
from src.healing import RemediationEngine
from src.slack_notifier import SlackNotifier
from src.snowflake_client import SnowflakeClient

load_dotenv()
logger = structlog.get_logger()

SYSTEM_PROMPT = """You are a senior data engineer AI agent monitoring production fintech data pipelines.
Your job is to analyze data quality check failures and provide concise, actionable guidance.

When given check results:
1. Identify the root cause (pipeline stale, upstream issue, data anomaly, etc.)
2. Assess business impact (regulatory reporting, credit decisions, customer-facing)
3. Recommend whether to auto-heal or escalate to the on-call engineer
4. Write a clear, non-technical summary for the Slack alert

You have deep knowledge of:
- Snowflake pipelines (dbt, Snowpipe, incremental models)
- Fintech data patterns (credit cards, payments, compliance)
- HIPAA/CFPB/FDIC regulatory requirements

Always respond in valid JSON matching the schema provided."""


@dataclass
class AgentDecision:
    """Claude's analysis and recommendation for a set of check failures."""

    severity: str               # ok | low | medium | high | critical
    root_cause: str             # Short root cause description
    analysis: str               # Detailed analysis for engineers
    slack_summary: str          # Human-friendly summary for Slack
    recommended_action: str     # Specific next step
    auto_heal: bool             # Whether to trigger auto-healing
    heal_action: str | None     # Healing action to execute
    heal_target: str | None     # Target of healing action


class DataQualityAgent:
    """
    Orchestrates data quality checks, Claude analysis, and self-healing.

    Flow:
      1. Load checks config
      2. Run all checks (freshness, completeness, anomaly)
      3. Send failures to Claude for analysis
      4. Execute healing if Claude recommends it
      5. Send Slack alert with Claude's explanation
    """

    def __init__(self):
        self._sf = SnowflakeClient()
        self._claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self._slack = SlackNotifier()
        self._healer = RemediationEngine(
            self._sf,
            max_retries=int(os.environ.get("MAX_HEALING_RETRIES", 3)),
        )
        self._auto_heal_enabled = os.environ.get("AUTO_HEAL_ENABLED", "true").lower() == "true"
        self._config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load checks configuration from YAML."""
        config_path = os.path.join(os.path.dirname(__file__), "..", "config", "checks.yaml")
        with open(config_path) as f:
            return yaml.safe_load(f)

    def run(self) -> dict[str, Any]:
        """
        Execute the full data quality pipeline:
        run checks → analyze with Claude → heal → alert.

        Returns summary dict of all results.
        """
        logger.info("agent_run_started")

        freshness_results = FreshnessChecker(self._sf).run_all(
            self._config["checks"].get("freshness", [])
        )
        completeness_results = CompletenessChecker(self._sf).run_all(
            self._config["checks"].get("completeness", [])
        )
        anomaly_results = AnomalyChecker(self._sf).run_all(
            self._config["checks"].get("anomaly", [])
        )

        all_failures = []
        for r in freshness_results:
            if not r.passed:
                all_failures.append({"type": "freshness", **asdict(r)})
        for r in completeness_results:
            if not r.passed:
                all_failures.append({"type": "completeness", **asdict(r)})
        for r in anomaly_results:
            if not r.passed:
                all_failures.append({"type": "anomaly", **asdict(r)})

        if not all_failures:
            logger.info("all_checks_passed")
            return {"status": "healthy", "failures": 0}

        logger.warning("check_failures_detected", count=len(all_failures))

        decision = self._analyze_with_claude(all_failures)

        healing_result = None
        if self._auto_heal_enabled and decision.auto_heal and decision.heal_action:
            logger.info(
                "auto_healing_triggered",
                action=decision.heal_action,
                target=decision.heal_target,
            )
            result = self._healer.execute(decision.heal_action, decision.heal_target or "")
            healing_result = result.message

        check_summary = "\n".join(f["message"] for f in all_failures)
        self._slack.send_alert(
            title=f"Data Quality Alert: {decision.root_cause}",
            severity=decision.severity,
            check_summary=check_summary,
            claude_analysis=decision.analysis,
            recommended_action=decision.recommended_action,
            healing_triggered=decision.auto_heal and self._auto_heal_enabled,
            healing_result=healing_result,
        )

        logger.info("agent_run_complete", failures=len(all_failures), severity=decision.severity)

        return {
            "status": "failures_detected",
            "failures": len(all_failures),
            "severity": decision.severity,
            "auto_healed": decision.auto_heal and self._auto_heal_enabled,
            "healing_result": healing_result,
        }

    def _analyze_with_claude(self, failures: list[dict[str, Any]]) -> AgentDecision:
        """
        Send check failures to Claude for root cause analysis and recommendation.

        Uses adaptive thinking for complex multi-failure scenarios.
        """
        failures_json = json.dumps(failures, indent=2, default=str)

        user_message = f"""Analyze these data quality check failures from our fintech Snowflake pipelines.

FAILURES:
{failures_json}

Respond ONLY with a JSON object matching this exact schema:
{{
  "severity": "<ok|low|medium|high|critical>",
  "root_cause": "<one sentence root cause>",
  "analysis": "<2-3 sentence detailed analysis for engineers>",
  "slack_summary": "<1-2 sentence non-technical summary for Slack>",
  "recommended_action": "<specific actionable next step>",
  "auto_heal": <true|false>,
  "heal_action": "<refresh_materialized_view|trigger_dbt_run|resume_snowpipe|null>",
  "heal_target": "<target object name or null>"
}}"""

        logger.info("sending_failures_to_claude", failure_count=len(failures))

        response = self._claude.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        # Extract text block (thinking blocks come first with adaptive thinking)
        text_content = next(
            (block.text for block in response.content if block.type == "text"),
            None,
        )

        if not text_content:
            logger.error("claude_returned_no_text")
            return self._fallback_decision(failures)

        try:
            # Strip markdown code fences if present
            clean = text_content.strip()
            if clean.startswith("```"):
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
            data = json.loads(clean.strip())

            return AgentDecision(
                severity=data.get("severity", "high"),
                root_cause=data.get("root_cause", "Unknown"),
                analysis=data.get("analysis", ""),
                slack_summary=data.get("slack_summary", ""),
                recommended_action=data.get("recommended_action", "Investigate immediately."),
                auto_heal=bool(data.get("auto_heal", False)),
                heal_action=data.get("heal_action"),
                heal_target=data.get("heal_target"),
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("claude_response_parse_failed", error=str(e), raw=text_content[:300])
            return self._fallback_decision(failures)

    def _fallback_decision(self, failures: list[dict[str, Any]]) -> AgentDecision:
        """Return a safe default decision when Claude analysis fails."""
        return AgentDecision(
            severity="high",
            root_cause=f"{len(failures)} data quality check(s) failed",
            analysis="Automated analysis unavailable. Manual investigation required.",
            slack_summary=f"{len(failures)} pipeline checks failed. Investigate immediately.",
            recommended_action="Check Snowflake pipeline logs and dbt run history.",
            auto_heal=False,
            heal_action=None,
            heal_target=None,
        )
