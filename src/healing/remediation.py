"""
Self-healing remediation engine.
Executes auto-fix actions when Claude recommends them.
"""

import subprocess
from dataclasses import dataclass

import structlog
from tenacity import retry, stop_after_attempt, wait_fixed

from src.snowflake_client import SnowflakeClient

logger = structlog.get_logger()


@dataclass
class HealingResult:
    """Result of a remediation action."""

    action: str
    target: str
    success: bool
    message: str
    retries_used: int = 0


class RemediationEngine:
    """
    Executes self-healing actions recommended by the Claude agent.

    Available actions:
    - refresh_materialized_view: Refreshes a Snowflake MV
    - trigger_dbt_run:           Re-runs specific dbt models
    - resume_snowpipe:           Resumes a paused Snowpipe
    """

    def __init__(self, snowflake_client: SnowflakeClient, max_retries: int = 3):
        self._sf = snowflake_client
        self._max_retries = max_retries

    def execute(self, action: str, target: str, **kwargs) -> HealingResult:
        """
        Dispatch a healing action by name.

        Args:
            action: One of 'refresh_materialized_view', 'trigger_dbt_run', 'resume_snowpipe'
            target: The object to act on (view name, model list, pipe name)

        Returns:
            HealingResult with success status and message
        """
        logger.info("healing_action_dispatched", action=action, target=target)

        action_map = {
            "refresh_materialized_view": self._refresh_mv,
            "trigger_dbt_run": self._trigger_dbt,
            "resume_snowpipe": self._resume_snowpipe,
        }

        handler = action_map.get(action)
        if not handler:
            return HealingResult(
                action=action,
                target=target,
                success=False,
                message=f"Unknown healing action: {action}",
            )

        return handler(target, **kwargs)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def _refresh_mv(self, view_name: str, **_) -> HealingResult:
        """Refresh a Snowflake materialized view."""
        success = self._sf.refresh_materialized_view(view_name)
        return HealingResult(
            action="refresh_materialized_view",
            target=view_name,
            success=success,
            message=f"MV {view_name} refreshed successfully."
            if success
            else f"Failed to refresh MV {view_name}.",
        )

    def _trigger_dbt(self, models: str | list[str], **_) -> HealingResult:
        """
        Trigger a dbt run for specific models.

        Args:
            models: Comma-separated model names or list of model names
        """
        if isinstance(models, list):
            model_selector = " ".join(models)
        else:
            model_selector = models

        cmd = ["dbt", "run", "--select", model_selector, "--no-partial-parse"]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 min timeout
                check=True,
            )
            logger.info("dbt_run_success", models=model_selector, stdout=result.stdout[-500:])
            return HealingResult(
                action="trigger_dbt_run",
                target=model_selector,
                success=True,
                message=f"dbt run succeeded for: {model_selector}",
            )
        except subprocess.CalledProcessError as e:
            logger.error("dbt_run_failed", models=model_selector, stderr=e.stderr[-500:])
            return HealingResult(
                action="trigger_dbt_run",
                target=model_selector,
                success=False,
                message=f"dbt run failed: {e.stderr[-300:]}",
            )
        except subprocess.TimeoutExpired:
            return HealingResult(
                action="trigger_dbt_run",
                target=model_selector,
                success=False,
                message="dbt run timed out after 10 minutes.",
            )

    def _resume_snowpipe(self, pipe_name: str, **_) -> HealingResult:
        """Resume a paused Snowpipe."""
        try:
            self._sf.query(f"ALTER PIPE {pipe_name} RESUME")
            logger.info("snowpipe_resumed", pipe=pipe_name)
            return HealingResult(
                action="resume_snowpipe",
                target=pipe_name,
                success=True,
                message=f"Snowpipe {pipe_name} resumed.",
            )
        except Exception as e:
            logger.error("snowpipe_resume_failed", pipe=pipe_name, error=str(e))
            return HealingResult(
                action="resume_snowpipe",
                target=pipe_name,
                success=False,
                message=f"Failed to resume Snowpipe: {e}",
            )
