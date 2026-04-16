# Contributing Guidelines

## Development Setup

```bash
git clone https://github.com/Snehabankapalli/ai-data-quality-agent-llm-data-validation-automation
cd ai-data-quality-agent-llm-data-validation-automation

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install flake8 black mypy pytest pytest-cov pytest-mock
```

## Required Environment Variables

```bash
export ANTHROPIC_API_KEY=your-key    # Never hardcode
export AIRFLOW_API_URL=http://your-airflow:8080
export SNOWFLAKE_ACCOUNT=your-account
export SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

## Workflow

1. Fork the repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Add implementation + tests (mock external APIs in tests)
4. Run quality checks
5. Commit: `git commit -m 'feat: add your feature'`
6. Open a Pull Request

## Quality Standards

```bash
black src/ tests/
flake8 src/ tests/ --max-line-length=100
mypy src/
pytest tests/ -v --cov=src --cov-fail-under=80
```

## Testing Guidelines

- Mock all external API calls (Claude, Airflow, Snowflake, Slack)
- Use `sample_failures/` fixtures for diagnosis tests
- Test both success and failure paths
- No real credentials in test code

## Commit Format

`<type>: <description>` — types: `feat`, `fix`, `refactor`, `docs`, `test`

Questions? Open an issue or contact [@Snehabankapalli](https://github.com/Snehabankapalli)
