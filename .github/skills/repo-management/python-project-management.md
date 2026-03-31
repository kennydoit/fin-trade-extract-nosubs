# Skill: Python Project Standards

## Purpose
Ensure all Python code in this repository follows consistent, secure, and reproducible patterns for:
- Dependency management using uv and pyproject.toml
- Environment variable–based authentication using .env
- Secure configuration loading
- Codespaces-friendly workflows

---

## Dependency Management Rules
- Always use **uv** for dependency management.
- Never suggest `pip install`, `requirements.txt`, or virtualenv.
- When adding dependencies, use the pattern:

