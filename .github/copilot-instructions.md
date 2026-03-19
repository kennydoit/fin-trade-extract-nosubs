# Project Instructions

## Dependency Management

- All project dependencies are managed in `pyproject.toml` under `[project].dependencies`.
- Do **not** use or create `requirements.txt`. It has been removed from this project.
- To add a dependency: `uv add <package>`
- To remove a dependency: `uv remove <package>`
- To install/sync all dependencies: `uv sync`

## Virtual Environment

- The virtual environment is provisioned and managed by `uv`.
- The venv is located at `.venv/` in the project root.
- Always use the `uv`-managed venv: `source .venv/bin/activate` or prefix commands with `uv run`.
- Do **not** use `pip install` directly. Use `uv add` or `uv pip install` instead.
