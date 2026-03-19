---
description: "Generate a conventional commit message from staged changes and push to GitHub"
agent: "agent"
tools: [changes, run_in_terminal]
---

1. Run `git diff --staged` to see what is staged. If nothing is staged, run `git add -A` to stage all changes first, then re-run `git diff --staged`.
2. Analyze the staged diff and generate a concise conventional commit message following this format:
   - `<type>(<scope>): <short summary>` on the first line (max 72 chars)
   - Types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `style`, `ci`
   - Optional body with bullet points for notable details
3. Run `git commit -m "<message>"` with the generated message.
4. Run `git push` to push the commit to GitHub.
5. Report the commit hash and a summary of what was pushed.
