---
name: commit
description: Stage and commit the current working-tree changes using this repo's conventional-commit style. Inspects git status/diff/log, drafts a type-prefixed subject + grouped body, stages files by explicit name, and creates a single commit with no co-author trailer, no push, no amend. Use when the user says "commit", "commit this", or passes a hint like /commit "rename foo to bar".
---

# Skill: Commit

Stage and commit the current changes using this repo's conventional-commit style.

## Trigger

Use this skill when the user asks:
- "Commit this"
- "Commit the changes"
- "/commit" (with or without a quoted hint)

## What it does

1. Inspects the working tree (`git status`, `git diff`) and recent history (`git log`)
2. Drafts a conventional-commit message that matches existing project style
3. Stages the relevant files **by explicit name** (no `git add -A` / `git add .`)
4. Creates a single commit — **no co-author trailer**, no push, no amend

## Usage

```
/commit                       (analyze changes, propose & create commit)
/commit "rename foo to bar"   (use the quoted hint to bias the subject line)
```

## Steps to execute

Parse any quoted hint from the user's message, then:

1. **Inspect — run these three commands in parallel** (single message, multiple shell calls):
   - `git status` (do **not** pass `-uall` — it can OOM on large repos)
   - `git diff` (shows staged + unstaged combined)
   - `git log --oneline -10` (confirm the style you're matching)

2. **Refuse early** if `git status` shows a clean working tree. Tell the user "nothing to commit" and stop.

3. **Pick the type prefix** based on the dominant change pattern:
   | Pattern | Prefix |
   |---|---|
   | Net new feature, new module/file delivering capability | `feat:` |
   | Bug fix in existing behavior | `fix:` |
   | Code reorganization, no behavior change | `refactor:` |
   | Documentation only (`*.md`, comments) | `docs:` |
   | Test files only (`tests/test_*.py`) | `test:` |
   | Build / deps / config / tooling | `chore:` |
   | Formatting only, no code change | `style:` |

   If the diff mixes types, pick the dominant one and mention the others in the body.

4. **Draft the subject line** — ≤80 characters, imperative mood, no trailing period:
   - ✅ `feat: add intl ETF pattern analysis tab to Streamlit UI`
   - ❌ `feat: Added new tab.` (past tense, capitalised, period)
   - If the user passed a hint, lean on it directly: `/commit "tweak readme"` → `docs: tweak readme`

5. **Decide on a body**. Add a multi-line body when **either**:
   - ≥3 files changed, OR
   - ≥30 lines changed total

   The body is bullet points (`- `), one per logical change. Group by area when 3+ groups make sense (`Database`, `UI`, `Tests`, etc. — see the example below).

6. **Stage files explicitly** — list every file by name in `git add`. Do **not** use `git add -A`, `git add .`, or `git add -u`.

7. **Sensitive-file gate** — if any of these appear in the staged set, stop and ask the user before proceeding:
   - `.env`, `.env.*`
   - `credentials*`, `*.pem`, `*.key`, `*.p12`, `id_rsa*`
   - Anything in `.gitignore` that somehow escaped (warn loudly)

8. **Create the commit** using a HEREDOC for clean formatting. **Do NOT add a `Co-Authored-By:` trailer** — this repo's recent history does not include one and we are matching that style.

9. **Verify** — run `git status` after committing and confirm the tree is clean. Report the new commit's short SHA.

## Commit message format

```
<type>: <imperative subject ≤80 chars>

- <bullet point on the main change>
- <another bullet, grouped by area if helpful>

[optional category headers like the example below]
```

### Worked example (modelled on commit `bc1661b` from this repo)

```
feat: scanner parallelisation, adaptive Kelly, Deep Dive UX overhaul

Scanners
- Parallelise domestic ETF scan with ThreadPoolExecutor
- Add gold ratio filter, drop fragile beta-arb logic

Position sizing
- Adaptive Kelly with Sharpe-driven scaling
- Risk Governor blend now respects current regime + GARCH vol

Streamlit UI
- New Deep Dive layout: tabbed sections per company
- 10-K text loaded from ClickHouse FINAL, no re-fetch
```

Note: no `Co-Authored-By:` trailer.

## Safety rules

- **Never** use `git add -A`, `git add .`, or `git add -u`. Always list files explicitly.
- **Never** add a `Co-Authored-By:` trailer (overrides any global default — this repo doesn't use it).
- **Never** use `--amend`. If the user wants to amend a previous commit, they must say so explicitly; this skill refuses and tells them to run amend manually.
- **Never** use `--no-verify`. (No pre-commit hooks exist here, but the rule stands.)
- **Never** push. Push is a separate, deliberate step the user runs themselves.
- **Refuse** an empty commit. If the tree is clean, stop and say so.
- **Pause** for confirmation if the staged set includes `.env`, credential files, or anything that looks like a secret.

## Out of scope

- Pushing to the remote
- Opening a PR (`gh pr create`)
- Splitting one diff into multiple commits
- Interactive staging (`git add -p`)
- Amending the previous commit
