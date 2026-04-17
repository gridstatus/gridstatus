## Review code changes against CLAUDE.md guidelines to catch issues and ensure maintainability

Follow the steps below and only output the final Report Out section.

1. **Collect Inputs**
   - Review changes in this branch only
   - Base branch for the diff: `$ARGUMENTS` (default to `main` if empty)
   - Use `git diff <base>...HEAD` to show all changes
   - Note any context the user provided (goal, constraints, known tradeoffs)

2. **Load Guidelines**
   - Read the workspace `../CLAUDE.md` for monorepo-wide rules
   - Read any repo-level `AGENTS.md` / `CONTRIBUTING.md` if touched areas need it
   - No repo-local `CLAUDE.md` exists here — rely on conventions evident in nearby code

3. **Build a Mental Model**
   - Identify what the change is trying to accomplish
   - Map what's touched: ISO parser (`gridstatus/<iso>.py`), base classes, HTTP helpers, tests, fixtures
   - Flag risk zones:
     - **VCR cassettes**: new tests must use fixed recent dates (not `today`/`latest`), cassettes must not contain 4xx/5xx responses, and must be uploaded via `make fixtures-upload iso=<iso>` — they are NOT committed to git
     - **Python version compat**: library supports Python 3.10–3.12; no 3.13-only syntax (no `type` statement aliases, etc.)
     - **ISO API breakage**: parsers that silently return empty DataFrames on schema changes
     - **Timezone handling**: every ISO has its own local tz; confirm tz-aware timestamps
     - **Network retries and rate limits**
     - **Public API surface**: renamed/removed methods are breaking changes for library users

4. **Review the Diff**
   - Go file by file
   - Verify correctness and intent
   - Look for bugs, logic errors, regressions, and common AI-generated mistakes (unused abstractions, inconsistent naming, unnecessary complexity, over-broad `except Exception`)
   - For parser changes: confirm returned DataFrame columns/dtypes match sibling ISOs
   - For new tests: confirm `api_vcr.use_cassette(...)` is used with a fixed recent date; confirm cassette recording plan is explicit
   - For `@pytest.mark.integration` / `@pytest.mark.slow` usage: confirm tests that hit live APIs are marked so CI can skip them

5. **Validate Against CLAUDE.md + Best Practices**
   - Run `make lint` and surface any failures
   - For changed parser files, confirm `make test-<iso>` is runnable (don't execute unless the user asks)
   - Flag important best-practice issues not covered by guidelines
   - Cite the relevant guideline / convention for each issue when applicable

6. **Report Out**
   - List all issues directly. NEVER add a higher-level summary of the review
   - Pair each issue with a clear, actionable fix
   - Order issues with sections by priority (blocking, important, nit) with global numbering so each issue can unambiguously be referenced
   - At the end, list out each issue one by one with a concise (no more than 20 words) description, so the user can reply with numbers. Call this section "Next Steps". The last line should say "Which issue(s) do you want to fix first?".
   - Propose architectural changes only if they materially improve maintainability
