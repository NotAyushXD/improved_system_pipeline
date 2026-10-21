# How to write documentation in this repo

This guide captures the approach used for `sql/themes/seasonality/`, so the next module (need states, NBA, opportunities...) is documented the same way. It is a standard to follow, not a rule book: adapt it when a module needs something different, and update this page when you do.

**Reference example:** [`sql/themes/seasonality/`](../sql/themes/seasonality/README.md). When in doubt, look at how it is laid out.

## 1. Principles

1. **Write for someone who arrives cold.** They know the business but not this code. Assume they will read one page, not the whole folder.
2. **Plain words first, detail second.** Every page opens with what it is for in a sentence or two, then the detail.
3. **Navigation, not a wall.** A reader entering any folder should know what to run, in what order, and where to read more, within one screen.
4. **Document what the code does, not what we wish it did.** If the code and the intent disagree, say so (see section 6).
5. **One source of truth per fact.** Define a term once (glossary) and link to it; keep thresholds in one table (parameters page).
6. **Docs change with the code.** A change to SQL or Python is not finished until its page is updated.

## 2. The README ladder

Every folder that someone is expected to *enter* gets a `README.md`. Each level answers a different question and links to the next.

| Level | Answers | Example |
|---|---|---|
| Repo | What is this repo, how is it laid out | `README.md` |
| Area (`sql/`, `src/cie/`, `data/`) | What lives here, shared conventions, which modules exist | `sql/README.md` |
| Module (`sql/themes/`) | Which sub-modules exist, in what order they run, what leaves the folder | `sql/themes/README.md` |
| Sub-module (`sql/themes/seasonality/`) | What to run, in order, what to export, key concepts, FAQ | `.../seasonality/README.md` |
| Deep reference (`.../seasonality/docs/`) | How each script or stage works in detail | `docs/00` to `docs/04`, plus catalog, parameters, known issues |

A reader should be able to start at the repo README and reach any detail by following links down. Do not make them guess a path.

When you add a new module folder, **create its README first**, even as a stub, with the headings from section 4.1.

## 3. What goes in the deep-reference `docs/` folder

Use this structure for any sub-module with more than a script or two. Keep the numbered files in run order, and keep the cross-cutting pages unnumbered.

| File | Purpose |
|---|---|
| `00_concepts_and_glossary.md` | Vocabulary, a worked example, a decision diagram |
| `01_...md`, `02_...md`, ... | One file per script or stage, **numbered in run order** to match the script numbers |
| `NN_model_ready_output.md` (last number) | Column dictionary of the file handed to Python |
| `table_catalog.md` | Every table or object: grain, status, reads, read-by |
| `parameters.md` | Every tunable threshold and where it lives, plus hard-coded constants |
| `known_issues.md` | Problems, gotchas, open questions |

Rules of thumb

- If one stage's page would be very long, **keep it as one numbered file** with a table of contents at the top rather than splitting into `a/b/c` files. (The seasonality engine page is one file with four parts.)
- Cross-cutting pages (`table_catalog`, `parameters`, `known_issues`) stay unnumbered so the run order stays obvious.
- Name files `NN_snake_case.md`, matching the script they describe where possible.

## 4. Templates

Copy these and fill them in. Delete headings that truly do not apply; do not leave empty ones.

### 4.1 Folder README (module or sub-module)

```markdown
# <Name>

## In 30 seconds
What it answers, how to run it, what to read first. Include a warning here if there is a known blocker.

## 1. Run order
| # | Script | Role in plain words | Reads | Builds (final) | Reference |
Counts of tables, required access, engine/runtime.

### What to export / hand over
Exact object name -> exact destination path (and file name).

### Quick checks after each step
A few copy-paste queries or commands with the expected result.

## 2. How it works (one page)
One diagram and a short numbered walkthrough. A table of the headline output fields.

## 3. Where to find things
"I want to ... -> go to ..." table.

## 4. Things you must know
The five to seven caveats that would surprise a newcomer.

## 5. FAQ

## 6. Keeping this documentation current
```

### 4.2 Script or stage reference

```markdown
# Script NN: <what it does>

File: link to the script.   Prerequisites: scripts / data needed first.

**What it does in one sentence.**  **What it does not do.**

(diagram)

## <Table / step name>
- Grain: one row per ...
- Inputs: ...
- Logic: numbered steps, formulas, thresholds in a table
- Key columns: ...
- Output used by: ...

## Decision rules
Ordered table: first match wins. Say "first match wins" explicitly.

## Caveats
```

### 4.3 Table catalog row
`Object | Line | Grain | Status | What it holds | Reads | Read by`

Status values: `EXPORT` (consumed by Python), `FINAL` (canonical stage output), `intermediate` (read by a later object), `terminal` (nothing reads it).

### 4.4 Column dictionary
Group columns as the code groups them. For each: name, meaning, allowed values or range, unit, and whether it can be null or empty. State the type conventions once at the top (flags are 0/1, lists are comma strings, arrays have fixed length).

### 4.5 Known-issue entry
```
### N. <Short title>
What was observed, and how it was checked.
Expected consequence.
Options (do not decide silently).
```
Severity heading groups: High (breaks or misleads), Medium (needs care), Low (housekeeping), then open questions for owners.

## 5. Writing style

- **Lead with the question the page answers.** Put the answer in the first paragraph.
- **Tables for facts, prose for reasoning.** Thresholds, columns, statuses and run order belong in tables. Explain *why* in a sentence next to the table.
- **Show an ordered rule as an ordered table.** Where logic is a `CASE WHEN` cascade, document the order and the outcome label for each row, and say first match wins.
- **Give a worked example** for any non-obvious method, with illustrative numbers clearly marked as illustrative.
- **Define every term once** in the glossary, and link there on first use in other pages.
- **Name things exactly as they appear in the code** (table, column, parameter names in backticks) so readers can search for them.
- **Keep sentences short.** Avoid filler. Avoid "simply", "just", "obviously".
- **Use consistent terms.** Pick one name for a concept (for example "parent occasion") and keep it.
- Write the unit and scale: percent 0-100 or fraction 0-1, days before or after, per year or per occurrence.

## 6. Accuracy rules (important)

1. **Derive documentation from the code.** Read the query bodies, not only the header comments. Header comments go stale.
2. **Verify claims mechanically where you can.** Examples used for the seasonality docs: parse the SQL to build the table lineage; check that every column selected by an output view exists in the upstream table.
3. **Separate the three kinds of statement.**
   - *Observed in code:* state plainly.
   - *Inferred* (a meaning guessed from names or context): mark it as inferred, for example "meaning inferred from the code".
   - *Unknown:* list it under open questions. Do not invent business definitions.
4. **Record discrepancies; do not paper over them.** If the code disagrees with its comments, its file name or another script, write it in `known_issues.md`.
5. **Do not document intended behaviour as fact.** If something is provisional (for example calendar months standing in for commercial periods), say so wherever it appears.
6. **Dates and numbers that can drift** (row counts, data windows, counts of tables) should either be generated, or carry the date they were true, or be avoided.
7. **Do not copy code into docs** unless it is a short formula that explains the idea. Link to the line or table instead; copies go stale.

## 7. Diagrams

- Use Mermaid in fenced blocks (`mermaid`). They render on GitHub and in most IDE previews.
- Use `<br/>` for line breaks inside labels, not `\n`.
- Quote subgraph titles: `subgraph S1["From script 01"]`.
- Avoid `<`, `>` and `&` in labels; spell them out.
- One idea per diagram. A data-flow diagram per stage plus one overview is usually enough.
- Every diagram needs one sentence above it saying what to look for.

## 8. Linking

- Use **relative links**, never absolute paths or URLs to your machine.
- Link to headings with GitHub-style anchors. If you rename a heading, search for links to it.
- When a file is renamed, merged or deleted, update every link in the same change.
- Run the link check below before committing.

```python
# python3 -I check_links.py   (run from the repo root)
import re, glob, os
def slug(t):
    t = re.sub(r"`", "", t.strip().lower()); t = re.sub(r"[^\w\- ]", "", t); return t.replace(" ", "-")
heads = {}
for p in glob.glob("**/*.md", recursive=True):
    seen, fence, S = {}, False, set()
    for l in open(p).read().split("\n"):
        if l.startswith("```"): fence = not fence
        m = None if fence else re.match(r"#{1,6} (.*)", l)
        if m:
            s = slug(m.group(1)); n = seen.get(s, 0); seen[s] = n + 1
            S.add(s if n == 0 else f"{s}-{n}")
    heads[os.path.normpath(p)] = S
bad = []
for p in glob.glob("**/*.md", recursive=True):
    for m in re.finditer(r"\]\(([^)#\s]*)(?:#([^)]*))?\)", open(p).read()):
        f, a = m.group(1), m.group(2)
        if f.startswith("http"): continue
        t = os.path.normpath(os.path.join(os.path.dirname(p), f)) if f else os.path.normpath(p)
        if not os.path.exists(t): bad.append((p, f, a)); continue
        if a and t.endswith(".md") and a not in heads.get(t, set()): bad.append((p, f, "#" + a))
print("broken:", bad)
```

## 9. Documenting each part of this repo

### SQL (`sql/<module>/...`)
- Follow sections 2 to 4. Put the **run order** and the **export object** in the folder README.
- Because SQL is run outside this repo, always say: required access, engine, what to export, where the file lands in `data/<module>/raw/` and its file name.
- Keep an up-to-date table catalog with grain, status and dependencies. Generate the "reads" and "read by" columns by parsing `CREATE TABLE` / `CREATE VIEW` statements rather than typing them.
- Put every tunable threshold in `parameters.md`, including constants written directly into the logic.
- Version-named tables (`v2_6`, `v2_10`): say which are final and which are superseded.

### Python (`src/cie/<module>/`)
- Each module gets a short `README.md`: purpose, entry point, inputs (the Parquet files it expects and their contract), outputs, how to run it.
- Public functions and classes get docstrings: what it does, arguments with types and units, what it returns, what it raises. Keep them truthful rather than long.
- Prefer type hints. Do not restate types in prose twice.
- Module-level docstring: one paragraph saying what the module is for and what it must not do.

### Data contracts (`docs/data_contracts.md`)
For every file the pipeline reads or writes under `data/<module>/{raw,interim,processed}/`:

| Item | Content |
|---|---|
| Path and name | Exact path and file-name pattern |
| Producer | Which SQL object or Python step creates it |
| Grain and key | One row per ..., primary key |
| Columns | Name, type, meaning, nullability, allowed values (link to the column dictionary if one exists) |
| Validation | Row-count and uniqueness checks the loader runs |
| Refresh | How and when it is regenerated |

Validate the contract in code and keep the page and the validation in step.

### Notebooks (`notebooks/<module>/`)
Start each notebook with a markdown cell stating its question, the data it reads, and the date it was last run. If a notebook contains logic worth keeping, move it into `src/` and document it there.

### Configs (`configs/*.yaml`)
Comment each key with its meaning and unit. Document differences between environments (dev vs prod) in the config README or in comments, not in people's heads.

### Pipelines (`pipelines/*.py`)
Docstring at the top: what runs, in what order, what it needs, what it writes. Update the module READMEs if the order changes.

## 10. Workflow

### When you change code
1. Find the page(s) that describe it (use the "Where to find things" table).
2. Update them in the same commit or pull request.
3. If a table or column is added, renamed or removed: update the table catalog and the column dictionary.
4. If a threshold changes: update `parameters.md`.
5. If you discover a problem you are not fixing now: add it to `known_issues.md`.
6. Run the link check.

### Pull request checklist
- [ ] Docs updated for every behaviour change
- [ ] New folders have a README
- [ ] Run order and export object still correct
- [ ] Parameters page matches the code
- [ ] Catalog regenerated if tables changed
- [ ] Inferred or unknown items are marked, not asserted
- [ ] Links checked
- [ ] No absolute paths or personal details in docs

### Review
Reviewers should read the doc diff as a newcomer. Ask: could I run this from the README alone? Is anything stated as fact that the code does not show?

## 11. Maintenance

- Revisit `known_issues.md` whenever you touch the area; close items that are fixed and say how.
- Do not leave stale files: delete or merge, and fix links in the same change.
- Keep each page's top paragraph accurate; it is the part most people read.
- Keep numbered pages in run order. If the run order changes, rename the files and update links.
- Prefer merging two thin pages over keeping a page that is only a pointer.
