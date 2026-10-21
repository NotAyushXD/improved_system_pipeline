# CIE Product Pipeline

Minimal CIE product repository with module-specific data areas.

- sql/: version-controlled SQL by module.
- src/cie/: runnable module entry points; add implementation files only as required.
- pipelines/: SQL, science, and end-to-end orchestration.
- data/<module>/raw/: SQL extracts or source inputs.
- data/<module>/interim/: intermediate outputs.
- data/<module>/processed/: final module outputs for downstream consumption.
- Dataset contents are ignored by Git; .gitkeep preserves directory structure.

Documentation: start at [sql/README.md](sql/README.md) for the SQL run order and exports. To write or update docs, follow [docs/documentation_guide.md](docs/documentation_guide.md).
