- Changelog entries are now collected from `changelog.d/` fragments, one file per
  change, so pull requests no longer conflict on `CHANGELOG.md`.
  `scripts/changelog_assemble.py` folds them into the release section, and the
  release workflow refuses a tag while fragments remain. Entries written directly
  in `CHANGELOG.md` still work. See `changelog.d/README.md`.
