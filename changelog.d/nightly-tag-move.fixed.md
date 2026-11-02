- The nightly build moves the `nightly` tag and release with the release bot
  App's token. The workflow's own token cannot point a tag at a commit that
  changes workflow files, so the tag stayed on an old commit after any such
  change.
- Image signing retries up to three times when the transparency log answers
  that an equivalent entry already exists, instead of failing the release.
