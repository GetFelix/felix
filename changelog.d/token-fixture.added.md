- The client fixture mints tokens with a chosen life (`POST <control_url>/tokens`, optionally forged), and the catalogue
  has seven optional `token.*` scenarios (lazy, single flight, refused retry once, keep a valid token on failure, expiry
  refresh, failure message, timeout joins), tested in Rust, C, Python and TypeScript.
