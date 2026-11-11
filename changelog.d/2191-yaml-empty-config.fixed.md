- An empty, comment-only or `---`-only YAML config file still loads as "no
  settings" after the serde-saphyr 2.1 upgrade (#2191). The new parser reads such
  a file as a null document and refuses a struct, so the services and the client
  now treat it as an empty map before parsing.
