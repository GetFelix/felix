- A truncation or reset no longer races a background segment roll (#1222). The
  roll's seal could put back the length a truncation had cut, or cut off records
  appended after it. Truncation, reset and restore now wait for the seal.
