- A refused start offset now tells every client where to resume (#1684). The
  cursor error in the C ABI (`felix_last_cursor_error`), C#
  (`FelixException.Cursor`), Python (`CursorError`) and TypeScript
  (`CursorError`) carries the reason, the requested and nearest available
  offsets, and the head trim's headers, as the Rust client's already did.
