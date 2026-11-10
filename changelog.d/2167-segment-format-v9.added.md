- Storage reads segment format v9, whose record headers carry the writing
  leader's generation (#2167). Startup recovery treats a v9 header cut short,
  generation included, as a torn tail and refuses a log whose generations
  fall, inside a segment or from one segment to the next.
