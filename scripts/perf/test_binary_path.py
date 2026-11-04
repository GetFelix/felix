"""Print the executable cargo built for the target named in argv[1], reading
`cargo test --no-run --message-format=json` on stdin."""

import json
import sys

for line in sys.stdin:
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    if msg.get("executable") and msg["target"]["name"] == sys.argv[1]:
        print(msg["executable"])
