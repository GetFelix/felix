// Token provider shapes the page describes but does not show in a block; the
// blocks themselves are checked through test/doc-examples.mjs.

import { Client } from "../index";

// A synchronous provider fits the type too, and so does a per-user handle.
const client = await Client.connect("127.0.0.1:5000", "t1", { tokenProvider: () => "a-felix-token" });
const alice = await client.withIdentity("t1", { tokenProvider: async () => "a-felix-token" });
alice.close();
client.close();
