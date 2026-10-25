// Names the examples on the TypeScript client page use without defining them,
// for the modules test/doc-examples.mjs writes. A script, not a module, so
// these are globals and an example that defines its own shadows them.

declare const token: string;
declare const caFile: string;
declare const oidcToken: string;
declare const aliceToken: string;
declare const addrs: string[];
declare const tenantId: string;
declare const serverName: string;
declare const offerAlpn: boolean;
declare const tokenProvider: import("../index").TokenProvider;

declare const client: import("../index").Client;
declare const events: import("../index").SubscriptionHandle;
declare const sharded: import("../index").ShardedSubscriptionHandle;
declare const positions: import("../index").ShardPositions;
declare const record: import("../index").GroupRecord;

declare const payload: Buffer;
declare const data: Buffer;
declare const expected: Buffer;
declare const customerId: string;
declare const orders: { customerId: string }[];
declare const shard: number;
declare const fixedTheCause: boolean;

declare function handle(payload: Buffer): Promise<void>;
declare function charge(payload: Buffer): Promise<void>;
declare function quarantine(record: import("../index").GroupRecord): Promise<void>;
declare function reconcile(): Promise<void>;
declare function retry(): Promise<void>;
declare function giveUp(): Promise<void>;
declare function createStream(): Promise<void>;
