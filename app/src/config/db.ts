import { Pool, PoolClient } from "pg";

////////////////  Connection Pools

export const primaryPool = new Pool({
  host: process.env.DB_PRIMARY_HOST || "postgres-primary",
  port: Number(process.env.DB_PRIMARY_PORT) || 5432,
  user: process.env.DB_USER || "admin",
  password: process.env.DB_PASSWORD || "password",
  database: process.env.DB_NAME || "feeddb",
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 2_000,
});

export const replicaPool = new Pool({
  host: process.env.DB_REPLICA_HOST || "postgres-replica",
  port: Number(process.env.DB_REPLICA_PORT) || 5432,
  user: process.env.DB_USER || "admin",
  password: process.env.DB_PASSWORD || "password",
  database: process.env.DB_NAME || "feeddb",
  max: 20,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 2_000,
});

const SHARD_COUNT = 2;

const shardPools: Pool[] = Array.from({ length: SHARD_COUNT }, (_, i) => {
  console.log(`[SHARD] Initializing shard pool #${i}`);
  return new Pool({
    host: process.env.DB_PRIMARY_HOST || "postgres-primary",
    port: Number(process.env.DB_PRIMARY_PORT) || 5432,
    user: process.env.DB_USER || "admin",
    password: process.env.DB_PASSWORD || "password",
    database: process.env.DB_NAME || "feeddb",
    max: 5,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 2_000,
  });
});

export function getShardIndex(userId: number): number {
  const shard = userId % SHARD_COUNT;
  console.log(`[SHARD] userId=${userId} → shard #${shard}`);
  return shard;
}

export function getShardPool(userId: number): Pool {
  return shardPools[getShardIndex(userId)]!;
}

export async function testDatabaseConnections(): Promise<void> {
  console.log("\n[DB] ── Testing database connections ──────────────────");

  // Primary
  try {
    const client: PoolClient = await primaryPool.connect();
    const res = await client.query(
      "SELECT NOW() AS now, pg_is_in_recovery() AS is_replica",
    );
    console.log(
      `[DB] PRIMARY  ✅  connected | time=${res.rows[0].now} | replica=${res.rows[0].is_replica}`,
    );
    client.release();
  } catch (err) {
    console.error(
      "[DB] PRIMARY  ❌  connection failed:",
      (err as Error).message,
    );
  }

  // Replica
  try {
    const client: PoolClient = await replicaPool.connect();
    const res = await client.query(
      "SELECT NOW() AS now, pg_is_in_recovery() AS is_replica",
    );
    console.log(
      `[DB] REPLICA  ✅  connected | time=${res.rows[0].now} | replica=${res.rows[0].is_replica}`,
    );
    client.release();
  } catch (err) {
    console.error(
      "[DB] REPLICA  ❌  connection failed:",
      (err as Error).message,
    );
  }

  // Shards
  for (let i = 0; i < SHARD_COUNT; i++) {
    try {
      const client: PoolClient = await shardPools[i]!.connect();
      await client.query("SELECT 1");
      console.log(`[DB] SHARD #${i} ✅  connected`);
      client.release();
    } catch (err) {
      console.error(
        `[DB] SHARD #${i} ❌  connection failed:`,
        (err as Error).message,
      );
    }
  }

  console.log("[DB] ──────────────────────────────────────────────────\n");
}
