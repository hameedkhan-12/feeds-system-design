import { PoolClient } from "pg";
import { primaryPool } from "../config/db";
import { INSTANCE_ID } from "../lib/logger";

//////////////  LEADER ELECTION via PostgreSQL Advisory Locks

const LOCK_KEY = 987654321;
const LOCK_CLASSID = 0;
const LOCK_OBJID = 987654321;
const HEARTBEAT_INTERVAL_MS = 3_000;
const KEEPALIVE_INTERVAL_MS = 2_000;
const ELECTION_POLL_MS = 4_000;

let leaderConn: PoolClient | null = null;
let _isLeader = false;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let keepaliveTimer: ReturnType<typeof setInterval> | null = null;
let electionTimer: ReturnType<typeof setInterval> | null = null;

type LeaderCallback = () => Promise<void> | void;
let onBecomeLeaderCb: LeaderCallback = async () => {};
let onLoseLeadershipCb: LeaderCallback = async () => {};

export function onBecomeLeader(cb: LeaderCallback): void {
  onBecomeLeaderCb = cb;
}
export function onLoseLeadership(cb: LeaderCallback): void {
  onLoseLeadershipCb = cb;
}
export function isLeader(): boolean {
  return _isLeader;
}

//////////////////  Leader status table
export async function getCurrentLeader(): Promise<string | null> {
  try {
    const res = await primaryPool.query<{ instance_id: string }>(
      `SELECT instance_id FROM leader_status WHERE id = 1`,
    );
    return res.rows[0]?.instance_id ?? null;
  } catch {
    return null;
  }
}

async function writeLeaderStatus(instanceId: string): Promise<void> {
  await primaryPool.query(
    `INSERT INTO leader_status (id, instance_id, elected_at)
     VALUES (1, $1, NOW())
     ON CONFLICT (id) DO UPDATE
       SET instance_id = $1, elected_at = NOW()`,
    [instanceId],
  );
}

async function verifyLockStillHeld(): Promise<boolean> {
  if (!leaderConn) return false;
  try {
    const res = await leaderConn.query<{ held: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM pg_locks
         WHERE  locktype = 'advisory'
           AND  classid  = $1
           AND  objid    = $2
           AND  pid      = pg_backend_pid()
           AND  granted  = true
       ) AS held`,
      [LOCK_CLASSID, LOCK_OBJID],
    );
    return res.rows[0]?.held ?? false;
  } catch (err) {
    console.error("[LEADER] ❌  verifyLock error:", String(err));
    return false;
  }
}

async function tryAcquireLock(): Promise<boolean> {
  const conn = await primaryPool.connect();

  const res = await conn.query<{ acquired: boolean }>(
    `SELECT pg_try_advisory_lock($1) AS acquired`,
    [LOCK_KEY],
  );

  const acquired = res.rows[0]?.acquired ?? false;

  if (acquired) {
    leaderConn = conn;
  } else {
    conn.release();
  }

  return acquired;
}

////////////////  Become leader
async function becomeLeader(): Promise<void> {
  _isLeader = true;
  await writeLeaderStatus(INSTANCE_ID);

  console.log(
    `\n[LEADER] 👑  ${INSTANCE_ID} is now LEADER (Postgres advisory lock)`,
  );
  console.log(`[LEADER]     Failover is INSTANT on crash — no TTL wait`);

  await onBecomeLeaderCb();
  if (electionTimer) {
    clearInterval(electionTimer);
    electionTimer = null;
  }

  // Keepalive — prevent idle TCP timeout from dropping the lock connection
  keepaliveTimer = setInterval(async () => {
    if (!leaderConn) return;
    try {
      await leaderConn.query("SELECT 1");
    } catch (err) {
      console.warn(
        "[LEADER] ⚠️  Keepalive failed — stepping down:",
        String(err),
      );
      await stepDown();
    }
  }, KEEPALIVE_INTERVAL_MS);

  // Heartbeat — verify lock is still in pg_locks every 3s
  heartbeatTimer = setInterval(async () => {
    const stillHeld = await verifyLockStillHeld();
    if (stillHeld) {
      console.log(`[LEADER] 💓  ${INSTANCE_ID} lock verified`);
    } else {
      console.warn(`[LEADER] ⚠️  ${INSTANCE_ID} lock gone — stepping down`);
      await stepDown();
    }
  }, HEARTBEAT_INTERVAL_MS);
}

async function stepDown(): Promise<void> {
  if (!_isLeader) return;
  _isLeader = false;

  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (keepaliveTimer) {
    clearInterval(keepaliveTimer);
    keepaliveTimer = null;
  }

  if (leaderConn) {
    try {
      await leaderConn.query(`SELECT pg_advisory_unlock($1)`, [LOCK_KEY]);
      leaderConn.release();
    } catch {
      /* ignore on shutdown */
    }
    leaderConn = null;
  }

  console.log(
    `[LEADER] 📉  ${INSTANCE_ID} stepped down — lock released instantly`,
  );
  await onLoseLeadershipCb();
  startFollowerPolling();
}

////////////  Follower polling — watch pg_locks for vacancy

function startFollowerPolling(): void {
  if (electionTimer) return;

  electionTimer = setInterval(async () => {
    try {
      const res = await primaryPool.query<{ held: boolean }>(
        `SELECT EXISTS (
           SELECT 1 FROM pg_locks
           WHERE  locktype = 'advisory'
             AND  classid  = $1
             AND  objid    = $2
             AND  granted  = true
         ) AS held`,
        [LOCK_CLASSID, LOCK_OBJID],
      );

      const lockHeld = res.rows[0]?.held ?? false;

      if (lockHeld) {
        const leader = await getCurrentLeader();
        console.log(
          `[LEADER] 👀  ${INSTANCE_ID} following leader="${leader ?? "unknown"}"`,
        );
        return;
      }

      // Vacancy detected — race!
      console.log(
        `[LEADER] 🗳️   ${INSTANCE_ID} detected vacancy — running for election…`,
      );
      const won = await tryAcquireLock();

      if (won) {
        await becomeLeader();
      } else {
        const newLeader = await getCurrentLeader();
        console.log(
          `[LEADER] 🗳️   Lost election — new leader="${newLeader ?? "unknown"}"`,
        );
      }
    } catch (err) {
      console.error("[LEADER] ❌  Poll error:", String(err));
    }
  }, ELECTION_POLL_MS);
}

export async function startElection(): Promise<void> {
  await primaryPool.query(`
    CREATE TABLE IF NOT EXISTS leader_status (
      id          INT PRIMARY KEY DEFAULT 1,
      instance_id TEXT        NOT NULL,
      elected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT  single_row  CHECK (id = 1)
    )
  `);

  console.log(`\n[LEADER] 🗳️   ${INSTANCE_ID} entering election…`);

  const won = await tryAcquireLock();
  if (won) {
    await becomeLeader();
  } else {
    const leader = await getCurrentLeader();
    console.log(
      `[LEADER] 👤  ${INSTANCE_ID} is FOLLOWER | leader="${leader ?? "unknown"}"`,
    );
    startFollowerPolling();
  }
}

export async function resignLeadership(): Promise<void> {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (electionTimer) {
    clearInterval(electionTimer);
    electionTimer = null;
  }
  if (keepaliveTimer) {
    clearInterval(keepaliveTimer);
    keepaliveTimer = null;
  }
  await stepDown();
}
