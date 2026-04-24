import { redisClient, TTL, CacheKeys } from '../config/redis';
import { replicaPool, primaryPool } from '../config/db';

function hrMs(): number {
  return Number(process.hrtime.bigint()) / 1_000_000;
}

export interface TimingResult<T> {
  data: T;
  durationMs: number;
  source: 'redis_cache' | 'postgres_primary' | 'postgres_replica';
}

///////////////////  STRATEGY 1 — LAZY POPULATION  (cache-aside)

export async function lazyGetUserFeed(userId: number): Promise<TimingResult<unknown[]>> {
  const cacheKey = CacheKeys.userFeed(userId);
  const t0 = hrMs();

  const cached = await redisClient.get(cacheKey);
  if (cached !== null) {
    const duration = hrMs() - t0;
    console.log(`[CACHE][LAZY] 🟢 HIT   key=${cacheKey} | ${duration.toFixed(2)} ms`);
    return { data: JSON.parse(cached) as unknown[], durationMs: duration, source: 'redis_cache' };
  }

  console.log(`[CACHE][LAZY] 🔴 MISS  key=${cacheKey} → querying replica…`);
  const dbStart = hrMs();

  const result = await replicaPool.query(
    `SELECT id, user_id, content, created_at, likes
     FROM   posts
     WHERE  user_id = $1
     ORDER  BY created_at DESC
     LIMIT  20`,
    [userId],
  );

  const dbDuration = hrMs() - dbStart;
  console.log(`[CACHE][LAZY] 🗄️  DB query took ${dbDuration.toFixed(2)} ms (replica)`);

  await redisClient.setEx(cacheKey, TTL.FEED, JSON.stringify(result.rows));
  console.log(`[CACHE][LAZY] 💾 Stored key=${cacheKey} TTL=${TTL.FEED}s`);

  const totalDuration = hrMs() - t0;
  return { data: result.rows, durationMs: totalDuration, source: 'postgres_replica' };
}

/////////////////////  STRATEGY 2 — EAGER POPULATION  (write-through)

export async function eagerWarmUserFeed(userId: number): Promise<void> {
  const cacheKey = CacheKeys.userFeed(userId);
  console.log(`[CACHE][EAGER] 🔥 Warming cache key=${cacheKey} after write…`);
  const t0 = hrMs();

  const feedResult = await primaryPool.query(
    `SELECT id, user_id, content, created_at, likes
     FROM   posts
     WHERE  user_id = $1
     ORDER  BY created_at DESC
     LIMIT  20`,
    [userId],
  );

  await redisClient.setEx(cacheKey, TTL.FEED, JSON.stringify(feedResult.rows));
  const duration = hrMs() - t0;
  console.log(`[CACHE][EAGER] 💾 Cache warmed key=${cacheKey} (${feedResult.rowCount} posts) | ${duration.toFixed(2)} ms`);
}

///////////////////  Pre-warm popular feed on startup (eager, proactive)
export async function initializeEagerCache(): Promise<void> {
  console.log('\n[CACHE][EAGER] 🔥 Pre-warming popular feed cache…');
  const t0 = hrMs();

  try {
    const result = await replicaPool.query(
      `SELECT id, user_id, content, created_at, likes
       FROM   posts
       ORDER  BY likes DESC, created_at DESC
       LIMIT  50`,
    );

    await redisClient.setEx(
      CacheKeys.popularFeed(),
      TTL.POPULAR_FEED,
      JSON.stringify(result.rows),
    );

    const duration = hrMs() - t0;
    console.log(
      `[CACHE][EAGER] ✅  Popular feed warmed: ${result.rowCount} posts in ${duration.toFixed(2)} ms | TTL=${TTL.POPULAR_FEED}s`,
    );
  } catch (err) {
    console.warn('[CACHE][EAGER] ⚠️  Pre-warm skipped:', (err as Error).message);
  }
}

///////////////////  LATENCY COMPARISON — Redis cache vs direct Postgres
export async function compareLatency(userId: number): Promise<{
  redis: number;
  postgres: number;
  speedupFactor: string;
}> {
  console.log(`\n[BENCH] ── Latency comparison for userId=${userId} ────────────`);

  const pgStart = hrMs();
  await replicaPool.query(
    `SELECT id, user_id, content, created_at, likes
     FROM   posts WHERE user_id = $1
     ORDER  BY created_at DESC LIMIT 20`,
    [userId],
  );
  const pgDuration = hrMs() - pgStart;
  console.log(`[BENCH] 🗄️  Postgres (replica): ${pgDuration.toFixed(2)} ms`);

  await lazyGetUserFeed(userId);

  const redisStart = hrMs();
  await redisClient.get(CacheKeys.userFeed(userId));
  const redisDuration = hrMs() - redisStart;
  console.log(`[BENCH] ⚡ Redis (cache hit):   ${redisDuration.toFixed(2)} ms`);

  const speedup = pgDuration / (redisDuration || 0.01);
  console.log(`[BENCH] 🚀 Redis is ~${speedup.toFixed(1)}x faster`);
  console.log('[BENCH] ──────────────────────────────────────────────────────\n');

  return {
    redis:         redisDuration,
    postgres:      pgDuration,
    speedupFactor: `${speedup.toFixed(1)}x`,
  };
}