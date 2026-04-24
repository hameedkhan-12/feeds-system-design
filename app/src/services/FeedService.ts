import { replicaPool } from '../config/db';
import { redisClient, CacheKeys, TTL } from '../config/redis';
import {
  lazyGetUserFeed,
  eagerWarmUserFeed,
  compareLatency,
  initializeEagerCache,
  type TimingResult,
} from './CacheService';
import {
  shardedCreatePost,
  shardedGetUserPosts,
  crossShardPopularPosts,
  shardedLikePost,
  type Post,
} from './ShardService';

export { initializeEagerCache };

function hrMs(): number {
  return Number(process.hrtime.bigint()) / 1_000_000;
}

///////////////  GET FEED  (lazy cache + read replica)
export async function getFeed(userId: number): Promise<{
  posts: unknown[];
  meta: { source: string; durationMs: number };
}> {
  console.log(`\n[FEED] getFeed(userId=${userId})`);

  const result: TimingResult<unknown[]> = await lazyGetUserFeed(userId);

  console.log(
    `[FEED] ✅  Served ${(result.data as unknown[]).length} posts | source=${result.source} | ${result.durationMs.toFixed(2)} ms`,
  );

  return {
    posts: result.data,
    meta: { source: result.source, durationMs: result.durationMs },
  };
}
export async function createPost(
  userId: number,
  content: string,
): Promise<{
  post: Post;
  meta: { durationMs: number; shardIndex: number };
}> {
  console.log(`\n[FEED] createPost(userId=${userId})`);
  const t0 = hrMs();

  // Single write — goes to the correct shard
  const post = await shardedCreatePost(userId, content);

  await eagerWarmUserFeed(userId);

  const totalMs = hrMs() - t0;
  const shardIndex = userId % 2;
  console.log(`[FEED] ✅  Post created id=${post.id} | ${totalMs.toFixed(2)} ms total`);

  return {
    post,
    meta: { durationMs: totalMs, shardIndex },
  };
}

////////////////////  POPULAR FEED  (eager pre-warmed → cross-shard fallback)
export async function getPopularFeed(): Promise<{
  posts: Post[];
  meta: { source: string; durationMs: number };
}> {
  console.log('\n[FEED] getPopularFeed()');
  const t0 = hrMs();

  const cached = await redisClient.get(CacheKeys.popularFeed());
  if (cached !== null) {
    const duration = hrMs() - t0;
    const posts = JSON.parse(cached) as Post[];
    console.log(`[FEED] ⚡ Popular feed from Redis cache | ${duration.toFixed(2)} ms`);
    return { posts, meta: { source: 'redis_cache', durationMs: duration } };
  }

  console.log('[FEED] 🔄 Popular feed cache miss → cross-shard fan-out');
  const posts = await crossShardPopularPosts(50);

  await redisClient.setEx(CacheKeys.popularFeed(), TTL.POPULAR_FEED, JSON.stringify(posts));
  console.log(`[FEED] 💾 Popular feed re-warmed (${posts.length} posts)`);

  const duration = hrMs() - t0;
  return { posts, meta: { source: 'cross_shard_postgres', durationMs: duration } };
}

///////////////  LIKE POST  (shard-aware update + cache invalidation)
export async function likePost(
  postId: number,
  ownerUserId: number,
): Promise<Post | null> {
  console.log(`\n[FEED] likePost(postId=${postId}, ownerUserId=${ownerUserId})`);

  const updated = await shardedLikePost(postId, ownerUserId);
  if (!updated) return null;

  const userFeedKey = CacheKeys.userFeed(ownerUserId);
  await redisClient.del(userFeedKey);
  console.log(`[FEED] 🗑️  Cache invalidated key=${userFeedKey}`);

  await redisClient.del(CacheKeys.popularFeed());
  console.log('[FEED] 🗑️  Popular feed cache invalidated');

  return updated;
}

///////////////  BENCHMARK
export async function runBenchmark(userId: number): Promise<{
  redisDurationMs: number;
  postgresDurationMs: number;
  speedupFactor: string;
  summary: string;
}> {
  console.log(`\n[BENCH] Starting benchmark for userId=${userId}`);
  const comparison = await compareLatency(userId);

  const summary =
    `Redis answered in ${comparison.redis.toFixed(2)} ms vs ` +
    `Postgres in ${comparison.postgres.toFixed(2)} ms → ` +
    `Redis is ${comparison.speedupFactor} faster`;

  console.log(`[BENCH] 📊  ${summary}`);
  return {
    redisDurationMs:    comparison.redis,
    postgresDurationMs: comparison.postgres,
    speedupFactor:      comparison.speedupFactor,
    summary,
  };
}