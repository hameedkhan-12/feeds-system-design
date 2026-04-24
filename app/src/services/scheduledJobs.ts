import { isLeader } from './leaderElection';
import { initializeEagerCache } from './CacheService';
import { crossShardPopularPosts } from './ShardService';
import { redisClient, CacheKeys, TTL } from '../config/redis';

///////////////////  LEADER-ONLY SCHEDULED JOBS


const POPULAR_FEED_REFRESH_MS = 60_000;  
const HEALTH_REPORT_MS        = 15_000;  

let jobTimers: ReturnType<typeof setInterval>[] = [];

async function refreshPopularFeed(): Promise<void> {
  if (!isLeader()) return;   // 🔒 guard — followers skip entirely

  console.log('\n[JOB] 🔄  [Leader-only] Refreshing popular feed cache…');
  const t0 = Number(process.hrtime.bigint()) / 1_000_000;

  try {
    const posts = await crossShardPopularPosts(50);
    await redisClient.setEx(
      CacheKeys.popularFeed(),
      TTL.POPULAR_FEED,
      JSON.stringify(posts),
    );
    const duration = (Number(process.hrtime.bigint()) / 1_000_000) - t0;
    console.log(`[JOB] ✅  Popular feed refreshed: ${posts.length} posts | ${duration.toFixed(2)} ms`);
  } catch (err) {
    console.error('[JOB] ❌  Popular feed refresh failed:', String(err));
  }
}

/////////////////  Job 2 — Cluster health report

async function logClusterHealth(): Promise<void> {
  if (!isLeader()) return;   // 🔒 guard

  try {
    const leaderKey  = await redisClient.get('leader:lock');
    const feedKeys   = await redisClient.keys('feed:*');
    const memInfo    = await redisClient.info('memory');
    const usedMem    = memInfo.match(/used_memory_human:(\S+)/)?.[1] ?? 'unknown';

    console.log('\n[JOB] 📊  [Leader-only] Cluster health report:');
    console.log(`[JOB]     Leader         : ${leaderKey}`);
    console.log(`[JOB]     Redis feed keys: ${feedKeys.length}`);
    console.log(`[JOB]     Redis memory   : ${usedMem}`);
  } catch (err) {
    console.error('[JOB] ❌  Health report error:', String(err));
  }
}

/////////////////////  Start all scheduled jobs

export function startScheduledJobs(): void {
  console.log('[JOB] ⏰  Scheduled jobs registered (leader-guarded)');

  jobTimers.push(
    setInterval(refreshPopularFeed, POPULAR_FEED_REFRESH_MS),
    setInterval(logClusterHealth,   HEALTH_REPORT_MS),
  );
}

export function stopScheduledJobs(): void {
  jobTimers.forEach(clearInterval);
  jobTimers = [];
  console.log('[JOB] 🛑  Scheduled jobs stopped');
}

////////////////  Leader lifecycle hooks

export async function onThisInstanceBecomesLeader(): Promise<void> {
  console.log('\n[JOB] 👑  This instance is now leader — running initial leader tasks…');

  // Immediately warm the cache — don't wait for the next timer tick
  await initializeEagerCache();
  await refreshPopularFeed();

  console.log('[JOB] ✅  Leader tasks complete — serving as primary coordinator');
}

export async function onThisInstanceLosesLeadership(): Promise<void> {
  console.log('\n[JOB] 📉  This instance lost leadership — pausing leader-only tasks');
}