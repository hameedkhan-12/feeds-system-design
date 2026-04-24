import { createClient, RedisClientType } from "redis";

const REDIS_HOST = process.env.REDIS_HOST ?? "redis";
const REDIS_PORT = process.env.REDIS_PORT ?? "6379";
const REDIS_URL = `redis://${REDIS_HOST}:${REDIS_PORT}`;

console.log("[REDIS] Configured URL:", REDIS_URL);

export const redisClient: RedisClientType = createClient({
  url: REDIS_URL,
  socket: {
    connectTimeout: 5_000, // 5 s to establish TCP
    reconnectStrategy: (retries) => {
      if (retries >= 10) {
        console.error("[REDIS] ❌  Giving up after 10 retries");
        return new Error("Redis unreachable after 10 retries");
      }
      const delay = Math.min(retries * 200, 2_000);
      console.log(`[REDIS] 🔄  Retry #${retries} in ${delay} ms…`);
      return delay;
    },
  },
});

redisClient.on("connect", () =>
  console.log("[REDIS] ✅  Connected to", REDIS_URL),
);
redisClient.on("ready", () =>
  console.log("[REDIS] ✅  Ready to accept commands"),
);
redisClient.on("error", (err) =>
  console.error("[REDIS] ❌  Error:", String(err)),
);
redisClient.on("end", () => console.log("[REDIS]    Connection closed"));

export async function connectRedis(): Promise<void> {
  const connectWithTimeout = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Redis did not connect within 15 s (url=${REDIS_URL})`));
    }, 15_000);

    redisClient
      .connect()
      .then(() => {
        clearTimeout(timer);
        resolve();
      })
      .catch((err: unknown) => {
        clearTimeout(timer);
        reject(err);
      });
  });

  await connectWithTimeout;
}

export const TTL = {
  FEED: 60,
  FEED_ITEM: 300,
  POPULAR_FEED: 600,
  USER_PROFILE: 120,
} as const;

export const CacheKeys = {
  userFeed: (userId: number) => `feed:user:${userId}`,
  feedItem: (postId: number) => `feed:post:${postId}`,
  popularFeed: () => `feed:popular`,
  userProfile: (userId: number) => `user:profile:${userId}`,
} as const;
