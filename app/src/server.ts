import express, { Request, Response, NextFunction } from "express";
import { testDatabaseConnections } from "./config/db";
import { connectRedis } from "./config/redis";
import { initializeEagerCache } from "./services/CacheService";
import { feedRouter } from "./routes/feed";
import {
  timingMiddleware,
  scalingInfoMiddleware,
  INSTANCE_ID,
} from "./lib/logger";
import {
  startElection,
  resignLeadership,
  onBecomeLeader,
  onLoseLeadership,
} from "./services/leaderElection";
import {
  startScheduledJobs,
  stopScheduledJobs,
  onThisInstanceBecomesLeader,
  onThisInstanceLosesLeadership,
} from "./services/scheduledJobs";

const app = express();
const PORT = process.env.PORT ?? 3000;

/////////////////  Global middleware
app.use(express.json());
app.use(timingMiddleware);
app.use(scalingInfoMiddleware);

app.use("/api/feed", feedRouter);

app.get("/health", (_req: Request, res: Response) => {
  res.json({
    status: "ok",
    instance: INSTANCE_ID,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "Not found" });
});

app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error("[ERROR] ❌ ", err); // log full stack
  res.status(500).json({ error: message });
});

async function bootstrap(): Promise<void> {
  console.log("\n" + "═".repeat(60));
  console.log(`[BOOT] 🚀  Starting instance: ${INSTANCE_ID}`);
  console.log(`[BOOT]     PID=${process.pid} | PORT=${PORT}`);
  console.log("═".repeat(60));

  // 1. Redis
  console.log("\n[BOOT] Step 1/3 — Connecting to Redis…");
  try {
    await connectRedis();
  } catch (err) {
    console.error("[BOOT] ❌  Redis connection failed:", String(err));
    console.error(
      "[BOOT]     Make sure REDIS_HOST env var is set and redis service is reachable",
    );
    process.exit(1);
  }

  console.log("[BOOT] Step 2/3 — Testing Postgres connections…");
  await testDatabaseConnections();

  console.log("[BOOT] Step 3/3 — Starting leader election…");
  onBecomeLeader(onThisInstanceBecomesLeader);
  onLoseLeadership(onThisInstanceLosesLeadership);
  await startElection();

  startScheduledJobs();

  app.listen(PORT, () => {
    console.log("\n" + "═".repeat(60));
    console.log(`[BOOT] ✅  Instance "${INSTANCE_ID}" ready on port ${PORT}`);
    console.log("[BOOT]     Endpoints:");
    console.log("[BOOT]       GET  /health");
    console.log(
      "[BOOT]       GET  /api/feed/:userId                 (lazy cache)",
    );
    console.log(
      "[BOOT]       POST /api/feed/:userId/post            (eager cache)",
    );
    console.log(
      "[BOOT]       GET  /api/feed/popular/all            (pre-warmed)",
    );
    console.log(
      "[BOOT]       POST /api/feed/:userId/post/:id/like  (shard-aware)",
    );
    console.log(
      "[BOOT]       GET  /api/feed/bench/:userId          (latency bench)",
    );
    console.log("═".repeat(60) + "\n");
  });
}

async function shutdown(signal: string): Promise<void> {
  console.log(`\n[BOOT] 🛑  ${signal} received — shutting down gracefully…`);
  stopScheduledJobs();
  await resignLeadership();
  process.exit(0);
}

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});
process.on("SIGINT", () => {
  void shutdown("SIGINT");
});

bootstrap().catch((err: unknown) => {
  console.error("[BOOT] ❌  Fatal startup error:", err);
  process.exit(1);
});
