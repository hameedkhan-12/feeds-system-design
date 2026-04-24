

import { getShardIndex, getShardPool } from "../config/db";

export interface Post {
  id: number;
  user_id: number;
  content: string;
  created_at: string;
  likes: number;
}

export async function shardedCreatePost(
  userId: number,
  content: string,
): Promise<Post> {
  const shardIndex = getShardIndex(userId);
  const pool = getShardPool(userId);

  console.log(`[SHARD] ✏️  INSERT post → shard #${shardIndex} (userId=${userId})`);

  const result = await pool.query<Post>(
    `INSERT INTO posts (user_id, content, created_at, likes)
     VALUES ($1, $2, NOW(), 0)
     RETURNING id, user_id, content, created_at::text, likes`,
    [userId, content],
  );

  console.log(`[SHARD] ✅  Post created id=${result.rows[0]!.id} on shard #${shardIndex}`);
  return result.rows[0]!;
}

/** Read posts for a userId — routes directly to the correct shard */
export async function shardedGetUserPosts(userId: number): Promise<Post[]> {
  const shardIndex = getShardIndex(userId);
  const pool = getShardPool(userId);

  console.log(`[SHARD] 🔍  SELECT posts from shard #${shardIndex} (userId=${userId})`);

  const result = await pool.query<Post>(
    `SELECT id, user_id, content, created_at::text, likes
     FROM   posts
     WHERE  user_id = $1
     ORDER  BY created_at DESC
     LIMIT  20`,
    [userId],
  );

  console.log(`[SHARD] 📦  shard #${shardIndex} returned ${result.rowCount} rows`);
  return result.rows;
}

export async function crossShardPopularPosts(limit = 10): Promise<Post[]> {
  const SHARD_COUNT = 2;
  console.log(`[SHARD] 🌐  Cross-shard fan-out (${SHARD_COUNT} shards, top ${limit} each)…`);

  const shardQueries = Array.from({ length: SHARD_COUNT }, (_, i) => {
    const pool = getShardPool(i * 2);
    console.log(`[SHARD] → querying shard #${i}`);
    return pool.query<Post>(
      `SELECT id, user_id, content, created_at::text, likes
       FROM   posts
       ORDER  BY likes DESC
       LIMIT  $1`,
      [limit],
    );
  });

  const shardResults = await Promise.all(shardQueries);

  const merged: Post[] = shardResults.flatMap(r => r.rows);
  merged.sort((a, b) => b.likes - a.likes);

  console.log(`[SHARD] ✅  Fan-out complete: ${merged.length} total rows merged`);
  return merged.slice(0, limit);
}

export async function shardedLikePost(
  postId: number,
  ownerUserId: number,
): Promise<Post | null> {
  const shardIndex = getShardIndex(ownerUserId);
  const pool = getShardPool(ownerUserId);

  console.log(`[SHARD] 👍  Incrementing likes on post ${postId} in shard #${shardIndex}`);

  const result = await pool.query<Post>(
    `UPDATE posts
     SET    likes = likes + 1
     WHERE  id = $1
     RETURNING id, user_id, content, created_at::text, likes`,
    [postId],
  );

  if (result.rowCount === 0) {
    console.warn(`[SHARD] ⚠️  Post ${postId} not found on shard #${shardIndex}`);
    return null;
  }

  console.log(`[SHARD] ✅  Post ${postId} likes → ${result.rows[0]!.likes}`);
  return result.rows[0]!;
}