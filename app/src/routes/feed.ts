import { Router, Request, Response } from 'express';
import {
  getFeed,
  createPost,
  getPopularFeed,
  likePost,
  runBenchmark,
} from '../services/FeedService';

export const feedRouter: Router = Router();


type AsyncHandler = (req: Request, res: Response) => Promise<void>;
const wrap = (fn: AsyncHandler) =>
  (req: Request, res: Response, next: (err?: unknown) => void): void => {
    fn(req, res).catch(next);
  };

feedRouter.get('/popular/all', wrap(async (_req, res) => {
  const { posts, meta } = await getPopularFeed();
  res.json({ posts, meta });
}));

feedRouter.get('/bench/:userId', wrap(async (req, res) => {
  const userId = Number(req.params['userId']);
  if (isNaN(userId)) { res.status(400).json({ error: 'userId must be a number' }); return; }

  const result = await runBenchmark(userId);
  res.json(result);
}));


feedRouter.get('/:userId', wrap(async (req, res) => {
  const userId = Number(req.params['userId']);
  if (isNaN(userId)) { res.status(400).json({ error: 'userId must be a number' }); return; }

  const { posts, meta } = await getFeed(userId);
  res.json({ userId, posts, meta });
}));

feedRouter.post('/:userId/post', wrap(async (req, res) => {
  const userId  = Number(req.params['userId']);
  const content = (req.body as { content?: string }).content;
  if (isNaN(userId))                       { res.status(400).json({ error: 'userId must be a number' }); return; }
  if (!content || !content.trim())         { res.status(400).json({ error: 'content is required' }); return; }

  const { post, meta } = await createPost(userId, content.trim());
  res.status(201).json({ userId, post, meta });
}));


feedRouter.post('/:userId/post/:postId/like', wrap(async (req, res) => {
  const ownerUserId = Number(req.params['userId']);
  const postId      = Number(req.params['postId']);
  if (isNaN(ownerUserId) || isNaN(postId)) { res.status(400).json({ error: 'userId and postId must be numbers' }); return; }

  const updated = await likePost(postId, ownerUserId);
  if (!updated) { res.status(404).json({ error: 'Post not found' }); return; }
  res.json({ post: updated });
}));