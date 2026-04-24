import { Request, Response, NextFunction } from "express";
import os from 'os'


export const INSTANCE_ID = process.env.INSTANCE_ID || process.env.HOSTNAME || os.hostname() || 'unknown'
console.log(INSTANCE_ID)

export function timingMiddleware(req: Request, res: Response, next: NextFunction): void{
    const start = process.hrtime.bigint()
    console.log(start)
    res.setHeader('X-Instance-Id', INSTANCE_ID)
   res.on('finish', () => {
    const durationMs = Number(process.hrtime.bigint() - start) / 1000000;
    console.log(durationMs)
    const status = res.statusCode;
    const emoji  = status < 400 ? '✅' : status < 500 ? '⚠️ ' : '❌';

    console.log(
      `[HTTP] ${emoji} ${req.method} ${req.originalUrl} → ${status} | ` +
      `${durationMs.toFixed(2)} ms | instance=${INSTANCE_ID}`,
    );
  });
 
  next();
}

export function scalingInfoMiddleware(req: Request, res: Response, next: NextFunction): void{
    const forwardedFor = req.headers['x-forwarded-for'] ?? req.socket.remoteAddress;
    console.log(
    `[SCALE] 📥 Incoming request from ${String(forwardedFor)} ` +
    `handled by instance="${INSTANCE_ID}"`,
  );
  next();
}