import Redis from "ioredis";

export const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
export const REDIS_PORT = process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT, 10) : 6379;

export const createTestRedisConfig = (db: number) => {
  const flushRedis = async () => {
    const redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT, db });
    await redis.flushdb();
    await redis.quit();
    await wait(200); // eventual consistency..
  };

  const redisOptions = {
    host: REDIS_HOST,
    port: REDIS_PORT,
    db,
  };

  return { db, flushRedis, redisOptions };
};

export const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
