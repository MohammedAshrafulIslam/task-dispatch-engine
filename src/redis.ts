import { Redis }from "ioredis";

export const redis = new Redis({
  host: "localhost",
  port: 6379,
});

redis.on("connect", () => console.log("Redis connected"));
redis.on("error", (err: Error) => console.error("Redis error:", err));