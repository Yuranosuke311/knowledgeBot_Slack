import { Redis } from '@upstash/redis'

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
})

const TTL_SECONDS = 60 * 60 * 24 * 30 // 30日

export function buildRedisKey(channelId: string, messageTs: string, reaction: string): string {
  return `knowledge:${channelId}:${messageTs}:${reaction}`
}

export async function isDuplicate(key: string): Promise<boolean> {
  try {
    const exists = await redis.exists(key)
    return exists === 1
  } catch (err) {
    console.error('[Redis] isDuplicate error:', err)
    // 接続失敗時は重複として扱い、安全側に倒す
    return true
  }
}

export async function markAsProcessed(key: string): Promise<void> {
  try {
    await redis.set(key, '1', { ex: TTL_SECONDS })
  } catch (err) {
    console.error('[Redis] markAsProcessed error:', err)
  }
}
