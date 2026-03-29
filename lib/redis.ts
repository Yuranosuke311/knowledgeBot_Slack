import { Redis } from '@upstash/redis'

const TTL_SECONDS = 60 * 60 * 24 * 30 // 30日

let _redis: Redis | null = null
function getRedis(): Redis {
  if (!_redis) {
    _redis = new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL!,
      token: process.env.UPSTASH_REDIS_REST_TOKEN!,
    })
  }
  return _redis
}

export function buildRedisKey(channelId: string, messageTs: string, reaction: string): string {
  return `knowledge:${channelId}:${messageTs}:${reaction}`
}

export async function isDuplicate(key: string): Promise<boolean> {
  try {
    const exists = await getRedis().exists(key)
    return exists === 1
  } catch (err) {
    console.error('[Redis] isDuplicate error:', err)
    return true
  }
}

export async function markAsProcessed(key: string): Promise<void> {
  try {
    await getRedis().set(key, '1', { ex: TTL_SECONDS })
  } catch (err) {
    console.error('[Redis] markAsProcessed error:', err)
  }
}

// ─── フェーズ5: Notion ページID キャッシュ（スレッド返信追記用）───

function notionTsKey(channelId: string, ts: string): string {
  return `notion_ts:${channelId}:${ts}`
}

export async function getNotionPageId(channelId: string, ts: string): Promise<string | null> {
  try {
    return await getRedis().get<string>(notionTsKey(channelId, ts))
  } catch {
    return null
  }
}

export async function setNotionPageId(channelId: string, ts: string, pageId: string): Promise<void> {
  try {
    await getRedis().set(notionTsKey(channelId, ts), pageId, { ex: TTL_SECONDS })
  } catch (err) {
    console.error('[Redis] setNotionPageId error:', err)
  }
}

// ─── フェーズ5: 自動収集モード ───

function autoCollectKey(channelId: string): string {
  return `autocollect:${channelId}`
}

export async function isAutoCollect(channelId: string): Promise<boolean> {
  try {
    return (await getRedis().exists(autoCollectKey(channelId))) === 1
  } catch {
    return false
  }
}

export async function setAutoCollect(channelId: string): Promise<void> {
  try {
    await getRedis().set(autoCollectKey(channelId), '1')
  } catch (err) {
    console.error('[Redis] setAutoCollect error:', err)
  }
}

// ─── フェーズ4: チャンネル収集中フラグ ───

function collectingKey(channelId: string): string {
  return `collecting:${channelId}`
}

export async function isCollecting(channelId: string): Promise<boolean> {
  try {
    return (await getRedis().exists(collectingKey(channelId))) === 1
  } catch {
    return false
  }
}

export async function setCollecting(channelId: string, ttlSeconds = 7200): Promise<void> {
  try {
    await getRedis().set(collectingKey(channelId), '1', { ex: ttlSeconds })
  } catch (err) {
    console.error('[Redis] setCollecting error:', err)
  }
}

export async function clearCollecting(channelId: string): Promise<void> {
  try {
    await getRedis().del(collectingKey(channelId))
  } catch (err) {
    console.error('[Redis] clearCollecting error:', err)
  }
}
