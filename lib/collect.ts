/**
 * フェーズ4: チャンネル一括収集 — メッセージ1件の処理ロジック
 *
 * リアクション不問で全メッセージを対象とする（フェーズ1との違い）。
 * Redis キーのサフィックスを '_bulk' にすることでリアクション用キーと競合しない。
 */

import { WebClient } from '@slack/web-api'
import { buildEnrichment } from '@/lib/extractor'
import { createKnowledgePage } from '@/lib/notion'
import { buildRedisKey, isDuplicate, markAsProcessed } from '@/lib/redis'
import { embedText } from '@/lib/gemini'
import { upsertVector } from '@/lib/vector'

function getSlack(): WebClient {
  return new WebClient(process.env.SLACK_BOT_TOKEN)
}

function cleanText(text: string): string {
  return text
    .replace(/<@[A-Z0-9]+>/g, '')
    .replace(/<#[A-Z0-9]+\|([^>]+)>/g, '#$1')
    .replace(/<https?:\/\/[^|>]+\|([^>]+)>/g, '$1')
    .replace(/<https?:\/\/[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .trim()
}

/**
 * Slack メッセージ1件を Notion + Upstash Vector に保存する。
 * @returns 保存した Notion ページID、スキップ時は null
 */
export async function processSingleMessage(
  channelId: string,
  channelName: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  msg: Record<string, any>
): Promise<string | null> {
  const ts      = msg.ts as string
  const rawText = (msg.text as string) ?? ''
  const userId  = (msg.user as string) ?? ''

  // bot メッセージ・短すぎるメッセージはスキップ
  if (msg.bot_id) return null
  if (rawText.trim().length < 5) return null

  // Redis 重複チェック（_bulk サフィックスでリアクション用キーと区別）
  const redisKey = buildRedisKey(channelId, ts, '_bulk')
  if (await isDuplicate(redisKey)) return null

  const slack = getSlack()

  // スレッド返信を取得して fullText を構築
  let fullText = cleanText(rawText)
  try {
    const repliesRes = await slack.conversations.replies({ channel: channelId, ts })
    const replies = (repliesRes.messages ?? []).slice(1) // 先頭は親メッセージなのでスキップ
    if (replies.length > 0) {
      const replyTexts = replies
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map((r: any) => cleanText((r.text as string) ?? ''))
        .filter(Boolean)
      if (replyTexts.length > 0) fullText += '\n\n---\n\n' + replyTexts.join('\n\n---\n\n')
    }
  } catch { /* スレッド取得失敗は無視 */ }

  // フェーズ3エンリッチメント（URL / PDF / 画像）
  try {
    const files = ((msg.files as Array<Record<string, string>>) ?? [])
      .filter((f) => f.url_private_download && f.mimetype)
      .map((f) => ({
        urlPrivateDownload: f.url_private_download,
        mimetype: f.mimetype,
        name: f.name ?? 'file',
      }))
    const enrichment = await buildEnrichment(rawText, files, process.env.SLACK_BOT_TOKEN!)
    if (enrichment) fullText += enrichment
  } catch { /* エンリッチメント失敗は無視 */ }

  // 投稿者名を取得
  let postedBy = 'Unknown'
  try {
    const userInfo = await slack.users.info({ user: userId })
    postedBy = userInfo.user?.profile?.display_name || userInfo.user?.real_name || userId
  } catch { /* 取得失敗は無視 */ }

  const savedAt = new Date(parseFloat(ts) * 1000).toISOString()
  const title   = cleanText(rawText).slice(0, 80) || '（本文なし）'

  // Notion 保存
  const notionResult = await createKnowledgePage({
    title,
    category: '一般',
    postedBy,
    slackChannel: channelName,
    savedAt,
    fullText,
  })
  await markAsProcessed(redisKey)

  // Upstash Vector 保存
  try {
    const vector = await embedText(fullText)
    await upsertVector(notionResult.id, vector, {
      title,
      category: '一般',
      channel: channelName,
      savedAt,
      notionUrl: notionResult.url,
      fullText: fullText.slice(0, 1000),
    })
  } catch (err) {
    console.error('[Vector] upsert error:', err)
  }

  return notionResult.id
}
