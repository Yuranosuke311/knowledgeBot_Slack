import { NextRequest, NextResponse } from 'next/server'
import {
  verifySlackSignature,
  fetchThreadMessages,
  getUserDisplayName,
  getChannelName,
  postSaveNotification,
} from '@/lib/slack'
import { createKnowledgePage } from '@/lib/notion'
import { buildRedisKey, isDuplicate, markAsProcessed } from '@/lib/redis'
import {
  CATEGORY_MAP,
  TARGET_REACTIONS,
  type ReactionEmoji,
} from '@/types/knowledge'

export async function POST(req: NextRequest): Promise<NextResponse> {
  const rawBody = await req.text()

  // URL Verification（Slack App 初回設定時）
  let body: Record<string, unknown>
  try {
    body = JSON.parse(rawBody)
  } catch {
    return NextResponse.json({ error: 'invalid_json' }, { status: 400 })
  }

  if (body.type === 'url_verification') {
    return NextResponse.json({ challenge: body.challenge })
  }

  // 署名検証
  const signature = req.headers.get('x-slack-signature') ?? ''
  const timestamp = req.headers.get('x-slack-request-timestamp') ?? ''

  if (!verifySlackSignature(signature, timestamp, rawBody)) {
    return NextResponse.json({ error: 'invalid_signature' }, { status: 403 })
  }

  const event = body.event as Record<string, unknown> | undefined

  // reaction_added 以外は無視
  if (!event || event.type !== 'reaction_added') {
    return NextResponse.json({ ok: true, skipped: true })
  }

  const reaction = event.reaction as string
  const item = event.item as Record<string, string>

  // 対象絵文字以外は無視
  if (!TARGET_REACTIONS.has(reaction as ReactionEmoji)) {
    return NextResponse.json({ ok: true, skipped: true })
  }

  const channelId = item.channel
  const messageTs = item.ts
  const reactingUserId = event.user as string

  // 重複チェック
  const redisKey = buildRedisKey(channelId, messageTs, reaction)
  if (await isDuplicate(redisKey)) {
    return NextResponse.json({ ok: true, skipped: true })
  }

  // スレッド全文・投稿者名・チャンネル名を並列取得
  const [messages, channelName] = await Promise.all([
    fetchThreadMessages(channelId, messageTs),
    getChannelName(channelId),
  ])

  // 投稿者（スレッド親メッセージの user）の表示名を取得
  const originalMessage = messages[0]
  const posterId = originalMessage?.user ?? reactingUserId
  const postedBy = await getUserDisplayName(posterId)

  // スレッド全文を結合
  const fullText = messages
    .map((m) => m.text)
    .filter(Boolean)
    .join('\n\n---\n\n')

  const title = (originalMessage?.text ?? '').slice(0, 80) || '（本文なし）'
  const category = CATEGORY_MAP[reaction as ReactionEmoji]

  // Notion へ保存 と Redis への書き込みを並列実行
  const [notionResult] = await Promise.all([
    createKnowledgePage({
      title,
      category,
      postedBy,
      slackChannel: channelName,
      savedAt: new Date().toISOString(),
      fullText,
    }),
    markAsProcessed(redisKey),
  ])

  // 保存完了通知をスレッドへ返信（失敗しても 200 を返す）
  try {
    await postSaveNotification(channelId, messageTs, category, notionResult.url)
  } catch (err) {
    console.error('[Slack] postSaveNotification error:', err)
  }

  return NextResponse.json({ ok: true })
}
