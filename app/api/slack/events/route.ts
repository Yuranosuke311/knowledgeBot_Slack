export const dynamic = 'force-dynamic'

import { NextRequest, NextResponse } from 'next/server'
import {
  verifySlackSignature,
  fetchThreadMessages,
  fetchMessageFiles,
  getUserDisplayName,
  getChannelName,
  postSaveNotification,
  postUsageHint,
  postAnswer,
} from '@/lib/slack'
import { buildEnrichment } from '@/lib/extractor'
import { createKnowledgePage } from '@/lib/notion'
import { buildRedisKey, isDuplicate, markAsProcessed } from '@/lib/redis'
import { embedText, generateAnswer } from '@/lib/gemini'
import { upsertVector, searchVector } from '@/lib/vector'
import {
  CATEGORY_MAP,
  TARGET_REACTIONS,
  type ReactionEmoji,
} from '@/types/knowledge'

export async function POST(req: NextRequest): Promise<NextResponse> {
  // Slack リトライリクエストは無視（処理遅延による2回返信を防止）
  if (req.headers.get('x-slack-retry-num')) {
    return NextResponse.json({ ok: true, skipped: 'retry' })
  }

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
  if (!event) {
    return NextResponse.json({ ok: true, skipped: true })
  }

  if (event.type === 'reaction_added') {
    await handleReactionAdded(event)
  } else if (event.type === 'app_mention') {
    await handleAppMention(event)
  }

  return NextResponse.json({ ok: true })
}

async function handleReactionAdded(event: Record<string, unknown>): Promise<void> {
  const reaction = event.reaction as string
  const item = event.item as Record<string, string>

  if (!TARGET_REACTIONS.has(reaction as ReactionEmoji)) return

  const channelId = item.channel
  const messageTs = item.ts
  const reactingUserId = event.user as string

  const redisKey = buildRedisKey(channelId, messageTs, reaction)
  if (await isDuplicate(redisKey)) return

  const [messages, channelName, files] = await Promise.all([
    fetchThreadMessages(channelId, messageTs),
    getChannelName(channelId),
    fetchMessageFiles(channelId, messageTs),
  ])

  const originalMessage = messages[0]
  const posterId = originalMessage?.user ?? reactingUserId
  const postedBy = await getUserDisplayName(posterId)

  const rawText = originalMessage?.text ?? ''
  let fullText = messages
    .map((m) => m.text)
    .filter(Boolean)
    .join('\n\n---\n\n')

  // URL・PDF・画像から追加テキストを抽出して付加（失敗しても続行）
  try {
    const enrichment = await buildEnrichment(rawText, files, process.env.SLACK_BOT_TOKEN!)
    if (enrichment) fullText += enrichment
  } catch (err) {
    console.error('[Extractor] enrichment error:', err)
  }

  const title = rawText.slice(0, 80) || '（本文なし）'
  const category = CATEGORY_MAP[reaction as ReactionEmoji]
  const savedAt = new Date().toISOString()

  const [notionResult] = await Promise.all([
    createKnowledgePage({
      title,
      category,
      postedBy,
      slackChannel: channelName,
      savedAt,
      fullText,
    }),
    markAsProcessed(redisKey),
  ])

  // 保存完了通知（失敗しても続行）
  try {
    await postSaveNotification(channelId, messageTs, category, notionResult.url)
  } catch (err) {
    console.error('[Slack] postSaveNotification error:', err)
  }

  // Upstash Vector への登録（失敗してもフェーズ1 の動作に影響しない）
  try {
    const vector = await embedText(fullText)
    await upsertVector(notionResult.id, vector, {
      title,
      category,
      channel: channelName,
      savedAt,
      notionUrl: notionResult.url,
      fullText: fullText.slice(0, 1000),
    })
  } catch (err) {
    console.error('[Vector] upsert error:', err)
  }
}

async function handleAppMention(event: Record<string, unknown>): Promise<void> {
  // ボット自身の投稿によるメンションは無視（無限ループ防止）
  if (event.bot_id) return

  const channelId = event.channel as string
  const threadTs = (event.thread_ts ?? event.ts) as string

  // メンション部分を除いた質問テキストを抽出
  const question = ((event.text as string) ?? '').replace(/<@[A-Z0-9]+>/g, '').trim()

  if (!question) {
    try {
      await postUsageHint(channelId, threadTs)
    } catch (err) {
      console.error('[Slack] postUsageHint error:', err)
    }
    return
  }

  try {
    const vector = await embedText(question)
    const results = await searchVector(vector, 5)

    if (results.length === 0) {
      await postAnswer(channelId, threadTs, '関連する情報が見つかりませんでした。', [])
      return
    }

    const answer = await generateAnswer(question, results)
    await postAnswer(channelId, threadTs, answer, results)
  } catch (err) {
    console.error('[RAG] error:', err)
    try {
      await postAnswer(channelId, threadTs, '検索に失敗しました。しばらく待ってから再試行してください。', [])
    } catch {
      // 返信失敗は無視
    }
  }
}
