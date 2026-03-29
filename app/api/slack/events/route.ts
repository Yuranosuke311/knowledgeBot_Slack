export const dynamic = 'force-dynamic'

import { NextRequest, NextResponse } from 'next/server'
import { WebClient } from '@slack/web-api'
import {
  verifySlackSignature,
  postUsageHint,
  postAnswer,
  postCollectConfirmation,
} from '@/lib/slack'
import { buildRedisKey, isDuplicate, markAsProcessed, isAutoCollect, setAutoCollect, getNotionPageId, setNotionPageId } from '@/lib/redis'
import { embedText, generateAnswer } from '@/lib/gemini'
import { searchVector } from '@/lib/vector'
import { processSingleMessage } from '@/lib/collect'
import { appendToKnowledgePage } from '@/lib/notion'

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

  if (event.type === 'message') {
    await handleMessage(event)
  } else if (event.type === 'app_mention') {
    await handleAppMention(event)
  } else if (event.type === 'member_joined_channel') {
    await handleMemberJoined(event)
  }

  return NextResponse.json({ ok: true })
}

async function handleMessage(event: Record<string, unknown>): Promise<void> {
  if (event.bot_id) return
  if (event.subtype) return

  const channelId = event.channel as string
  if (!await isAutoCollect(channelId)) return

  const ts = event.ts as string
  const threadTs = event.thread_ts as string | undefined
  const rawText = ((event.text as string) ?? '').trim()
  if (rawText.length < 5) return

  // @knowledgeBot へのメンションメッセージはスキップ（app_mention で処理）
  if (process.env.SLACK_BOT_USER_ID && rawText.includes(`<@${process.env.SLACK_BOT_USER_ID}>`)) return

  const isReply = threadTs !== undefined && threadTs !== ts

  if (isReply) {
    // スレッド返信 → 親メッセージの Notion ページに追記
    const redisKey = buildRedisKey(channelId, ts, '_bulk')
    if (await isDuplicate(redisKey)) return

    const pageId = await getNotionPageId(channelId, threadTs)
    if (pageId) {
      try {
        await appendToKnowledgePage(pageId, `\n---\n${rawText}`)
        await markAsProcessed(redisKey)
      } catch (err) {
        console.error('[AutoCollect] appendToKnowledgePage error:', err)
      }
    }
    // 親ページが見つからない場合は個別保存しない（バルク収集時にスレッドごと取得済みのため）
  } else {
    // 親メッセージ → 新規 Notion ページ作成
    const redisKey = buildRedisKey(channelId, ts, '_bulk')
    if (await isDuplicate(redisKey)) return

    const slack = new WebClient(process.env.SLACK_BOT_TOKEN)
    let channelName = channelId
    try {
      const info = await slack.conversations.info({ channel: channelId })
      const ch = info.channel as Record<string, unknown> | undefined
      channelName = '#' + ((ch?.name as string) ?? channelId)
    } catch { /* 取得失敗時はIDをそのまま */ }

    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const pageId = await processSingleMessage(channelId, channelName, event as Record<string, any>)
      if (pageId) {
        await setNotionPageId(channelId, ts, pageId)
      }
    } catch (err) {
      console.error('[AutoCollect] processSingleMessage error:', err)
    }
  }
}

async function handleMemberJoined(event: Record<string, unknown>): Promise<void> {
  // ボット自身の参加のみ対象
  if (event.user !== process.env.SLACK_BOT_USER_ID) return
  const channelId = event.channel as string
  // 自動収集モードをON
  await setAutoCollect(channelId)
  // 過去メッセージ収集確認（フェーズ4）
  try {
    await postCollectConfirmation(channelId)
  } catch (err) {
    console.error('[Slack] postCollectConfirmation error:', err)
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
