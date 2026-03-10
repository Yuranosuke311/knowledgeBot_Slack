export const dynamic = 'force-dynamic'

import { NextRequest, NextResponse } from 'next/server'
import { Receiver, Client as QStashClient } from '@upstash/qstash'
import { WebClient } from '@slack/web-api'
import { clearCollecting } from '@/lib/redis'
import { postCollectComplete } from '@/lib/slack'
import { processSingleMessage } from '@/lib/collect'

interface CollectJob {
  channelId: string
  cursor:    string | null
  collected: number
}

export async function POST(req: NextRequest): Promise<NextResponse> {
  const rawBody = await req.text()

  // QStash 署名検証
  const receiver = new Receiver({
    currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY!,
    nextSigningKey:    process.env.QSTASH_NEXT_SIGNING_KEY!,
  })
  const isValid = await receiver.verify({
    signature: req.headers.get('upstash-signature') ?? '',
    body: rawBody,
  }).catch(() => false)

  if (!isValid) {
    return NextResponse.json({ error: 'invalid_signature' }, { status: 403 })
  }

  const job: CollectJob = JSON.parse(rawBody)
  const { channelId, cursor, collected } = job

  // チャンネル名を取得
  const slack = new WebClient(process.env.SLACK_BOT_TOKEN)
  let channelName = channelId
  try {
    const info = await slack.conversations.info({ channel: channelId })
    const ch = info.channel as Record<string, unknown> | undefined
    channelName = '#' + ((ch?.name as string) ?? channelId)
  } catch { /* 取得失敗時はIDをそのまま使用 */ }

  // conversations.history で 100件取得
  const result = await slack.conversations.history({
    channel: channelId,
    ...(cursor ? { cursor } : {}),
    limit: 100,
  })

  let newCollected = collected
  for (const msg of result.messages ?? []) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      newCollected += await processSingleMessage(channelId, channelName, msg as Record<string, any>)
    } catch (err) {
      console.error('[Collect] processSingleMessage error:', err)
    }
  }

  const nextCursor = result.response_metadata?.next_cursor
  if (result.has_more && nextCursor) {
    // 続きを QStash に積む（QSTASH_URL でリージョン指定）
    const qstash = new QStashClient({
      token: process.env.QSTASH_TOKEN!,
      ...(process.env.QSTASH_URL ? { baseUrl: process.env.QSTASH_URL } : {}),
    })
    const baseUrl = process.env.VERCEL_PROJECT_PRODUCTION_URL
      ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`
      : process.env.NEXT_PUBLIC_BASE_URL!

    await qstash.publishJSON({
      url: `${baseUrl}/api/slack/collect`,
      body: { channelId, cursor: nextCursor, collected: newCollected },
    })
  } else {
    // 収集完了
    await clearCollecting(channelId)
    await postCollectComplete(channelId, newCollected)
  }

  return NextResponse.json({ ok: true })
}
