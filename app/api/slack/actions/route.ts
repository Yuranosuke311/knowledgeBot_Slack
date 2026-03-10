export const dynamic = 'force-dynamic'

import { NextRequest, NextResponse } from 'next/server'
import { Client as QStashClient } from '@upstash/qstash'
import { verifySlackSignature } from '@/lib/slack'
import { updateCollectMessage } from '@/lib/slack'
import { isCollecting, setCollecting, clearCollecting } from '@/lib/redis'

export async function POST(req: NextRequest): Promise<NextResponse> {
  const rawBody = await req.text()

  // Slack 署名検証
  const signature = req.headers.get('x-slack-signature') ?? ''
  const timestamp = req.headers.get('x-slack-request-timestamp') ?? ''
  if (!verifySlackSignature(signature, timestamp, rawBody)) {
    return NextResponse.json({ error: 'invalid_signature' }, { status: 403 })
  }

  // application/x-www-form-urlencoded をパース
  const params  = new URLSearchParams(rawBody)
  const payload = JSON.parse(params.get('payload') ?? '{}')

  const actionId   = payload.actions?.[0]?.action_id as string | undefined
  const channelId  = payload.channel?.id as string
  const messageTs  = payload.message?.ts as string

  if (!actionId || !channelId || !messageTs) {
    return NextResponse.json({ ok: true })
  }

  if (actionId === 'collect_yes') {
    // 多重起動チェック
    if (await isCollecting(channelId)) {
      await updateCollectMessage(channelId, messageTs, '⚠️ 現在収集中です。完了までお待ちください。')
      return NextResponse.json({ ok: true })
    }

    // 進行中フラグをセット（最大 2時間）
    await setCollecting(channelId)

    // ボタンを除去してメッセージ更新
    await updateCollectMessage(channelId, messageTs, '📥 収集を開始しました。完了後に通知します。')

    // QStash にジョブを発行（QSTASH_URL でリージョン指定）
    const qstash = new QStashClient({
      token: process.env.QSTASH_TOKEN!,
      ...(process.env.QSTASH_URL ? { baseUrl: process.env.QSTASH_URL } : {}),
    })
    const baseUrl = process.env.VERCEL_PROJECT_PRODUCTION_URL
      ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`
      : process.env.NEXT_PUBLIC_BASE_URL!

    try {
      await qstash.publishJSON({
        url: `${baseUrl}/api/slack/collect`,
        body: { channelId, cursor: null, collected: 0 },
      })
    } catch (err) {
      console.error('[QStash] publish error:', err)
      await clearCollecting(channelId)
      await updateCollectMessage(channelId, messageTs, '⚠️ 収集の開始に失敗しました。再度お試しください。')
    }

  } else if (actionId === 'collect_no') {
    await updateCollectMessage(channelId, messageTs, 'キャンセルしました。')
  }

  return NextResponse.json({ ok: true })
}
