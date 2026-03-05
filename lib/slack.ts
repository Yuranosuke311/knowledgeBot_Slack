import { WebClient } from '@slack/web-api'
import { createHmac, timingSafeEqual } from 'crypto'
import type { CategoryLabel, SlackMessage } from '@/types/knowledge'
import { CATEGORY_EMOJI } from '@/types/knowledge'

export const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN)

// 署名検証
export function verifySlackSignature(
  signature: string,
  timestamp: string,
  rawBody: string
): boolean {
  const signingSecret = process.env.SLACK_SIGNING_SECRET!

  // タイムスタンプが5分以上古い場合は拒否
  const now = Math.floor(Date.now() / 1000)
  if (Math.abs(now - parseInt(timestamp, 10)) > 300) return false

  const baseString = `v0:${timestamp}:${rawBody}`
  const expected = 'v0=' + createHmac('sha256', signingSecret).update(baseString).digest('hex')

  try {
    return timingSafeEqual(Buffer.from(signature), Buffer.from(expected))
  } catch {
    return false
  }
}

// スレッド全文取得
export async function fetchThreadMessages(
  channelId: string,
  threadTs: string
): Promise<SlackMessage[]> {
  const result = await slackClient.conversations.replies({
    channel: channelId,
    ts: threadTs,
  })

  return (result.messages ?? []).map((m) => ({
    user: m.user ?? 'unknown',
    text: m.text ?? '',
    ts: m.ts ?? '',
  }))
}

// 投稿者の表示名を取得
export async function getUserDisplayName(userId: string): Promise<string> {
  try {
    const result = await slackClient.users.info({ user: userId })
    return result.user?.profile?.display_name || result.user?.real_name || userId
  } catch {
    return userId
  }
}

// チャンネル名を取得
export async function getChannelName(channelId: string): Promise<string> {
  try {
    const result = await slackClient.conversations.info({ channel: channelId })
    return '#' + (result.channel?.name ?? channelId)
  } catch {
    return channelId
  }
}

// 保存完了通知をスレッドへ返信
export async function postSaveNotification(
  channelId: string,
  threadTs: string,
  category: CategoryLabel,
  notionPageUrl: string
): Promise<void> {
  const emoji = CATEGORY_EMOJI[category]

  await slackClient.chat.postMessage({
    channel: channelId,
    thread_ts: threadTs,
    blocks: [
      {
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: '✅ *Notion に保存しました*',
        },
      },
      {
        type: 'context',
        elements: [
          {
            type: 'mrkdwn',
            text: `カテゴリ: ${emoji} ${category}　｜　<${notionPageUrl}|ページを開く>`,
          },
        ],
      },
    ],
  })
}
