import { WebClient } from '@slack/web-api'
import { createHmac, timingSafeEqual } from 'crypto'
import type { CategoryLabel, SlackMessage } from '@/types/knowledge'
import { CATEGORY_EMOJI } from '@/types/knowledge'
import type { SlackFile } from '@/lib/extractor'

let _slackClient: WebClient | null = null
function getSlackClient(): WebClient {
  if (!_slackClient) _slackClient = new WebClient(process.env.SLACK_BOT_TOKEN)
  return _slackClient
}

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
  const result = await getSlackClient().conversations.replies({
    channel: channelId,
    ts: threadTs,
  })

  return (result.messages ?? []).map((m: { user?: string; text?: string; ts?: string }) => ({
    user: m.user ?? 'unknown',
    text: m.text ?? '',
    ts: m.ts ?? '',
  }))
}

// 投稿者の表示名を取得
export async function getUserDisplayName(userId: string): Promise<string> {
  try {
    const result = await getSlackClient().users.info({ user: userId })
    return result.user?.profile?.display_name || result.user?.real_name || userId
  } catch {
    return userId
  }
}

// チャンネル名を取得（DM・グループDM にも対応）
export async function getChannelName(channelId: string): Promise<string> {
  try {
    const result = await getSlackClient().conversations.info({ channel: channelId })
    const ch = result.channel as Record<string, unknown> | undefined
    if (ch?.is_im) return 'DM'
    if (ch?.is_mpim) return 'グループDM'
    return '#' + (ch?.name ?? channelId)
  } catch {
    return channelId
  }
}

// 元メッセージの Slack 添付ファイル一覧を取得
export async function fetchMessageFiles(
  channelId: string,
  messageTs: string
): Promise<SlackFile[]> {
  try {
    const result = await getSlackClient().conversations.history({
      channel: channelId,
      oldest: messageTs,
      latest: messageTs,
      limit: 1,
      inclusive: true,
    })
    const msg = result.messages?.[0] as Record<string, unknown> | undefined
    if (!msg?.files) return []
    const files = msg.files as Array<Record<string, string>>
    return files
      .filter((f) => f.url_private_download && f.mimetype)
      .map((f) => ({
        urlPrivateDownload: f.url_private_download,
        mimetype: f.mimetype,
        name: f.name ?? 'file',
      }))
  } catch {
    return []
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

  await getSlackClient().chat.postMessage({
    channel: channelId,
    thread_ts: threadTs,
    text: `✅ Notion に保存しました（カテゴリ: ${emoji} ${category}）`,
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

// 使い方ヒントをスレッドへ返信
export async function postUsageHint(
  channelId: string,
  threadTs: string
): Promise<void> {
  await getSlackClient().chat.postMessage({
    channel: channelId,
    thread_ts: threadTs,
    text: '使い方: `@knowledgeBot <質問内容>` と話しかけてください',
  })
}

// RAG 回答をスレッドへ返信（出典リンク付き）
export async function postAnswer(
  channelId: string,
  threadTs: string,
  answer: string,
  sources: import('@/types/knowledge').VectorSearchResult[]
): Promise<void> {
  const sourceLines = sources
    .map((r) => {
      const m = r.metadata
      const savedDate = m.savedAt.slice(0, 10)
      return `・${m.title} (${m.channel} / ${savedDate})　<${m.notionUrl}|開く>`
    })
    .join('\n')

  const blocks = [
    { type: 'section', text: { type: 'mrkdwn', text: answer } },
    ...(sourceLines
      ? [
          { type: 'divider' },
          {
            type: 'context',
            elements: [{ type: 'mrkdwn', text: `*参照したナレッジ*
${sourceLines}` }],
          },
        ]
      : []),
  ]

  await getSlackClient().chat.postMessage({
    channel: channelId,
    thread_ts: threadTs,
    text: answer,
    blocks,
  })
}
