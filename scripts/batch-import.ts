/**
 * Slack 全チャンネル → Notion + Upstash Vector 一括インポートスクリプト
 *
 * 実行方法:
 *   npx tsx scripts/batch-import.ts
 *
 * 中断した場合は再実行すると progress.json を読み込んで続きから再開します。
 */

import * as dotenv from 'dotenv'
import * as fs from 'fs'
import * as path from 'path'
import { WebClient } from '@slack/web-api'
import { Client as NotionClient } from '@notionhq/client'
import { GoogleGenerativeAI } from '@google/generative-ai'
import { Index } from '@upstash/vector'
import { buildEnrichment, type SlackFile } from '../lib/extractor'

// .env.local を読み込む
dotenv.config({ path: path.resolve(process.cwd(), '.env.local') })

// ─────────────────────────────────────────────
// 設定
// ─────────────────────────────────────────────
const MIN_TEXT_LENGTH = 30       // これ未満の文字数はスキップ
const SLACK_DELAY_MS  = 1200     // Slack API 呼び出し間隔（ms）
const NOTION_DELAY_MS = 400      // Notion API 呼び出し間隔（ms）
const GEMINI_DELAY_MS = 200      // Gemini Embedding 呼び出し間隔（ms）
const PROGRESS_FILE   = path.resolve(process.cwd(), 'scripts/progress.json')

// ─────────────────────────────────────────────
// クライアント
// ─────────────────────────────────────────────
// ユーザートークン（xoxp-...）があれば優先して使用。
// ユーザートークンはボット未参加チャンネルの履歴も取得できる。
// 取得方法: Slack App → OAuth & Permissions → User Token Scopes に
//   channels:history / groups:history / channels:read / groups:read を追加 → 再インストール
const slackToken = process.env.SLACK_USER_TOKEN || process.env.SLACK_BOT_TOKEN
if (!process.env.SLACK_USER_TOKEN) {
  console.warn('[WARN] SLACK_USER_TOKEN が未設定のため SLACK_BOT_TOKEN を使用します。')
  console.warn('       ボットが参加していないチャンネルはスキップされます。\n')
}
const slack  = new WebClient(slackToken)
const notion = new NotionClient({ auth: process.env.NOTION_TOKEN })
const genai  = new GoogleGenerativeAI(process.env.GEMINI_API_KEY!)
const vector = new Index({
  url:   process.env.UPSTASH_VECTOR_REST_URL!,
  token: process.env.UPSTASH_VECTOR_REST_TOKEN!,
})

// ─────────────────────────────────────────────
// 進捗管理（中断 → 再開対応）
// ─────────────────────────────────────────────
interface Progress {
  processedIds: string[]   // 処理済みの Slack message ts
}

function loadProgress(): Progress {
  if (fs.existsSync(PROGRESS_FILE)) {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'))
  }
  return { processedIds: [] }
}

function saveProgress(p: Progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2))
}

// ─────────────────────────────────────────────
// ユーティリティ
// ─────────────────────────────────────────────
function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/** メンション・URL 等を除去してプレーンテキスト化 */
function cleanText(text: string): string {
  return text
    .replace(/<@[A-Z0-9]+>/g, '')   // ユーザーメンション
    .replace(/<#[A-Z0-9]+\|([^>]+)>/g, '#$1') // チャンネルメンション
    .replace(/<https?:\/\/[^|>]+\|([^>]+)>/g, '$1') // リンクテキスト
    .replace(/<https?:\/\/[^>]+>/g, '') // URLのみ
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .trim()
}

/** スキップ対象メッセージか判定 */
function shouldSkip(msg: Record<string, unknown>): boolean {
  if (msg.subtype)          return true  // システムメッセージ・ボット投稿
  if (!msg.text)            return true
  if (msg.bot_id)           return true
  const cleaned = cleanText(msg.text as string)
  if (cleaned.length < MIN_TEXT_LENGTH) return true
  return false
}

/** テキストを Notion の 1900 文字制限に合わせて分割 */
function splitForNotion(text: string): string[] {
  const chunks: string[] = []
  for (let i = 0; i < text.length; i += 1900) {
    chunks.push(text.slice(i, i + 1900))
  }
  return chunks.length > 0 ? chunks : ['']
}

// ─────────────────────────────────────────────
// Gemini Embedding
// ─────────────────────────────────────────────
async function embedText(text: string): Promise<number[]> {
  const model = genai.getGenerativeModel({ model: 'gemini-embedding-001' })
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await model.embedContent({ content: { parts: [{ text }], role: 'user' }, outputDimensionality: 1536 } as any)
  return result.embedding.values
}

// ─────────────────────────────────────────────
// Notion へ保存
// ─────────────────────────────────────────────
async function saveToNotion(params: {
  title: string
  channel: string
  postedBy: string
  savedAt: string
  fullText: string
}): Promise<{ id: string; url: string }> {
  const textChunks = splitForNotion(params.fullText)
  const response = await notion.pages.create({
    parent: { database_id: process.env.NOTION_DATABASE_ID! },
    properties: {
      Title:       { title:     [{ text: { content: params.title.slice(0, 200) } }] },
      Category:    { select:    { name: '一般' } },
      PostedBy:    { rich_text: [{ text: { content: params.postedBy.slice(0, 200) } }] },
      SlackChannel:{ rich_text: [{ text: { content: params.channel.slice(0, 200) } }] },
      SavedAt:     { date:      { start: params.savedAt } },
    },
    children: textChunks.map(chunk => ({
      object: 'block' as const,
      type: 'paragraph' as const,
      paragraph: { rich_text: [{ text: { content: chunk } }] },
    })),
  })
  return { id: response.id, url: (response as unknown as { url: string }).url }
}

// ─────────────────────────────────────────────
// メイン処理
// ─────────────────────────────────────────────
async function main() {
  const progress = loadProgress()
  const processed = new Set(progress.processedIds)

  let totalSaved   = 0
  let totalSkipped = 0
  let totalError   = 0

  console.log('=== Slack 一括インポート開始 ===')
  console.log(`再開: 処理済み ${processed.size} 件をスキップ\n`)

  // 1. 全チャンネル取得
  const channels: { id: string; name: string }[] = []
  let cursor: string | undefined
  do {
    const res = await slack.conversations.list({
      types: 'public_channel,private_channel',
      limit: 200,
      cursor,
    })
    for (const ch of res.channels ?? []) {
      if (ch.id && ch.name && !ch.is_archived) {
        channels.push({ id: ch.id, name: ch.name })
      }
    }
    cursor = res.response_metadata?.next_cursor || undefined
    if (cursor) await sleep(SLACK_DELAY_MS)
  } while (cursor)

  console.log(`対象チャンネル: ${channels.length} 件`)
  console.log(channels.map(c => `  #${c.name}`).join('\n'))
  console.log()

  // 2. チャンネルごとに処理
  for (const channel of channels) {
    console.log(`\n▶ #${channel.name} (${channel.id}) 処理開始`)
    let msgCursor: string | undefined
    let channelSaved = 0

    do {
      await sleep(SLACK_DELAY_MS)
      let res: Awaited<ReturnType<typeof slack.conversations.history>>
      try {
        res = await slack.conversations.history({
          channel: channel.id,
          limit: 200,
          cursor: msgCursor,
        })
      } catch (e: unknown) {
        const errMsg = e instanceof Error ? e.message : String(e)
        console.log(`\n  [SKIP] #${channel.name}: アクセス不可（${errMsg}）`)
        break
      }

      for (const msg of res.messages ?? []) {
        const ts = msg.ts ?? ''
        const uniqueId = `${channel.id}:${ts}`

        // スキップ判定
        if (processed.has(uniqueId)) { totalSkipped++; continue }
        if (shouldSkip(msg as Record<string, unknown>)) { totalSkipped++; continue }

        // スレッド返信を取得してまとめる
        const rawMsgText = msg.text as string
        let fullText = cleanText(rawMsgText)
        if ((msg.reply_count ?? 0) > 0) {
          await sleep(SLACK_DELAY_MS)
          const thread = await slack.conversations.replies({ channel: channel.id, ts })
          const replies = (thread.messages ?? []).slice(1) // 先頭は親メッセージ
          for (const reply of replies) {
            if (!reply.text || reply.bot_id) continue
            const replyText = cleanText(reply.text)
            if (replyText.length > 0) fullText += `\n${replyText}`
          }
        }

        // URL・PDF・画像からテキストを抽出して追記
        const msgFiles: SlackFile[] = ((msg as Record<string, unknown>).files as Array<Record<string, string>> ?? [])
          .filter((f) => f.url_private_download && f.mimetype)
          .map((f) => ({ urlPrivateDownload: f.url_private_download, mimetype: f.mimetype, name: f.name ?? 'file' }))
        try {
          const enrichment = await buildEnrichment(rawMsgText, msgFiles, slackToken!)
          if (enrichment) fullText += enrichment
        } catch { /* 抽出失敗は無視 */ }

        // ユーザー名取得
        let postedBy = 'Unknown'
        try {
          const userInfo = await slack.users.info({ user: msg.user as string })
          postedBy = userInfo.user?.real_name ?? userInfo.user?.name ?? 'Unknown'
        } catch (e: unknown) {
          const errMsg = e instanceof Error ? e.message : String(e)
          console.warn(`\n  [WARN] users.info 失敗 (user=${msg.user}): ${errMsg}`)
          console.warn('  → Slack App に users:read スコープが必要です')
        }
        await sleep(SLACK_DELAY_MS)

        const savedAt = new Date(parseFloat(ts) * 1000).toISOString()
        const title   = fullText.slice(0, 80).replace(/\n/g, ' ')

        try {
          // Notion 保存
          await sleep(NOTION_DELAY_MS)
          const { id: notionId, url: notionUrl } = await saveToNotion({
            title,
            channel: `#${channel.name}`,
            postedBy,
            savedAt,
            fullText,
          })

          // Embedding → Vector 保存
          await sleep(GEMINI_DELAY_MS)
          const vec = await embedText(fullText)
          await vector.upsert({
            id: notionId,
            vector: vec,
            metadata: {
              title,
              category: '一般',
              channel: `#${channel.name}`,
              savedAt,
              notionUrl,
              fullText,
            },
          })

          // 進捗記録
          processed.add(uniqueId)
          progress.processedIds = Array.from(processed)
          saveProgress(progress)

          totalSaved++
          channelSaved++
          process.stdout.write(`\r  保存: ${channelSaved} 件 (全体: ${totalSaved} 件)`)

        } catch (err) {
          console.error(`\n  [ERROR] ts=${ts}:`, err)
          totalError++
        }
      }

      msgCursor = res.response_metadata?.next_cursor || undefined
    } while (msgCursor)

    console.log(`\n  #${channel.name} 完了: ${channelSaved} 件保存`)
  }

  console.log('\n=== 完了 ===')
  console.log(`保存: ${totalSaved} 件 / スキップ: ${totalSkipped} 件 / エラー: ${totalError} 件`)

  // 正常完了したら progress.json を削除
  if (totalError === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE)
    console.log('progress.json を削除しました')
  }
}

main().catch(err => {
  console.error('致命的エラー:', err)
  process.exit(1)
})
