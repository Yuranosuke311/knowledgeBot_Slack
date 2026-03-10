/**
 * 既存の Notion + Upstash Vector データに Phase 3（URL・PDF・画像抽出）を適用するスクリプト
 *
 * 処理内容:
 *   1. Notion DB から全ページを取得
 *   2. 各ページの SavedAt / SlackChannel から元の Slack メッセージを逆引き
 *   3. URL・PDF・画像のテキストを抽出して Notion ページに追記（重複ブロックなし）
 *   4. fullText + enrichment で再 Embedding → Upstash Vector を upsert
 *
 * 実行方法:
 *   npx tsx scripts/enrich-existing.ts
 *
 * 中断した場合は再実行すると enrich-progress.json を読み込んで続きから再開します。
 */

import * as dotenv from 'dotenv'
import * as fs from 'fs'
import * as path from 'path'
import { WebClient } from '@slack/web-api'
import { Client as NotionClient } from '@notionhq/client'
import { GoogleGenerativeAI } from '@google/generative-ai'
import { Index } from '@upstash/vector'
import { buildEnrichment, type SlackFile } from '../lib/extractor'

dotenv.config({ path: path.resolve(process.cwd(), '.env.local') })

const SLACK_DELAY_MS  = 1200
const NOTION_DELAY_MS = 400
const GEMINI_DELAY_MS = 300
const PROGRESS_FILE   = path.resolve(process.cwd(), 'scripts/enrich-progress.json')

const slackToken = process.env.SLACK_USER_TOKEN || process.env.SLACK_BOT_TOKEN
const slack  = new WebClient(slackToken)
const notion = new NotionClient({ auth: process.env.NOTION_TOKEN })
const genai  = new GoogleGenerativeAI(process.env.GEMINI_API_KEY!)
const vector = new Index({
  url:   process.env.UPSTASH_VECTOR_REST_URL!,
  token: process.env.UPSTASH_VECTOR_REST_TOKEN!,
})

// ─────────────────────────────────────────────
// 進捗管理
// ─────────────────────────────────────────────
interface EnrichProgress {
  enrichedIds: string[]   // 処理済み Notion ページ ID
  skippedIds:  string[]   // スキップ済み（元メッセージ見つからず等）
}

function loadProgress(): EnrichProgress {
  if (fs.existsSync(PROGRESS_FILE)) {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'))
  }
  return { enrichedIds: [], skippedIds: [] }
}

function saveProgress(p: EnrichProgress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2))
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/** batch-import.ts と同じクリーン処理（タイトル照合に使用） */
function cleanText(text: string): string {
  return text
    .replace(/<@[A-Z0-9]+>/g, '')
    .replace(/<#[A-Z0-9]+\|([^>]+)>/g, '#$1')
    .replace(/<https?:\/\/[^|>]+\|([^>]+)>/g, '$1')
    .replace(/<https?:\/\/[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .trim()
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
// Slack チャンネル名 → ID マップ
// ─────────────────────────────────────────────
async function buildChannelMap(): Promise<Map<string, string>> {
  const map = new Map<string, string>()
  let cursor: string | undefined
  do {
    await sleep(SLACK_DELAY_MS)
    const res = await slack.conversations.list({
      types: 'public_channel,private_channel',
      limit: 200,
      cursor,
    })
    for (const ch of res.channels ?? []) {
      if (ch.id && ch.name) map.set(ch.name, ch.id)
    }
    cursor = res.response_metadata?.next_cursor || undefined
  } while (cursor)
  return map
}

// ─────────────────────────────────────────────
// Notion 全ページ取得
// ─────────────────────────────────────────────
interface NotionPage {
  id: string
  title: string
  savedAt: string
  channelName: string
  notionUrl: string
}

async function fetchAllPages(): Promise<NotionPage[]> {
  const pages: NotionPage[] = []
  let cursor: string | undefined

  do {
    await sleep(NOTION_DELAY_MS)
    const res = await notion.databases.query({
      database_id: process.env.NOTION_DATABASE_ID!,
      start_cursor: cursor,
      page_size: 100,
    })

    for (const page of res.results) {
      if (page.object !== 'page') continue
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const p = page as any
      const savedAt     = p.properties?.SavedAt?.date?.start ?? ''
      const channelRaw  = p.properties?.SlackChannel?.rich_text?.[0]?.text?.content ?? ''
      const channelName = channelRaw.replace(/^#/, '')
      const title       = p.properties?.Title?.title?.[0]?.text?.content ?? ''
      const notionUrl   = p.url ?? ''
      if (savedAt && channelName) {
        pages.push({ id: page.id, title, savedAt, channelName, notionUrl })
      }
    }

    cursor = res.has_more ? (res.next_cursor ?? undefined) : undefined
  } while (cursor)

  return pages
}

// ─────────────────────────────────────────────
// Notion ページにブロックを追記
// ─────────────────────────────────────────────
async function appendBlocksToNotion(pageId: string, text: string): Promise<void> {
  // 1900 文字ずつ分割（Notion の制限）
  const chunks: string[] = []
  for (let i = 0; i < text.length; i += 1900) {
    chunks.push(text.slice(i, i + 1900))
  }
  await notion.blocks.children.append({
    block_id: pageId,
    children: chunks.map(chunk => ({
      object: 'block' as const,
      type: 'paragraph' as const,
      paragraph: { rich_text: [{ text: { content: chunk } }] },
    })),
  })
}

// ─────────────────────────────────────────────
// メイン処理
// ─────────────────────────────────────────────
async function main() {
  const progress = loadProgress()
  const enriched = new Set(progress.enrichedIds)
  const skipped  = new Set(progress.skippedIds)

  console.log('=== 既存データ Phase 3 エンリッチ開始 ===')
  console.log(`処理済み(エンリッチ): ${enriched.size} 件 / スキップ済み: ${skipped.size} 件\n`)

  console.log('チャンネル一覧を取得中...')
  const channelMap = await buildChannelMap()
  console.log(`${channelMap.size} チャンネル取得完了\n`)

  console.log('Notion DB から全ページを取得中...')
  const pages = await fetchAllPages()
  console.log(`${pages.length} 件取得\n`)

  let countEnriched = 0
  let countSkipped  = 0
  let countError    = 0

  for (const page of pages) {
    // 処理済みスキップ
    if (enriched.has(page.id) || skipped.has(page.id)) {
      countSkipped++
      continue
    }

    // チャンネルID逆引き
    const channelId = channelMap.get(page.channelName)
    if (!channelId) {
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    // savedAt → Slack ts 逆変換（時刻なし → 1日分ウィンドウ）
    const hasTime = page.savedAt.includes('T') && page.savedAt.includes(':')
    const tsNum   = new Date(page.savedAt).getTime() / 1000
    const oldest  = hasTime ? String(tsNum - 120) : String(tsNum)
    const latest  = hasTime ? String(tsNum + 120) : String(tsNum + 86400)

    // Slack 履歴からメッセージを逆引き
    const allMessages: Array<{ text?: string; files?: Array<Record<string, string>>; user?: string; bot_id?: string }> = []
    let msgCursor: string | undefined
    try {
      do {
        await sleep(SLACK_DELAY_MS)
        const histRes = await slack.conversations.history({
          channel: channelId,
          oldest,
          latest,
          limit: 200,
          cursor: msgCursor,
        })
        allMessages.push(...(histRes.messages ?? []) as typeof allMessages)
        msgCursor = histRes.response_metadata?.next_cursor || undefined
      } while (msgCursor)
    } catch {
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    // タイトルで照合
    const match = allMessages.find(m => {
      if (!m.text || m.bot_id) return false
      const candidate = cleanText(m.text).replace(/\n/g, ' ').slice(0, 80)
      return candidate === page.title || page.title.startsWith(candidate.slice(0, 30))
    })

    if (!match?.text) {
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    // ファイル情報を整形
    const msgFiles: SlackFile[] = (match.files ?? [])
      .filter(f => f.url_private_download && f.mimetype)
      .map(f => ({ urlPrivateDownload: f.url_private_download, mimetype: f.mimetype, name: f.name ?? 'file' }))

    // エンリッチメント生成（URL・PDF・画像）
    let enrichment = ''
    try {
      enrichment = await buildEnrichment(match.text, msgFiles, slackToken!)
    } catch {
      // 抽出失敗は無視してスキップ扱い
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    if (!enrichment) {
      // 追記内容なし → スキップ扱い（URL も添付もなかった）
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    try {
      // Notion ページにブロック追記
      await sleep(NOTION_DELAY_MS)
      await appendBlocksToNotion(page.id, enrichment)

      // 再 Embedding → Vector upsert
      await sleep(GEMINI_DELAY_MS)
      const combined = page.title + '\n' + enrichment
      const vec = await embedText(combined)
      await vector.upsert({
        id: page.id,
        vector: vec,
        metadata: {
          title:     page.title,
          category:  '一般',
          channel:   `#${page.channelName}`,
          savedAt:   page.savedAt,
          notionUrl: page.notionUrl,
          fullText:  combined.slice(0, 1000),
        },
      })

      enriched.add(page.id)
      progress.enrichedIds = Array.from(enriched)
      saveProgress(progress)
      countEnriched++
      process.stdout.write(`\rエンリッチ: ${countEnriched} 件完了`)

    } catch (err) {
      console.error(`\n[ERROR] pageId=${page.id}:`, err)
      countError++
    }
  }

  console.log('\n\n=== 完了 ===')
  console.log(`エンリッチ: ${countEnriched} 件 / スキップ: ${countSkipped} 件 / エラー: ${countError} 件`)
  console.log(`※ スキップ = 元Slackメッセージ未照合 or URL・添付なし`)

  if (countError === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE)
    console.log('enrich-progress.json を削除しました')
  }
}

main().catch(err => {
  console.error('致命的エラー:', err)
  process.exit(1)
})
