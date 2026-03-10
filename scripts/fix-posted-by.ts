/**
 * PostedBy = 'Unknown' の Notion ページを Slack 履歴から逆引きして修正するスクリプト
 *
 * 実行前の準備:
 *   Slack App → OAuth & Permissions → Bot Token Scopes に users:read を追加 → 再インストール
 *   → .env.local の SLACK_BOT_TOKEN を更新（または SLACK_USER_TOKEN に users:read を付与）
 *
 * 実行方法:
 *   npx tsx scripts/fix-posted-by.ts
 *
 * 中断した場合は再実行すると fix-progress.json を読み込んで続きから再開します。
 */

import * as dotenv from 'dotenv'
import * as fs from 'fs'
import * as path from 'path'
import { WebClient } from '@slack/web-api'
import { Client as NotionClient } from '@notionhq/client'

dotenv.config({ path: path.resolve(process.cwd(), '.env.local') })

const SLACK_DELAY_MS  = 1200
const NOTION_DELAY_MS = 400
const PROGRESS_FILE   = path.resolve(process.cwd(), 'scripts/fix-progress.json')

/** batch-import.ts と同じクリーン処理（タイトル照合に必要） */
function cleanText(text: string): string {
  return text
    .replace(/<@[A-Z0-9]+>/g, '')
    .replace(/<#[A-Z0-9]+\|([^>]+)>/g, '#$1')
    .replace(/<https?:\/\/[^|>]+\|([^>]+)>/g, '$1')
    .replace(/<https?:\/\/[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .trim()
}

const slackToken = process.env.SLACK_USER_TOKEN || process.env.SLACK_BOT_TOKEN
const slack  = new WebClient(slackToken)
const notion = new NotionClient({ auth: process.env.NOTION_TOKEN })

// ─────────────────────────────────────────────
// 進捗管理
// ─────────────────────────────────────────────
interface FixProgress {
  updatedIds: string[]
  skippedIds: string[]
}

function loadProgress(): FixProgress {
  if (fs.existsSync(PROGRESS_FILE)) {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'))
  }
  return { updatedIds: [], skippedIds: [] }
}

function saveProgress(p: FixProgress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2))
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// ─────────────────────────────────────────────
// Slack チャンネル名 → ID マップ構築
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
// Notion から PostedBy = 'Unknown' のページを全件取得
// ─────────────────────────────────────────────
interface UnknownPage {
  id: string
  savedAt: string
  channelName: string
  title: string
}

async function fetchUnknownPages(): Promise<UnknownPage[]> {
  const pages: UnknownPage[] = []
  let cursor: string | undefined

  do {
    await sleep(NOTION_DELAY_MS)
    const res = await notion.databases.query({
      database_id: process.env.NOTION_DATABASE_ID!,
      filter: {
        property: 'PostedBy',
        rich_text: { equals: 'Unknown' },
      },
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
      if (savedAt && channelName) {
        pages.push({ id: page.id, savedAt, channelName, title })
      }
    }

    cursor = res.has_more ? (res.next_cursor ?? undefined) : undefined
  } while (cursor)

  return pages
}

// ─────────────────────────────────────────────
// メイン処理
// ─────────────────────────────────────────────
async function main() {
  const progress = loadProgress()
  const updated  = new Set(progress.updatedIds)
  const skipped  = new Set(progress.skippedIds)

  console.log('=== PostedBy 修正スクリプト開始 ===')
  console.log(`処理済み(更新): ${updated.size} 件 / スキップ済み: ${skipped.size} 件\n`)

  // チャンネルマップ構築
  console.log('チャンネル一覧を取得中...')
  const channelMap = await buildChannelMap()
  console.log(`${channelMap.size} チャンネル取得完了\n`)

  // Unknown ページ取得
  console.log('Notion から PostedBy=Unknown のページを取得中...')
  const pages = await fetchUnknownPages()
  console.log(`${pages.length} 件取得\n`)

  let countUpdated = 0
  let countSkipped = 0
  let countError   = 0

  // ユーザーIDキャッシュ（同一ユーザーの重複 API 呼び出しを防ぐ）
  const userCache = new Map<string, string>()

  for (const page of pages) {
    // 処理済みスキップ
    if (updated.has(page.id) || skipped.has(page.id)) {
      countSkipped++
      continue
    }

    // チャンネルID逆引き
    const channelId = channelMap.get(page.channelName)
    if (!channelId) {
      console.log(`\n[SKIP] チャンネル不明: #${page.channelName} (pageId: ${page.id})`)
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    // savedAt → Slack ts に逆変換
    // Notion date型が時刻を切り捨てる場合に備え、1日分を検索ウィンドウとして確保
    const savedDate = new Date(page.savedAt)
    const hasTime   = page.savedAt.includes('T') && page.savedAt.includes(':')
    const tsNum     = savedDate.getTime() / 1000

    // 時刻あり → ±120秒、時刻なし（日付のみ） → 翌日0時まで丸1日
    const oldest = hasTime
      ? String(tsNum - 120)
      : String(tsNum)
    const latest = hasTime
      ? String(tsNum + 120)
      : String(tsNum + 86400)

    console.log(`\n[INFO] savedAt=${page.savedAt} hasTime=${hasTime}`)

    // ウィンドウ内のメッセージを全件収集（ページネーション対応）
    const allMessages: Array<{ text?: string; user?: string; bot_id?: string }> = []
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
        allMessages.push(...(histRes.messages ?? []))
        msgCursor = histRes.response_metadata?.next_cursor || undefined
      } while (msgCursor)
    } catch (e: unknown) {
      const errMsg = e instanceof Error ? e.message : String(e)
      console.log(`[SKIP] history 取得失敗: ${errMsg}`)
      countError++
      continue
    }

    console.log(`[INFO] ウィンドウ内メッセージ数: ${allMessages.length}`)

    // cleanText 処理後の先頭 80 文字で照合
    const match = allMessages.find(m => {
      if (!m.text || m.bot_id) return false
      const candidate = cleanText(m.text).replace(/\n/g, ' ').slice(0, 80)
      return candidate === page.title || page.title.startsWith(candidate.slice(0, 30))
    })

    if (!match?.user) {
      console.log(`\n[SKIP] メッセージ照合失敗: ${page.title.slice(0, 40)}`)
      skipped.add(page.id)
      progress.skippedIds = Array.from(skipped)
      saveProgress(progress)
      countSkipped++
      continue
    }

    try {
      // ユーザー名取得（キャッシュ利用）
      let realName = userCache.get(match.user)
      if (!realName) {
        await sleep(SLACK_DELAY_MS)
        const userInfo = await slack.users.info({ user: match.user })
        realName = userInfo.user?.real_name ?? userInfo.user?.name ?? 'Unknown'
        userCache.set(match.user, realName)
      }

      if (realName === 'Unknown') {
        console.log(`\n[SKIP] ユーザー名取得失敗: user=${match.user}`)
        skipped.add(page.id)
        progress.skippedIds = Array.from(skipped)
        saveProgress(progress)
        countSkipped++
        continue
      }

      // Notion 更新
      await sleep(NOTION_DELAY_MS)
      await notion.pages.update({
        page_id: page.id,
        properties: {
          PostedBy: { rich_text: [{ text: { content: realName } }] },
        },
      })

      updated.add(page.id)
      progress.updatedIds = Array.from(updated)
      saveProgress(progress)
      countUpdated++
      process.stdout.write(`\r更新: ${countUpdated} 件完了`)

    } catch (err) {
      console.error(`\n[ERROR] pageId=${page.id}:`, err)
      countError++
    }
  }

  console.log('\n\n=== 完了 ===')
  console.log(`更新: ${countUpdated} 件 / スキップ: ${countSkipped} 件 / エラー: ${countError} 件`)

  if (countError === 0 && fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE)
    console.log('fix-progress.json を削除しました')
  }
}

main().catch(err => {
  console.error('致命的エラー:', err)
  process.exit(1)
})
