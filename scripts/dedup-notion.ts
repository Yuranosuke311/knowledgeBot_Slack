/**
 * Notion DB の重複ページを検出・削除するスクリプト
 *
 * 重複判定キー: SlackChannel + SavedAt + Title先頭30文字 が一致
 * 残すページ : fullText ブロックの合計文字数が最も多いもの（エンリッチ済み優先）
 * 削除処理   : Notion → アーカイブ / Upstash Vector → 物理削除
 *
 * 実行方法:
 *   npx tsx scripts/dedup-notion.ts
 *
 * --dry-run オプションで削除せず確認のみ:
 *   npx tsx scripts/dedup-notion.ts --dry-run
 */

import * as dotenv from 'dotenv'
import * as path from 'path'
import { Client as NotionClient } from '@notionhq/client'
import { Index } from '@upstash/vector'

dotenv.config({ path: path.resolve(process.cwd(), '.env.local') })

const DRY_RUN      = process.argv.includes('--dry-run')
const NOTION_DELAY = 350

const notion = new NotionClient({ auth: process.env.NOTION_TOKEN })
const vector = new Index({
  url:   process.env.UPSTASH_VECTOR_REST_URL!,
  token: process.env.UPSTASH_VECTOR_REST_TOKEN!,
})

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// ─────────────────────────────────────────────
// Notion 全ページ取得（ブロック文字数込み）
// ─────────────────────────────────────────────
interface PageInfo {
  id:          string
  title:       string
  channel:     string
  savedAt:     string
  bodyLength:  number   // ページ本文の総文字数（エンリッチ度の指標）
}

async function fetchAllPages(): Promise<PageInfo[]> {
  const pages: PageInfo[] = []
  let cursor: string | undefined

  do {
    await sleep(NOTION_DELAY)
    const res = await notion.databases.query({
      database_id: process.env.NOTION_DATABASE_ID!,
      start_cursor: cursor,
      page_size: 100,
    })

    for (const page of res.results) {
      if (page.object !== 'page') continue
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const p = page as any
      const title   = p.properties?.Title?.title?.[0]?.text?.content ?? ''
      const channel = p.properties?.SlackChannel?.rich_text?.[0]?.text?.content ?? ''
      const savedAt = p.properties?.SavedAt?.date?.start ?? ''

      // ページ本文（blocks）の文字数を取得（キャッシュのため軽量に）
      let bodyLength = 0
      try {
        await sleep(NOTION_DELAY)
        const blocks = await notion.blocks.children.list({ block_id: page.id, page_size: 100 })
        for (const block of blocks.results) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const b = block as any
          const richText = b?.paragraph?.rich_text ?? []
          for (const rt of richText) {
            bodyLength += (rt?.text?.content ?? '').length
          }
        }
      } catch { /* ignore */ }

      pages.push({ id: page.id, title, channel, savedAt, bodyLength })
    }

    cursor = res.has_more ? (res.next_cursor ?? undefined) : undefined
  } while (cursor)

  return pages
}

// ─────────────────────────────────────────────
// メイン処理
// ─────────────────────────────────────────────
async function main() {
  console.log(`=== Notion 重複削除 ${DRY_RUN ? '[DRY RUN]' : ''} ===\n`)

  console.log('全ページを取得中（ブロック文字数も確認するため時間がかかります）...')
  const pages = await fetchAllPages()
  console.log(`${pages.length} ページ取得完了\n`)

  // 重複グループを作成
  // キー: channel + savedAt + title先頭30文字
  const groups = new Map<string, PageInfo[]>()
  for (const page of pages) {
    const key = [
      page.channel,
      page.savedAt,
      page.title.slice(0, 30),
    ].join('|')
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key)!.push(page)
  }

  // 重複グループのみ抽出
  const dupGroups = Array.from(groups.values()).filter(g => g.length > 1)
  console.log(`重複グループ: ${dupGroups.length} 件 / 重複ページ合計: ${dupGroups.reduce((s, g) => s + g.length - 1, 0)} 件\n`)

  if (dupGroups.length === 0) {
    console.log('重複なし。終了します。')
    return
  }

  let countDeleted = 0
  let countError   = 0

  for (const group of dupGroups) {
    // bodyLength が最大のページを残す（エンリッチ済み優先）
    group.sort((a: PageInfo, b: PageInfo) => b.bodyLength - a.bodyLength)
    const keep    = group[0]
    const deletes = group.slice(1)

    console.log(`\n[保持] ${keep.title.slice(0, 40)} (${keep.channel} / ${keep.savedAt}) bodyLen=${keep.bodyLength}`)
    for (const del of deletes) {
      console.log(`  [削除] ${del.id} bodyLen=${del.bodyLength}`)

      if (DRY_RUN) {
        countDeleted++
        continue
      }

      try {
        // Notion アーカイブ
        await sleep(NOTION_DELAY)
        await notion.pages.update({ page_id: del.id, archived: true })

        // Upstash Vector 削除
        await vector.delete(del.id)

        countDeleted++
      } catch (err) {
        console.error(`  [ERROR] ${del.id}:`, err)
        countError++
      }
    }
  }

  console.log('\n=== 完了 ===')
  console.log(`削除${DRY_RUN ? '予定' : '済み'}: ${countDeleted} 件 / エラー: ${countError} 件`)
  if (DRY_RUN) {
    console.log('\n※ --dry-run のため実際には削除していません。')
    console.log('   削除を実行するには: npx tsx scripts/dedup-notion.ts')
  }
}

main().catch(err => {
  console.error('致命的エラー:', err)
  process.exit(1)
})
