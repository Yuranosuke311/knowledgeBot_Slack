/**
 * フェーズ5: 既存チャンネルに自動収集モードを一括で有効化するスクリプト
 *
 * ボットがすでに参加しているチャンネルすべてに `autocollect:{channelId}` キーをセットする。
 * フェーズ5デプロイ後に一度だけ実行すること。
 *
 * 使用例:
 *   npx tsx scripts/enable-autocollect.ts           # 実際に有効化
 *   npx tsx scripts/enable-autocollect.ts --dry-run # 対象チャンネルの確認のみ
 */

import * as dotenv from 'dotenv'
import * as path from 'path'
import { WebClient } from '@slack/web-api'
import { Redis } from '@upstash/redis'

dotenv.config({ path: path.resolve(process.cwd(), '.env.local') })

const DRY_RUN = process.argv.includes('--dry-run')

const slack = new WebClient(process.env.SLACK_BOT_TOKEN!)
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
})

async function main() {
  console.log(`=== 自動収集モード 一括有効化 ${DRY_RUN ? '[DRY RUN]' : ''} ===\n`)

  // ボットが参加しているチャンネルを全件取得
  const channels: { id: string; name: string }[] = []
  let cursor: string | undefined = undefined

  do {
    const res = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
      limit: 200,
      ...(cursor ? { cursor } : {}),
    })

    for (const ch of res.channels ?? []) {
      if (ch.is_member && ch.id && ch.name) {
        channels.push({ id: ch.id, name: ch.name })
      }
    }

    cursor = res.response_metadata?.next_cursor ?? undefined
  } while (cursor)

  if (channels.length === 0) {
    console.log('ボットが参加しているチャンネルが見つかりませんでした。')
    return
  }

  console.log(`対象チャンネル: ${channels.length} 件`)
  channels.forEach((ch) => console.log(`  #${ch.name} (${ch.id})`))

  if (DRY_RUN) {
    console.log('\n--dry-run のため有効化しません。')
    return
  }

  console.log('\n有効化中...')
  let count = 0
  for (const ch of channels) {
    const key = `autocollect:${ch.id}`
    const exists = await redis.exists(key)
    if (exists) {
      console.log(`  スキップ（既設定）: #${ch.name}`)
      continue
    }
    await redis.set(key, '1')
    console.log(`  ✅ 有効化: #${ch.name}`)
    count++
  }

  console.log(`\n完了: ${count} チャンネルを有効化しました。`)
}

main().catch((err) => {
  console.error('エラー:', err)
  process.exit(1)
})
