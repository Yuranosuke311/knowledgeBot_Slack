export const dynamic = 'force-dynamic'

import { NextRequest, NextResponse } from 'next/server'
import { createKnowledgePage } from '@/lib/notion'
import { embedText } from '@/lib/gemini'
import { upsertVector } from '@/lib/vector'
import { isDuplicate, markAsProcessed } from '@/lib/redis'

// OAuth2 リフレッシュトークンからアクセストークンを取得する
async function getGoogleAccessToken(): Promise<string> {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: process.env.GOOGLE_OAUTH_CLIENT_ID!,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET!,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN!,
    }),
  })
  const data = await res.json() as { access_token: string }
  return data.access_token
}

// Google Drive から Googleドキュメントをプレーンテキストとして取得する
async function fetchDriveDocText(fileId: string, accessToken: string): Promise<string> {
  const res = await fetch(
    `https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=text/plain`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  )
  if (!res.ok) throw new Error(`Drive API error: ${res.status} ${res.statusText}`)
  return res.text()
}

export async function POST(req: NextRequest): Promise<NextResponse> {
  // n8n からの簡易認証
  const secret = req.headers.get('x-api-secret')
  if (secret !== process.env.MEET_API_SECRET) {
    return NextResponse.json({ error: 'unauthorized' }, { status: 401 })
  }

  const { driveFileId, fileName, meetingDate } = await req.json() as {
    driveFileId: string
    fileName: string
    meetingDate?: string
  }

  if (!driveFileId || !fileName) {
    return NextResponse.json({ error: 'driveFileId and fileName are required' }, { status: 400 })
  }

  // 重複チェック（driveFileId をキーに使用）
  const redisKey = `meet:${driveFileId}`
  if (await isDuplicate(redisKey)) {
    return NextResponse.json({ ok: true, skipped: true })
  }

  // Google Drive からドキュメントのテキストを取得
  const accessToken = await getGoogleAccessToken()
  const fullText = await fetchDriveDocText(driveFileId, accessToken)

  const title = fileName.replace(/\.[^/.]+$/, '').slice(0, 80)
  const savedAt = meetingDate ?? new Date().toISOString()

  // Notion 保存
  const notionResult = await createKnowledgePage({
    title,
    category: '議事録',
    postedBy: 'Meet録画',
    slackChannel: 'AilaB_議事録',
    savedAt,
    fullText,
  })
  await markAsProcessed(redisKey)

  // Vector 保存
  try {
    const vector = await embedText(fullText)
    await upsertVector(notionResult.id, vector, {
      title,
      category: '議事録',
      channel: 'AilaB_議事録',
      savedAt,
      notionUrl: notionResult.url,
      fullText: fullText.slice(0, 1000),
    })
  } catch (err) {
    console.error('[Vector] upsert error:', err)
  }

  return NextResponse.json({ ok: true, notionUrl: notionResult.url })
}
