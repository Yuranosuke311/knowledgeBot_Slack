/**
 * フェーズ3: URL・PDF・画像からテキストを抽出するユーティリティ
 *
 * 処理優先順: PDF → URL → 画像
 * 抽出テキストは fullText に追記され、そのままベクトル化・Notion保存される
 */

import * as cheerio from 'cheerio'
import { GoogleGenerativeAI } from '@google/generative-ai'

// Slack メッセージ内のファイル情報
export interface SlackFile {
  urlPrivateDownload: string
  mimetype: string
  name: string
}

// ─────────────────────────────────────────────
// URL 抽出
// ─────────────────────────────────────────────

/**
 * Slack の生テキスト（cleanText 前）から URL を抽出する
 * 対象: <https://...> および <https://...|text> 形式
 */
export function extractRawUrls(slackRawText: string): string[] {
  const regex = /<(https?:\/\/[^|>]+)(?:\|[^>]*)?>/g
  const urls: string[] = []
  let match
  while ((match = regex.exec(slackRawText)) !== null) {
    urls.push(match[1])
  }
  // 取得不可・不要な URL は除外
  const SKIP_DOMAINS = [
    'app.slack.com', 'files.slack.com',   // Slack内部
    'twitter.com', 'x.com', 't.co',       // X(Twitter): 認証必須
  ]
  return urls.filter((u) => !SKIP_DOMAINS.some((d) => u.includes(d)))
}

/**
 * URL のページ内容をテキスト取得（HTML → 本文のみ、最大 3000 文字）
 * HTML 以外（PDF, 動画等）は空文字を返す
 */
export async function fetchUrlText(url: string): Promise<string> {
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; KnowledgeBot/1.0)' },
      signal: AbortSignal.timeout(8000),
    })
    if (!res.ok) return ''
    const contentType = res.headers.get('content-type') ?? ''
    if (!contentType.includes('text/html')) return ''
    const html = await res.text()
    const $ = cheerio.load(html)
    $('script, style, nav, header, footer, aside, [role="navigation"]').remove()
    const text = $('body').text().replace(/\s+/g, ' ').trim()
    return text.slice(0, 3000)
  } catch {
    return ''
  }
}

// ─────────────────────────────────────────────
// PDF 抽出
// ─────────────────────────────────────────────

/**
 * PDF バッファを Gemini に送信してテキスト化（最大 5000 文字）
 * pdf-parse の代わりに Gemini Vision を使用（Vercel 対応・ネイティブ依存なし）
 */
export async function extractPdfText(buffer: Buffer): Promise<string> {
  try {
    const genai = new GoogleGenerativeAI(process.env.GEMINI_API_KEY!)
    const model = genai.getGenerativeModel({ model: 'gemini-2.5-flash' })
    const result = await model.generateContent([
      { inlineData: { data: buffer.toString('base64'), mimeType: 'application/pdf' } },
      'このPDFの内容を日本語でテキストとして抽出・要約してください。',
    ])
    return result.response.text().slice(0, 5000)
  } catch {
    return ''
  }
}

// ─────────────────────────────────────────────
// 画像抽出（Gemini Vision）
// ─────────────────────────────────────────────

/**
 * 画像バッファを Gemini Vision で解析してテキスト化（最大 2000 文字）
 */
export async function extractImageText(
  buffer: Buffer,
  mimeType: string
): Promise<string> {
  try {
    const genai = new GoogleGenerativeAI(process.env.GEMINI_API_KEY!)
    const model = genai.getGenerativeModel({ model: 'gemini-2.5-flash' })
    const result = await model.generateContent([
      { inlineData: { data: buffer.toString('base64'), mimeType } },
      '画像の内容を日本語で詳しく説明・テキスト化してください。図表や文字が含まれる場合はその内容も記述してください。',
    ])
    return result.response.text().slice(0, 2000)
  } catch {
    return ''
  }
}

// ─────────────────────────────────────────────
// Slack 添付ファイルのダウンロード & 抽出
// ─────────────────────────────────────────────

/**
 * Slack の private URL からファイルをダウンロードして、種別に応じてテキスト化する
 * - application/pdf → Gemini Vision（PDF ネイティブ対応）
 * - image/*         → Gemini Vision
 * - その他          → 空文字
 */
export async function extractSlackFileContent(
  urlPrivateDownload: string,
  mimeType: string,
  token: string
): Promise<string> {
  try {
    const res = await fetch(urlPrivateDownload, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(30000),
    })
    if (!res.ok) return ''
    const buffer = Buffer.from(await res.arrayBuffer())

    if (mimeType === 'application/pdf') {
      return extractPdfText(buffer)
    }
    if (mimeType.startsWith('image/')) {
      return extractImageText(buffer, mimeType)
    }
    return ''
  } catch {
    return ''
  }
}

// ─────────────────────────────────────────────
// メイン: fullText を URL・添付ファイルで拡張する
// ─────────────────────────────────────────────

/**
 * メッセージ本文（raw Slack テキスト）と添付ファイル一覧を受け取り、
 * URL / PDF / 画像の内容を取得して追記した拡張テキストを返す。
 *
 * @param rawSlackText cleanText 前の Slack 生テキスト（URL 抽出に使用）
 * @param files        Slack 添付ファイル一覧
 * @param token        Slack トークン（ファイルダウンロード用）
 * @returns 追記テキスト（元の fullText への追記分のみ。空の場合は ''）
 */
export async function buildEnrichment(
  rawSlackText: string,
  files: SlackFile[],
  token: string
): Promise<string> {
  const parts: string[] = []

  // 1. PDF 添付ファイル（優先度高）
  for (const file of files) {
    if (file.mimetype !== 'application/pdf') continue
    const text = await extractSlackFileContent(file.urlPrivateDownload, file.mimetype, token)
    if (text) parts.push(`\n[添付PDF: ${file.name}]\n${text}`)
  }

  // 2. URL コンテンツ
  const urls = extractRawUrls(rawSlackText)
  for (const url of urls.slice(0, 3)) {  // 最大3URL
    const text = await fetchUrlText(url)
    if (text) parts.push(`\n[URL: ${url}]\n${text}`)
  }

  // 3. 画像添付ファイル（優先度低）
  for (const file of files) {
    if (!file.mimetype.startsWith('image/')) continue
    const text = await extractSlackFileContent(file.urlPrivateDownload, file.mimetype, token)
    if (text) parts.push(`\n[添付画像: ${file.name}]\n${text}`)
  }

  return parts.join('\n')
}
