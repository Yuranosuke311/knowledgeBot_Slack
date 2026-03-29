export type CategoryLabel = '一般' | 'アイデア' | '決定事項' | '議事録'

export const CATEGORY_EMOJI: Record<CategoryLabel, string> = {
  '一般': '📌',
  'アイデア': '💡',
  '決定事項': '✅',
  '議事録': '🎙️',
}

export interface SlackMessage {
  user: string
  text: string
  ts: string
}

export interface KnowledgeData {
  title: string
  category: CategoryLabel
  postedBy: string
  slackChannel: string
  savedAt: string
  fullText: string
}

export interface VectorMetadata {
  title: string
  category: CategoryLabel
  channel: string
  savedAt: string
  notionUrl: string
  fullText: string
}

export interface VectorSearchResult {
  score: number
  metadata: VectorMetadata
}
