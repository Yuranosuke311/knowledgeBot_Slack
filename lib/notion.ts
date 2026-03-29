import { Client } from '@notionhq/client'
import type { KnowledgeData } from '@/types/knowledge'

export async function createKnowledgePage(
  data: KnowledgeData
): Promise<{ id: string; url: string }> {
  const notion = new Client({ auth: process.env.NOTION_TOKEN })
  const response = await notion.pages.create({
    parent: { database_id: process.env.NOTION_DATABASE_ID! },
    properties: {
      Title: {
        title: [{ text: { content: data.title } }],
      },
      Category: {
        select: { name: data.category },
      },
      PostedBy: {
        rich_text: [{ text: { content: data.postedBy } }],
      },
      SlackChannel: {
        rich_text: [{ text: { content: data.slackChannel } }],
      },
      SavedAt: {
        date: { start: data.savedAt },
      },
    },
    children: (() => {
      const chunks: string[] = []
      for (let i = 0; i < data.fullText.length; i += 1900) {
        chunks.push(data.fullText.slice(i, i + 1900))
      }
      if (chunks.length === 0) chunks.push('')
      return chunks.map((chunk) => ({
        object: 'block' as const,
        type: 'paragraph' as const,
        paragraph: { rich_text: [{ text: { content: chunk } }] },
      }))
    })(),
  })

  return {
    id: response.id,
    url: (response as { url: string }).url,
  }
}

export async function appendToKnowledgePage(pageId: string, text: string): Promise<void> {
  const notion = new Client({ auth: process.env.NOTION_TOKEN })
  const chunks: string[] = []
  for (let i = 0; i < text.length; i += 1900) {
    chunks.push(text.slice(i, i + 1900))
  }
  if (chunks.length === 0) return
  await notion.blocks.children.append({
    block_id: pageId,
    children: chunks.map((chunk) => ({
      object: 'block' as const,
      type: 'paragraph' as const,
      paragraph: { rich_text: [{ text: { content: chunk } }] },
    })),
  })
}
