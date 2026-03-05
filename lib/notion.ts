import { Client } from '@notionhq/client'
import type { KnowledgeData } from '@/types/knowledge'

const notion = new Client({ auth: process.env.NOTION_TOKEN })
const DATABASE_ID = process.env.NOTION_DATABASE_ID!

export async function createKnowledgePage(
  data: KnowledgeData
): Promise<{ id: string; url: string }> {
  const response = await notion.pages.create({
    parent: { database_id: DATABASE_ID },
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
    children: [
      {
        object: 'block',
        type: 'paragraph',
        paragraph: {
          rich_text: [{ text: { content: data.fullText } }],
        },
      },
    ],
  })

  return {
    id: response.id,
    url: (response as { url: string }).url,
  }
}
