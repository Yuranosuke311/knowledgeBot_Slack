import { GoogleGenerativeAI } from '@google/generative-ai'
import type { VectorSearchResult } from '@/types/knowledge'
import { CATEGORY_EMOJI } from '@/types/knowledge'

function getGenAI(): GoogleGenerativeAI {
  return new GoogleGenerativeAI(process.env.GEMINI_API_KEY!)
}

export async function embedText(text: string): Promise<number[]> {
  const model = getGenAI().getGenerativeModel({ model: 'gemini-embedding-001' })
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await model.embedContent({ content: { parts: [{ text }], role: 'user' }, outputDimensionality: 1536 } as any)
  return result.embedding.values
}

const SYSTEM_PROMPT = `あなたは社内ナレッジ検索アシスタントです。
以下のナレッジ一覧はSlackで重要とマークされ保存された社内情報です。
ユーザーの質問に対して、ナレッジの内容をそのまま使って直接的に日本語で回答してください。

【ルール】
- 「ナレッジによると〜」「記録によると〜」などの前置きは不要。質問に対して直接答えること
- ナレッジに書かれている具体的な情報（日付・名前・数値など）をそのまま回答に含めること
- ナレッジに記載のない情報は回答に含めないこと
- 推測・憶測は行わないこと
- 回答は簡潔に。冗長な説明は不要`

export async function generateAnswer(
  question: string,
  context: VectorSearchResult[]
): Promise<string> {
  const model = getGenAI().getGenerativeModel({
    model: 'gemini-2.5-flash',
    systemInstruction: SYSTEM_PROMPT,
  })

  const knowledgeList = context
    .map((r) => {
      const m = r.metadata
      const savedDate = m.savedAt.slice(0, 10)
      return `---\nタイトル: ${m.title}\nカテゴリ: ${CATEGORY_EMOJI[m.category]} ${m.category}\nチャンネル: ${m.channel}\n保存日: ${savedDate}\n内容:\n${m.fullText}`
    })
    .join('\n')

  const prompt = `【ナレッジ一覧】\n${knowledgeList}\n\n【質問】\n${question}`

  const result = await model.generateContent(prompt)
  return result.response.text()
}
