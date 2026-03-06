import { Index } from '@upstash/vector'
import type { VectorMetadata, VectorSearchResult } from '@/types/knowledge'

const SCORE_THRESHOLD = 0.75

let _vectorClient: Index | null = null
function getVectorClient(): Index {
  if (!_vectorClient) {
    _vectorClient = new Index({
      url: process.env.UPSTASH_VECTOR_REST_URL!,
      token: process.env.UPSTASH_VECTOR_REST_TOKEN!,
    })
  }
  return _vectorClient
}

export async function upsertVector(
  id: string,
  vector: number[],
  metadata: VectorMetadata
): Promise<void> {
  await getVectorClient().upsert({ id, vector, metadata: metadata as unknown as Record<string, unknown> })
}

export async function searchVector(
  vector: number[],
  topK: number
): Promise<VectorSearchResult[]> {
  const results = await getVectorClient().query({
    vector,
    topK,
    includeMetadata: true,
  })
  return results
    .filter((r) => r.score >= SCORE_THRESHOLD)
    .map((r) => ({ score: r.score, metadata: r.metadata as unknown as VectorMetadata }))
}
