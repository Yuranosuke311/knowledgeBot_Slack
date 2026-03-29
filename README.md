# Slack → Notion ナレッジ収集ボット（RAG 検索付き）

Slack のリアクションをトリガーに、メッセージとスレッドを Notion データベースへ自動保存するボット。
保存したナレッジは `@knowledgeBot` へのメンションで AI 検索できます（RAG）。

## 機能

### フェーズ1：ナレッジ収集

| リアクション | カテゴリ | 動作 |
|-------------|---------|------|
| 📌 `:pushpin:` | 一般 | Notion に保存 → スレッドに完了通知 |
| 💡 `:bulb:` | アイデア | Notion に保存 → スレッドに完了通知 |
| ✅ `:white_check_mark:` | 決定事項 | Notion に保存 → スレッドに完了通知 |

保存完了後、ボットがスレッドへ返信します：

```
✅ Notion に保存しました
カテゴリ: 💡 アイデア　｜　ページを開く
```

### フェーズ2：AI 検索（RAG）

`@knowledgeBot` にメンションするとナレッジを検索して回答します：

```
@knowledgeBot 先月の開発方針はどうなりましたか？
```

- 保存済みナレッジから関連情報を類似検索（ベクター検索）
- Gemini AI が内容を要約して日本語で回答
- 回答末尾に参照したナレッジの出典リンクを表示

### フェーズ3：コンテンツ拡張抽出

メッセージに含まれる外部コンテンツを自動取得し、Notion への保存内容とベクトル検索精度を向上させる。

| 対象 | 処理方法 | 文字数上限 |
|------|----------|-----------|
| URL（最大3件） | fetch + cheerio で HTML 本文抽出（タイムアウト 8秒） | 3,000文字/件 |
| PDF 添付 | Gemini Vision で解析 | 5,000文字 |
| 画像添付 | Gemini Vision で説明テキスト化 | 2,000文字 |
| X (Twitter) URL | **現在除外**（認証必須・ボット検知リスクのため） | — |

抽出テキストはメッセージ本文に追記され、Notion ページおよびベクトル DB に保存されます。
X については将来的に Playwright を使ったローカル専用スクリプトで対応予定。

## RAG の仕組み

### ナレッジ蓄積フロー（リアクション時）

```
リアクション絵文字
  → スレッド全文を取得
  → Gemini Embedding API でベクトル化（1536次元の数値配列）
  → Upstash Vector に保存（ベクトル＋メタデータ）
  → Notion にも同時保存
```

テキストの「意味」を数値の配列（ベクトル）に変換して保存します。単語の一致ではなく意味的な近さで検索できるのがポイントです。

### 検索・回答フロー（@メンション時）

```
@knowledgeBot への質問
  → 質問文を同じく Gemini Embedding でベクトル化
  → Upstash Vector で類似度の高いナレッジを上位5件取得
  → 取得したナレッジ全文 + 質問を Gemini Flash に渡す
  → Gemini が内容を要約して日本語で回答
  → Slack に回答＋出典リンクを返信
```

質問文と意味的に近いナレッジだけを絞り込んでから AI に渡すことで、ナレッジ量が増えても精度を保ちます（ナレッジ全件を AI に渡すのではなく、関連するものだけに絞るのが RAG の核心です）。

---

## 技術スタック

- **Next.js 14** (App Router) / Vercel
- **@slack/web-api** — イベント受信・スレッド取得・返信
- **@notionhq/client** — Notion DB への書き込み
- **@upstash/redis** — 重複保存防止
- **@upstash/vector** — ベクター検索インデックス（Dense / 1536次元 / Cosine）
- **@google/generative-ai** — Embedding 生成（`gemini-embedding-001`）・回答生成 / PDF・画像解析（`gemini-2.5-flash`）
- **cheerio** — URL コンテンツの HTML 解析（フェーズ3）

---

## セットアップ

### 1. 依存パッケージのインストール

```bash
npm install
```

### 2. Notion の設定

**2-1. データベースを作成**

Notion で新規ページを作成し、フルページデータベースを挿入。以下のプロパティを追加する。

| プロパティ名 | 種別 | 備考 |
|------------|------|------|
| `Title` | タイトル | デフォルトの「名前」を `Title` に改名すること |
| `Category` | セレクト | |
| `PostedBy` | テキスト | |
| `SlackChannel` | テキスト | |
| `SavedAt` | 日付 | |

> ⚠️ プロパティ名は大文字・小文字を含め完全一致が必要です。

**2-2. インテグレーションを作成**

1. [notion.so/my-integrations](https://www.notion.so/my-integrations) で「新しいインテグレーション」を作成
2. 種類は **「内部インテグレーション」** を選択（会社名・URLなど不要）
3. 発行されたトークン（`secret_...`）をコピー

**2-3. データベースにインテグレーションを接続**

データベースページを開き、右上「**...**」→「**コネクト先**」→ 作成したインテグレーションを選択。

> ⚠️ この接続をしないと API からアクセスできません（`object_not_found` エラー）。

**2-4. データベース ID を取得**

データベースページの URL から 32 文字の ID を取得：
```
https://www.notion.so/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?v=...
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ← これが NOTION_DATABASE_ID
```

### 3. Slack App の設定

1. [api.slack.com/apps](https://api.slack.com/apps) で「Create New App」→「From scratch」
2. **OAuth & Permissions** → Bot Token Scopes に以下を追加：

| スコープ | 用途 |
|---------|------|
| `reactions:read` | リアクションイベントの受信 |
| `channels:history` | パブリックチャンネルのスレッド取得 |
| `groups:history` | プライベートチャンネルのスレッド取得 |
| `chat:write` | 保存完了通知の送信 |
| `users:read` | 投稿者の表示名取得 |

3. 「Install to Workspace」でインストール → `xoxb-...` トークンをコピー
4. **Basic Information** → Signing Secret をコピー

### 4. Upstash Redis の設定

[upstash.com](https://upstash.com) で Redis データベースを作成し、REST URL と REST Token をコピー。既存のデータベースがあればそのまま流用可（キーに `knowledge:` プレフィックスを使用するため競合しない）。

### 5. Upstash Vector の設定（フェーズ2）

[console.upstash.com](https://console.upstash.com) で Vector インデックスを作成：

| 設定項目 | 値 |
|---------|-----|
| Type | **Dense** |
| Dimensions | **1536** |
| Distance Metric | **Cosine** |

作成後、REST URL と REST Token をコピー。

### 6. Gemini API キーの取得（フェーズ2）

[aistudio.google.com](https://aistudio.google.com) で API キーを作成し、コピー。

### 7. Slack App に `app_mentions:read` スコープを追加（フェーズ2）

**OAuth & Permissions** → Bot Token Scopes に追加：

| スコープ | 用途 |
|---------|------|
| `app_mentions:read` | @メンションイベントの受信 |

追加後、**Event Subscriptions → Subscribe to bot events** に `app_mention` を追加。

### 8. 環境変数の設定

```bash
cp .env.local.example .env.local
```

`.env.local` を編集して各値を入力：

```
SLACK_BOT_TOKEN=xoxb-YOUR_BOT_TOKEN
SLACK_SIGNING_SECRET=YOUR_SIGNING_SECRET
UPSTASH_REDIS_REST_URL=https://YOUR_REDIS_HOST.upstash.io
UPSTASH_REDIS_REST_TOKEN=YOUR_REDIS_TOKEN
NOTION_TOKEN=secret_YOUR_NOTION_TOKEN
NOTION_DATABASE_ID=YOUR_DATABASE_ID（32文字）

# フェーズ2：RAG
GEMINI_API_KEY=YOUR_GEMINI_API_KEY
UPSTASH_VECTOR_REST_URL=https://YOUR_VECTOR_HOST.upstash.io
UPSTASH_VECTOR_REST_TOKEN=YOUR_VECTOR_TOKEN

# バッチスクリプト用（非メンバーチャンネルアクセス・users:read スコープ必須）
SLACK_USER_TOKEN=xoxp-YOUR_USER_TOKEN
```

---

## ローカル開発

```bash
# 開発サーバー起動
npm run dev

# 別ターミナルで ngrok を起動
npx ngrok http 3000
```

Slack App の **Event Subscriptions** → Request URL に ngrok の URL を設定：
```
https://xxxx.ngrok-free.app/api/slack/events
```

> ⚠️ `/api/slack/events` まで含めること。ルートパスのみでは 405 エラーになります。

---

## Vercel デプロイ

1. GitHub にプッシュして Vercel に連携
2. Vercel ダッシュボード → **Settings → Environment Variables** に `.env.local` の全変数を追加
3. 環境変数追加後に **Redeploy** を実行
4. Slack App の Request URL を Vercel の本番 URL に更新：
   ```
   https://your-app.vercel.app/api/slack/events
   ```

---

## 対応している会話種別

| 会話種別 | 対応 |
|---------|------|
| パブリックチャンネル | ✅ |
| プライベートチャンネル | ✅ |
| グループ DM（3人以上） | ✅ |
| 人間同士の1対1 DM | ❌ |

> 1対1 DM は Slack の仕様上ボットが参加できないため、イベントが届きません。代わりに2人だけのプライベートチャンネルを作成してボットを招待してください。

---

## ボットをチャンネルに追加

リアクションを使うチャンネルで以下を実行：

```
/invite @ボット名
```

> ⚠️ ボットが参加していないチャンネルのリアクションはイベントが届きません。

---

## ディレクトリ構成

```
app/
  api/
    slack/events/route.ts   # Slack Events API 受信（メイン処理）
    slack/actions/route.ts  # [フェーズ4] Block Kit ボタン応答
    slack/collect/route.ts  # [フェーズ4] QStash 経由の一括収集処理
lib/
  slack.ts                  # 署名検証・スレッド取得・返信送信・添付ファイル取得
  notion.ts                 # Notion ページ作成（1,900文字チャンク分割）
  redis.ts                  # 重複防止・収集フラグ管理（Upstash Redis）
  gemini.ts                 # Embedding 生成・RAG 回答生成（Gemini API）
  vector.ts                 # ベクター登録・類似検索（Upstash Vector）
  extractor.ts              # [フェーズ3] URL・PDF・画像テキスト抽出
  collect.ts                # [フェーズ4] 一括収集時の単一メッセージ処理
types/
  knowledge.ts              # 型定義・カテゴリマッピング
scripts/
  batch-import.ts           # 過去メッセージ一括取り込み
  enrich-existing.ts        # 既存データへのフェーズ3遡及適用
  dedup-notion.ts           # Notion 重複ページ削除
  fix-posted-by.ts          # PostedBy=Unknown の修正
  clear-redis-keys.ts       # Redis キー確認・削除
.env.local.example          # 環境変数テンプレート
```

---

## 今後の拡張予定

### フェーズ5：自動収集モード（開発中）

ボットが参加しているチャンネルで、リアクション不要で全メッセージを自動保存する。

- チャンネル参加時に自動収集モードをON（確認不要）
- 既存チャンネルへの一括適用スクリプト（`scripts/enable-autocollect.ts`）
- 自動収集済みメッセージへのリアクション保存は重複防止のためスキップ

### フェーズ6：タスク管理機能（構想中）

フェーズ5の自動収集基盤を活かし、メッセージからタスクを自動検出・管理する。

| 機能 | 内容 |
|------|------|
| タスク登録 | 「〇〇までに〇〇をする」を検出 → Notion Tasks DB に登録 |
| タスク完了 | 「〇〇完了した」を検出 → 対応タスクを自動で完了状態に更新 |
| 期日管理 | 自然言語の日付（「来週月曜」「3/31」など）を解釈してNotionに保存 |
