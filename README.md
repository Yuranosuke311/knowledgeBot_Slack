# Slack → Notion ナレッジ収集ボット

Slack のリアクションをトリガーに、メッセージとスレッドを Notion データベースへ自動保存するボット。

## 機能

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

## 技術スタック

- **Next.js 14** (App Router) / Vercel
- **@slack/web-api** — イベント受信・スレッド取得・返信
- **@notionhq/client** — Notion DB への書き込み
- **@upstash/redis** — 重複保存防止

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

### 5. 環境変数の設定

```bash
cp .env.local.example .env.local
```

`.env.local` を編集して各値を入力：

```
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...
UPSTASH_REDIS_REST_URL=https://...upstash.io
UPSTASH_REDIS_REST_TOKEN=...
NOTION_TOKEN=secret_...
NOTION_DATABASE_ID=...（32文字）
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
2. Vercel ダッシュボード → **Settings → Environment Variables** に `.env.local` の6変数を全て追加
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
lib/
  slack.ts                  # 署名検証・スレッド取得・返信送信
  notion.ts                 # Notion ページ作成
  redis.ts                  # 重複防止（Upstash Redis）
types/
  knowledge.ts              # 型定義・カテゴリマッピング
.env.local.example          # 環境変数テンプレート
```
