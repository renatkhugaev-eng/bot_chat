# 🚀 Vercel API для генерации сводок

Serverless API на Vercel для генерации сводок чата через Claude AI.

## Деплой на Vercel

### 1. Установи Vercel CLI (если нет)

```bash
npm install -g vercel
```

### 2. Залогинься

```bash
vercel login
```

### 3. Задеплой

```bash
cd vercel-api
vercel
```

При первом деплое ответь на вопросы:
- Set up and deploy? → Y
- Which scope? → Выбери свой аккаунт
- Link to existing project? → N
- Project name? → chat-summary-api (или любое)
- Directory? → ./
- Override settings? → N

### 4. Добавь Environment Variable

```bash
vercel env add ANTHROPIC_API_KEY
```

Введи свой API ключ от Anthropic (https://console.anthropic.com/)

### 5. Передеплой с переменными

```bash
vercel --prod
```

### 6. Получи URL

После деплоя получишь URL вида:
```
https://chat-summary-api.vercel.app
```

API endpoint будет:
```
https://chat-summary-api.vercel.app/api/generate-summary
```

## Использование

### POST /api/generate-summary

**Body (JSON):**
```json
{
  "statistics": {
    "total_messages": 156,
    "top_authors": [
      {"first_name": "Вася", "msg_count": 47},
      {"first_name": "Петя", "msg_count": 23}
    ],
    "message_types": {"text": 120, "sticker": 30, "photo": 6},
    "reply_pairs": [
      {"first_name": "Вася", "reply_to_first_name": "Маша", "replies": 15}
    ],
    "hourly_activity": {"14": 45, "15": 67, "16": 44},
    "recent_messages": [
      {"first_name": "Вася", "message_text": "Привет всем!"}
    ]
  },
  "chat_title": "Чат пацанов",
  "hours": 5
}
```

**Response:**
```json
{
  "summary": "📺 КРИМИНАЛЬНАЯ СВОДКА...",
  "tokens_used": 1234
}
```

## Стоимость

Claude 3.5 Sonnet:
- Input: $3 / 1M tokens
- Output: $15 / 1M tokens

Примерно **$0.01-0.03** за одну сводку.

## Настройка бота

После деплоя добавь URL в `.env` бота:

```env
VERCEL_API_URL=https://твой-проект.vercel.app/api/generate-summary
```
