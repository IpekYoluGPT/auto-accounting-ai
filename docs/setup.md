# Setup Guide

## Prerequisites

- Python 3.11+
- A Meta Developer App with WhatsApp Cloud API access
- or a Periskope account with a connected WhatsApp number
- A Google Cloud project with Gemini API enabled

## Local Development

### 1. Clone the repository

```bash
git clone https://github.com/IpekYoluGPT/auto-accounting-ai.git
cd auto-accounting-ai
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate.bat     # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 5. Run the server

```bash
uvicorn app.main:app --reload --port 8000
```

### 6. Expose locally (for webhook testing)

Use [ngrok](https://ngrok.com/) or the [Cloudflare tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/) to expose your local server:

```bash
ngrok http 8000
```

Copy the HTTPS URL and register it in the provider you use:

- Meta Cloud API webhook: `https://<your-ngrok-id>.ngrok.io/webhook`
- Periskope webhook: `https://<your-ngrok-id>.ngrok.io/integrations/periskope/webhook`

## Running Tests

```bash
pytest tests/ -v
```

## Railway Deployment

### 1. Push to GitHub

Ensure all changes are committed and pushed.

### 2. Create a Railway project

- Visit [railway.app](https://railway.app) and create a new project from your GitHub repo.

### 3. Set environment variables in Railway

In your service's **Variables** tab, add all variables from `.env.example`:

| Variable | Description |
|---|---|
| `PORT` | Railway sets this automatically |
| `WHATSAPP_VERIFY_TOKEN` | Your chosen verification token |
| `WHATSAPP_ACCESS_TOKEN` | Meta permanent access token |
| `WHATSAPP_PHONE_NUMBER_ID` | Meta phone number ID |
| `WHATSAPP_GROUPS_ONLY` | Keep direct 1:1 intake disabled and only process official group chats |
| `PERISKOPE_API_KEY` | Periskope API key for outbound replies and notes |
| `PERISKOPE_PHONE` | Phone number or `phone-xxxx` value used as the `x-phone` header |
| `PERISKOPE_API_BASE_URL` | Defaults to `https://api.periskope.app/v1` |
| `PERISKOPE_MEDIA_BASE_URL` | Defaults to `https://api.periskope.app` |
| `PERISKOPE_SIGNING_KEY` | HMAC signing key from Periskope Webhooks settings |
| `PERISKOPE_TOOL_TOKEN` | Shared secret for custom tool endpoints |
| `GEMINI_API_KEY` | Google Gemini API key |
| `GEMINI_CLASSIFIER_MODEL` | Defaults to `gemini-flash-lite-latest` |
| `GEMINI_EXTRACTOR_MODEL` | Defaults to `gemini-flash-lite-latest` |
| `STORAGE_DIR` | `/app/storage` (Railway persistent volume) |
| `LOG_LEVEL` | `INFO` |

### 4. Start command

Railway auto-detects Python. Add a `Procfile` or use the start command:

```
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

### 5. Register the Railway URL as your WhatsApp webhook

In the Meta Developer Portal → WhatsApp → Configuration:
- Webhook URL: `https://your-app.railway.app/webhook`
- Verify token: value of `WHATSAPP_VERIFY_TOKEN`
- Subscribe to: `messages`

## WhatsApp Webhook Setup

1. Create a Meta App at [developers.facebook.com](https://developers.facebook.com).
2. Add the WhatsApp product.
3. Generate a permanent access token.
4. Note your Phone Number ID.
5. Set the webhook URL and verify token.
6. Subscribe to the `messages` webhook field.

## Periskope Webhook Setup

1. Go to `Periskope > Settings > Webhooks`.
2. Add a webhook pointing to `https://your-app.railway.app/integrations/periskope/webhook`.
3. Generate and copy the signing key into `PERISKOPE_SIGNING_KEY`.
4. Subscribe to `message.created`.
5. Set `PERISKOPE_API_KEY` and `PERISKOPE_PHONE` so the backend can send replies and private notes back through Periskope.

## Periskope Custom Tools

Create these tools in `AI Agent > Built-in and Custom Tools`:

- `create_accounting_record`
  - `POST https://your-app.railway.app/integrations/periskope/tools/create_accounting_record`
- `get_submission_status`
  - `POST https://your-app.railway.app/integrations/periskope/tools/get_submission_status`
- `assign_to_human`
  - `POST https://your-app.railway.app/integrations/periskope/tools/assign_to_human`

Use Periskope's Bearer Token auth mode with `PERISKOPE_TOOL_TOKEN` and pass `chat_id` from the built-in context when needed.

## Official Group Onboarding

Once the API is running, you can create and manage official WhatsApp groups through the backend:

- `POST /groups/onboard`
- `GET /groups`
- `GET /groups/{group_id}`
- `GET /groups/{group_id}/invite-link`
- `POST /groups/{group_id}/invite-link/reset`
- `GET /groups/{group_id}/join-requests`
- `POST /groups/{group_id}/join-requests/approve`

This project assumes official API-managed groups, not arbitrary existing WhatsApp groups created in the consumer app.
