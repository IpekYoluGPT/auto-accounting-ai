# Setup Guide

## Prerequisites

- Python 3.11+
- A Meta Developer App with WhatsApp Cloud API access
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

Copy the HTTPS URL and register it in the Meta Developer Portal as your webhook URL:
`https://<your-ngrok-id>.ngrok.io/webhook`

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
| `GEMINI_API_KEY` | Google Gemini API key |
| `GEMINI_CLASSIFIER_MODEL` | Defaults to `gemini-3-flash-preview` |
| `GEMINI_EXTRACTOR_MODEL` | Defaults to `gemini-3-flash-preview` |
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
