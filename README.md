# auto-accounting-ai

> WhatsApp'tan gelen fatura, fis, cek ve dekont fotograflarini Gemini AI ile isleyip Google Sheets'e otomatik kaydeden muhasebe backend sistemi.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Ne Yapar?

1. WhatsApp grubundan gelen her mesaji Periskope webhook ile alir
2. Gemini AI ile finansal belge mi degil mi siniflandirir
3. Belge kategorisini belirler (fatura, dekont, fis, cek, malzeme, iade)
4. Yapilandirilmis muhasebe alanlarini cikarir (firma, tutar, KDV, tarih...)
5. **Aylik Google Sheets** tablosuna otomatik yazar
6. WhatsApp'a onay mesaji gonderir

### Coklu Belge Destegi

Tek bir fotografta birden fazla belge varsa (ornegin 3 cek yan yana), sistem her birini ayri ayri algilar ve kaydeder.

---

## Mimari

```
WhatsApp Grubu
      |  (fatura / fis / cek fotografi)
      v
Periskope Webhook  ────>  POST /integrations/periskope/webhook
                              |
                    +---------+-----------+
                    v                     v
              bill_classifier       doc_classifier
              (finansal mi?)        (hangi kategori?)
                    |                     |
                    v                     v
              gemini_extractor (coklu belge destekli)
                    |
                    +---> record_store (CSV + tekrar korumasi)
                    +---> google_sheets (aylik spreadsheet)
                    +---> WhatsApp onay mesaji
```

---

## Proje Yapisi

```
auto-accounting-ai/
├── app/
│   ├── main.py                        # FastAPI uygulama giris noktasi
│   ├── config.py                      # Ortam degiskenleri (Pydantic Settings)
│   ├── models/schemas.py              # Veri modelleri
│   ├── routes/
│   │   ├── periskope.py               # Periskope webhook + arac endpointleri
│   │   ├── webhooks.py                # Meta Cloud API webhook
│   │   ├── groups.py                  # Resmi WhatsApp grup yonetimi
│   │   └── setup.py                   # Google OAuth2 kurulum akisi
│   ├── services/
│   │   ├── gemini_client.py           # Gemini API istemcisi
│   │   ├── accounting/
│   │   │   ├── intake.py              # Merkezi mesaj isleme hatti
│   │   │   ├── bill_classifier.py     # Belge siniflandirma
│   │   │   ├── doc_classifier.py      # Kategori belirleme
│   │   │   ├── gemini_extractor.py    # Coklu belge cikarimi
│   │   │   ├── record_store.py        # CSV kayit + tekrar korumasi
│   │   │   └── exporter.py            # CSV/XLSX formatlama
│   │   └── providers/
│   │       ├── google_sheets.py       # Aylik tablo yonetimi + OAuth + retry
│   │       ├── periskope.py           # Periskope API istemcisi
│   │       └── whatsapp.py            # Meta Cloud API istemcisi
│   └── utils/
├── tests/                             # 88 test
├── docs/
├── .env.example
├── requirements.txt
└── railway.toml
```

---

## Hizli Baslangic

### 1. Klonla ve kur

```bash
git clone https://github.com/IpekYoluGPT/auto-accounting-ai.git
cd auto-accounting-ai
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Yapilandir

```bash
cp .env.example .env
# Kimlik bilgilerini doldurun
```

### 3. Calistir

```bash
uvicorn app.main:app --reload --port 8000
```

---

## Ortam Degiskenleri

### Zorunlu

| Degisken | Aciklama |
|----------|----------|
| `GEMINI_API_KEY` | Gemini AI API anahtari |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Base64 kodlanmis servis hesabi JSON |
| `PERISKOPE_API_KEY` | Periskope API anahtari (giden mesajlar icin) |
| `PERISKOPE_PHONE` | Periskope telefon numarasi / phone_id |

### Google Sheets (Aylik Otomatik Olusturma)

| Degisken | Aciklama |
|----------|----------|
| `GOOGLE_OAUTH_CLIENT_ID` | OAuth2 istemci ID |
| `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth2 istemci sifresi |
| `GOOGLE_OAUTH_REFRESH_TOKEN` | OAuth2 yenileme tokeni (`/setup/google-auth` ile alinir) |
| `GOOGLE_DRIVE_PARENT_FOLDER_ID` | Aylik alt klasorlerin olusturulacagi ust klasor |
| `GOOGLE_SHEETS_OWNER_EMAIL` | Tablolarin paylasilacagi e-posta |

### Periskope

| Degisken | Aciklama | Varsayilan |
|----------|----------|------------|
| `PERISKOPE_API_BASE_URL` | Periskope API temel URL | `https://api.periskope.app/v1` |
| `PERISKOPE_MEDIA_BASE_URL` | Medya indirme temel URL | `https://api.periskope.app` |
| `PERISKOPE_SIGNING_KEY` | HMAC imza anahtari (onerilen) | *(bos)* |
| `PERISKOPE_TOOL_TOKEN` | Arac endpoint guvenlik tokeni | *(bos)* |
| `PERISKOPE_ALLOWED_CHAT_IDS` | Izin verilen sohbet ID'leri (virgul ile ayrilmis) | *(bos = tumunu reddet)* |

### Diger

| Degisken | Aciklama | Varsayilan |
|----------|----------|------------|
| `GEMINI_CLASSIFIER_MODEL` | Siniflandirma modeli | `gemini-2.5-flash` |
| `GEMINI_EXTRACTOR_MODEL` | Cikarim modeli | `gemini-2.5-flash` |
| `MANAGER_PHONE_NUMBER` | Elden odeme icin yonetici telefon numarasi | *(bos)* |
| `WHATSAPP_GROUPS_ONLY` | Sadece grup mesajlarini isle | `true` |
| `STORAGE_DIR` | Dosya depolama dizini | `./storage` |
| `LOG_LEVEL` | Log seviyesi | `INFO` |

---

## Google Sheets Kurulumu

### Adim 1: OAuth2 Kurulumu (Tek Seferlik)

1. Google Cloud Console'da OAuth2 istemcisi olusturun (Web application)
2. Redirect URI ekleyin: `https://<railway-url>/setup/google-auth/callback`
3. Railway'de `GOOGLE_OAUTH_CLIENT_ID` ve `GOOGLE_OAUTH_CLIENT_SECRET` ayarlayin
4. `https://<railway-url>/setup/google-auth` adresini ziyaret edin
5. Google izin ekranini onaylayin
6. Gosterilen refresh token'i `GOOGLE_OAUTH_REFRESH_TOKEN` olarak Railway'e ekleyin

### Adim 2: Servis Hesabi Paylasimi

Google Drive'daki muhasebe klasorunu servis hesabi e-postasi ile paylasin:
`whatsappsheet@whatsapp-account-manager-ai.iam.gserviceaccount.com`

### Otomatik Aylik Akis

Her ayin ilk belgesi geldiginde:
1. "Belgeler -- Nisan 2026" alt klasoru olusturulur
2. "Muhasebe -- Nisan 2026" spreadsheet'i olusturulur
3. Tum sekmeler bootstrap edilir (Faturalar, Dekontlar, Harcama Fisleri, Cekler, Elden Odemeler, Malzeme, Iadeler, Ozet)
4. Sonraki belgeler ayni spreadsheet'e eklenir

---

## Belge Kategorileri ve Sekmeler

| Kategori | Sekme | Aciklama |
|----------|-------|----------|
| fatura | Faturalar | Resmi KDV'li faturalar |
| odeme_dekontu | Dekontlar | Banka transferleri, EFT, FAST |
| harcama_fisi | Harcama Fisleri | POS fisleri, akaryakit, market |
| cek | Cekler | Banka cekleri |
| elden_odeme | Elden Odemeler | Nakit odemeler (yonetici mesaji) |
| malzeme | Malzeme | Irsaliye, malzeme teslim belgeleri |
| iade | Iadeler | Iade ve iptal belgeleri |

---

## API Endpointleri

### Saglik / Disari Aktarma

- `GET /health` - Canlilik kontrolu
- `GET /export.csv` - Gunun CSV disari aktarmasi
- `GET /export.xlsx` - Gunun XLSX disari aktarmasi

### Periskope (Birincil Yol)

- `POST /integrations/periskope/webhook` - Gelen mesajlar
- `POST /integrations/periskope/tools/create_accounting_record` - Dogrudan kayit olusturma
- `POST /integrations/periskope/tools/get_submission_status` - Kayit durumu sorgulama
- `POST /integrations/periskope/tools/assign_to_human` - Insana yonlendirme

### OAuth Kurulumu

- `GET /setup/google-auth` - OAuth2 akisi baslat
- `GET /setup/google-auth/callback` - OAuth2 geri donus

### Meta Cloud API (Ikincil)

- `GET /webhook` - Meta dogrulama
- `POST /webhook` - Gelen WhatsApp mesajlari

---

## Testler

```bash
python -m pytest tests/ -q           # Tam paket (88 test)
python -m pytest tests/ -x -q        # Ilk hatada dur
```

---

## Railway Deployment

**Baslangic komutu:**
```
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

GitHub `main` dalina push yapildiginda otomatik deploy olur.

---

## Lisans

[MIT](LICENSE)
