# auto-accounting-ai

WhatsApp üzerinden gelen muhasebe belgelerini dayanıklı şekilde kuyruğa alıp Gemini ile işleyen, CSV + canonical store + Google Sheets projection katmanlarına yazan backend.

## Güncel Sistem Özeti

Bugünkü üretim akışı doğrudan "webhook geldi, satır yazıldı" modeli değil.

1. Meta Cloud API veya Periskope webhook'u mesajı alır.
2. Görsel/PDF önce disk destekli dayanıklı inbound queue'ya alınır; webhook hızlı şekilde `200 OK` dönebilir.
3. Worker medya dosyasını indirir ve Gemini tabanlı sınıflandırma / extraction çalıştırır.
4. `record_store` günlük CSV'ye yalnızca bir kez yazar; hem `source_message_id` hem de içerik fingerprint'leri ile tekrar girişleri ezer.
5. Aynı kayıtlar canonical SQLite store'a upsert edilir.
6. Google Sheets worker'ı canonical store'dan görünür sekmeleri yeniden projection ederek yazar.
7. Orijinal belge dosyası Google Drive'a aynı anda gitmek zorunda değildir; ayrı worker daha sonra yükler ve `Belge` linkini backfill eder.
8. Kullanıcı geri bildirimi kademelidir: ilk anda `⌛`, kuyruk uzarsa gecikme notu, görünür satırlar bekliyorsa `📝`, visible projection tamamlanınca `✅`, terminal hatada `⚠️`.

## Mimari Özeti

```text
Meta / Periskope webhook
          |
          v
  durable inbound queue
          |
          v
Gemini classify + extract
          |
          v
 CSV dedupe gate (record_store)
          |
          +----> daily CSV export
          |
          v
 canonical SQLite store
          |
          v
 Google Sheets projection worker
          |
          +----> visible tabs refreshed
          |
          v
 delayed Drive upload + link backfill
          |
          v
 staged WhatsApp feedback
```

## Görünür Çıktı Katmanları

| Katman | Amaç |
| --- | --- |
| `storage/exports/records_YYYY-MM-DD.csv` | Append-only günlük export |
| `storage/state/canonical_store.sqlite3` | Canonical belge kayıtları ve pending projection durumu |
| Google Sheets | Operatörlerin gördüğü aylık workbook projection'u |
| Google Drive | Orijinal belge dosyası ve sonradan eklenen `Belge` linki |

Sheets tarafında görünür sekmeler şu anda şunlardır:

| Belge kategorisi | Görünür sekme |
| --- | --- |
| `fatura`, `belirsiz`, `iade` | `Faturalar` |
| `odeme_dekontu`, `cek` | `Banka Ödemeleri` |
| `harcama_fisi`, `elden_odeme` | `Masraf Kayıtları` |
| `malzeme` | `Sevk Fişleri` |

Sistem ayrıca `__Raw Belgeler`, `__Ödeme_Dağıtımları`, `__Fatura Kalemleri`, `__Çek_Dekont_Detay` ve `__Cari_Kartlar` gibi teknik sekmeleri de kullanır.

## Google Document AI / OCR Notu

Google Document AI ve OCR yardımcıları projede halen mevcut. OCR hazırlama, parse etme ve fallback altyapısı korunuyor; ancak bugünkü sıcak intake yolunda belge triage ve structured extraction aslen Gemini-first çalışıyor. Başka bir ifadeyle OCR altyapısı destekleyici durumda, ana ingestion bottleneck'i değil.

## Hızlı Başlangıç

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --port 8000
```

Kurulum ayrıntıları ve eksik Google env değişkenleri için:

- [docs/setup.md](docs/setup.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/runtime-pipeline.md](docs/runtime-pipeline.md)

## API Yüzeyi

Birincil ingress ve operasyon endpoint'leri:

- `POST /webhook`
- `POST /integrations/periskope/webhook`
- `GET /health`
- `GET /export.csv`
- `GET /export.xlsx`
- `GET /setup/google-auth`
- `GET /setup/google-auth/callback`

Periskope tool endpoint'leri:

- `POST /integrations/periskope/tools/create_accounting_record`
- `POST /integrations/periskope/tools/get_submission_status`
- `POST /integrations/periskope/tools/assign_to_human`

## Notlar

- Dayanıklı kuyruk, canonical store ve Drive backfill `STORAGE_DIR` altında tutulur; production ortamında kalıcı disk zorunludur.
- Tek bir görüntüde birden fazla belge varsa extractor bunları `wamid__doc1`, `wamid__doc2` benzeri ayrı canonical belge kimliklerine ayırabilir.
- Şirket yöneticisinden gelen uygun text mesajları ayrı bir `elden_odeme` kısa yolundan geçebilir.

## Lisans

[MIT](LICENSE)
