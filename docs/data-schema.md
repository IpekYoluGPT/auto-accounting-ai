# Data Schema

## Internal Model: `BillRecord`

All extracted invoice/receipt data is stored in a `BillRecord` Pydantic model.

| Internal Field | Type | Description |
|---|---|---|
| `company_name` | `str \| null` | Business or vendor name |
| `tax_number` | `str \| null` | Turkish tax/VKN number |
| `tax_office` | `str \| null` | Turkish tax office name |
| `document_number` | `str \| null` | Generic document number |
| `invoice_number` | `str \| null` | Fatura number |
| `receipt_number` | `str \| null` | Fiş/makbuz number |
| `document_date` | `str \| null` | ISO 8601: `YYYY-MM-DD` |
| `document_time` | `str \| null` | 24-hour: `HH:MM` |
| `currency` | `TRY \| EUR \| USD \| null` | Currency code |
| `subtotal` | `float \| null` | Amount before VAT |
| `vat_rate` | `float \| null` | VAT rate (e.g. 18.0) |
| `vat_amount` | `float \| null` | VAT amount in currency |
| `total_amount` | `float \| null` | Total including VAT |
| `payment_method` | `str \| null` | Nakit / Kredi Kartı / Banka Transferi / Diğer |
| `expense_category` | `str \| null` | See category list below |
| `description` | `str \| null` | Summary of goods/services |
| `notes` | `str \| null` | Additional extracted text |
| `source_message_id` | `str \| null` | WhatsApp message ID |
| `source_filename` | `str \| null` | Original file name |
| `source_type` | `str \| null` | `image` or `document` |
| `confidence` | `float \| null` | AI confidence score (0–1) |

## Export: Turkish Column Names

| Internal Field | Turkish Column |
|---|---|
| `company_name` | Firma Adı |
| `tax_number` | Vergi Numarası |
| `tax_office` | Vergi Dairesi |
| `document_number` | Belge Numarası |
| `invoice_number` | Fatura Numarası |
| `receipt_number` | Fiş Numarası |
| `document_date` | Tarih |
| `document_time` | Saat |
| `currency` | Para Birimi |
| `subtotal` | Ara Toplam |
| `vat_rate` | KDV Oranı |
| `vat_amount` | KDV Tutarı |
| `total_amount` | Genel Toplam |
| `payment_method` | Ödeme Yöntemi |
| `expense_category` | Gider Kategorisi |
| `description` | Açıklama |
| `notes` | Notlar |
| `source_message_id` | Kaynak Mesaj ID |
| `source_filename` | Kaynak Dosya Adı |
| `source_type` | Kaynak Türü |
| `confidence` | Güven Skoru |

## Expense Categories (Turkish)

| Category | Description |
|---|---|
| Yemek | Food & beverages |
| Ulaşım | Transportation (fuel, taxi, transit) |
| Konaklama | Accommodation |
| Ofis | Office supplies & equipment |
| Yazılım | Software subscriptions |
| Donanım | Hardware purchases |
| Abonelik | Other subscriptions |
| Kargo | Shipping & courier |
| Vergi | Tax payments |
| Diğer | Other / uncategorised |

## Classification Model: `ClassificationResult`

| Field | Type | Description |
|---|---|---|
| `is_bill` | `bool` | Whether message is a financial document |
| `reason` | `str \| null` | One-sentence explanation |
| `confidence` | `float` | 0 to 1 confidence score |

## Normalisation Rules

- **Dates**: Gemini output may be `DD.MM.YYYY` or `DD/MM/YYYY`; normalised to `YYYY-MM-DD`.
- **Numbers**: Turkish decimal separator (comma) is converted to dot.
- **Currency**: Defaults to `TRY` if not found or unrecognised.
- **Null handling**: Missing Gemini fields become `null` internally and empty strings in CSV export.
- **Confidence**: Clamped to `[0.0, 1.0]`.
