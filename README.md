# FastAPI Data Processor

Coze-compatible data processing API for handling large Excel/CSV files.

## Quick Start

### Local Development
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### Docker
```bash
docker build -t data-processor:latest .
docker run -d -p 8000:8000 -e PROCESSOR_API_KEY="yourkey" --name data_proc data-processor:latest
```

### API Endpoints

#### Process Files
```http
POST /process-files
Content-Type: application/json
x-api-key: yourkey

{
  "video_excel_file": "https://example.com/video.csv",
  "live_bi_file": "https://example.com/live.csv",
  "Spending_file": "https://example.com/spending.xlsx",
  "spending_sheet_names": "sheet1,sheet2"
}
```

#### Download Results
```http
POST /process-and-download
Content-Type: application/json
x-api-key: yourkey

(same request body as above)
Returns: JSON file download
```

#### Health Check
```http
GET /health
```

## Environment Variables
- `PROCESSOR_API_KEY`: API authentication key
- `TMP_ROOT`: Temporary directory for file processing (default: /tmp/fastapi_data_proc)
- `LEVEL_NORMALIZE_BY_NSC`: Feature flag to enable the new level aggregation logic. Set to `true`, `1`, `yes`, or `on` to activate. (Default: `false`)

## Supported File Types
- CSV files
- Excel files (.xlsx, .xls)
- URLs or local file paths (file:// prefix)

## Memory Requirements
- 500MB total data → 4GB memory recommended
- 300MB single file → 2.5GB memory used
- Zeabur $5/month plan sufficient