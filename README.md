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

## 数据契约核心逻辑

### DR (线索日志) 表处理逻辑

- **职责**: 将原始、逐条的线索日志，按 `(NSC_CODE, date)` 聚合为日度指标。
- **输入列**:
    - `reg_dealer`
    - `register_time`
    - `leads_type`
    - `mkt_second_channel_name`
    - `send2dealer_id`
- **输出列**:
    - `NSC_CODE` (String)
    - `date` (Date)
    - `natural_leads` (Float64)
    - `paid_leads` (Float64)
    - `local_leads` (Float64)
    - `cheyundian_paid_leads` (Float64)
    - `regional_paid_leads` (Float64)
- **聚合计算逻辑**:
    - `natural_leads`: `count(当 leads_type == '自然' 时)`
    - `paid_leads`: `count(当 leads_type == '广告' 时)`
    - `local_leads`: `count(当 reg_dealer == send2dealer_id 时)`
    - `cheyundian_paid_leads`: `count(当 leads_type == '广告' 且 mkt_second_channel_name 在 ['抖音车云店_BMW_本市_LS直发', '抖音车云店_LS直发'] 中时)`
    - `regional_paid_leads`: `count(当 leads_type == '广告' 且 mkt_second_channel_name == '抖音车云店_BMW_总部BDT_LS直发' 时)`

### spending (投放消耗) 表处理逻辑

- **职责**: 高效读取多个Sheet页，合并后按 `(NSC_CODE, date)` 聚合日度消耗。
- **前置步骤 (Processor)**:
    1. 从API参数 `spending_sheet_names` 获取Sheet名列表。
    2. 遍历列表，从Excel中**只选择** `['NSC CODE', 'Date', 'Spending(Net)']` 这三列读取。
    3. 将所有读取到的“小”DataFrame垂直合并为单一DataFrame。
- **Transform逻辑**:
    - **输入**: 已合并的、只包含三列的DataFrame。
    - **聚合计算**: 按 `(NSC_CODE, date)` 分组, `sum(Spending(Net))`。

### live (直播数据) 表处理逻辑

- **职责**: 将日度直播数据，按 `(NSC_CODE, date)` 聚合。
- **输入列**: `主机厂经销商id列表`, `开播日期`, `超25分钟直播时长(分)`, `直播有效时长（小时）`, `超25min直播总场次`, `曝光人数`, `场观`, `小风车点击次数（不含小雪花）`
- **输出列**: `NSC_CODE`, `date`, `over25_min_live_mins`, `live_effective_hours`, `effective_live_sessions`, `exposures`, `viewers`, `small_wheel_clicks`
- **Transform逻辑**: 按 `(NSC_CODE, date)` 分组, `sum()` 所有指标列。

### msg (私信数据) 表处理逻辑

- **职责**: 从Excel Sheet名中提取日期，合并后按 `(NSC_CODE, date)` 聚合私信数据。
- **前置步骤 (Processor)**:
    1. 读取所有Sheet页。
    2. 从每个Sheet名中解析日期，并作为新列 `日期` 添加到对应Sheet数据中。
    3. 将所有带日期的Sheet数据垂直合并为单一DataFrame。
- **Transform逻辑**:
    - **输入**: 已合并的、包含 `主机厂经销商ID`, `日期`, `进入私信客户数`, `主动咨询客户数`, `私信留资客户数` 列的DataFrame。
    - **聚合计算**: 按 `(NSC_CODE, date)` 分组, `sum()` 所有指标列。

### video (视频数据) 表处理逻辑

- **职责**: 将日度视频数据，按 `(NSC_CODE, date)` 聚合。
- **输入列**: `主机厂经销商id`, `日期`, `锚点曝光次数`, `锚点点击次数`, `新发布视频数`, `短视频表单提交商机量`
- **输出列**: `NSC_CODE`, `date`, `anchor_exposure`, `component_clicks`, `short_video_count`, `short_video_leads`
- **Transform逻辑**: 按 `(NSC_CODE, date)` 分组, `sum()` 所有指标列。

### account_bi (BI账户数据) 表处理逻辑

- **职责**: 将日度BI账户数据，按 `(NSC_CODE, date)` 聚合。
- **输入列**: `主机厂经销商id列表`, `日期`, `直播间表单提交商机量`, `短-播放量`
- **输出列**: `NSC_CODE`, `date`, `live_leads`, `short_video_plays`
- **Transform逻辑**: 按 `(NSC_CODE, date)` 分组, `sum()` 所有指标列。

### leads (线索数据) 表处理逻辑

- **职责**: 将日度线索数据，按 `(NSC_CODE, date)` 聚合。
- **输入列**: `主机厂经销商id列表`, `留资日期`, `直播间表单提交商机量(去重)`
- **输出列**: `NSC_CODE`, `date`, `small_wheel_leads`
- **Transform逻辑**: 按 `(NSC_CODE, date)` 分组, `sum()` 所有指标列。

### account_base (经销商维度表) 表处理逻辑

- **职责**: 从多个特定Sheet中提取经销商维度信息，合并后按 `NSC_CODE` 聚合，生成唯一的经销商维度表。
- **前置步骤 (Processor)**:
    1. 识别为 `account_base` Excel 文件。
    2. 读取所有Sheet页并识别:
        - 对于每个Sheet，检查其包含的列（英文大小写不敏感匹配）：
            - 如果Sheet包含 `NSC_id` 和 `第二期层级`，则识别为**层级信息 Sheet**。
                - **只选择** `NSC_id` 和 `第二期层级` 两列。
                - 将 `NSC_id` 重命名为 `NSC_CODE`，`第二期层级` 重命名为 `level`。
            - 如果Sheet包含 `NSC Code` 和 `抖音id`，则识别为**门店信息 Sheet**。
                - **只选择** `NSC Code` 和 `抖音id` 两列。
                - 将 `NSC Code` 重命名为 `NSC_CODE`，`抖音id` 重命名为 `store_name`。
            - **注意**: 如果找到多个层级信息Sheet或多个门店信息Sheet，或者没有找到任何一个，需要有明确的错误处理或警告机制。
    3. 将识别出的层级信息DataFrame和门店信息DataFrame**外连接 (outer join)** 到一起，以 `NSC_CODE` 为键。
- **Transform逻辑**:
    - **输入**: 经过Processor合并后的DataFrame，包含 `NSC_CODE`, `level`, `store_name`。
    - **聚合计算**: 只按 `NSC_CODE` 分组, `first(非空的 level 值)` 和 `first(非空的 store_name 值)`。
    - **确保类型**: 所有列都应为 `String` 类型。

## Environment Variables
- `PROCESSOR_API_KEY`: API authentication key
- `TMP_ROOT`: Temporary directory for file processing (default: /tmp/fastapi_data_proc)
- `LEVEL_NORMALIZE_BY_NSC`: Feature flag to enable the new level aggregation logic. Set to `true`, `1`, `yes`, or `on` to activate. (Default: `true`)

## Supported File Types
- CSV files
- Excel files (.xlsx, .xls)
- URLs or local file paths (file:// prefix)

## Memory Requirements
- 500MB total data → 4GB memory recommended
- 300MB single file → 2.5GB memory used
- Zeabur $5/month plan sufficient
