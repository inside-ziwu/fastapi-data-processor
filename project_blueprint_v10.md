# V10 - 最终执行蓝图 (The Final Executable Blueprint)

## 第一部分：核心设计原则

1.  **数据类型统一**: 所有数值型业务指标，在处理流程中统一使用 `Float64` 类型。
2.  **字段命名归一化**: 严格遵循 `原始名` -> `内部英文标准名` -> `最终输出中文名` 的三层体系。所有核心逻辑仅使用内部英文标准名。

---

## 第二部分：英文-中文映射总表

| 内部英文标准名 (Internal English Name) | 最终输出中文名 (Final Chinese Name) |
| :--- | :--- |
| `nsc_code` | 经销商ID |
| `date` | 日期 |
| `level` | 层级 |
| `store_name` | 门店名 |
| `natural_leads` | 自然线索量 |
| `paid_leads` | 付费线索量 |
| `local_leads` | 本地线索量 |
| `cloud_store_paid_leads` | 车云店付费线索量 |
| `regional_paid_leads` | 区域加码付费线索量 |
| `total_leads` | 线索总量 |
| `total_paid_leads` | 直播车云店+区域付费线索量 |
| `msg_private_entrants` | 进私人数 |
| `msg_active_consultations` | 私信开口人数 |
| `msg_leads_from_private` | 咨询留资人数 |
| `video_anchor_exposures` | 锚点曝光量 |
| `video_anchor_clicks` | 组件点击次数 |
| `video_new_posts` | 短视频条数 |
| `video_form_leads` | 组件留资人数（获取线索量） |
| `live_gt_25min_duration_min` | 超25分钟直播时长(分) |
| `live_effective_duration_hr` | 直播时长 |
| `live_gt_25min_sessions` | 有效直播场次 |
| `live_exposures` | 曝光人数 |
| `live_views` | 场观 |
| `live_widget_clicks` | 小风车点击次数 |
| `leads_from_live_form` | 小风车留资量|
| `account_bi_live_form_leads` | 直播线索量 |
| `account_bi_video_views` | 短视频播放量 |
| `spending_net` | 车云店+区域投放总金额 |
| `effective_days` | 有效天数 |
| `level_nsc_count` | (层级维度)门店数 |
| `cpl_total` | 车云店+区域综合CPL |
| `cpl_paid` | 付费CPL（车云店+区域） |
| `local_leads_ratio` | 本地线索占比 |
| `live_cpl` | 直播付费CPL |
| `exposure_to_view_ratio` | 曝光进入率 |
| `widget_click_ratio` | 小风车点击率 |
| `widget_lead_ratio` | 小风车点击留资率 |
| `component_click_ratio` | 组件点击率 |
| `component_lead_ratio` | 组件留资率 |
| `private_msg_consult_ratio` | 私信咨询率=开口/进私 |
| `consult_to_lead_ratio` | 咨询留资率=留资/咨询 |
| `private_msg_conversion_ratio` | 私信转化率=留资/进私 |
| `avg_exposure_per_session` | 场均曝光人数 |
| `avg_view_per_session` | 场均场观 |
| `avg_widget_leads_per_session` | 场均小风车留资量 |
| `avg_widget_clicks_per_session` | 场均小风车点击次数 |
| `avg_daily_spending` | 直播车云店+区域日均消耗 |
| `avg_daily_effective_duration_hr` | 日均有效（25min以上）时长（h） |
| `avg_daily_private_entrants` | 日均进私人数 |
| `avg_daily_active_consultations` | 日均私信开口人数 |
| `avg_daily_msg_leads` | 日均咨询留资人数 |

---

## 第三部分：数据准备管道 (ETL)

此部分定义了从9个源文件到日粒度总宽表的完整流程。

### 3.1 `DR1_file`, `DR2_file` 处理
- **重命名**: `reg_dealer`->`nsc_code`, `register_time`->`date`, etc.
- **计算**: `groupBy(['nsc_code', 'date'])` 并 `agg` 以下字段：
    - `natural_leads` = `filter(leads_type == '自然').count()`
    - `paid_leads` = `filter(leads_type == '广告').count()`
    - `local_leads` = `filter(nsc_code == send2dealer_id).count()`
    - `cloud_store_paid_leads` = `filter(leads_type == '广告' & mkt_second_channel_name.is_in(['抖音车云店_BMW_本市_LS直发', '抖音车云店_LS直发'])).count()`
    - `regional_paid_leads` = `filter(leads_type == '广告' & mkt_second_channel_name == '抖音车云店_BMW_总部BDT_LS直发').count()`
- **输出**: `nsc_code`, `date`, 及以上计算出的 `Float64` 指标。

### 3.2 `msg_excel_file` 处理
- 遍历 Sheets, 提取并重命名 (`主机厂经销商ID`->`nsc_code`, etc.)，从 Sheet 名生成 `date` 列，然后纵向合并。
- **整理**: `groupBy(['nsc_code', 'date'])` 并 `sum()` 所有数值指标。

### 3.3 `Spending_file` 处理
- 读取指定 Sheets, 提取并重命名 (`NSC CODE`->`nsc_code`, etc.)，然后纵向合并。
- **整理**: `groupBy(['nsc_code', 'date'])` 并 `sum('spending_net')`。

### 3.4 其他文件 (`video`, `live`, `leads`, `account_bi`) 处理
- 各自根据映射表提取并重命名为内部英文名。
- **整理**: `groupBy(['nsc_code', 'date'])` 并 `sum()` 各自的数值指标。

### 3.5 `account_base_file` 处理
- 按精确列名对 (`NSC_id`+`第二期层级`, `NSC Code`+`抖音id`) 识别 Sheet，提取并重命名为 `nsc_code`, `level`, `store_name`，最后外连接成维度表。

### 3.6 合并
1.  **DR合并**: 纵向合并 `DR1` 和 `DR2` 的结果。
2.  **总键表**: 从所有交易型数据源（除`account_base`）中提取 `['nsc_code', 'date']`，合并去重，形成总键表。
3.  **最终连接**: 以总键表为基础，`left_join` 所有整理好的数据源。
4.  **填充**: 将所有数值列的 `null` 填充为 `0.0`。

---

## 第四部分：最终结算聚合

### 4.1 周期性指标生成
对第三部分产出的总宽表，为清单中的所有相关指标，派生出 `t_` 和 `t-1_` 前缀的列。

### 4.2 预计算复合指标
在聚合前，计算 `total_leads`, `total_paid_leads` 及其周期版本。

### 4.3 最终分组聚合
- 根据 `dimension` 参数选择分组键 (`['nsc_code', ...]` 或 `['level']`)。
- `agg` 操作：对所有数值指标执行 `sum()`；对 `date` 执行 `n_unique()` 以获得 `effective_days`；若按 `level` 分组，则额外计算 `nsc_code` 的 `n_unique()` 以获得 `level_nsc_count`。

### 4.4 后聚合派生指标计算
- 在聚合结果上，根据 **第五部分** 的公式计算所有比率和均值。
- **`组件留资率`** 的计算公式**已更正**为: `component_lead_ratio` = `SAFE_DIV(video_form_leads_sum, video_anchor_exposures_sum)`。

### 4.5 层级归一化 (Level Normalization)
- **触发条件**: `if dimension == '层级'`
- **需归一化处理的指标清单 (最终版)**: 当维度为“层级”时，以下指标的最终值需要除以该层级下的经销商总数 (`level_nsc_count`)，以体现“平均每店”的概念。
    - `natural_leads`
    - `paid_leads`
    - `local_leads`
    - `cloud_store_paid_leads`
    - `regional_paid_leads`
    - `total_leads`
    - `total_paid_leads`
    - `msg_private_entrants`
    - `msg_active_consultations`
    - `msg_leads_from_private`
    - `video_anchor_exposures`
    - `video_anchor_clicks`
    - `video_new_posts`
    - `video_form_leads`
    - `live_gt_25min_duration_min`
    - `live_effective_duration_hr`
    - `live_gt_25min_sessions`
    - `live_exposures`
    - `live_views`
    - `live_widget_clicks`
    - `leads_from_live_form`
    - `account_bi_live_form_leads`
    - `account_bi_video_views`
    - `spending_net`
    - `avg_daily_spending`
    - `avg_daily_private_entrants`
    - `avg_daily_active_consultations`
    - `avg_daily_msg_leads`
    - `avg_daily_effective_duration_hr`
- **处理逻辑**: 对于上述列表中的每一个指标，其最终值将更新为 `SAFE_DIV(pl.col('metric_name'), pl.col('level_nsc_count'))`。

---

## 第五部分：最终业务指标计算逻辑 (中文可读版)

| 最终输出中文名 | 计算逻辑 |
| :--- | :--- |
| **车云店+区域综合CPL** | `SAFE_DIV( [车云店+区域投放总金额的总和], [线索总量的总和] )` |
| **付费CPL（车云店+区域）** | `SAFE_DIV( [车云店+区域投放总金额的总和], [直播车云店+区域付费线索量的总和] )` |
| **本地线索占比** | `SAFE_DIV( [本地线索量的总和], [线索总量的总和] )` |
| **直播付费CPL** | `SAFE_DIV( [车云店+区域投放总金额的总和], [直播车云店+区域付费线索量的总和] )` |
| **曝光进入率** | `SAFE_DIV( [场观的总和], [曝光人数的总和] )` |
| **小风车点击率** | `SAFE_DIV( [小风车点击次数的总和], [场观的总和] )` |
| **小风车点击留资率** | `SAFE_DIV( [小风车留资量的总和], [小风车点击次数的总和] )` |
| **组件点击率** | `SAFE_DIV( [组件点击次数的总和], [锚点曝光量的总和] )` |
| **组件留资率** | `SAFE_DIV( [组件留资人数（获取线索量）的总和], [锚点曝光量的总和] )` |
| **私信咨询率=开口/进私** | `SAFE_DIV( [私信开口人数的总和], [进私人数的总和] )` |
| **咨询留资率=留资/咨询** | `SAFE_DIV( [咨询留资人数的总和], [私信开口人数的总和] )` |
| **私信转化率=留资/进私** | `SAFE_DIV( [咨询留资人数的总和], [进私人数的总和] )` |
| **场均曝光人数** | `SAFE_DIV( [曝光人数的总和], [有效直播场次的总和] )` |
| **场均场观** | `SAFE_DIV( [场观的总和], [有效直播场次的总和] )` |
| **场均小风车留资量** | `SAFE_DIV( [小风车留资量的总和], [有效直播场次的总和] )` |
| **场均小风车点击次数** | `SAFE_DIV( [小风车点击次数的总和], [有效直播场次的总和] )` |
| **直播车云店+区域日均消耗** | `SAFE_DIV( [车云店+区域投放总金额的总和], [有效天数的总和] )` |
| **日均有效（25min以上）时长（h）** | `SAFE_DIV( ([超25分钟直播时长(分)的总和] / 60), [有效天数的总和] )` |
| **日均进私人数** | `SAFE_DIV( [进私人数的总和], [有效天数的总和] )` |
| **日均私信开口人数** | `SAFE_DIV( [私信开口人数的总和], [有效天数的总和] )` |
| **日均咨询留资人数** | `SAFE_DIV( [咨询留资人数的总和], [有效天数的总和] )` |

*(注: T月和T-1月指标的计算逻辑与上表完全相同，只是将`[指标的总和]`替换为`[指标的T月总和]`或`[指标的T-1月总和]`)*
*(注2: 层级维度下，所有在4.5节清单中列出的指标，其最终值会在此表计算结果的基础上，再除以该层级下的【(层级维度)门店数】，以得出“平均每店”的数值。)*

---

## 第六部分：最终输出

1.  **列名映射**: 使用 **第二部分** 的映射总表，将所有内部英文名 `rename` 为最终输出的中文名。
2.  **列序重排**: 按预设的 `OUTPUT_CONTRACT_ORDER` 排序。
3.  **序列化交付**: 转换为字典列表，输出为 JSON 或写入飞书。
