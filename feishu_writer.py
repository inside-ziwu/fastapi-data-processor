import httpx
import asyncio
import json
from typing import List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger("feishu")

def to_ms_timestamp(v: Any) -> int:
    """Helper to convert ISO date string to millisecond timestamp."""
    if isinstance(v, int):
        return v # Already a timestamp
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    return int(datetime.fromisoformat(str(v).replace('Z', '+00:00')).timestamp() * 1000)

# Correct and robust Feishu field type mapping
FIELD_TYPE_HANDLERS = {
    1: str,          # Text (文本)
    2: float,        # Number (数字)
    3: float,        # Progress (百分比 0-1)
    4: float,        # Currency (货币)
    5: int,          # Rating (评分 1-5)
    7: to_ms_timestamp, # Date (日期)
    11: bool,        # Checkbox (复选框)
    13: str,         # SingleSelect (单选)
    15: lambda v: [str(i) for i in (v if isinstance(v, list) else [v])],  # MultiSelect (多选)
    17: lambda v: [{"id": str(i)} for i in (v if isinstance(v, list) else [v])],  # User (人员)
    18: lambda v: [{"id": str(i)} for i in (v if isinstance(v, list) else [v])],  # GroupChat (群组)
    21: lambda v: [str(i) for i in (v if isinstance(v, list) else [v])],  # SingleLink (单向关联)
    22: lambda v: [str(i) for i in (v if isinstance(v, list) else [v])],  # DuplexLink (双向关联)
    23: str,         # Location (地理位置 "lng,lat")
    1001: to_ms_timestamp, # DateTime (日期时间)
    1003: str,       # Phone (电话)
    1004: str,       # Email (邮箱)
    1005: lambda v: {"text": str(v), "link": str(v)} if isinstance(v, str) else v,  # URL (超链接)
    1006: str,       # Barcode (条码)
    1007: lambda v: [{"file_token": str(i)} for i in (v if isinstance(v, list) else [v])],  # Attachment (附件)
}

class FeishuWriter:
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False)
        self.app_id = config.get("app_id", "")
        self.app_secret = config.get("app_secret", "")
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        self.base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        if not all([self.app_id, self.app_secret, self.app_token, self.table_id]):
            self.enabled = False
            logger.warning("[飞书] 配置不完整 (app_id, app_secret, app_token, table_id)，写入功能已禁用。")

        self.FIELD_EN_MAP = {
            "主机厂经销商ID": "NSC_CODE",
            "层级": "level", 
            "门店名": "store_name",
            "自然线索总量": "natural_leads_total",
            "T月自然线索量": "natural_leads_t",
            "T-1月自然线索量": "natural_leads_t_minus_1",
            "广告线索总量": "ad_leads_total",
            "T月广告线索量": "ad_leads_t",
            "T-1月广告线索量": "ad_leads_t_minus_1",
            "总消耗": "spending_net_total",
            "T月消耗": "spending_net_t",
            "T-1月消耗": "spending_net_t_minus_1",
            "付费线索总量": "paid_leads_total",
            "T月付费线索量": "paid_leads_t",
            "T-1月付费线索量": "paid_leads_t_minus_1",
            "区域线索总量": "area_leads_total",
            "T月区域线索量": "area_leads_t",
            "T-1月区域线索量": "area_leads_t_minus_1",
            "本地线索总量": "local_leads_total",
            "T月本地线索量": "local_leads_t",
            "T-1月本地线索量": "local_leads_t_minus_1",
            "有效直播时长总量(小时)": "live_effective_hours_total",
            "T月有效直播时长(小时)": "live_effective_hours_t",
            "T-1月有效直播时长(小时)": "live_effective_hours_t_minus_1",
            "有效直播场次总量": "effective_live_sessions_total",
            "T月有效直播场次": "effective_live_sessions_t",
            "T-1月有效直播场次": "effective_live_sessions_t_minus_1",
            "总曝光人数": "exposures_total",
            "T月曝光人数": "exposures_t",
            "T-1月曝光人数": "exposures_t_minus_1",
            "总场观": "viewers_total",
            "T月场观": "viewers_t",
            "T-1月场观": "viewers_t_minus_1",
            "小风车点击总量": "small_wheel_clicks_total",
            "T月小风车点击": "small_wheel_clicks_t",
            "T-1月小风车点击": "small_wheel_clicks_t_minus_1",
            "小风车留资总量": "small_wheel_leads_total",
            "T月小风车留资": "small_wheel_leads_t",
            "T-1月小风车留资": "small_wheel_leads_t_minus_1",
            "直播线索总量": "live_leads_total",
            "T月直播线索": "live_leads_t",
            "T-1月直播线索": "live_leads_t_minus_1",
            "锚点曝光总量": "anchor_exposure_total",
            "T月锚点曝光": "anchor_exposure_t",
            "T-1月锚点曝光": "anchor_exposure_t_minus_1",
            "组件点击总量": "component_clicks_total",
            "T月组件点击": "component_clicks_t",
            "T-1月组件点击": "component_clicks_t_minus_1",
            "短视频留资总量": "short_video_leads_total",
            "T月短视频留资": "short_video_leads_t",
            "T-1月短视频留资": "short_video_leads_t_minus_1",
            "短视频发布总量": "short_video_count_total",
            "T月短视频发布": "short_video_count_t",
            "T-1月短视频发布": "short_video_count_t_minus_1",
            "短视频播放总量": "short_video_plays_total",
            "T月短视频播放": "short_video_plays_t",
            "T-1月短视频播放": "short_video_plays_t_minus_1",
            "进私总量": "enter_private_count_total",
            "T月进私": "enter_private_count_t",
            "T-1月进私": "enter_private_count_t_minus_1",
            "私信开口总量": "private_open_count_total",
            "T月私信开口": "private_open_count_t",
            "T-1月私信开口": "private_open_count_t_minus_1",
            "私信留资总量": "private_leads_count_total",
            "T月私信留资": "private_leads_count_t",
            "T-1月私信留资": "private_leads_count_t_minus_1",
            "车云店+区域综合CPL": "total_cpl",
            "付费CPL（车云店+区域）": "paid_cpl",
            "本地线索占比": "local_leads_ratio",
            "直播车云店+区域日均消耗": "avg_daily_spending",
            "T月直播车云店+区域日均消耗": "avg_daily_spending_t",
            "T-1月直播车云店+区域日均消耗": "avg_daily_spending_t_minus_1",
            "直播车云店+区域付费线索量日均": "avg_daily_paid_leads",
            "T月直播车云店+区域付费线索量日均": "avg_daily_paid_leads_t",
            "T-1月直播车云店+区域付费线索量日均": "avg_daily_paid_leads_t_minus_1",
            "T月直播付费CPL": "paid_cpl_t",
            "T-1月直播付费CPL": "paid_cpl_t_minus_1",
            "有效（25min以上）时长（h）": "effective_live_hours_25min",
            "T月有效（25min以上）时长（h）": "effective_live_hours_25min_t",
            "T-1月有效（25min以上）时长（h）": "effective_live_hours_25min_t_minus_1",
            "日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min",
            "T月日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min_t",
            "T-1月日均有效（25min以上）时长（h）": "avg_daily_effective_live_hours_25min_t_minus_1",
            "场均曝光人数": "avg_exposures_per_session",
            "T月场均曝光人数": "avg_exposures_per_session_t",
            "T-1月场均曝光人数": "avg_exposures_per_session_t_minus_1",
            "曝光进入率": "exposure_to_viewer_rate",
            "T月曝光进入率": "exposure_to_viewer_rate_t",
            "T-1月曝光进入率": "exposure_to_viewer_rate_t_minus_1",
            "场均场观": "avg_viewers_per_session",
            "T月场均场观": "avg_viewers_per_session_t",
            "T-1月场均场观": "avg_viewers_per_session_t_minus_1",
            "小风车点击率": "small_wheel_click_rate",
            "T月小风车点击率": "small_wheel_click_rate_t",
            "T-1月小风车点击率": "small_wheel_click_rate_t_minus_1",
            "小风车点击留资率": "small_wheel_leads_rate",
            "T月小风车点击留资率": "small_wheel_leads_rate_t",
            "T-1月小风车点击留资率": "small_wheel_leads_rate_t_minus_1",
            "场均小风车留资量": "avg_small_wheel_leads_per_session",
            "T月场均小风车留资量": "avg_small_wheel_leads_per_session_t",
            "T-1月场均小风车留资量": "avg_small_wheel_leads_per_session_t_minus_1",
            "组件点击率": "component_click_rate",
            "T月组件点击率": "component_click_rate_t",
            "T-1月组件点击率": "component_click_rate_t_minus_1",
            "组件留资率": "component_leads_rate",
            "T月组件留资率": "component_leads_rate_t",
            "T-1月组件留资率": "component_leads_rate_t_minus_1",
            "日均进私人数": "avg_daily_private_entry_count",
            "T月日均进私人数": "avg_daily_private_entry_count_t",
            "T-1月日均进私人数": "avg_daily_private_entry_count_t_minus_1",
            "日均私信开口人数": "avg_daily_private_open_count",
            "T月日均私信开口人数": "avg_daily_private_open_count_t",
            "T-1月日均私信开口人数": "avg_daily_private_open_count_t_minus_1",
            "日均咨询留资人数": "avg_daily_private_leads_count",
            "T月日均咨询留资人数": "avg_daily_private_leads_count_t",
            "T-1月日均咨询留资人数": "avg_daily_private_leads_count_t_minus_1",
            "私信咨询率": "private_open_rate",
            "T月私信咨询率": "private_open_rate_t",
            "T-1月私信咨询率": "private_open_rate_t_minus_1",
            "咨询留资率": "private_leads_rate",
            "T月咨询留资率": "private_leads_rate_t",
            "T-1月咨询留资率": "private_leads_rate_t_minus_1",
            "私信转化率": "private_conversion_rate",
            "T月私信转化率": "private_conversion_rate_t",
            "T-1月私信转化率": "private_conversion_rate_t_minus_1"
        }
        self.EN_TO_CN_MAP = {v: k for k, v in self.FIELD_EN_MAP.items()}

    async def get_tenant_token(self) -> str:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            result = resp.json()
            if "tenant_access_token" not in result:
                raise ValueError(f"获取token失败: {result.get('msg', '未知错误')}")
            return result["tenant_access_token"]

    async def get_table_schema(self) -> Dict[str, Dict]:
        if not self.enabled:
            logger.warning("[飞书] 功能未启用，无法获取schema。")
            return {}

        schema_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        retries = 3
        last_exception = None

        for attempt in range(retries):
            try:
                logger.info(f"[飞书] 开始获取表格 {self.table_id} 的 schema (尝试 {attempt + 1}/{retries})...")
                token = await self.get_tenant_token()
                headers = {"Authorization": f"Bearer {token}"}
                simplified_schema = {}
                page_token = None
                
                async with httpx.AsyncClient() as client:
                    while True:
                        params = {'page_size': 100}
                        if page_token:
                            params['page_token'] = page_token
                        
                        resp = await client.get(schema_url, headers=headers, params=params, timeout=30)
                        resp.raise_for_status()
                        data = resp.json().get("data", {})
                        items = data.get("items", [])
                        
                        for field in items:
                            if (field_name := field.get("field_name")) and (field_id := field.get("field_id")):
                                simplified_schema[field_name] = {
                                    "id": field_id,
                                    "type": field.get("type"),
                                }
                        
                        if data.get("has_more") and (page_token := data.get("page_token")):
                            continue
                        else:
                            break
                
                logger.info(f"[飞书] Schema获取成功，共解析 {len(simplified_schema)} 个字段。")
                return simplified_schema

            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema API请求失败: {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 404:
                    logger.error("[飞书] 收到 404 错误，可能是 app_token 或 table_id 无效，不再重试。")
                    break
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema时发生网络错误: {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"[飞书] 解析schema时发生未知错误: {e}", exc_info=True)
            
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"[飞书] 将在 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        raise RuntimeError(f"获取飞书表格结构失败: {last_exception}")

    async def write_records(self, records: List[Dict], schema: Dict = None) -> bool:
        if not self.enabled:
            logger.info("[飞书] 写入未启用或配置不完整，跳过写入。")
            return True
        
        if not records:
            logger.info("[飞书] 没有记录可写入。")
            return True

        logger.info(f"[飞书] 开始写入，共 {len(records)} 条记录。")
        
        if schema:
            records = self._fix_data_types(records, schema)
        
        try:
            token = await self.get_tenant_token()
            headers = {"Authorization": f"Bearer {token}"}
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"[飞书] 获取token失败，写入中止: {e}")
            return False

        records = [r for r in records if r and any(r.values())]
        if not records:
            logger.info("[飞书] 过滤后没有有效记录可写入。")
            return True
            
        chunks = [records[i:i + 50] for i in range(0, len(records), 50)]
        logger.info(f"[飞书] 数据分 {len(chunks)} 批写入，使用安全批次大小 50。")

        total_success_count = 0
        total_failed_count = 0
        
        async with httpx.AsyncClient() as client:
            for i, chunk in enumerate(chunks):
                payload = {"records": [{"fields": item} for item in chunk]}
                
                batch_success = False
                for attempt in range(3):
                    try:
                        resp = await client.post(self.base_url, json=payload, headers=headers, timeout=60)
                        
                        if resp.status_code == 200:
                            result = resp.json()
                            added_records = result.get("data", {}).get("records", [])
                            total_success_count += len(added_records)
                            logger.info(f"[飞书] 第 {i+1}/{len(chunks)} 批写入成功: {len(added_records)}/{len(chunk)} 条。")
                            batch_success = True
                            break

                        elif resp.status_code in [400, 404, 413]:
                            logger.error(f"[飞书] 第 {i+1} 批发生不可恢复错误: {resp.status_code} - {resp.text}，中止该批次。")
                            break
                        
                        logger.warning(f"[飞书] 第 {i+1} 批写入失败 (尝试 {attempt+1}/3): {resp.status_code} - {resp.text}")

                    except (httpx.RequestError, httpx.TimeoutException) as e:
                        logger.warning(f"[飞书] 第 {i+1} 批发生网络错误 (尝试 {attempt+1}/3): {e}")
                    
                    if attempt < 2:
                        wait_time = 2 ** (attempt + 1)
                        logger.info(f"[飞书] 将在 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)

                if not batch_success:
                    logger.error(f"[飞书] 第 {i+1} 批在3次尝试后彻底失败，转为逐条写入...")
                    s_count, f_count = await self._write_single_records(client, chunk, headers)
                    total_success_count += s_count
                    total_failed_count += f_count
                    logger.info(f"[飞书] 第 {i+1} 批逐条写入完成: 成功 {s_count} 条, 失败 {f_count} 条。")

        logger.info(f"[飞书] 写入总结: 总记录数 {len(records)}，成功 {total_success_count} 条，失败 {total_failed_count} 条。")
        return total_failed_count == 0

    def _fix_data_types(self, records: List[Dict], schema: Dict) -> List[Dict]:
        if not schema or not records:
            return []

        fixed_records = []
        for record in records:
            if not record:
                continue
            
            fixed_record = {}
            for en_key, value in record.items():
                chinese_key = self.EN_TO_CN_MAP.get(en_key)
                if not chinese_key:
                    continue

                field_schema = schema.get(chinese_key)
                if not field_schema:
                    continue

                target_field_id = field_schema.get("id")
                field_type = field_schema.get("type")

                if not target_field_id:
                    continue
                
                if value is None or (isinstance(value, str) and not value.strip()):
                    fixed_record[target_field_id] = None
                    continue

                try:
                    handler = FIELD_TYPE_HANDLERS.get(field_type)
                    if handler:
                        converted_value = handler(value)
                    else:
                        logger.warning(f"[飞书] 未知或未处理的字段类型 {field_type}，将使用字符串转换。")
                        converted_value = str(value)
                    
                    fixed_record[target_field_id] = converted_value

                except (ValueError, TypeError, Exception) as e:
                    logger.warning(f"[飞书] 字段 '{chinese_key}' (ID: {target_field_id}) 的值 '{value}' 类型转换失败: {e}")

            if fixed_record:
                fixed_records.append(fixed_record)
        
        logger.info(f"[飞书] 字段映射和类型转换完成，有效记录数: {len(fixed_records)}/{len(records)}")
        return fixed_records

    async def _write_single_records(self, client: httpx.AsyncClient, records: List[Dict], headers: Dict) -> (int, int):
        success_count = 0
        failed_count = 0
        
        single_record_url = self.base_url.replace('/batch_create', '')

        for record in records:
            if not record or not any(record.values()):
                failed_count += 1
                continue
            
            single_payload = {"fields": record}
            
            try:
                single_resp = await client.post(single_record_url, json=single_payload, headers=headers, timeout=30)
                if single_resp.status_code == 200:
                    success_count += 1
                else:
                    if single_resp.status_code in [400, 404, 413]:
                        logger.error(f"[飞书] 单条记录因不可恢复错误 {single_resp.status_code} 被跳过: {single_resp.text}")
                    else:
                        logger.warning(f"[飞书] 单条记录写入失败: {single_resp.status_code} - {single_resp.text}")
                    failed_count += 1

            except Exception as e:
                failed_count += 1
                logger.error(f"[飞书] 单条记录写入时发生异常: {e} | 数据: {json.dumps(record, ensure_ascii=False)[:100]}...")
        
        return success_count, failed_count
