import httpx
import asyncio
import json
from typing import List, Dict
import logging
from datetime import datetime

logger = logging.getLogger("feishu")

class FeishuWriter:
    def __init__(self, config: dict):
        self.enabled = config.get("enabled", False)
        self.app_id = config.get("app_id", "")
        self.app_secret = config.get("app_secret", "")
        self.app_token = config.get("app_token", "")
        self.table_id = config.get("table_id", "")
        self.base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        if not all([self.app_id, self.app_secret, self.app_token, self.table_id]):
            self.enabled = False
            logger.warning("[飞书] 配置不完整 (app_id, app_secret, app_token, table_id)，写入功能已禁用。")

        # Business logic mapping from Chinese (Feishu) to English (internal)
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
        """获取tenant_access_token"""
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
        """
        获取并解析飞书多维表格的结构 (schema)，包含重试逻辑。
        如果多次尝试后仍然失败，则会抛出异常。
        """
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
                        if not items and not simplified_schema: # Only warn if it's the first empty response
                            logger.warning("[飞书] API首次返回的字段列表为空。")
                        
                        for field in items:
                            field_name = field.get("field_name")
                            field_id = field.get("field_id")
                            field_type = field.get("type")
                            options = []
                            
                            if field_type in [3, 4]: # 3: 单选, 4: 多选
                                options = [opt["name"] for opt in field.get("property", {}).get("options", [])]

                            if field_name and field_id:
                                simplified_schema[field_name] = {
                                    "id": field_id,
                                    "type": field_type,
                                    "options": options
                                }
                        
                        if data.get("has_more") and data.get("page_token"):
                            page_token = data.get("page_token")
                        else:
                            break # Exit pagination loop on success
                
                logger.info(f"[飞书] Schema获取成功，共解析 {len(simplified_schema)} 个字段。")
                return simplified_schema # Return on success

            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema API请求失败: {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 404:
                    logger.error("[飞书] 收到 404 错误，可能是 app_token 或 table_id 无效，不再重试。")
                    break # Stop retrying on 404
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema时发生网络错误: {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"[飞书] 解析schema时发生未知错误: {e}", exc_info=True)
            
            # If we are here, it means an exception occurred. Wait before retrying.
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"[飞书] 将在 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        # If loop finishes, all retries failed
        logger.error(f"[飞书] 获取 schema 彻底失败，已重试 {retries} 次。")
        raise RuntimeError(f"获取飞书表格结构失败: {last_exception}")

    async def write_records(self, records: List[Dict], schema: Dict = None) -> bool:
        """
        分批写入记录到飞书多维表格。
        如果批量写入失败，则会尝试逐条写入以跳过有问题的记录。
        """
        if not self.enabled:
            logger.info("[飞书] 写入未启用或配置不完整，跳过写入。")
            return True
        
        if not records:
            logger.info("[飞书] 没有记录可写入。")
            return True

        logger.info(f"[飞书] 开始写入，共 {len(records)} 条记录。")
        
        # 使用字段映射
        if schema:
            records = self._fix_data_types(records, schema)
            logger.error(f"[GEMINI_PROBE] Records after fix: {records[0] if records else 'No records'}")
            # _fix_data_types现在会处理字段映射
        
        try:
            token = await self.get_tenant_token()
            headers = {"Authorization": f"Bearer {token}"}
            logger.info("[飞书] 获取token成功。")
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"[飞书] 获取token失败，写入中止: {e}")
            return False

        # 过滤掉空记录
        records = [r for r in records if r and any(r.values())]
        if not records:
            logger.info("[飞书] 过滤后没有有效记录可写入。")
            return True
            
        chunks = [records[i:i + 480] for i in range(0, len(records), 480)]
        logger.info(f"[飞书] 数据分 {len(chunks)} 批写入，过滤后共 {len(records)} 条记录。")

        total_success_count = 0
        total_failed_count = 0
        
        async with httpx.AsyncClient() as client:
            for i, chunk in enumerate(chunks):
                # 验证chunk中的每个记录
                valid_chunk = []
                for item in chunk:
                    if item and any(item.values()):
                        valid_chunk.append(item)
                    else:
                        logger.warning(f"[飞书] 跳过无效记录: {item}")
                        total_failed_count += 1
                
                if not valid_chunk:
                    logger.warning(f"[飞书] 第 {i+1} 批所有记录无效，跳过")
                    continue
                
                payload = {"records": [{"fields": item} for item in valid_chunk]}
                
                try:
                    resp = await client.post(self.base_url, json=payload, headers=headers, timeout=60)
                    
                    if resp.status_code == 200:
                        result = resp.json()
                        added_records = result.get("data", {}).get("records", [])
                        batch_success_count = len(added_records)
                        total_success_count += batch_success_count
                        logger.info(f"[飞书] 第 {i+1}/{len(chunks)} 批写入成功: {batch_success_count}/{len(chunk)} 条。")
                    else:
                        # 批量写入失败，启动单条重试
                        error_resp = resp.json()
                        logger.error(f"[飞书] 第 {i+1}/{len(chunks)} 批写入失败: {resp.status_code} - {error_resp.get('msg', '未知错误')}")
                        logger.info(f"[飞书] 开始对第 {i+1} 批进行逐条写入...")
                        
                        batch_success_count, batch_failed_count = await self._write_single_records(client, chunk, headers)
                        total_success_count += batch_success_count
                        total_failed_count += batch_failed_count
                        logger.info(f"[飞书] 第 {i+1} 批逐条写入完成: 成功 {batch_success_count} 条, 失败 {batch_failed_count} 条。")

                except httpx.HTTPError as e:
                    logger.error(f"[飞书] 第 {i+1} 批发生网络错误: {e}")
                    logger.info(f"[飞书] 开始对第 {i+1} 批进行逐条写入...")
                    batch_success_count, batch_failed_count = await self._write_single_records(client, chunk, headers)
                    total_success_count += batch_success_count
                    total_failed_count += batch_failed_count
                except Exception as e:
                    logger.error(f"[飞书] 第 {i+1} 批发生未知异常: {e}")
                    total_failed_count += len(chunk)

        logger.info(f"[飞书] 写入总结: 总记录数 {len(records)}，成功 {total_success_count} 条，失败 {total_failed_count} 条。")
        return total_failed_count == 0

    def _fix_data_types(self, records: List[Dict], schema: Dict) -> List[Dict]:
        """
        根据飞书schema修正数据类型和字段名，返回修正后的数据副本。
        现在使用动态schema将中文列名映射到飞书的真实field_id。
        """
        if not schema or not records:
            return []

        fixed_records = []
        for record in records:
            if not record:
                continue
            
            fixed_record = {}
            for en_key, value in record.items():
                # 1. Translate English key to Chinese key
                chinese_key = self.EN_TO_CN_MAP.get(en_key)
                if not chinese_key:
                    logger.warning(f"[飞书] 忽略未映射的英文字段: '{en_key}'")
                    continue

                # 2. Find the field's schema using the Chinese key
                field_schema = schema.get(chinese_key)
                if not field_schema:
                    logger.warning(f"[飞书] 忽略字段: '{chinese_key}' (from '{en_key}')，因为它在飞书表格中不存在。")
                    continue

                # 3. Get the real field_id and type from the schema
                target_field_id = field_schema.get("id")
                field_type = field_schema.get("type")

                if not target_field_id:
                    logger.warning(f"[飞书] 字段 '{chinese_key}' 在schema中缺少 'id'，已跳过。")
                    continue
                
                # 4. Convert value to the correct type
                try:
                    converted_value = None
                    if value is None or value == '':
                        converted_value = None
                    elif field_type == 1: # Text
                        converted_value = str(value)
                    elif field_type == 2: # Number
                        converted_value = float(value)
                    elif field_type == 5: # DateTime
                        # Assuming value is an ISO 8601 string
                        converted_value = int(datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp() * 1000)
                    else:
                        converted_value = value
                    
                    fixed_record[target_field_id] = converted_value

                except (ValueError, TypeError) as e:
                    logger.warning(f"[飞书] 字段 '{chinese_key}' (ID: {target_field_id}) 的值 '{value}' 类型转换失败: {e}")

            if fixed_record:
                fixed_records.append(fixed_record)
        
        logger.info(f"[飞书] 字段映射和类型转换完成，有效记录数: {len(fixed_records)}/{len(records)}")
        return fixed_records

    async def _write_single_records(self, client: httpx.AsyncClient, records: List[Dict], headers: Dict) -> (int, int):
        """私有方法，用于逐条写入记录并返回成功和失败的计数"""
        success_count = 0
        failed_count = 0
        
        for record in records:
            if not record or not any(record.values()):  # 跳过空记录或所有字段都空的记录
                failed_count += 1
                continue
            
            single_payload = {"records": [{"fields": record}]}
            
            # 验证payload完整性
            if not record or not any(record.values()):
                logger.warning(f"[飞书] 跳过无效记录: {record}")
                failed_count += 1
                continue
                
            try:
                logger.debug(f"[飞书] 写入单条记录: {single_payload}")
                single_resp = await client.post(self.base_url, json=single_payload, headers=headers, timeout=30)
                if single_resp.status_code == 200:
                    success_count += 1
                else:
                    failed_count += 1
                    error_detail = single_resp.text
                    logger.warning(f"[飞书] 跳过问题记录: {error_detail} | 数据: {json.dumps(record, ensure_ascii=False)[:200]}...")
            except Exception as e:
                failed_count += 1
                logger.error(f"[飞书] 单条记录写入异常: {e} | 数据: {json.dumps(record, ensure_ascii=False)[:100]}...")
        
        return success_count, failed_count
