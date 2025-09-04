# feishu_writer.py
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("feishu")


def to_ms_timestamp(v: Any) -> int:
    """Convert ISO string or datetime to millisecond timestamp; keep int as-is."""
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        # treat float seconds
        return int(v * 1000)
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    s = str(v)
    # normalize potential 'Z'
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return int(datetime.fromisoformat(s).timestamp() * 1000)


# Baseline handlers for primitive types; complex types will be handled with schema-aware encoder
FIELD_TYPE_HANDLERS_BASELINE = {
    1: lambda v: str(v),            # Text
    2: lambda v: float(v),          # Number
    3: lambda v: float(v),          # Progress (0-1)
    4: lambda v: float(v),          # Currency
    5: lambda v: int(v),            # Rating
    7: to_ms_timestamp,             # Date
    11: lambda v: bool(v),          # Checkbox
    23: lambda v: str(v),           # Location
    1001: to_ms_timestamp,          # DateTime
    1003: lambda v: str(v),         # Phone
    1004: lambda v: str(v),         # Email
    1005: lambda v: {"text": str(v), "link": str(v)} if isinstance(v, str) else v,  # URL
    1006: lambda v: str(v),         # Barcode
    1007: lambda v: [{"file_token": str(i)} for i in (v if isinstance(v, list) else [v])],  # Attachment
    # NOTE: 13/15/17/18/21/22 handled by schema-aware encoder
}


def _has_meaningful_value(v: Any) -> bool:
    """True if value is not None and not empty string after strip; 0 is meaningful."""
    if v is None:
        return False
    if isinstance(v, str) and v.strip() == "":
        return False
    return True


def _parse_bitable_result(resp_json: Dict[str, Any]) -> Tuple[Optional[int], Optional[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (code, msg, records, failures) with sensible defaults."""
    code = resp_json.get("code")
    msg = resp_json.get("msg")
    data = resp_json.get("data") or {}
    records = data.get("records") or []
    failures = data.get("failures") or []
    return code, msg, records, failures


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

        # 中文列名 -> 英文数据键 映射（你已有的定义）
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
            "私信开口总量": "avg_daily_private_open_count_total_DEPRECATED",  # 避免歧义，保持占位
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
            "T-1月私信转化率": "private_conversion_rate_t_minus_1",
        }
        self.EN_TO_CN_MAP = {v: k for k, v in self.FIELD_EN_MAP.items()}

    async def get_tenant_token(self) -> str:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=30)
            raw = resp.text
            logger.debug(f"[飞书调试] token HTTP {resp.status_code} body: {raw[:400]}...")
            resp.raise_for_status()
            result = resp.json()
            if "tenant_access_token" not in result:
                raise ValueError(f"获取token失败: {result.get('msg', '未知错误')}")
            return result["tenant_access_token"]

    async def get_table_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        返回: { field_name: { id, type, property } }
        property 用于 select/user 等类型的选项/结构编码。
        """
        if not self.enabled:
            logger.warning("[飞书] 功能未启用，无法获取schema。")
            return {}

        schema_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        retries = 3
        last_exception: Optional[Exception] = None

        for attempt in range(retries):
            try:
                logger.info(f"[飞书] 开始获取表格 {self.table_id} 的 schema (尝试 {attempt + 1}/{retries})...")
                token = await self.get_tenant_token()
                headers = {"Authorization": f"Bearer {token}"}
                simplified_schema: Dict[str, Dict[str, Any]] = {}
                page_token: Optional[str] = None

                async with httpx.AsyncClient() as client:
                    while True:
                        params = {'page_size': 100}
                        if page_token:
                            params['page_token'] = page_token

                        resp = await client.get(schema_url, headers=headers, params=params, timeout=30)
                        raw = resp.text
                        logger.debug(f"[飞书调试] schema HTTP {resp.status_code} body: {raw[:800]}...")
                        resp.raise_for_status()
                        data = resp.json().get("data", {})
                        items = data.get("items", [])

                        for field in items:
                            field_name = field.get("field_name")
                            field_id = field.get("field_id")
                            if field_name and field_id:
                                simplified_schema[field_name] = {
                                    "id": field_id,
                                    "type": field.get("type"),
                                    "property": field.get("property", {}) or {},
                                }

                        if data.get("has_more") and data.get("page_token"):
                            page_token = data["page_token"]
                            continue
                        break

                logger.info(f"[飞书] Schema获取成功，共解析 {len(simplified_schema)} 个字段。")
                for name, info in list(simplified_schema.items())[:5]:
                    logger.info(f"[飞书调试] Schema字段: {name} -> ID: {info.get('id')}, Type: {info.get('type')}")
                return simplified_schema

            except httpx.HTTPStatusError as e:
                last_exception = e
                text = e.response.text
                logger.warning(f"[飞书] 获取schema API失败: {e.response.status_code} - {text[:300]}")
                if e.response.status_code == 404:
                    logger.error("[飞书] 404：app_token 或 table_id 可能无效，不再重试。")
                    break
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                logger.warning(f"[飞书] 获取schema网络错误: {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"[飞书] 解析schema未知错误: {e}", exc_info=True)

            if attempt < retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"[飞书] 将在 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        raise RuntimeError(f"获取飞书表格结构失败: {last_exception}")

    @staticmethod
    def _build_schema_encoder(schema: Dict[str, Dict[str, Any]]):
        """
        返回一个 encoder(field_id, field_type, value) -> encoded_value 的函数，
        对 Select/User/Group/关联类用 property 进行正确编码，其它回退到 baseline handler。
        """
        # 构建 select 选项映射：field_id -> { name->id }
        select_maps_by_field_id: Dict[str, Dict[str, str]] = {}
        for name, meta in schema.items():
            ftype = meta.get("type")
            if ftype in (13, 15):  # 13 SingleSelect, 15 MultiSelect
                prop = meta.get("property") or {}
                opts = prop.get("options", []) or []
                name2id = {}
                for o in opts:
                    oid = o.get("id")
                    oname = str(o.get("name")) if o.get("name") is not None else None
                    if oid and oname:
                        name2id[oname] = oid
                select_maps_by_field_id[meta["id"]] = name2id

        def encode(field_id: str, field_type: int, v: Any) -> Any:
            # Selects
            if field_type == 13:  # SingleSelect
                m = select_maps_by_field_id.get(field_id, {})
                if isinstance(v, dict) and "id" in v:
                    return {"id": str(v["id"])}
                # allow using name or id string
                candidate = str(v)
                opt_id = m.get(candidate, candidate)
                return {"id": opt_id}

            if field_type == 15:  # MultiSelect
                m = select_maps_by_field_id.get(field_id, {})
                vals = v if isinstance(v, list) else [v]
                out = []
                for item in vals:
                    if isinstance(item, dict) and "id" in item:
                        out.append({"id": str(item["id"])})
                    else:
                        candidate = str(item)
                        opt_id = m.get(candidate, candidate)
                        out.append({"id": opt_id})
                return out

            # User / GroupChat: 17,18 -> list[{"id": "..."}]
            if field_type in (17, 18):
                vals = v if isinstance(v, list) else [v]
                return [{"id": str(i["id"] if isinstance(i, dict) and "id" in i else i)} for i in vals]

            # Link types (SingleLink/DuplexLink) often accept record_id list strings
            if field_type in (21, 22):
                vals = v if isinstance(v, list) else [v]
                return [str(i) for i in vals]

            # Fallback to baseline type conversion
            handler = FIELD_TYPE_HANDLERS_BASELINE.get(field_type)
            return handler(v) if handler else v

        return encode

    def _fix_data_types(self, records: List[Dict[str, Any]], schema: Optional[Dict[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if not records:
            return []
        if not schema:
            logger.warning("[飞书] schema 为空，跳过类型转换并直接使用原始字段（可能失败）。")
            return records

        # name -> {id,type,property}, build name->id and id set to check unknowns
        name_to_meta = schema
        encoder = self._build_schema_encoder(schema)

        fixed_records: List[Dict[str, Any]] = []
        for record in records:
            if not record:
                continue
            fixed_record: Dict[str, Any] = {}
            for en_key, value in record.items():
                # 允许英文键映射到中文列名，然后再通过 schema 查到 id/type
                chinese_key = self.EN_TO_CN_MAP.get(en_key, None)
                # 如果用户已经直接给了 field_id（fldXXXX），也允许直通（高级用法）
                if chinese_key is None and en_key.startswith("fld"):
                    # 直通 field_id，但必须有类型信息才能编码；如果 schema 找不到这个 id，只能生吞
                    # 在直通模式下，我们不做类型转换（除非能反查到 type）
                    fixed_record[en_key] = value
                    continue

                if not chinese_key:
                    # 英文键在映射中找不到，跳过
                    continue

                field_schema = name_to_meta.get(chinese_key)
                if not field_schema:
                    # 中文列名不在 schema 中，跳过
                    continue

                target_field_id = field_schema.get("id")
                field_type = field_schema.get("type")

                if not target_field_id:
                    continue

                # 跳过 None/空字符串，避免“设置为空”的副作用；保留 0、False
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    continue

                try:
                    converted_value = encoder(target_field_id, field_type, value)
                    fixed_record[target_field_id] = converted_value
                except Exception as e:
                    logger.warning(f"[飞书] 字段 '{chinese_key}'(ID:{target_field_id},T:{field_type}) 值 '{value}' 转换失败: {e}")

            if fixed_record:
                fixed_records.append(fixed_record)

        logger.info(f"[飞书] 字段映射与类型转换完成：有效记录 {len(fixed_records)}/{len(records)}")
        # 打印首条记录的字段 id 分布与 schema 差集，便于快速定位未知字段
        if fixed_records:
            first_ids = set(fixed_records[0].keys())
            table_ids = {meta["id"] for meta in schema.values() if meta.get("id")}
            unknown = first_ids - table_ids
            if unknown:
                logger.warning(f"[飞书] 发现未知字段ID（可能导致失败）：{list(unknown)[:10]}... 共 {len(unknown)}")
        return fixed_records

    async def write_records(self, records: List[Dict[str, Any]], schema: Optional[Dict[str, Dict[str, Any]]] = None) -> bool:
        if not self.enabled:
            logger.info("[飞书] 写入未启用或配置不完整，跳过写入。")
            return True

        if not records:
            logger.info("[飞书] 没有记录可写入。")
            return True

        logger.info(f"[飞书] 开始写入，共 {len(records)} 条记录。")

        # 类型与字段映射
        if schema:
            records = self._fix_data_types(records, schema)
        else:
            logger.warning("[飞书] 未提供 schema，将直接使用原始键值（成功率可能较低）。")

        # 过滤掉完全无意义的记录（但保留 0/False）
        records = [r for r in records if r and any(_has_meaningful_value(x) for x in r.values())]
        if not records:
            logger.info("[飞书] 过滤后没有有效记录可写入。")
            return True

        # 获取 token
        try:
            token = await self.get_tenant_token()
            headers = {"Authorization": f"Bearer {token}"}
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"[飞书] 获取token失败，写入中止: {e}")
            return False

        # 分批
        chunks = [records[i:i + 50] for i in range(0, len(records), 50)]
        logger.info(f"[飞书] 数据分 {len(chunks)} 批写入，使用安全批次大小 50。")

        # 调试：打印第一条记录
        if chunks and chunks[0]:
            sample_fields = list(chunks[0][0].keys())
            logger.info(f"[飞书调试] 第一条记录字段数: {len(sample_fields)}, 前5个: {sample_fields[:5]}")
            logger.info(f"[飞书调试] 第一条记录样本: {json.dumps(chunks[0][0], ensure_ascii=False)[:600]}")

        total_success_count = 0
        total_failed_count = 0

        async with httpx.AsyncClient() as client:
            for i, chunk in enumerate(chunks):
                payload = {"records": [{"fields": item} for item in chunk]}
                logger.info(f"[飞书调试] 第{i+1}批payload: {json.dumps(payload, ensure_ascii=False)[:800]}...")

                batch_done = False
                for attempt in range(3):
                    try:
                        resp = await client.post(self.base_url, json=payload, headers=headers, timeout=60)
                        raw = resp.text
                        logger.debug(f"[飞书调试] 第{i+1}批 HTTP {resp.status_code} body: {raw[:2000]}")

                        if resp.status_code == 200:
                            try:
                                result = resp.json()
                            except Exception:
                                logger.error(f"[飞书] 第{i+1}批 响应非JSON：{raw[:400]}")
                                # 计作整批失败尝试重试
                                pass
                            else:
                                code, msg, added_records, failures = _parse_bitable_result(result)
                                if code != 0:
                                    logger.error(f"[飞书] 第{i+1}批 业务失败 code={code}, msg={msg}")
                                    # 业务整体失败，整批计失败
                                    total_failed_count += len(chunk)
                                    break

                                succ = len(added_records)
                                fail = len(failures)
                                total_success_count += succ
                                total_failed_count += fail
                                logger.info(f"[飞书] 第 {i+1}/{len(chunks)} 批: 成功 {succ}/{len(chunk)}，失败 {fail}。")
                                if (succ + fail) != len(chunk):
                                    logger.warning(f"[飞书] 第{i+1}批响应条数不一致：返回 {succ}+{fail} != 发送 {len(chunk)}。")

                                batch_done = True
                                break

                        elif resp.status_code in (400, 404, 413):
                            logger.error(f"[飞书] 第{i+1}批 不可恢复HTTP错误: {resp.status_code} - {raw[:800]}")
                            total_failed_count += len(chunk)
                            batch_done = True
                            break
                        else:
                            logger.warning(f"[飞书] 第{i+1}批 HTTP失败(尝试 {attempt+1}/3): {resp.status_code} - {raw[:600]}")

                    except (httpx.RequestError, httpx.TimeoutException) as e:
                        logger.warning(f"[飞书] 第{i+1}批 网络错误(尝试 {attempt+1}/3): {e}")

                    if attempt < 2:
                        wait_time = 2 ** (attempt + 1)
                        logger.info(f"[飞书] 第{i+1}批 将在 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)

                if not batch_done:
                    logger.error(f"[飞书] 第{i+1}批在3次尝试后失败，将逐条写入以获取明细。")
                    s_count, f_count = await self._write_single_records(client, chunk, headers)
                    total_success_count += s_count
                    total_failed_count += f_count
                    logger.info(f"[飞书] 第 {i+1} 批逐条完成: 成功 {s_count} 条, 失败 {f_count} 条。")

        logger.info(f"[飞书] 写入总结: 总记录数 {len(records)}，成功 {total_success_count} 条，失败 {total_failed_count} 条。")
        return total_failed_count == 0

    async def _write_single_records(self, client: httpx.AsyncClient, records: List[Dict[str, Any]], headers: Dict[str, str]) -> Tuple[int, int]:
        success_count = 0
        failed_count = 0
        single_record_url = self.base_url.replace("/batch_create", "")

        for idx, record in enumerate(records, start=1):
            if not record or not any(_has_meaningful_value(v) for v in record.values()):
                failed_count += 1
                continue

            single_payload = {"fields": record}
            try:
                single_resp = await client.post(single_record_url, json=single_payload, headers=headers, timeout=30)
                raw = single_resp.text

                if single_resp.status_code == 200:
                    try:
                        result = single_resp.json()
                    except Exception:
                        logger.error(f"[飞书] 单条响应非JSON：{raw[:400]} | 数据: {json.dumps(record, ensure_ascii=False)[:400]}")
                        failed_count += 1
                        continue

                    code, msg, recs, fails = _parse_bitable_result(result)
                    if code == 0 and recs:
                        success_count += 1
                    else:
                        logger.warning(f"[飞书] 单条业务失败 code={code}, msg={msg}, fails={len(fails)} | data={raw[:600]}")
                        failed_count += 1
                elif single_resp.status_code in (400, 404, 413):
                    logger.error(f"[飞书] 单条不可恢复HTTP错误 {single_resp.status_code}: {raw[:800]} | 数据: {json.dumps(record, ensure_ascii=False)[:400]}")
                    failed_count += 1
                else:
                    logger.warning(f"[飞书] 单条HTTP失败 {single_resp.status_code}: {raw[:600]} | 数据: {json.dumps(record, ensure_ascii=False)[:300]}")
                    failed_count += 1

            except Exception as e:
                failed_count += 1
                logger.error(f"[飞书] 单条写入异常: {e} | 数据: {json.dumps(record, ensure_ascii=False)[:400]}")

        return success_count, failed_count
