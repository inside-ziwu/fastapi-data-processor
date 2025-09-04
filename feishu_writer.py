import httpx
import asyncio
import json
from typing import List, Dict
import logging

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
                            field_type = field.get("type")
                            options = []
                            
                            if field_type in [3, 4]: # 3: 单选, 4: 多选
                                options = [opt["name"] for opt in field.get("property", {}).get("options", [])]

                            if field_name:
                                simplified_schema[field_name] = {
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

    async def write_records(self, records: List[Dict], schema: Dict = None, field_mapping: Dict = None) -> bool:
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
        不修改原始数据。
        """
        if not schema or not records:
            return records
            
        # 打印飞书表格实际字段名
        logger.info("[飞书] 飞书表格实际字段名:")
        for field_name, field_info in schema.items():
            field_type = field_info.get('type', 'unknown')
            field_title = field_info.get('name', field_name)  # 显示名称
            logger.info(f"[飞书]   '{field_name}' [类型:{field_type}] 标题:'{field_title}'")
            
        # 显示数据中的字段名对比
        if records:
            data_fields = set(records[0].keys())
            logger.info(f"[飞书] 数据中的字段名: {list(data_fields)}")
            logger.info(f"[飞书] 匹配的字段: {list(data_fields & set(schema.keys()))}")
            logger.info(f"[飞书] 不匹配的字段: {list(data_fields - set(schema.keys()))}")
            
# 强制打印所有字段信息
        logger.error("=" * 80)
        logger.error("[飞书] 紧急调试：字段名不匹配")
        logger.error("=" * 80)
        
        logger.error("[飞书] 飞书表格实际字段名:")
        for field_name, field_info in schema.items():
            field_type = field_info.get('type', 'unknown')
            field_title = field_info.get('name', field_name)
            logger.error(f"[飞书]   飞书字段: '{field_name}' [类型:{field_type}] 显示名:'{field_title}'")
            
        if records:
            data_fields = set(records[0].keys())
            schema_fields = set(schema.keys())
            logger.error(f"[飞书] 数据中文字段: {sorted(list(data_fields))}")
            logger.error(f"[飞书] 飞书实际字段: {sorted(list(schema_fields))}")
            
            # 使用字段映射或尝试匹配
            field_mapping = {
                '层级': 'field_1',
                '自然线索总量': 'field_2',
                'T月自然线索量': 'field_3',
                'T-1月自然线索量': 'field_4',
                '广告线索总量': 'field_5',
                'T月广告线索量': 'field_6',
                'T-1月广告线索量': 'field_7',
                '总消耗': 'field_8',
                'T月消耗': 'field_9',
                'T-1月消耗': 'field_10',
                '付费线索总量': 'field_11',
                'T月付费线索量': 'field_12',
                'T-1月付费线索量': 'field_13',
                '区域线索总量': 'field_14',
                'T月区域线索量': 'field_15',
                'T-1月区域线索量': 'field_16',
                '本地线索总量': 'field_17',
                'T月本地线索量': 'field_18',
                'T-1月本地线索量': 'field_19',
                '有效直播时长总量(小时)': 'field_20',
                'T月有效直播时长(小时)': 'field_21',
                'T-1月有效直播时长(小时)': 'field_22',
                '有效直播场次总量': 'field_23',
                'T月有效直播场次': 'field_24',
                'T-1月有效直播场次': 'field_25',
                '总曝光人数': 'field_26',
                'T月曝光人数': 'field_27',
                'T-1月曝光人数': 'field_28',
                '总场观': 'field_29',
                'T月场观': 'field_30',
                'T-1月场观': 'field_31',
                '小风车点击总量': 'field_32',
                'T月小风车点击': 'field_33',
                'T-1月小风车点击': 'field_34',
                '小风车留资总量': 'field_35',
                'T月小风车留资': 'field_36',
                'T-1月小风车留资': 'field_37',
                '直播线索总量': 'field_38',
                'T月直播线索': 'field_39',
                'T-1月直播线索': 'field_40',
                '锚点曝光总量': 'field_41',
                'T月锚点曝光': 'field_42',
                'T-1月锚点曝光': 'field_43',
                '组件点击总量': 'field_44',
                'T月组件点击': 'field_45',
                'T-1月组件点击': 'field_46',
                '短视频留资总量': 'field_47',
                'T月短视频留资': 'field_48',
                'T-1月短视频留资': 'field_49',
                '短视频发布总量': 'field_50',
                'T月短视频发布': 'field_51',
                'T-1月短视频发布': 'field_52',
                '短视频播放总量': 'field_53',
                'T月短视频播放': 'field_54',
                'T-1月短视频播放': 'field_55',
                '进私总量': 'field_56',
                'T月进私': 'field_57',
                'T-1月进私': 'field_58',
                '私信开口总量': 'field_59',
                'T月私信开口': 'field_60',
                'T-1月私信开口': 'field_61',
                '私信留资总量': 'field_62',
                'T月私信留资': 'field_63',
                'T-1月私信留资': 'field_64',
                '车云店+区域综合CPL': 'field_65',
                '付费CPL（车云店+区域）': 'field_66',
                '本地线索占比': 'field_67',
                '直播车云店+区域日均消耗': 'field_68',
                'T月直播车云店+区域日均消耗': 'field_69',
                'T-1月直播车云店+区域日均消耗': 'field_70',
                '直播车云店+区域付费线索量日均': 'field_71',
                'T月直播车云店+区域付费线索量日均': 'field_72',
                'T-1月直播车云店+区域付费线索量日均': 'field_73',
                'T月直播付费CPL': 'field_74',
                'T-1月直播付费CPL': 'field_75',
                '日均有效（25min以上）时长（h）': 'field_76',
                'T月日均有效（25min以上）时长（h）': 'field_77',
                'T-1月日均有效（25min以上）时长（h）': 'field_78',
                '场均曝光人数': 'field_79',
                'T月场均曝光人数': 'field_80',
                'T-1月场均曝光人数': 'field_81',
                '曝光进入率': 'field_82',
                'T月曝光进入率': 'field_83',
                'T-1月曝光进入率': 'field_84',
                '场均场观': 'field_85',
                'T月场均场观': 'field_86',
                'T-1月场均场观': 'field_87',
                '小风车点击率': 'field_88',
                'T月小风车点击率': 'field_89',
                'T-1月小风车点击率': 'field_90',
                '小风车点击留资率': 'field_91',
                'T月小风车点击留资率': 'field_92',
                'T-1月小风车点击留资率': 'field_93',
                '场均小风车留资量': 'field_94',
                'T月场均小风车留资量': 'field_95',
                'T-1月场均小风车留资量': 'field_96',
                '组件点击率': 'field_97',
                'T月组件点击率': 'field_98',
                'T-1月组件点击率': 'field_99',
                '组件留资率': 'field_100',
                'T月组件留资率': 'field_101',
                'T-1月组件留资率': 'field_102',
                '日均进私人数': 'field_103',
                'T月日均进私人数': 'field_104',
                'T-1月日均进私人数': 'field_105',
                '日均私信开口人数': 'field_106',
                'T月日均私信开口人数': 'field_107',
                'T-1月日均私信开口人数': 'field_108',
                '日均咨询留资人数': 'field_109',
                'T月日均咨询留资人数': 'field_110',
                'T-1月日均咨询留资人数': 'field_111',
                '私信咨询率': 'field_112',
                'T月私信咨询率': 'field_113',
                'T-1月私信咨询率': 'field_114',
                '咨询留资率': 'field_115',
                'T月咨询留资率': 'field_116',
                'T-1月咨询留资率': 'field_117',
                '私信转化率': 'field_118',
                'T月私信转化率': 'field_119',
                'T-1月私信转化率': 'field_120'
            }
            
            # 使用映射创建新记录
            fixed_records = []
            for record in records:
                if not record:
                    continue
                    
                fixed_record = {}
                for data_field, value in record.items():
                    if data_field in field_mapping:
                        target_field = field_mapping[data_field]
                        if target_field in schema:
                            # 获取字段类型并转换
                            field_schema = schema[target_field]
                            field_type = field_schema.get('type', 'text')
                            
                            try:
                                if field_type == 'number' and value is not None:
                                    if isinstance(value, (int, float)):
                                        fixed_record[target_field] = value
                                    elif isinstance(value, str) and value.strip():
                                        fixed_record[target_field] = float(value.strip())
                                    else:
                                        fixed_record[target_field] = 0
                                elif field_type == 'text' and value is not None:
                                    fixed_record[target_field] = str(value)
                                elif field_type == 'url' and value is not None:
                                    fixed_record[target_field] = str(value).strip()
                                elif field_type == 'checkbox' and value is not None:
                                    if isinstance(value, bool):
                                        fixed_record[target_field] = value
                                    elif isinstance(value, str):
                                        str_value = value.strip().lower()
                                        fixed_record[target_field] = str_value in ['true', '1', 'yes', '是', 'on']
                                    else:
                                        fixed_record[target_field] = bool(value)
                                else:
                                    fixed_record[target_field] = value
                            except (ValueError, TypeError):
                                logger.warning(f"[飞书] 字段转换失败: {data_field}({value}) -> {target_field}({field_type})")
                                fixed_record[target_field] = value
                        else:
                            logger.warning(f"[飞书] 映射目标字段不在schema中: {target_field}")
                    else:
                        logger.warning(f"[飞书] 无映射的中文字段: {data_field}")
                
                if fixed_record:  # 确保有有效字段
                    fixed_records.append(fixed_record)
            
            logger.info(f"[飞书] 字段映射完成，从 {len(records)} 条记录映射为 {len(fixed_records)} 条")
            return fixed_records
        
        # 使用硬编码映射
        field_mapping = {
            '层级': 'field_1',
            '自然线索总量': 'field_2',
            'T月自然线索量': 'field_3',
            'T-1月自然线索量': 'field_4',
            '广告线索总量': 'field_5',
            'T月广告线索量': 'field_6',
            'T-1月广告线索量': 'field_7',
            '总消耗': 'field_8',
            'T月消耗': 'field_9',
            'T-1月消耗': 'field_10',
            '付费线索总量': 'field_11',
            'T月付费线索量': 'field_12',
            'T-1月付费线索量': 'field_13',
            '区域线索总量': 'field_14',
            'T月区域线索量': 'field_15',
            'T-1月区域线索量': 'field_16',
            '本地线索总量': 'field_17',
            'T月本地线索量': 'field_18',
            'T-1月本地线索量': 'field_19',
            '有效直播时长总量(小时)': 'field_20',
            'T月有效直播时长(小时)': 'field_21',
            'T-1月有效直播时长(小时)': 'field_22',
            '有效直播场次总量': 'field_23',
            'T月有效直播场次': 'field_24',
            'T-1月有效直播场次': 'field_25',
            '总曝光人数': 'field_26',
            'T月曝光人数': 'field_27',
            'T-1月曝光人数': 'field_28',
            '总场观': 'field_29',
            'T月场观': 'field_30',
            'T-1月场观': 'field_31',
            '小风车点击总量': 'field_32',
            'T月小风车点击': 'field_33',
            'T-1月小风车点击': 'field_34',
            '小风车留资总量': 'field_35',
            'T月小风车留资': 'field_36',
            'T-1月小风车留资': 'field_37',
            '直播线索总量': 'field_38',
            'T月直播线索': 'field_39',
            'T-1月直播线索': 'field_40',
            '锚点曝光总量': 'field_41',
            'T月锚点曝光': 'field_42',
            'T-1月锚点曝光': 'field_43',
            '组件点击总量': 'field_44',
            'T月组件点击': 'field_45',
            'T-1月组件点击': 'field_46',
            '短视频留资总量': 'field_47',
            'T月短视频留资': 'field_48',
            'T-1月短视频留资': 'field_49',
            '短视频发布总量': 'field_50',
            'T月短视频发布': 'field_51',
            'T-1月短视频发布': 'field_52',
            '短视频播放总量': 'field_53',
            'T月短视频播放': 'field_54',
            'T-1月短视频播放': 'field_55',
            '进私总量': 'field_56',
            'T月进私': 'field_57',
            'T-1月进私': 'field_58',
            '私信开口总量': 'field_59',
            'T月私信开口': 'field_60',
            'T-1月私信开口': 'field_61',
            '私信留资总量': 'field_62',
            'T月私信留资': 'field_63',
            'T-1月私信留资': 'field_64',
            '车云店+区域综合CPL': 'field_65',
            '付费CPL（车云店+区域）': 'field_66',
            '本地线索占比': 'field_67',
            '直播车云店+区域日均消耗': 'field_68',
            'T月直播车云店+区域日均消耗': 'field_69',
            'T-1月直播车云店+区域日均消耗': 'field_70',
            '直播车云店+区域付费线索量日均': 'field_71',
            'T月直播车云店+区域付费线索量日均': 'field_72',
            'T-1月直播车云店+区域付费线索量日均': 'field_73',
            'T月直播付费CPL': 'field_74',
            'T-1月直播付费CPL': 'field_75',
            '日均有效（25min以上）时长（h）': 'field_76',
            'T月日均有效（25min以上）时长（h）': 'field_77',
            'T-1月日均有效（25min以上）时长（h）': 'field_78',
            '场均曝光人数': 'field_79',
            'T月场均曝光人数': 'field_80',
            'T-1月场均曝光人数': 'field_81',
            '曝光进入率': 'field_82',
            'T月曝光进入率': 'field_83',
            'T-1月曝光进入率': 'field_84',
            '场均场观': 'field_85',
            'T月场均场观': 'field_86',
            'T-1月场均场观': 'field_87',
            '小风车点击率': 'field_88',
            'T月小风车点击率': 'field_89',
            'T-1月小风车点击率': 'field_90',
            '小风车点击留资率': 'field_91',
            'T月小风车点击留资率': 'field_92',
            'T-1月小风车点击留资率': 'field_93',
            '场均小风车留资量': 'field_94',
            'T月场均小风车留资量': 'field_95',
            'T-1月场均小风车留资量': 'field_96',
            '组件点击率': 'field_97',
            'T月组件点击率': 'field_98',
            'T-1月组件点击率': 'field_99',
            '组件留资率': 'field_100',
            'T月组件留资率': 'field_101',
            'T-1月组件留资率': 'field_102',
            '日均进私人数': 'field_103',
            'T月日均进私人数': 'field_104',
            'T-1月日均进私人数': 'field_105',
            '日均私信开口人数': 'field_106',
            'T月日均私信开口人数': 'field_107',
            'T-1月日均私信开口人数': 'field_108',
            '日均咨询留资人数': 'field_109',
            'T月日均咨询留资人数': 'field_110',
            'T-1月日均咨询留资人数': 'field_111',
            '私信咨询率': 'field_112',
            'T月私信咨询率': 'field_113',
            'T-1月私信咨询率': 'field_114',
            '咨询留资率': 'field_115',
            'T月咨询留资率': 'field_116',
            'T-1月咨询留资率': 'field_117',
            '私信转化率': 'field_118',
            'T月私信转化率': 'field_119',
            'T-1月私信转化率': 'field_120'
        }
        
        # 使用映射创建新记录
        fixed_records = []
        for record in records:
            if not record:
                continue
                
            fixed_record = {}
            for data_field, value in record.items():
                if data_field in field_mapping:
                    target_field = field_mapping[data_field]
                    if target_field in schema:
                        # 获取字段类型并转换
                        field_schema = schema[target_field]
                        field_type = field_schema.get('type', 'text')
                        
                        try:
                            if field_type == 'number' and value is not None:
                                if isinstance(value, (int, float)):
                                    fixed_record[target_field] = value
                                elif isinstance(value, str) and value.strip():
                                    fixed_record[target_field] = float(value.strip())
                                else:
                                    fixed_record[target_field] = 0
                            elif field_type == 'text' and value is not None:
                                fixed_record[target_field] = str(value)
                            elif field_type == 'url' and value is not None:
                                fixed_record[target_field] = str(value).strip()
                            elif field_type == 'checkbox' and value is not None:
                                if isinstance(value, bool):
                                    fixed_record[target_field] = value
                                elif isinstance(value, str):
                                    str_value = value.strip().lower()
                                    fixed_record[target_field] = str_value in ['true', '1', 'yes', '是', 'on']
                                else:
                                    fixed_record[target_field] = bool(value)
                            else:
                                fixed_record[target_field] = value
                        except (ValueError, TypeError):
                            logger.warning(f"[飞书] 字段转换失败: {data_field}({value}) -> {target_field}({field_type})")
                            fixed_record[target_field] = value
                    else:
                        logger.warning(f"[飞书] 映射目标字段不在schema中: {target_field}")
                else:
                    logger.warning(f"[飞书] 无映射的中文字段: {data_field}")
            
            if fixed_record:  # 确保有有效字段
                fixed_records.append(fixed_record)
        
        logger.info(f"[飞书] 字段映射完成，从 {len(records)} 条记录映射为 {len(fixed_records)} 条")
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
