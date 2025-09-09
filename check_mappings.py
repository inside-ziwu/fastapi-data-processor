#!/usr/bin/env python3
"""Check field mappings against Feishu API response."""

import json
import logging
from src.config.source_mappings import FIELD_MAPPINGS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Feishu API response data
feishu_fields = [
    "层级", "自然线索量", "付费线索量", "车云店+区域投放总金额", "车云店+区域综合CPL",
    "付费CPL（车云店+区域）", "本地线索占比", "直播车云店+区域日均消耗", "T月直播车云店+区域日均消耗",
    "T-1月直播车云店+区域日均消耗", "直播车云店+区域付费线索量", "T月直播车云店+区域付费线索量",
    "T-1月直播车云店+区域付费线索量", "直播付费CPL", "T月直播付费CPL", "T-1月直播付费CPL",
    "日均有效（25min以上）时长（h）", "T月日均有效（25min以上）时长（h）", "T-1月日均有效（25min以上）时长（h）",
    "场均曝光人数", "T月场均曝光人数", "T-1月场均曝光人数", "曝光进入率", "T月曝光进入率", "T-1月曝光进入率",
    "场均场观", "T月场均场观", "T-1月场均场观", "场均小风车点击次数", "T月场均小风车点击次数",
    "T-1月场均小风车点击次数", "小风车点击率", "T月小风车点击率", "T-1月小风车点击率",
    "场均小风车留资量", "T月场均小风车留资量", "T-1月场均小风车留资量", "小风车点击留资率",
    "T月小风车点击留资率", "T-1月小风车点击留资率", "直播线索量", "T月直播线索量", "T-1月直播线索量",
    "锚点曝光量", "T月锚点曝光量", "T-1月锚点曝光量", "组件点击次数", "T月组件点击次数", "T-1月组件点击次数",
    "组件点击率", "T月组件点击率", "T-1月组件点击率", "组件留资人数（获取线索量）", "T月组件留资人数（获取线索量）",
    "T-1月组件留资人数（获取线索量）", "组件留资率", "T月组件留资率", "T-1月组件留资率",
    "日均进私人数", "T月日均进私人数", "T-1月日均进私人数", "日均私信开口人数", "T月日均私信开口人数",
    "T-1月日均私信开口人数", "私信咨询率=开口|进私", "T月私信咨询率=开口|进私", "T-1月私信咨询率=开口|进私",
    "日均咨询留资人数", "T月日均咨询留资人数", "T-1月日均咨询留资人数", "咨询留资率=留资|咨询",
    "T月咨询留资率=留资|咨询", "T-1月咨询留资率=留资|咨询", "私信转化率=留资|进私", "T月私信转化率=留资|进私",
    "T-1月私信转化率=留资|进私"
]

def check_mappings():
    """Check which Feishu fields are missing from our mappings."""
    
    # 标准化函数（统一+和➕符号，|和/符号）
    def normalize_text(text):
        return text.replace('➕', '+').replace('|', '/')
    
    # Build reverse mapping (Chinese -> English) with normalization
    chinese_to_english = {}
    for english_name, chinese_list in FIELD_MAPPINGS.items():
        for chinese_name in chinese_list:
            normalized = normalize_text(chinese_name)
            chinese_to_english[normalized] = english_name
    
    logger.info("=== 字段映射检查 ===")
    logger.info(f"当前映射表中有 {len(FIELD_MAPPINGS)} 个英文字段")
    logger.info(f"飞书API返回了 {len(feishu_fields)} 个中文字段")
    
    missing_fields = []
    found_fields = []
    
    for field in feishu_fields:
        normalized_field = normalize_text(field)
        if normalized_field in chinese_to_english:
            found_fields.append(field)
        else:
            missing_fields.append(field)
    
    logger.info(f"\n✅ 已映射字段: {len(found_fields)} 个")
    logger.info(f"❌ 缺失字段: {len(missing_fields)} 个")
    
    if missing_fields:
        logger.info(f"\n=== 缺失字段列表 ===")
        for i, field in enumerate(missing_fields, 1):
            logger.info(f"{i:2d}. {field}")
    
    if found_fields:
        logger.info(f"\n=== 示例已映射字段 ===")
        for i, field in enumerate(found_fields[:10], 1):
            normalized_field = normalize_text(field)
            english_name = chinese_to_english[normalized_field]
            logger.info(f"{i:2d}. {field} -> {english_name}")
    
    return missing_fields

if __name__ == "__main__":
    missing = check_mappings()
    if missing:
        logger.info(f"\n🔧 建议添加这些缺失的映射")
    else:
        logger.info(f"\n✅ 所有字段都已正确映射！")
