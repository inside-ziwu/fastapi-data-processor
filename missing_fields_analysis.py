#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析飞书schema中缺失的16个字段与FIELD_EN_MAP映射表的差异
"""

# 飞书schema中缺失的16个字段
missing_fields = [
    'T-1月咨询留资率=留资|咨询', 
    'T-1月直播车云店➕区域付费线索量', 
    'T-1月直播车云店➕区域日均消耗', 
    'T-1月私信咨询率=开口|进私', 
    'T-1月私信转化率=留资|进私', 
    'T月咨询留资率=留资|咨询', 
    'T月直播车云店➕区域付费线索量', 
    'T月直播车云店➕区域日均消耗', 
    'T月私信咨询率=开口|进私', 
    'T月私信转化率=留资|进私', 
    '咨询留资率=留资|咨询', 
    '直播付费CPL', 
    '直播车云店➕区域付费线索量', 
    '直播车云店➕区域日均消耗', 
    '私信咨询率=开口|进私', 
    '私信转化率=留资|进私'
]

# data_processor.py中的FIELD_EN_MAP映射表
field_en_map = {
    "经销商ID": "NSC_CODE",
    "层级": "level",
    "门店名": "store_name",
    "自然线索量": "natural_leads_total",
    "T月自然线索量": "natural_leads_t",
    "T-1月自然线索量": "natural_leads_t_minus_1",
    "付费线索量": "ad_leads_total",
    "T月付费线索量": "ad_leads_t",
    "T-1月付费线索量": "ad_leads_t_minus_1",
    "车云店+区域投放总金额": "spending_net_total",
    "T月车云店+区域投放总金额": "spending_net_t",
    "T-1月车云店+区域投放总金额": "spending_net_t_minus_1",
    "车云店付费线索量": "paid_leads_total",
    "T月车云店付费线索量": "paid_leads_t",
    "T-1月车云店付费线索量": "paid_leads_t_minus_1",
    "区域线索量": "area_leads_total",
    "T月区域线索量": "area_leads_t",
    "T-1月区域线索量": "area_leads_t_minus_1",
    "本地线索量": "local_leads_total",
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
    "直播线索量": "live_leads_total",
    "T月直播线索量": "live_leads_t",
    "T-1月直播线索量": "live_leads_t_minus_1",
    "锚点曝光量": "anchor_exposure_total",
    "T月锚点曝光量": "anchor_exposure_t",
    "T-1月锚点曝光量": "anchor_exposure_t_minus_1",
    "组件点击次数": "component_clicks_total",
    "T月组件点击次数": "component_clicks_t",
    "T-1月组件点击次数": "component_clicks_t_minus_1",
    "组件留资人数（获取线索量）": "short_video_leads_total",
    "T月组件留资人数（获取线索量）": "short_video_leads_t",
    "T-1月组件留资人数（获取线索量）": "short_video_leads_t_minus_1",
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
    "付费CPL（车云店+区域）": "paid_cpl",
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
    "场均小风车点击次数": "avg_small_wheel_clicks_per_session",
    "T月场均小风车点击次数": "avg_small_wheel_clicks_per_session_t",
    "T-1月场均小风车点击次数": "avg_small_wheel_clicks_per_session_t_minus_1",
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
    "私信咨询率=开口/进私": "private_open_rate",
    "T月私信咨询率=开口/进私": "private_open_rate_t",
    "T-1月私信咨询率=开口/进私": "private_open_rate_t_minus_1",
    "咨询留资率=留资/咨询": "private_leads_rate",
    "T月咨询留资率=留资/咨询": "private_leads_rate_t",
    "T-1月咨询留资率=留资/咨询": "private_leads_rate_t_minus_1",
    "私信转化率=留资/进私": "private_conversion_rate",
    "T月私信转化率=留资/进私": "private_conversion_rate_t",
    "T-1月私信转化率=留资/进私": "private_conversion_rate_t_minus_1"
}

print("=== 飞书schema缺失字段分析 ===")
print()

# 分析每个缺失字段
for field in missing_fields:
    print(f"缺失字段: {field}")
    
    # 检查是否在FIELD_EN_MAP中存在
    found = False
    for cn_name, en_name in field_en_map.items():
        if field == cn_name:
            print(f"  ✅ 完全匹配: {cn_name} -> {en_name}")
            found = True
            break
    
    if not found:
        print(f"  ❌ 完全未匹配")
        
        # 分析可能的匹配模式
        if "咨询留资率" in field and "=留资|咨询" in field:
            print(f"  🔍 模式分析: 咨询留资率字段，但分隔符为'|'而非'/'")
        elif "私信咨询率" in field and "=开口|进私" in field:
            print(f"  🔍 模式分析: 私信咨询率字段，但分隔符为'|'而非'/'")
        elif "私信转化率" in field and "=留资|进私" in field:
            print(f"  🔍 模式分析: 私信转化率字段，但分隔符为'|'而非'/'")
        elif "直播车云店➕区域" in field:
            print(f"  🔍 模式分析: 包含特殊字符'➕'而非普通'+'")
        elif "直播付费CPL" in field:
            print(f"  🔍 模式分析: 可能是'付费CPL（车云店+区域）'的简化版本")
    
    print()

print("=== 总结分析 ===")
print()

# 分类统计
rate_fields = [f for f in missing_fields if "率" in f]
plus_fields = [f for f in missing_fields if "➕" in f]
cpl_fields = [f for f in missing_fields if "CPL" in f]
other_fields = [f for f in missing_fields if f not in rate_fields + plus_fields + cpl_fields]

print(f"费率类字段 ({len(rate_fields)}个):")
for field in rate_fields:
    print(f"  - {field}")
print()

print(f"特殊字符类字段 ({len(plus_fields)}个):")
for field in plus_fields:
    print(f"  - {field}")
print()

print(f"CPL类字段 ({len(cpl_fields)}个):")
for field in cpl_fields:
    print(f"  - {field}")
print()

print(f"其他字段 ({len(other_fields)}个):")
for field in other_fields:
    print(f"  - {field}")
print()

print("=== 修正建议 ===")
print()
print("1. 分隔符问题：")
print("   - 飞书使用'|'作为分隔符，但映射表使用'/'")
print("   - 建议：在field_match函数中统一处理这两种分隔符")
print()
print("2. 特殊字符问题：")
print("   - 飞书使用'➕'，但映射表使用'+'")
print("   - 建议：在normalize_symbol函数中添加'➕'->'+'的映射")
print()
print("3. 字段命名差异：")
print("   - '直播付费CPL' vs '付费CPL（车云店+区域）'")
print("   - 建议：添加别名映射或模糊匹配")
print()
print("4. 建议的映射补充：")
suggested_mappings = {
    "T-1月咨询留资率=留资|咨询": "private_leads_rate_t_minus_1",
    "T-1月直播车云店➕区域付费线索量": "paid_leads_t_minus_1",  # 需要确认
    "T-1月直播车云店➕区域日均消耗": "avg_daily_spending_t_minus_1",  # 需要确认
    "T-1月私信咨询率=开口|进私": "private_open_rate_t_minus_1",
    "T-1月私信转化率=留资|进私": "private_conversion_rate_t_minus_1",
    "T月咨询留资率=留资|咨询": "private_leads_rate_t",
    "T月直播车云店➕区域付费线索量": "paid_leads_t",  # 需要确认
    "T月直播车云店➕区域日均消耗": "avg_daily_spending_t",  # 需要确认
    "T月私信咨询率=开口|进私": "private_open_rate_t",
    "T月私信转化率=留资|进私": "private_conversion_rate_t",
    "咨询留资率=留资|咨询": "private_leads_rate",
    "直播付费CPL": "paid_cpl",
    "直播车云店➕区域付费线索量": "paid_leads_total",  # 需要确认
    "直播车云店➕区域日均消耗": "avg_daily_spending",  # 需要确认
    "私信咨询率=开口|进私": "private_open_rate",
    "私信转化率=留资|进私": "private_conversion_rate"
}

for cn, en in suggested_mappings.items():
    print(f'    "{cn}": "{en}",')