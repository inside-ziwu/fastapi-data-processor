#!/usr/bin/env python3
"""Update field mappings with missing Feishu fields."""

from src.config.field_mappings import FIELD_MAPPINGS
import json

def update_field_mappings():
    """Add missing field mappings based on Feishu API response."""
    
    # Missing mappings to add
    new_mappings = {
        # 综合CPL指标
        "total_cpl": ["车云店+区域综合CPL"],
        
        # 本地线索占比
        "local_leads_ratio": ["本地线索占比"],
        
        # 直播日均消耗相关
        "avg_daily_spending_paid_area": ["直播车云店➕区域日均消耗"],
        "avg_daily_spending_paid_area_t": ["T月直播车云店➕区域日均消耗"],
        "avg_daily_spending_paid_area_t_minus_1": ["T-1月直播车云店➕区域日均消耗"],
        
        # 直播付费线索量
        "paid_area_leads_total": ["直播车云店➕区域付费线索量"],
        "paid_area_leads_t": ["T月直播车云店➕区域付费线索量"],
        "paid_area_leads_t_minus_1": ["T-1月直播车云店➕区域付费线索量"],
        
        # 有效直播时长 (25min以上)
        "avg_daily_effective_live_hours_25min": ["日均有效（25min以上）时长（h）"],
        "avg_daily_effective_live_hours_25min_t": ["T月日均有效（25min以上）时长（h）"],
        "avg_daily_effective_live_hours_25min_t_minus_1": ["T-1月日均有效（25min以上）时长（h）"],
        
        # 场均指标
        "avg_exposures_per_session": ["场均曝光人数"],
        "avg_exposures_per_session_t": ["T月场均曝光人数"],
        "avg_exposures_per_session_t_minus_1": ["T-1月场均曝光人数"],
        
        # 曝光进入率
        "exposure_to_viewer_rate": ["曝光进入率"],
        "exposure_to_viewer_rate_t": ["T月曝光进入率"],
        "exposure_to_viewer_rate_t_minus_1": ["T-1月曝光进入率"],
        
        # 场均场观
        "avg_viewers_per_session": ["场均场观"],
        "avg_viewers_per_session_t": ["T月场均场观"],
        "avg_viewers_per_session_t_minus_1": ["T-1月场均场观"],
        
        # 场均小风车点击
        "avg_small_wheel_clicks_per_session": ["场均小风车点击次数"],
        "avg_small_wheel_clicks_per_session_t": ["T月场均小风车点击次数"],
        "avg_small_wheel_clicks_per_session_t_minus_1": ["T-1月场均小风车点击次数"],
        
        # 小风车点击率
        "small_wheel_click_rate": ["小风车点击率"],
        "small_wheel_click_rate_t": ["T月小风车点击率"],
        "small_wheel_click_rate_t_minus_1": ["T-1月小风车点击率"],
        
        # 场均小风车留资
        "avg_small_wheel_leads_per_session": ["场均小风车留资量"],
        "avg_small_wheel_leads_per_session_t": ["T月场均小风车留资量"],
        "avg_small_wheel_leads_per_session_t_minus_1": ["T-1月场均小风车留资量"],
        
        # 小风车点击留资率
        "small_wheel_leads_rate": ["小风车点击留资率"],
        "small_wheel_leads_rate_t": ["T月小风车点击留资率"],
        "small_wheel_leads_rate_t_minus_1": ["T-1月小风车点击留资率"],
        
        # 直播线索量
        "live_leads_total": ["直播线索量"],
        "live_leads_t": ["T月直播线索量"],
        "live_leads_t_minus_1": ["T-1月直播线索量"],
        
        # 锚点曝光量
        "anchor_exposure_total": ["锚点曝光量"],
        "anchor_exposure_t": ["T月锚点曝光量"],
        "anchor_exposure_t_minus_1": ["T-1月锚点曝光量"],
        
        # 组件点击次数
        "component_clicks_total": ["组件点击次数"],
        "component_clicks_t": ["T月组件点击次数"],
        "component_clicks_t_minus_1": ["T-1月组件点击次数"],
        
        # 组件点击率
        "component_click_rate": ["组件点击率"],
        "component_click_rate_t": ["T月组件点击率"],
        "component_click_rate_t_minus_1": ["T-1月组件点击率"],
        
        # 组件留资人数
        "component_leads_total": ["组件留资人数（获取线索量）"],
        "component_leads_t": ["T月组件留资人数（获取线索量）"],
        "component_leads_t_minus_1": ["T-1月组件留资人数（获取线索量）"],
        
        # 组件留资率
        "component_leads_rate": ["组件留资率"],
        "component_leads_rate_t": ["T月组件留资率"],
        "component_leads_rate_t_minus_1": ["T-1月组件留资率"],
        
        # 私信相关日均指标
        "avg_daily_private_entry_count": ["日均进私人数"],
        "avg_daily_private_entry_count_t": ["T月日均进私人数"],
        "avg_daily_private_entry_count_t_minus_1": ["T-1月日均进私人数"],
        
        "avg_daily_private_open_count": ["日均私信开口人数"],
        "avg_daily_private_open_count_t": ["T月日均私信开口人数"],
        "avg_daily_private_open_count_t_minus_1": ["T-1月日均私信开口人数"],
        
        # 私信咨询率
        "private_open_rate": ["私信咨询率=开口|进私"],
        "private_open_rate_t": ["T月私信咨询率=开口|进私"],
        "private_open_rate_t_minus_1": ["T-1月私信咨询率=开口|进私"],
        
        # 咨询留资相关
        "avg_daily_private_leads_count": ["日均咨询留资人数"],
        "avg_daily_private_leads_count_t": ["T月日均咨询留资人数"],
        "avg_daily_private_leads_count_t_minus_1": ["T-1月日均咨询留资人数"],
        
        # 咨询留资率
        "private_leads_rate": ["咨询留资率=留资|咨询"],
        "private_leads_rate_t": ["T月咨询留资率=留资|咨询"],
        "private_leads_rate_t_minus_1": ["T-1月咨询留资率=留资|咨询"],
        
        # 私信转化率
        "private_conversion_rate": ["私信转化率=留资|进私"],
        "private_conversion_rate_t": ["T月私信转化率=留资|进私"],
        "private_conversion_rate_t_minus_1": ["T-1月私信转化率=留资|进私"],
    }
    
    # Update the FIELD_MAPPINGS
    FIELD_MAPPINGS.update(new_mappings)
    
    print("=== 映射表更新完成 ===")
    print(f"新增 {len(new_mappings)} 个字段映射")
    print(f"总计 {len(FIELD_MAPPINGS)} 个字段映射")
    
    # Save updated mappings
    with open('/Users/lay/Documents/项目开发/fastapi-data-processor/updated_field_mappings.json', 'w', encoding='utf-8') as f:
        json.dump(FIELD_MAPPINGS, f, ensure_ascii=False, indent=2)
    
    return new_mappings

if __name__ == "__main__":
    new_mappings = update_field_mappings()