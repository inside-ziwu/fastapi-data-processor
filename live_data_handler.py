# 假设 Args 和 Output 类型由 Coze 平台环境提供
# from typing import TypedDict, List, Dict, Any

# class Args(TypedDict):
#     params: Dict[str, Any]

# class Fields(TypedDict):
#     # ... 这里是所有中文字段的类型定义
#     pass

# class Record(TypedDict):
#     fields: Fields

# class Output(TypedDict):
#     records: List[Record]

async def main(args: Args) -> Output:
    """
    接收直播数据，提取所有指标到独立变量，然后构建一个扁平化的记录。
    """
    
    # 完整的字段映射表 - 英文键到中文显示名的映射
    FIELD_MAPPING = {
        # T/T-1 详细数据
        "ad_leads_t": "T月广告线索量",
        "ad_leads_t_minus_1": "T-1月广告线索量",
        "area_leads_t": "T月区域线索量",
        "area_leads_t_minus_1": "T-1月区域线索量",
        "live_leads_t": "T月直播线索",
        "live_leads_t_minus_1": "T-1月直播线索",
        "local_leads_t": "T月本地线索量",
        "local_leads_t_minus_1": "T-1月本地线索量",
        "natural_leads_t": "T月自然线索量",
        "natural_leads_t_minus_1": "T-1月自然线索量",
        "paid_leads_t": "T月付费线索量",
        "paid_leads_t_minus_1": "T-1月付费线索量",
        "spending_net_t": "T月消耗",
        "spending_net_t_minus_1": "T-1月消耗",
        "viewers_t": "T月场观",
        "viewers_t_minus_1": "T-1月场观",
        "anchor_exposure_t": "T月锚点曝光",
        "anchor_exposure_t_minus_1": "T-1月锚点曝光",
        "component_clicks_t": "T月组件点击",
        "component_clicks_t_minus_1": "T-1月组件点击",
        "short_video_leads_t": "T月短视频留资",
        "short_video_leads_t_minus_1": "T-1月短视频留资",
        "short_video_count_t": "T月短视频发布",
        "short_video_count_t_minus_1": "T-1月短视频发布",
        "short_video_plays_t": "T月短视频播放",
        "short_video_plays_t_minus_1": "T-1月短视频播放",
        "enter_private_count_t": "T月进私",
        "enter_private_count_t_minus_1": "T-1月进私",
        "private_open_count_t": "T月私信开口",
        "private_open_count_t_minus_1": "T-1月私信开口",
        "private_leads_count_t": "T月私信留资",
        "private_leads_count_t_minus_1": "T-1月私信留资",
        "effective_live_sessions_t": "T月有效直播场次",
        "effective_live_sessions_t_minus_1": "T-1月有效直播场次",
        "exposures_t": "T月曝光人数",
        "exposures_t_minus_1": "T-1月曝光人数",
        "small_wheel_clicks_t": "T月小风车点击",
        "small_wheel_clicks_t_minus_1": "T-1月小风车点击",
        "small_wheel_leads_t": "T月小风车留资",
        "small_wheel_leads_t_minus_1": "T-1月小风车留资",
        "live_effective_hours_t": "T月有效直播时长(小时)",
        "live_effective_hours_t_minus_1": "T-1月有效直播时长(小时)",

        # 核心总览指标
        "ad_leads_total": "广告线索总量",
        "anchor_exposure_total": "锚点曝光总量",
        "area_leads_total": "区域线索总量",
        "effective_live_sessions_total": "有效直播场次总量",
        "exposures_total": "总曝光人数",
        "live_effective_hours_total": "有效直播时长总量(小时)",
        "live_leads_total": "直播线索总量",
        "local_leads_total": "本地线索总量",
        "natural_leads_total": "自然线索总量",
        "paid_leads_total": "付费线索总量",
        "private_leads_count_total": "私信留资总量",
        "private_open_count_total": "私信开口总量",
        "short_video_count_total": "短视频发布总量",
        "short_video_plays_total": "短视频播放总量",
        "small_wheel_leads_total": "小风车留资总量",
        "spending_net_total": "总消耗",
        "viewers_total": "总场观",
        "enter_private_count_total": "进私总量",
        "component_clicks_total": "组件点击总量",

        # 核心比率和平均指标
        "avg_daily_effective_live_hours_25min": "日均有效（25min以上）时长（h）",
        "avg_daily_paid_leads": "直播车云店+区域付费线索量日均",
        "avg_daily_private_entry_count": "日均进私人数",
        "avg_daily_private_leads_count": "日均咨询留资人数",
        "avg_daily_private_open_count": "日均私信开口人数",
        "avg_daily_spending": "直播车云店+区域日均消耗",
        "avg_exposures_per_session": "场均曝光人数",
        "avg_small_wheel_leads_per_session": "场均小风车留资量",
        "avg_viewers_per_session": "场均场观",
        "component_click_rate": "组件点击率",
        "component_leads_rate": "组件留资率",
        "exposure_to_viewer_rate": "曝光进入率",
        "level": "层级",
        "local_leads_ratio": "本地线索占比",
        "paid_cpl": "付费CPL（车云店+区域）",
        "private_conversion_rate": "私信转化率",
        "private_leads_rate": "咨询留资率",
        "private_open_rate": "私信咨询率",
        "small_wheel_click_rate": "小风车点击率",
        "small_wheel_leads_rate": "小风车点击留资率",
        "total_cpl": "车云店+区域综合CPL",
        "avg_daily_private_open_count_t_minus_1": "T-1月日均私信开口人数",
        "private_conversion_rate_t_minus_1": "T-1月私信转化率",
        "avg_exposures_per_session_t_minus_1": "T-1月场均曝光人数",
        "avg_viewers_per_session_t_minus_1": "T-1月场均场观",
        "component_leads_rate_t_minus_1": "T-1月组件留资率",
        "avg_daily_paid_leads_t": "T月直播车云店+区域付费线索量日均",
        "paid_cpl_t": "T月直播付费CPL",
        "paid_cpl_t_minus_1": "T-1月直播付费CPL",
        "component_click_rate_t_minus_1": "T-1月组件点击率",
        "exposure_to_viewer_rate_t_minus_1": "T-1月曝光进入率",
        "private_open_rate_t": "T月私信咨询率",
        "private_leads_rate_t_minus_1": "T-1月咨询留资率",
        "small_wheel_leads_rate_t_minus_1": "T-1月小风车点击留资率",
        "avg_daily_private_entry_count_t_minus_1": "T-1月日均进私人数",
        "avg_daily_private_leads_count_t_minus_1": "T-1月日均咨询留资人数",
        "avg_daily_effective_live_hours_25min_t": "T月日均有效（25min以上）时长（h）",
        "avg_daily_spending_t_minus_1": "T-1月直播车云店+区域日均消耗",
        "private_leads_rate_t": "T月咨询留资率",
        "avg_daily_private_open_count_t": "T月日均私信开口人数",
        "avg_daily_effective_live_hours_25min_t_minus_1": "T-1月日均有效（25min以上）时长（h）",
        "avg_daily_paid_leads_t_minus_1": "T-1月直播车云店+区域付费线索量日均",
        "avg_small_wheel_leads_per_session_t_minus_1": "T-1月场均小风车留资量",
        "avg_daily_private_leads_count_t": "T月日均咨询留资人数",
        "private_open_rate_t_minus_1": "T-1月私信咨询率",
        "small_wheel_leads_rate_t": "T月小风车点击留资率",
        "private_conversion_rate_t": "T月私信转化率",
        "avg_daily_private_entry_count_t": "T月日均进私人数",
        "small_wheel_click_rate_t": "T月小风车点击率",
        "avg_viewers_per_session_t": "T月场均场观",
        "avg_daily_spending_t": "T月直播车云店+区域日均消耗",
        "avg_small_wheel_leads_per_session_t": "T月场均小风车留资量",
        "small_wheel_click_rate_t_minus_1": "T-1月小风车点击率",
        "avg_exposures_per_session_t": "T月场均曝光人数",
        "component_click_rate_t": "T月组件点击率",
        "component_leads_rate_t": "T月组件留资率",
        "exposure_to_viewer_rate_t": "T月曝光进入率",
    }
    
    # 获取数据 - 支持两种数据结构
    if hasattr(args, 'input') and args.input:
        records = args.input
    elif hasattr(args, 'params') and args.params and len(args.params) > 0:
        records = args.params[0].get('input', [])
    else:
        records = []
    
    if not records:
        return {"records": []}
    
    # 处理每条记录
    output_records = []
    for input_record in records:
        # 使用字段映射表直接构建输出
        flat_fields = {}
        for field_key, display_name in FIELD_MAPPING.items():
            flat_fields[display_name] = input_record.get(field_key, 0)
        
        output_records.append({"fields": flat_fields})
    
    return {"records": output_records}