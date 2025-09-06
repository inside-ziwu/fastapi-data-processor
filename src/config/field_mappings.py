"""Field mappings for different data sources."""

# 基础字段映射
VIDEO_MAP = {
    "主机厂经销商id": "NSC_CODE",
    "日期": "date",
    "锚点曝光次数": "anchor_exposure",
    "锚点点击次数": "component_clicks",
    "新发布视频数": "short_video_count",
    "短视频表单提交商机量": "short_video_leads",
}

LIVE_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "开播日期": "date",
    "超25分钟直播时长(分)": "over25_min_live_mins",
    "直播有效时长（小时）": "live_effective_hours",
    "超25min直播总场次": "effective_live_sessions",
    "曝光人数": "exposures",
    "场观": "viewers",
    "小风车点击次数（不含小雪花）": "small_wheel_clicks",
}

MSG_MAP = {
    "主机厂经销商ID": "NSC_CODE",
    "日期": "date",
    "进入私信客户数": "enter_private_count",
    "主动咨询客户数": "private_open_count",
    "私信留资客户数": "private_leads_count",
}

ACCOUNT_BI_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "日期": "date",
    "直播间表单提交商机量": "live_leads",
    "短-播放量": "short_video_plays",
}

LEADS_MAP = {
    "主机厂经销商id列表": "NSC_CODE",
    "留资日期": "date",
    "直播间表单提交商机量(去重)": "small_wheel_leads",
}

DR_MAP = {
    "reg_dealer": "NSC_CODE",
    "register_time": "date",
    "leads_type": "leads_type",
    "mkt_second_channel_name": "mkt_second_channel_name",
    # 只接受 send2dealer_id，send2dealer 无效且丢弃
    "send2dealer_id": "send2dealer_id",
}

SPENDING_MAP = {
    "NSC CODE": "NSC_CODE",
    "Date": "date",
    "Spending(Net)": "spending_net",
}

ACCOUNT_BASE_MAP = {
    "NSC_id": "NSC_CODE",
    "NSC CODE": "NSC_CODE",
    "经销商ID": "NSC_CODE",
    "经销商id": "NSC_CODE",
    "第二期层级": "level",
    "层级": "level",
    # 统一为 NSC_CODE 以保证后续 Join 与标准化生效
    "NSC Code": "NSC_CODE",
    "抖音id": "store_name",
    "抖音ID": "store_name",
}

# 飞书多维表格字段映射（支持一对一和一对多映射）
FIELD_MAPPINGS = {
    # 一对多映射
    "paid_cpl": ["直播付费CPL", "付费CPL（车云店+区域）"],
    "paid_cpl_t": ["T月直播付费CPL"],
    "paid_cpl_t_minus_1": ["T-1月直播付费CPL"],
    
    # 基础字段
    "NSC_CODE": ["经销商ID"],
    "store_name": ["门店名", "门店名称", "门店", "店铺名", "抖音id", "抖音ID"],
    "level": ["层级"],
    
    # 线索量相关
    "natural_leads_total": ["自然线索量"],
    "natural_leads_t": ["T月自然线索量"],
    "natural_leads_t_minus_1": ["T-1月自然线索量"],
    "ad_leads_total": ["付费线索量"],
    "ad_leads_t": ["T月付费线索量"],
    "ad_leads_t_minus_1": ["T-1月付费线索量"],
    
    # 投放金额
    "spending_net_total": ["车云店+区域投放总金额"],
    "spending_net_t": ["T月车云店+区域投放总金额"],
    "spending_net_t_minus_1": ["T-1月车云店+区域投放总金额"],
    
    # 付费线索
    "paid_leads_total": ["车云店付费线索量"],
    "paid_leads_t": ["T月车云店付费线索量"],
    "paid_leads_t_minus_1": ["T-1月车云店付费线索量"],
    
    # 区域线索
    "area_leads_total": ["区域线索量"],
    "area_leads_t": ["T月区域线索量"],
    "area_leads_t_minus_1": ["T-1月区域线索量"],
    
    # 本地线索
    "local_leads_total": ["本地线索量"],
    "local_leads_t": ["T月本地线索量"],
    "local_leads_t_minus_1": ["T-1月本地线索量"],
    
    # 直播时长
    "live_effective_hours_total": ["有效直播时长总量(小时)", "直播时长"],
    "live_effective_hours_t": ["T月有效直播时长(小时)", "T月直播时长"],
    "live_effective_hours_t_minus_1": ["T-1月有效直播时长(小时)", "T-1月直播时长"],
    
    # 直播场次
    "effective_live_sessions_total": ["有效直播场次总量"],
    "effective_live_sessions_t": ["T月有效直播场次"],
    "effective_live_sessions_t_minus_1": ["T-1月有效直播场次"],
    
    # 曝光人数
    "exposures_total": ["总曝光人数"],
    "exposures_t": ["T月曝光人数"],
    "exposures_t_minus_1": ["T-1月曝光人数"],
    
    # 场观
    "viewers_total": ["总场观"],
    "viewers_t": ["T月场观"],
    "viewers_t_minus_1": ["T-1月场观"],
    
    # 小风车点击
    "small_wheel_clicks_total": ["小风车点击总量"],
    "small_wheel_clicks_t": ["T月小风车点击"],
    "small_wheel_clicks_t_minus_1": ["T-1月小风车点击"],
    
    # 小风车留资
    "small_wheel_leads_total": ["小风车留资量"],
    "small_wheel_leads_t": ["T月小风车留资量"],
    "small_wheel_leads_t_minus_1": ["T-1月小风车留资量"],
    
    # 锚点曝光
    "anchor_exposure_total": ["锚点曝光量"],
    "anchor_exposure_t": ["T月锚点曝光量"],
    "anchor_exposure_t_minus_1": ["T-1月锚点曝光量"],
    
    # 短视频
    "short_video_count_total": ["短视频发布量", "短视频条数"],
    "short_video_count_t": ["T月短视频发布量", "T月短视频条数"],
    "short_video_count_t_minus_1": ["T-1月短视频发布量", "T-1月短视频条数"],
    "short_video_leads_total": ["组件留资人数（获取线索量）"],
    "short_video_leads_t": ["T月组件留资人数（获取线索量）"],
    "short_video_leads_t_minus_1": ["T-1月组件留资人数（获取线索量）"],
    
    # 私信客户
    "enter_private_count_total": ["进私总量"],
    "enter_private_count_t": ["T月进私"],
    "enter_private_count_t_minus_1": ["T-1月进私"],
    "private_open_count_total": ["私信开口总量"],
    "private_open_count_t": ["T月私信开口"],
    "private_open_count_t_minus_1": ["T-1月私信开口"],
    "private_leads_count_total": ["私信留资总量"],
    "private_leads_count_t": ["T月私信留资"],
    "private_leads_count_t_minus_1": ["T-1月私信留资"],
    
    # 短视频播放
    "short_video_plays_total": ["短视频播放总量", "短视频播放量"],
    "short_video_plays_t": ["T月短视频播放量"],
    "short_video_plays_t_minus_1": ["T-1月短视频播放量"],
    
    # === 新增字段映射（基于飞书API）===
    # 综合CPL
    "total_cpl": ["车云店+区域综合CPL"],
    
    # 本地线索占比
    "local_leads_ratio": ["本地线索占比"],
    
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
    
    # 组件点击次数
    "component_clicks_total": ["组件点击次数"],
    "component_clicks_t": ["T月组件点击次数"],
    "component_clicks_t_minus_1": ["T-1月组件点击次数"],
    
    # 组件点击率
    "component_click_rate": ["组件点击率"],
    "component_click_rate_t": ["T月组件点击率"],
    "component_click_rate_t_minus_1": ["T-1月组件点击率"],
    
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
    "private_leads_rate": ["咨询留资率=留资|咨询", "咨询留资率=留资/咨询"],
    "private_leads_rate_t": ["T月咨询留资率=留资|咨询", "T月咨询留资率=留资/咨询"],
    "private_leads_rate_t_minus_1": ["T-1月咨询留资率=留资|咨询", "T-1月咨询留资率=留资/咨询"],
    
    # 私信转化率
    "private_conversion_rate": ["私信转化率=留资|进私", "私信转化率=留资/进私"],
    "private_conversion_rate_t": ["T月私信转化率=留资|进私", "T月私信转化率=留资/进私"],
    "private_conversion_rate_t_minus_1": ["T-1月私信转化率=留资|进私", "T-1月私信转化率=留资/进私"],

    # === 新增日均消耗和时长字段 ===
    "avg_daily_spending": ["直播车云店+区域日均消耗"],
    "avg_daily_spending_t": ["T月直播车云店+区域日均消耗"],
    "avg_daily_spending_t_minus_1": ["T-1月直播车云店+区域日均消耗"],
    "avg_daily_paid_leads": ["直播车云店+区域付费线索量日均"],
    "avg_daily_paid_leads_t": ["T月直播车云店+区域付费线索量日均"],
    "avg_daily_paid_leads_t_minus_1": ["T-1月直播车云店+区域付费线索量日均"],
    # 直播付费线索量（总量，非日均）
    "paid_area_leads_total": ["直播车云店+区域付费线索量"],
    "paid_area_leads_t": ["T月直播车云店+区域付费线索量"],
    "paid_area_leads_t_minus_1": ["T-1月直播车云店+区域付费线索量"],

    "effective_live_hours_25min": ["有效（25min以上）时长（h）"],
    "effective_live_hours_25min_t": ["T月有效（25min以上）时长（h）"],
    "effective_live_hours_25min_t_minus_1": ["T-1月有效（25min以上）时长（h）"],

}

# 数据源类型定义
DATA_SOURCE_TYPES = {
    "video": VIDEO_MAP,
    "live": LIVE_MAP,
    "msg": MSG_MAP,
    "account_bi": ACCOUNT_BI_MAP,
    "leads": LEADS_MAP,
    "dr": DR_MAP,
    "spending": SPENDING_MAP,
    "account_base": ACCOUNT_BASE_MAP,
}
