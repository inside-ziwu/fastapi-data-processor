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
    "send2dealer_id": "send2dealer_id",
}

SPENDING_MAP = {
    "NSC CODE": "NSC_CODE",
    "Date": "date",
    "Spending(Net)": "spending_net",
}

ACCOUNT_BASE_MAP = {
    "NSC_id": "NSC_CODE",
    "第二期层级": "level",
    "NSC Code": "NSC_Code",
    "抖音id": "store_name",
}

# 飞书多维表格字段映射（支持一对一和一对多映射）
FIELD_MAPPINGS = {
    # 一对多映射
    "paid_cpl": ["直播付费CPL", "付费CPL（车云店+区域）"],
    "paid_cpl_t": ["T月直播付费CPL"],
    "paid_cpl_t_minus_1": ["T-1月直播付费CPL"],
    # 一对一映射（统一为单元素列表）
    "NSC_CODE": ["NSC_CODE"],
    "store_name": ["门店名"],
    "level": ["层级"],
    "natural_leads_total": ["自然线索量"],
    "natural_leads_t": ["T月自然线索量"],
    "natural_leads_t_minus_1": ["T-1月自然线索量"],
    "ad_leads_total": ["付费线索量"],
    "ad_leads_t": ["T月付费线索量"],
    "ad_leads_t_minus_1": ["T-1月付费线索量"],
    "spending_net_total": ["车云店+区域投放总金额"],
    "spending_net_t": ["T月车云店+区域投放总金额"],
    "spending_net_t_minus_1": ["T-1月车云店+区域投放总金额"],
    "paid_leads_total": ["车云店付费线索量"],
    "paid_leads_t": ["T月车云店付费线索量"],
    "paid_leads_t_minus_1": ["T-1月车云店付费线索量"],
    "area_leads_total": ["区域线索量"],
    "area_leads_t": ["T月区域线索量"],
    "area_leads_t_minus_1": ["T-1月区域线索量"],
    "local_leads_total": ["本地线索量"],
    "local_leads_t": ["T月本地线索量"],
    "local_leads_t_minus_1": ["T-1月本地线索量"],
    "live_effective_hours_total": ["有效直播时长总量(小时)"],
    "live_effective_hours_t": ["T月有效直播时长(小时)"],
    "live_effective_hours_t_minus_1": ["T-1月有效直播时长(小时)"],
    "effective_live_sessions_total": ["有效直播场次总量"],
    "effective_live_sessions_t": ["T月有效直播场次"],
    "effective_live_sessions_t_minus_1": ["T-1月有效直播场次"],
    "exposures_total": ["总曝光人数"],
    "exposures_t": ["T月曝光人数"],
    "exposures_t_minus_1": ["T-1月曝光人数"],
    "viewers_total": ["总场观"],
    "viewers_t": ["T月场观"],
    "viewers_t_minus_1": ["T-1月场观"],
    "small_wheel_clicks_total": ["小风车点击总量"],
    "small_wheel_clicks_t": ["T月小风车点击"],
    "small_wheel_clicks_t_minus_1": ["T-1月小风车点击"],
    "small_wheel_leads_total": ["小风车留资总量"],
    "small_wheel_leads_t": ["T月小风车留资"],
    "small_wheel_leads_t_minus_1": ["T-1月小风车留资"],
    "anchor_exposure_total": ["锚点曝光总量"],
    "anchor_exposure_t": ["T月锚点曝光"],
    "anchor_exposure_t_minus_1": ["T-1月锚点曝光"],
    "component_clicks_total": ["锚点点击总量"],
    "component_clicks_t": ["T月锚点点击"],
    "component_clicks_t_minus_1": ["T-1月锚点点击"],
    "short_video_count_total": ["短视频发布总量"],
    "short_video_count_t": ["T月短视频发布"],
    "short_video_count_t_minus_1": ["T-1月短视频发布"],
    "short_video_leads_total": ["短视频留资总量"],
    "short_video_leads_t": ["T月短视频留资"],
    "short_video_leads_t_minus_1": ["T-1月短视频留资"],
    "enter_private_count_total": ["进入私信客户总量"],
    "enter_private_count_t": ["T月进入私信客户"],
    "enter_private_count_t_minus_1": ["T-1月进入私信客户"],
    "private_open_count_total": ["主动咨询客户总量"],
    "private_open_count_t": ["T月主动咨询客户"],
    "private_open_count_t_minus_1": ["T-1月主动咨询客户"],
    "private_leads_count_total": ["私信留资客户总量"],
    "private_leads_count_t": ["T月私信留资客户"],
    "private_leads_count_t_minus_1": ["T-1月私信留资客户"],
    "live_leads_total": ["直播间表单提交商机量"],
    "live_leads_t": ["T月直播间表单提交商机量"],
    "live_leads_t_minus_1": ["T-1月直播间表单提交商机量"],
    "short_video_plays_total": ["短视频播放总量"],
    "short_video_plays_t": ["T月短视频播放量"],
    "short_video_plays_t_minus_1": ["T-1月短视频播放量"],
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
