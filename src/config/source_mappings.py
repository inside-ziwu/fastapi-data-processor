"""Field mappings for different data sources.

This file now only contains the mapping from original source column names
to the internal standard English names.
"""

VIDEO_MAP = {
    "主机厂经销商id": "nsc_code",
    "日期": "date",
    "锚点曝光次数": "video_anchor_exposures",
    "锚点点击次数": "video_anchor_clicks",
    "新发布视频数": "video_new_posts",
    "短视频表单提交商机量": "video_form_leads",
}

LIVE_BI_MAP = {
    "主机厂经销商id列表": "nsc_code",
    "开播日期": "date",
    "超25分钟直播时长(分)": "live_gt_25min_duration_min",
    "直播有效时长（小时）": "live_effective_duration_hr",
    "超25min直播总场次": "live_gt_25min_sessions",
    "曝光人数": "live_exposures",
    "场观": "live_views",
    "小风车点击次数(不含小雪花)": "live_widget_clicks",
}

MSG_MAP = {
    "主机厂经销商ID": "nsc_code",
    # Note: date is derived from sheet name, not a column in the file
    "进入私信客户数": "msg_private_entrants",
    "主动咨询客户数": "msg_active_consultations",
    "私信留资客户数": "msg_leads_from_private",
}

ACCOUNT_BI_MAP = {
    "主机厂经销商id列表": "nsc_code",
    "日期": "date",
    "直播间表单提交商机量": "account_bi_live_form_leads",
    "短-播放量": "account_bi_video_views",
}

LEADS_MAP = {
    "主机厂经销商id列表": "nsc_code",
    "留资日期": "date",
    "直播间表单提交商机量(去重)": "leads_from_live_form",
}

DR_MAP = {
    "reg_dealer": "nsc_code",
    "register_time": "date",
    "leads_type": "leads_type",
    "mkt_second_channel_name": "mkt_second_channel_name",
    "send2dealer_id": "send2dealer_id",
}

SPENDING_MAP = {
    "NSC CODE": "nsc_code",
    "Date": "date",
    "Spending(Net)": "spending_net",
}

ACCOUNT_BASE_MAP = {
    "NSC_id": "nsc_code",
    "NSC Code": "nsc_code",
    "第二期层级": "level",
    "抖音id": "store_name",
}
