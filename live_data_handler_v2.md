async def main(args: Args) -> Output:
    """
    接收直播数据，遍历列表中的每条记录，提取所有指标到独立变量，然后构建一个扁平化的记录列表。
    """
    # 1. 获取上游节点传入的 input 列表
    list_of_records = args.params.get('input', [])
    
    processed_records = []

    # 2. 遍历列表中的每一条记录
    for input_data in list_of_records:

        # 3. 提取所有字段到具名变量
        # --- T / T-1 详细数据 ---
        ad_leads_t = input_data.get('ad_leads_t', 0)
        ad_leads_t_minus_1 = input_data.get('ad_leads_t_minus_1', 0)
        area_leads_t = input_data.get('area_leads_t', 0)
        area_leads_t_minus_1 = input_data.get('area_leads_t_minus_1', 0)
        live_leads_t = input_data.get('live_leads_t', 0)
        live_leads_t_minus_1 = input_data.get('live_leads_t_minus_1', 0)
        local_leads_t = input_data.get('local_leads_t', 0)
        local_leads_t_minus_1 = input_data.get('local_leads_t_minus_1', 0)
        natural_leads_t = input_data.get('natural_leads_t', 0)
        natural_leads_t_minus_1 = input_data.get('natural_leads_t_minus_1', 0)
        paid_leads_t = input_data.get('paid_leads_t', 0)
        paid_leads_t_minus_1 = input_data.get('paid_leads_t_minus_1', 0)
        spending_net_t = input_data.get('spending_net_t', 0.0)
        spending_net_t_minus_1 = input_data.get('spending_net_t_minus_1', 0.0)
        viewers_t = input_data.get('viewers_t', 0)
        viewers_t_minus_1 = input_data.get('viewers_t_minus_1', 0)
        anchor_exposure_t = input_data.get('anchor_exposure_t', 0)
        anchor_exposure_t_minus_1 = input_data.get('anchor_exposure_t_minus_1', 0)
        component_clicks_t = input_data.get('component_clicks_t', 0)
        component_clicks_t_minus_1 = input_data.get('component_clicks_t_minus_1', 0)
        short_video_leads_t = input_data.get('short_video_leads_t', 0)
        short_video_leads_t_minus_1 = input_data.get('short_video_leads_t_minus_1', 0)
        short_video_count_t = input_data.get('short_video_count_t', 0)
        short_video_count_t_minus_1 = input_data.get('short_video_count_t_minus_1', 0)
        short_video_plays_t = input_data.get('short_video_plays_t', 0)
        short_video_plays_t_minus_1 = input_data.get('short_video_plays_t_minus_1', 0)
        enter_private_count_t = input_data.get('enter_private_count_t')
        enter_private_count_t_minus_1 = input_data.get('enter_private_count_t_minus_1')
        private_open_count_t = input_data.get('private_open_count_t')
        private_open_count_t_minus_1 = input_data.get('private_open_count_t_minus_1')
        private_leads_count_t = input_data.get('private_leads_count_t')
        private_leads_count_t_minus_1 = input_data.get('private_leads_count_t_minus_1')
        effective_live_sessions_t = input_data.get('effective_live_sessions_t', 0)
        effective_live_sessions_t_minus_1 = input_data.get('effective_live_sessions_t_minus_1', 0)
        exposures_t = input_data.get('exposures_t', 0)
        exposures_t_minus_1 = input_data.get('exposures_t_minus_1', 0)
        small_wheel_clicks_t = input_data.get('small_wheel_clicks_t', 0)
        small_wheel_clicks_t_minus_1 = input_data.get('small_wheel_clicks_t_minus_1', 0)
        small_wheel_leads_t = input_data.get('small_wheel_leads_t', 0)
        small_wheel_leads_t_minus_1 = input_data.get('small_wheel_leads_t_minus_1', 0)
        live_effective_hours_t = input_data.get('live_effective_hours_t', 0.0)
        live_effective_hours_t_minus_1 = input_data.get('live_effective_hours_t_minus_1', 0.0)

        # --- 核心总览指标 ---
        ad_leads_total = input_data.get('ad_leads_total', 0)
        anchor_exposure_total = input_data.get('anchor_exposure_total', 0)
        area_leads_total = input_data.get('area_leads_total', 0)
        effective_live_sessions_total = input_data.get('effective_live_sessions_total', 0)
        exposures_total = input_data.get('exposures_total', 0)
        live_effective_hours_total = input_data.get('live_effective_hours_total', 0.0)
        live_leads_total = input_data.get('live_leads_total', 0)
        local_leads_total = input_data.get('local_leads_total', 0)
        natural_leads_total = input_data.get('natural_leads_total', 0)
        paid_leads_total = input_data.get('paid_leads_total', 0)
        private_leads_count_total = input_data.get('private_leads_count_total')
        private_open_count_total = input_data.get('private_open_count_total')
        short_video_count_total = input_data.get('short_video_count_total', 0)
        short_video_leads_total = input_data.get('short_video_leads_total', 0)
        short_video_plays_total = input_data.get('short_video_plays_total', 0)
        small_wheel_clicks_total = input_data.get('small_wheel_clicks_total', 0)
        small_wheel_leads_total = input_data.get('small_wheel_leads_total', 0)
        spending_net_total = input_data.get('spending_net_total', 0.0)
        viewers_total = input_data.get('viewers_total', 0)
        enter_private_count_total = input_data.get('enter_private_count_total')
        component_clicks_total = input_data.get('component_clicks_total', 0)

        # --- 核心比率和平均指标 ---
        avg_daily_effective_live_hours_25min = input_data.get('avg_daily_effective_live_hours_25min', 0.0)
        avg_daily_paid_leads = input_data.get('avg_daily_paid_leads')
        avg_daily_private_entry_count = input_data.get('avg_daily_private_entry_count', 0.0)
        avg_daily_private_leads_count = input_data.get('avg_daily_private_leads_count', 0.0)
        avg_daily_private_open_count = input_data.get('avg_daily_private_open_count', 0.0)
        avg_daily_spending = input_data.get('avg_daily_spending', 0.0)
        avg_exposures_per_session = input_data.get('avg_exposures_per_session', 0.0)
        avg_small_wheel_leads_per_session = input_data.get('avg_small_wheel_leads_per_session', 0.0)
        avg_viewers_per_session = input_data.get('avg_viewers_per_session', 0.0)
        component_click_rate = input_data.get('component_click_rate', 0.0)
        component_leads_rate = input_data.get('component_leads_rate', 0.0)
        exposure_to_viewer_rate = input_data.get('exposure_to_viewer_rate', 0.0)
        level = input_data.get('level', '')
        local_leads_ratio = input_data.get('local_leads_ratio', 0.0)
        paid_cpl = input_data.get('paid_cpl', 0.0)
        private_conversion_rate = input_data.get('private_conversion_rate', 0.0)
        private_leads_rate = input_data.get('private_leads_rate', 0.0)
        private_open_rate = input_data.get('private_open_rate', 0.0)
        small_wheel_click_rate = input_data.get('small_wheel_click_rate', 0.0)
        small_wheel_leads_rate = input_data.get('small_wheel_leads_rate', 0.0)
        total_cpl = input_data.get('total_cpl', 0.0)
        avg_daily_private_open_count_t_minus_1 = input_data.get('avg_daily_private_open_count_t_minus_1', 0.0)
        private_conversion_rate_t_minus_1 = input_data.get('private_conversion_rate_t_minus_1', 0.0)
        avg_exposures_per_session_t_minus_1 = input_data.get('avg_exposures_per_session_t_minus_1', 0.0)
        avg_viewers_per_session_t_minus_1 = input_data.get('avg_viewers_per_session_t_minus_1', 0.0)
        component_leads_rate_t_minus_1 = input_data.get('component_leads_rate_t_minus_1', 0.0)
        avg_daily_paid_leads_t = input_data.get('avg_daily_paid_leads_t')
        paid_cpl_t = input_data.get('paid_cpl_t', 0.0)
        paid_cpl_t_minus_1 = input_data.get('paid_cpl_t_minus_1', 0.0)
        component_click_rate_t_minus_1 = input_data.get('component_click_rate_t_minus_1', 0.0)
        exposure_to_viewer_rate_t_minus_1 = input_data.get('exposure_to_viewer_rate_t_minus_1', 0.0)
        private_open_rate_t = input_data.get('private_open_rate_t', 0.0)
        private_leads_rate_t_minus_1 = input_data.get('private_leads_rate_t_minus_1', 0.0)
        small_wheel_leads_rate_t_minus_1 = input_data.get('small_wheel_leads_rate_t_minus_1', 0.0)
        avg_daily_private_entry_count_t_minus_1 = input_data.get('avg_daily_private_entry_count_t_minus_1', 0.0)
        avg_daily_private_leads_count_t_minus_1 = input_data.get('avg_daily_private_leads_count_t_minus_1', 0.0)
        avg_daily_effective_live_hours_25min_t = input_data.get('avg_daily_effective_live_hours_25min_t', 0.0)
        avg_daily_spending_t_minus_1 = input_data.get('avg_daily_spending_t_minus_1', 0.0)
        private_leads_rate_t = input_data.get('private_leads_rate_t', 0.0)
        avg_daily_private_open_count_t = input_data.get('avg_daily_private_open_count_t', 0.0)
        avg_daily_effective_live_hours_25min_t_minus_1 = input_data.get('avg_daily_effective_live_hours_25min_t_minus_1', 0.0)
        avg_daily_paid_leads_t_minus_1 = input_data.get('avg_daily_paid_leads_t_minus_1')
        avg_small_wheel_leads_per_session_t_minus_1 = input_data.get('avg_small_wheel_leads_per_session_t_minus_1', 0.0)
        avg_daily_private_leads_count_t = input_data.get('avg_daily_private_leads_count_t', 0.0)
        private_open_rate_t_minus_1 = input_data.get('private_open_rate_t_minus_1', 0.0)
        small_wheel_leads_rate_t = input_data.get('small_wheel_leads_rate_t', 0.0)
        private_conversion_rate_t = input_data.get('private_conversion_rate_t', 0.0)
        avg_daily_private_entry_count_t = input_data.get('avg_daily_private_entry_count_t', 0.0)
        small_wheel_click_rate_t = input_data.get('small_wheel_click_rate_t', 0.0)
        avg_viewers_per_session_t = input_data.get('avg_viewers_per_session_t', 0.0)
        avg_daily_spending_t = input_data.get('avg_daily_spending_t', 0.0)
        avg_small_wheel_leads_per_session_t = input_data.get('avg_small_wheel_leads_per_session_t', 0.0)
        small_wheel_click_rate_t_minus_1 = input_data.get('small_wheel_click_rate_t_minus_1', 0.0)
        avg_exposures_per_session_t = input_data.get('avg_exposures_per_session_t', 0.0)
        component_click_rate_t = input_data.get('component_click_rate_t', 0.0)
        component_leads_rate_t = input_data.get('component_leads_rate_t', 0.0)
        exposure_to_viewer_rate_t = input_data.get('exposure_to_viewer_rate_t', 0.0)

        # 4. 构建记录对象
        record = {
            "fields": {
                "T-1月日均私信开口人数": avg_daily_private_open_count_t_minus_1,
                "T-1月锚点曝光": anchor_exposure_t_minus_1,
                "日均咨询留资人数": avg_daily_private_leads_count,
                "咨询留资率": private_leads_rate,
                "T月日均进私人数": avg_daily_private_entry_count_t,
                "T月短视频播放": short_video_plays_t,
                "T月小风车点击率": small_wheel_click_rate_t,
                "小风车点击留资率": small_wheel_leads_rate,
                "T-1月小风车留资": small_wheel_leads_t_minus_1,
                "总场观": viewers_total,
                "T月锚点曝光": anchor_exposure_t,
                "进私总量": enter_private_count_total,
                "短视频留资总量": short_video_leads_total,
                "T-1月广告线索量": ad_leads_t_minus_1,
                "日均有效（25min以上）时长（h）": avg_daily_effective_live_hours_25min,
                "T月曝光进入率": exposure_to_viewer_rate_t,
                "自然线索总量": natural_leads_total,
                "T-1月私信转化率": private_conversion_rate_t_minus_1,
                "车云店+区域综合CPL": total_cpl,
                "T-1月场均曝光人数": avg_exposures_per_session_t_minus_1,
                "T-1月场均场观": avg_viewers_per_session_t_minus_1,
                "T-1月组件留资率": component_leads_rate_t_minus_1,
                "直播车云店+区域日均消耗": avg_daily_spending,
                "T月进私": enter_private_count_t,
                "T月直播线索": live_leads_t,
                "T-1月付费线索量": paid_leads_t_minus_1,
                "T-1月短视频留资": short_video_leads_t_minus_1,
                "T月直播车云店+区域付费线索量日均": avg_daily_paid_leads_t,
                "场均曝光人数": avg_exposures_per_session,
                "组件点击总量": component_clicks_total,
                "本地线索占比": local_leads_ratio,
                "T-1月自然线索量": natural_leads_t_minus_1,
                "T月直播付费CPL": paid_cpl_t,
                "T-1月直播付费CPL": paid_cpl_t_minus_1,
                "短视频发布总量": short_video_count_total,
                "T月场均小风车留资量": avg_small_wheel_leads_per_session_t,
                "T-1月组件点击率": component_click_rate_t_minus_1,
                "T-1月有效直播场次": effective_live_sessions_t_minus_1,
                "T-1月曝光进入率": exposure_to_viewer_rate_t_minus_1,
                "T-1月本地线索量": local_leads_t_minus_1,
                "T月付费线索量": paid_leads_t,
                "T月私信咨询率": private_open_rate_t,
                "T月小风车点击": small_wheel_clicks_t,
                "组件留资率": component_leads_rate,
                "有效直播场次总量": effective_live_sessions_total,
                "T月私信开口": private_open_count_t,
                "私信开口总量": private_open_count_total,
                "T月本地线索量": local_leads_t,
                "T-1月咨询留资率": private_leads_rate_t_minus_1,
                "小风车点击率": small_wheel_click_rate,
                "T-1月小风车点击": small_wheel_clicks_t_minus_1,
                "T-1月小风车点击留资率": small_wheel_leads_rate_t_minus_1,
                "场均场观": avg_viewers_per_session,
                "T月曝光人数": exposures_t,
                "T-1月私信留资": private_leads_count_t_minus_1,
                "日均进私人数": avg_daily_private_entry_count,
                "T月场均场观": avg_viewers_per_session_t,
                "组件点击率": component_click_rate,
                "付费线索总量": paid_leads_total,
                "T月直播车云店+区域日均消耗": avg_daily_spending_t,
                "T-1月区域线索量": area_leads_t_minus_1,
                "直播车云店+区域付费线索量日均": avg_daily_paid_leads,
                "日均私信开口人数": avg_daily_private_open_count,
                "T-1月短视频播放": short_video_plays_t_minus_1,
                "T月小风车留资": small_wheel_leads_t,
                "T-1月日均咨询留资人数": avg_daily_private_leads_count_t_minus_1,
                "有效直播时长总量(小时)": live_effective_hours_total,
                "小风车点击总量": small_wheel_clicks_total,
                "T月日均有效（25min以上）时长（h）": avg_daily_effective_live_hours_25min_t,
                "T-1月直播车云店+区域日均消耗": avg_daily_spending_t_minus_1,
                "T-1月曝光人数": exposures_t_minus_1,
                "层级": level,
                "T-1月私信开口": private_open_count_t_minus_1,
                "T-1月消耗": spending_net_t_minus_1,
                "T-1月场观": viewers_t_minus_1,
                "T月组件留资率": component_leads_rate_t,
                "T月咨询留资率": private_leads_rate_t,
                "T月消耗": spending_net_t,
                "T月日均私信开口人数": avg_daily_private_open_count_t,
                "T-1月日均有效（25min以上）时长（h）": avg_daily_effective_live_hours_25min_t_minus_1,
                "T-1月直播车云店+区域付费线索量日均": avg_daily_paid_leads_t_minus_1,
                "场均小风车留资量": avg_small_wheel_leads_per_session,
                "T月有效直播时长(小时)": live_effective_hours_t,
                "T月广告线索量": ad_leads_t,
                "T月区域线索量": area_leads_t,
                "T-1月场均小风车留资量": avg_small_wheel_leads_per_session_t_minus_1,
                "短视频播放总量": short_video_plays_total,
                "T月日均咨询留资人数": avg_daily_private_leads_count_t,
                "T-1月组件点击": component_clicks_t_minus_1,
                "T月私信留资": private_leads_count_t,
                "T月短视频留资": short_video_leads_t,
                "T月自然线索量": natural_leads_t,
                "私信留资总量": private_leads_count_total,
                "私信咨询率": private_open_rate,
                "T-1月短视频发布": short_video_count_t_minus_1,
                "T-1月小风车点击率": small_wheel_click_rate_t_minus_1,
                "总消耗": spending_net_total,
                "T月组件点击": component_clicks_t,
                "T-1月进私": enter_private_count_t_minus_1,
                "T-1月私信咨询率": private_open_rate_t_minus_1,
                "T月短视频发布": short_video_count_t,
                "小风车留资总量": small_wheel_leads_total,
                "T月小风车点击留资率": small_wheel_leads_rate_t,
                "T月有效直播场次": effective_live_sessions_t,
                "区域线索总量": area_leads_total,
                "T-1月有效直播时长(小时)": live_effective_hours_t_minus_1,
                "付费CPL（车云店+区域）": paid_cpl,
                "T月场观": viewers_t,
                "总曝光人数": exposures_total,
                "私信转化率": private_conversion_rate,
                "锚点曝光总量": anchor_exposure_total,
                "T月场均曝光人数": avg_exposures_per_session_t,
                "T月组件点击率": component_click_rate_t,
                "曝光进入率": exposure_to_viewer_rate,
                "广告线索总量": ad_leads_total,
                "T-1月日均进私人数": avg_daily_private_entry_count_t_minus_1,
                "T-1月直播线索": live_leads_t_minus_1,
                "直播线索总量": live_leads_total,
                "本地线索总量": local_leads_total,
                "T月私信转化率": private_conversion_rate_t,
            }
        }
        processed_records.append(record)
    
    # 5. 构建最终输出对象
    ret: Output = {
        "records": processed_records
    }
    return ret
