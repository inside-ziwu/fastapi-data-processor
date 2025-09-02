import json
from typing import Dict, Any, List


def clean_data_for_save(data: Dict[str, Any]) -> Dict[str, Any]:
    """清理数据以确保可保存到飞书
    
    处理：
    - null值转换为0（数值字段）或空字符串（文本字段）
    - 浮点数精度限制到6位小数
    - 确保所有值都有有效类型
    """
    cleaned = {}
    
    for key, value in data.items():
        if value is None:
            # 基于key名判断数据类型
            if any(keyword in key for keyword in ['率', '占比', '率', '点击率', '留资率', '转化率']):
                cleaned[key] = 0.0  # 比率类用浮点
            elif any(keyword in key for keyword in ['量', '数', '时长', '消耗', 'CPL', '场观', '曝光', '点击', '线索']):
                cleaned[key] = 0  # 数值类用整数
            else:
                cleaned[key] = 0  # 默认整数
        elif isinstance(value, float):
            # 限制浮点数精度，避免精度问题
            if abs(value) < 1e-10:  # 处理接近0的浮点数
                cleaned[key] = 0.0
            else:
                cleaned[key] = round(value, 6)
        elif isinstance(value, int):
            cleaned[key] = int(value)
        else:
            cleaned[key] = str(value)  # 其他类型转字符串
    
    return cleaned


def clean_feishu_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """清理飞书记录列表"""
    cleaned_records = []
    
    for record in records:
        if 'fields' in record:
            record['fields'] = clean_data_for_save(record['fields'])
        cleaned_records.append(record)
    
    return cleaned_records


def save_cleaned_data(data: Dict[str, Any], output_path: str = None) -> str:
    """保存清理后的数据"""
    if 'feishu_records' in data:
        data['feishu_records'] = clean_feishu_records(data['feishu_records'])
    
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return output_path
    
    return json.dumps(data, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    # 测试用例
    test_data = {
        "feishu_records": [
            {
                "fields": {
                    "T-1月付费线索量": 81260,
                    "T-1月咨询留资率": 0.5912003578041858,
                    "T-1月场均场观": None,
                    "T月付费线索量": 49499.9999999999,
                    "层级": "A"
                }
            }
        ]
    }
    
    cleaned = save_cleaned_data(test_data)
    print("清理后的数据:")
    print(cleaned)