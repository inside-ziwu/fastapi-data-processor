# Coze.cn插件集成配置文档

## FastAPI与Coze.cn插件集成指南

### 1. FastAPI端配置（已完成）

#### 核心功能实现
- ✅ **标准JSON数据下载**：通过 `/get-result-file?file_path=...` 接口
- ✅ **飞书格式数据导出**：`{"records": [{"fields": {...}}, ...]}`
- ✅ **边界处理**：数据≤400条且≤1.8MB时不分割
- ✅ **Coze.cn规范响应**：`{"code": 200, "msg": "...", "records": [...]}`

#### 响应格式
```json
{
  "code": 200,
  "msg": "处理完成：150行数据，0.85MB",
  "records": [
    {
      "fields": {
        "主机厂经销商ID": "NSC001",
        "线索量": 25,
        "消耗金额": 1250.5
      }
    }
  ],
  "download_urls": {
    "standard_json": "https://dtc.zeabur.app/get-result-file?file_path=...",
    "feishu_pages": "https://dtc.zeabur.app/get-result-file?file_path=..."
  }
}
```

### 2. Coze插件端配置（handler函数）

#### 核心代码
```python
import requests

def handler(args):
    """Coze handler function - minimal wrapper"""
    input_obj = getattr(args, 'input', None)
    
    def get_input_arg(name):
        return getattr(input_obj, name, '') if input_obj is not None else ''

    # 构建参数 - 只传非空值
    data = {
        k: v for k in [
            "video_excel_file", "live_bi_file", "msg_excel_file", 
            "DR1_file", "DR2_file", "account_base_file", 
            "leads_file", "account_bi_file", "Spending_file",
            "spending_sheet_names", "dimension"
        ]
        if (v := get_input_arg(k))  # Python 3.8+ walrus operator
    }

    api_url = "https://dtc.zeabur.app/process-files"
    headers = {"Content-Type": "application/json", "x-api-key": "coze-api-key-2024"}

    try:
        resp = requests.post(api_url, json=data, headers=headers, timeout=300)
        resp.raise_for_status()
        
        # 关键：直接返回FastAPI响应，不要包装
        return resp.json()
        
    except requests.exceptions.HTTPError as e:
        # 返回Coze标准错误格式
        return {
            "code": e.response.status_code,
            "msg": f"处理失败: {e.response.text}",
            "records": []
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": str(e),
            "records": []
        }
```

### 3. Coze平台配置步骤

#### 创建插件
1. 登录 [coze.cn](https://coze.cn)
2. 进入「个人空间」→「插件」
3. 点击「创建插件」

#### 插件配置
**基本信息**
- 插件名称：DTC数据处理
- 插件描述：处理DTC相关Excel文件并返回结构化数据
- 插件标识：dtc-data-processor

**API配置**
- 请求方式：POST
- 请求路径：https://dtc.zeabur.app/process-files
- 鉴权方式：Header传参
- Header参数：
  - `x-api-key: coze-api-key-2024`
  - `Content-Type: application/json`

#### 输入参数定义
| 参数名 | 类型 | 必填 | 描述 |
|--------|------|------|------|
| video_excel_file | string | 否 | 视频数据Excel文件URL |
| live_bi_file | string | 否 | 直播BI数据文件URL |
| msg_excel_file | string | 否 | 消息数据Excel文件URL |
| DR1_file | string | 否 | DR1数据文件URL |
| DR2_file | string | 否 | DR2数据文件URL |
| account_base_file | string | 否 | 账户基础数据文件URL |
| leads_file | string | 否 | 线索数据文件URL |
| account_bi_file | string | 否 | 账户BI数据文件URL |
| Spending_file | string | 否 | 消耗数据文件URL |
| spending_sheet_names | string | 否 | 消耗数据工作表名称 |
| dimension | string | 否 | 聚合维度：NSC_CODE或level |

#### 输出参数定义
| 参数名 | 类型 | 描述 |
|--------|------|------|
| code | integer | 状态码：200成功 |
| msg | string | 处理结果描述 |
| records | array | 飞书格式数据数组 |
| download_urls.standard_json | string | 标准JSON下载链接 |
| download_urls.feishu_pages | string | 飞书分页数据下载链接 |

#### records数组结构
```json
{
  "fields": {
    "主机厂经销商ID": "NSC001",
    "线索量": 25,
    "消耗金额": 1250.5,
    "线索成本": 50.02,
    "线索率": 0.85,
    "层级": "一级"
  }
}
```

### 4. 使用示例

#### Coze工作流调用
```
调用插件：DTC数据处理
输入参数：
- video_excel_file: {{video_file_url}}
- dimension: "NSC_CODE"

输出使用：
- {{records[0].fields.主机厂经销商ID}}
- {{records[0].fields.线索量}}
- {{records[0].fields.消耗金额}}
```

#### 边界情况处理
- **小数据量**（≤400条且≤1.8MB）：返回单页完整数据
- **大数据量**：自动分页，保持维度完整性
- **无数据**：返回空records数组

### 5. 测试验证

#### 测试用例
1. **标准测试**：上传有效Excel文件
2. **边界测试**：399条数据，1.7MB大小
3. **空数据测试**：无有效数据
4. **错误测试**：无效文件URL

#### 验证方式
1. Coze平台「测试」功能
2. 查看返回records数组格式
3. 验证下载链接有效性