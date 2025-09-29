from fastapi import FastAPI, HTTPException, Query
from typing import Dict, Any, List
from datetime import datetime, timedelta
from 测试 import MaritimeDataProcessor
import requests
import json
import pandas as pd
import psycopg2
import re
from kingbase_config import get_kingbase_config
from kingbase_enhancement import enhance_vessel_data_with_risk_desc, get_enhancer

# 导入配置
from kingbase_config import get_lloyds_token

# STS API配置
STS_API_CONFIG = {
    "base_url": "https://api.lloydslistintelligence.com/v1/vesselstspairings_v2",
    "token": get_lloyds_token()
}

# 创建FastAPI应用
maritime_app = FastAPI(
    title="船舶信息综合API服务",
    description="提供船舶合规性检查、风险评估等综合服务",
    version="2.0.0"
)

# 添加请求日志中间件
from fastapi import Request
import time
import sys
import os

@maritime_app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    process_id = os.getpid()  # 获取当前进程ID
    
    # 记录请求开始 - 使用sys.stdout.flush()确保立即输出
    print(f"[PID:{process_id}] 🌐 API请求开始: {request.method} {request.url.path} - {datetime.now()}", flush=True)
    if request.query_params:
        print(f"[PID:{process_id}] 📋 查询参数: {dict(request.query_params)}", flush=True)
    
    response = await call_next(request)
    
    # 计算处理时间
    process_time = time.time() - start_time
    
    # 记录请求完成 - 使用sys.stdout.flush()确保立即输出
    print(f"[PID:{process_id}] ✅ API请求完成: {request.method} {request.url.path} - 状态码: {response.status_code} - 耗时: {process_time:.2f}秒 - {datetime.now()}", flush=True)
    
    return response

def get_processor():
    """获取处理器实例 - 每次创建新实例避免多进程冲突"""
    return MaritimeDataProcessor()

# 时间戳转换函数
def convert_timestamps_in_data(data: Any) -> Any:
    """
    递归转换数据中的所有时间戳字段
    支持Unix时间戳（10位和13位）转换为ISO格式
    """
    if isinstance(data, dict):
        converted_dict = {}
        for key, value in data.items():
            # 检查字段名是否包含时间相关关键词
            if any(time_keyword in key.lower() for time_keyword in ['date', 'time', 'timestamp']):
                converted_dict[key] = convert_timestamp(value)
            else:
                converted_dict[key] = convert_timestamps_in_data(value)
        return converted_dict
    elif isinstance(data, list):
        return [convert_timestamps_in_data(item) for item in data]
    else:
        return data

def convert_timestamp(value: Any) -> str:
    """
    转换单个时间戳值为简洁的时间格式字符串
    格式：YYYY-MM-DD HH:MM:SS
    """
    if value is None:
        return ""
    
    # 如果已经是字符串格式的时间，进行格式调整
    if isinstance(value, str):
        # 检查是否已经是ISO格式（带微秒）
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+', value):
            # 移除微秒部分，将T替换为空格
            return value.split('.')[0].replace('T', ' ')
        # 检查是否已经是ISO格式（不带微秒）
        elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
            # 将T替换为空格
            return value.replace('T', ' ')
        # 检查是否是简单的日期格式
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            return value
        # 检查是否已经是目标格式
        elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', value):
            return value
    
    # 处理数字时间戳
    if isinstance(value, (int, float)) and value > 0:
        try:
            # 处理毫秒级时间戳（13位）
            if len(str(int(value))) == 13:
                timestamp = int(value) / 1000
            else:
                timestamp = int(value)
            
            # 转换为简洁格式
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, OSError):
            pass
    
    # 如果无法转换，返回原值
    return str(value) if value is not None else ""

# 通用检查项创建函数
def create_check_item(name: str, title: str, description: str, data: Any, sanctions_lev: str = "无风险") -> Dict[str, Any]:
    """创建标准化的检查项"""
    item = {
        "name": name,
        "title": title,
        "description": description,
        "sanctions_lev": sanctions_lev
    }
    
    # 处理字段重命名
    field_mapping = {
        "SanctionedOwners": "sanctions_list",
        "data": "sanctions_list", 
        "risks": "sanctions_list",
        "raw_data": "sanctions_list"
    }
    
    # 需要注释掉的字段
    excluded_fields = {"sanctions_list", "VesselImo", "LoiteringStart", "LoiteringEnd", "VoyageRiskRating", 
                       "AisGapStartDateTime", "AisGapEndDateTime", "AisGapStartEezName", "is_sanctioned_eez"}
    
    if isinstance(data, dict):
        # 处理字典类型数据
        for key, value in data.items():
            if key not in ["name", "title", "description", "sanctions_lev"]:
                if key in field_mapping:
                    # 特殊处理UANI检查项，将字典包装成数组
                    if name == "uani_check" and field_mapping[key] == "sanctions_list":
                        item[field_mapping[key]] = [value] if value else []
                    else:
                        item[field_mapping[key]] = value
                elif key not in excluded_fields:
                    item[key] = value
    elif isinstance(data, list) and len(data) > 0:
        # 处理列表类型数据（如lloyds_sanctions）
        # 对于lloyds_sanctions，直接展开数据，不添加外层的sanctions_list
        if name == "lloyds_sanctions":
            # 直接展开列表中的每个项目
            for i, list_item in enumerate(data):
                if isinstance(list_item, dict):
                    for key, value in list_item.items():
                        if key not in ["name", "title", "description"]:
                            # 特殊处理sanctions_lev：使用原始数据中的值，而不是传入的参数
                            if key == "sanctions_lev":
                                item["sanctions_lev"] = value
                            else:
                                item[f"{key}_{i}" if i > 0 else key] = value
        elif name == "lloyds_sanctions_his":
            # 对于lloyds_sanctions_his，也直接展开数据
            for i, list_item in enumerate(data):
                if isinstance(list_item, dict):
                    for key, value in list_item.items():
                        if key not in ["name", "title", "description"]:
                            # 特殊处理sanctions_lev：使用原始数据中的值，而不是传入的参数
                            if key == "sanctions_lev":
                                item["sanctions_lev"] = value
                            else:
                                item[f"{key}_{i}" if i > 0 else key] = value
        elif name == "high_risk_port":
            # 特殊处理high_risk_port，去掉message字段，直接保留vessel_info内容
            processed_data = []
            for list_item in data:
                if isinstance(list_item, dict):
                    if "vessel_info" in list_item:
                        # 直接使用vessel_info的内容
                        processed_data.append(list_item["vessel_info"])
                    elif "message" not in list_item:
                        # 如果没有vessel_info但有其他有效数据，保留
                        processed_data.append(list_item)
                    # 如果有message但没有vessel_info，跳过
            item["sanctions_list"] = processed_data
        else:
            # 其他检查项保持原有逻辑
            item["sanctions_list"] = data
            # 添加其他可能的信息
            if isinstance(data[0], dict):
                first_item = data[0]
                if "vesselImo" in first_item:
                    item["vesselImo"] = first_item["vesselImo"]
                if "vesselName" in first_item:
                    item["vesselName"] = first_item["vesselName"]
    
    # 统一规则：无风险则清空 sanctions_list（若不存在则创建空数组）
    try:
        if item.get("sanctions_lev", "无风险") == "无风险":
            item["sanctions_list"] = []
    except Exception:
        # 兜底，不影响主流程
        pass

    return item

# 检查项配置
CHECK_CONFIG = {
    "lloyds_compliance": {
        "title": "船舶相关方涉制裁风险情况",
        "description": "劳氏船舶相关方涉制裁风险情况"
    },
    "lloyds_sanctions": {
        "title": "船舶涉制裁名单风险情况",
        "description": "劳氏船舶涉制裁名单风险情况"
    },
    "lloyds_sanctions_his": {
        "title": "船舶历史涉制裁名单风险情况",
        "description": "劳氏船舶历史涉制裁名单风险情况"
    },
    "uani_check": {
        "title": "船舶涉UANI清单风险情况",
        "description": "船舶涉UANI清单风险情况"
    },
    "ais_manipulation": {
        "title": "AIS信号伪造及篡改风险情况",
        "description": "劳氏AIS信号伪造及篡改风险情况"
    },
    "high_risk_port": {
        "title": "挂靠高风险港口风险情况",
        "description": "劳氏挂靠高风险港口风险情况"
    },
    "possible_dark_port": {
        "title": "暗港访问风险情况",
        "description": "劳氏暗港访问风险情况"
    },
    "suspicious_ais_gap": {
        "title": "AIS信号缺失风险情况",
        "description": "劳氏AIS信号缺失风险情况"
    },
    "dark_sts": {
        "title": "隐蔽STS事件风险情况",
        "description": "劳氏隐蔽STS事件风险情况"
    },
    "sanctioned_sts": {
        "title": "STS转运不合规风险情况",
        "description": "劳氏STS转运不合规风险情况"
    },
    "loitering_behavior": {
        "title": "可疑徘徊风险情况",
        "description": "劳氏可疑徘徊风险情况"
    }
}

# Kpler风险检查项配置
KPLER_RISK_CONFIG = {
    "has_sanctioned_cargo_risk": {
        "title": "船舶运输受制裁货物情况",
        "description": "开普勒船舶运输受制裁货物情况"
    },
    "has_sanctioned_trades_risk": {
        "title": "船舶涉及受制裁贸易风险情况",
        "description": "开普勒船舶涉及受制裁贸易风险情况"
    },
    # "has_sanctioned_flag_risk": {
    #     "title": "船旗受制裁风险情况",
    #     "description": "开普勒船旗受制裁风险情况"
    # },
    "has_port_calls_risk": {
        "title": "挂靠高风险港口风险情况",
        "description": "开普勒挂靠高风险港口风险情况"
    },
    "has_sts_events_risk": {
        "title": "STS转运不合规风险情况",
        "description": "开普勒STS转运不合规风险情况"
    },
    "has_ais_gap_risk": {
        "title": "AIS信号缺失风险情况",
        "description": "开普勒AIS信号缺失风险情况"
    },
    "has_ais_spoofs_risk": {
        "title": "AIS信号伪造及篡改风险情况",
        "description": "开普勒AIS信号伪造及篡改风险情况"
    },
    "has_dark_sts_risk": {
        "title": "隐蔽STS事件风险情况",
        "description": "开普勒隐蔽STS事件风险情况"
    },
    "has_sanctioned_companies_risk": {
        "title": "船舶相关方涉制裁风险情况",
        "description": "开普勒船舶相关方涉制裁风险情况"
    }
}

def parse_kpler_data(data_str: str) -> List[Dict[str, str]]:
    """解析Kpler数据为JSON KV结构"""
    if not isinstance(data_str, str) or not data_str.strip():
        return []
    
    try:
        records = data_str.split(" || ")
        parsed_records = []
        
        for record in records:
            if record.strip():
                kv_pairs = record.split(", ")
                record_dict = {}
                
                for kv in kv_pairs:
                    if ":" in kv:
                        key, value = kv.split(":", 1)
                        record_dict[key.strip()] = value.strip()
                
                if record_dict:
                    parsed_records.append(record_dict)
        
        return parsed_records
    except Exception as e:
        print(f"解析Kpler数据失败: {str(e)}")
        return []

def get_lloyds_sanctions_lev(risk_scores: List[Dict]) -> str:
    """根据OwnerIsCurrentlySanctioned字段计算劳氏制裁等级"""
    if not risk_scores:
        return "无风险"
    
    # 查找OwnerIsCurrentlySanctioned字段
    for risk in risk_scores:
        if 'OwnerIsCurrentlySanctioned' in risk:
            owner_sanctioned = risk['OwnerIsCurrentlySanctioned']
            if owner_sanctioned == "true":
                return '高风险'
            elif owner_sanctioned == "false":
                return '无风险'
    
    return '无风险'

def get_highest_risk_level(sanctions_levs):
    """
    获取最高风险等级
    高风险 > 中风险 > 无风险
    """
    if '高风险' in sanctions_levs:
        return '高风险'
    elif '中风险' in sanctions_levs:
        return '中风险'
    else:
        return '无风险'

def restructure_check_items_by_title(all_check_items):
    """
    按title重新组织检查项数据结构
    将具有相同title的检查项合并到一个大的检查项下，并添加nengyuan_第三方判断逻辑
    
    参数:
        all_check_items: 所有检查项的列表
        
    返回:
        按title重新组织的数据结构
    """
    print("🔍 开始按title重新组织检查项数据结构...")
    
    # 按title分组
    title_groups = {}
    for item in all_check_items:
        title = item.get('title', '未知检查项')
        
        # 特殊处理：将lloyds_sanctions_his合并到lloyds_sanctions的title下
        if item.get('name') == 'lloyds_sanctions_his':
            title = '船舶涉制裁名单风险情况'
        
        if title not in title_groups:
            title_groups[title] = []
        title_groups[title].append(item)
    
    print(f"🔍 发现 {len(title_groups)} 个不同的title分组:")
    for title, items in title_groups.items():
        print(f"  - {title}: {len(items)} 个检查项")
        for item in items:
            print(f"    * {item.get('name', 'unknown')} (sanctions_lev: {item.get('sanctions_lev', '无风险')})")
    
    # 构建新的数据结构
    restructured_data = {}
    
    for title, items in title_groups.items():
        restructured_data[title] = {}
        
        # 添加所有子检查项
        for item in items:
            name = item.get('name', 'unknown')
            restructured_data[title][name] = item
        
        # 添加第三方判断逻辑
        if len(items) > 1:
            # 多个检查项，需要合并判断
            sanctions_levs = [item.get('sanctions_lev', '无风险') for item in items]
            
            # 特殊处理：船舶涉制裁名单风险情况 = lloyds_sanctions + lloyds_sanctions_his
            if title == "船舶涉制裁名单风险情况":
                # 查找lloyds_sanctions和lloyds_sanctions_his
                lloyds_sanctions_item = None
                lloyds_sanctions_his_item = None
                
                for item in items:
                    if item.get('name') == 'lloyds_sanctions':
                        lloyds_sanctions_item = item
                    elif item.get('name') == 'lloyds_sanctions_his':
                        lloyds_sanctions_his_item = item
                
                # 组合判断逻辑：当前制裁 > 历史制裁 > 无风险
                if lloyds_sanctions_item and lloyds_sanctions_his_item:
                    current_lev = lloyds_sanctions_item.get('sanctions_lev', '无风险')
                    his_lev = lloyds_sanctions_his_item.get('sanctions_lev', '无风险')
                    
                    # 组合判断：当前制裁 > 历史制裁 > 无风险
                    if current_lev == '高风险':
                        highest_lev = '高风险'
                    elif his_lev == '中风险':
                        highest_lev = '中风险'
                    else:
                        highest_lev = '无风险'
                else:
                    highest_lev = get_highest_risk_level(sanctions_levs)
            else:
                # 其他检查项使用原有逻辑
                highest_lev = get_highest_risk_level(sanctions_levs)
            
            # 生成第三方判断逻辑名称 - 根据title生成更合适的名称
            third_party_name = generate_third_party_name(title, items)
            
            restructured_data[title][third_party_name] = {
                "name": third_party_name,
                "title": title,
                "description": f"能源公司{title}",
                "sanctions_lev": highest_lev
            }
            
            print(f"🔍 {title}: 合并 {len(items)} 个检查项，第三方判断 {third_party_name} = {highest_lev}")
        else:
            # 单个检查项，直接添加第三方判断
            item = items[0]
            third_party_name = generate_third_party_name(title, items)
            
            restructured_data[title][third_party_name] = {
                "name": third_party_name,
                "title": title,
                "description": f"能源公司{title}",
                "sanctions_lev": item.get('sanctions_lev', '无风险')
            }
            
            print(f"🔍 {title}: 单个检查项 {item.get('name', 'unknown')}，第三方判断 {third_party_name} = {item.get('sanctions_lev', '无风险')}")
    
    print(f"🔍 数据重构完成，共生成 {len(restructured_data)} 个title分组")
    return restructured_data

def group_check_items_by_risk_level(check_items_by_title):
    """
    根据第三方能源公司判断的sanctions_lev对check_items_by_title进行风险等级分组
    新的数据结构：将相同title的检查项合并到数组中，能源公司判断逻辑作为外层sanctions_lev
    
    参数:
        check_items_by_title: 按title重新组织的检查项数据
        
    返回:
        按风险等级分组的数据结构，使用数组格式
    """
    print("🔍 开始根据第三方能源公司判断进行风险等级分组（新格式）...")
    
    # 定义title到统一数组字段名的映射 - 使用统一的命名格式
    title_to_array_field = {
        "船舶相关方涉制裁风险情况": "risk_items",
        "挂靠高风险港口风险情况": "risk_items", 
        "船舶运输受制裁货物情况": "risk_items",
        "船舶涉及受制裁贸易风险情况": "risk_items",
        "AIS信号缺失风险情况": "risk_items",
        "隐蔽STS事件风险情况": "risk_items",
        "STS转运不合规风险情况": "risk_items",
        "船舶涉制裁名单风险情况": "risk_items",
        "船舶历史涉制裁名单风险情况": "risk_items",
        "船舶涉UANI清单风险情况": "risk_items",
        "AIS信号伪造及篡改风险情况": "risk_items",
        "暗港访问风险情况": "risk_items",
        "可疑徘徊风险情况": "risk_items"
    }
    
    risk_groups_by_title = {
        "high_risk": [],
        "mid_risk": [],
        "no_risk": []
    }
    
    for title, items in check_items_by_title.items():
        # 查找第三方能源公司判断逻辑
        nengyuan_item = None
        for item_name, item in items.items():
            if item_name.startswith("nengyuan_"):
                nengyuan_item = item
                break
        
        if nengyuan_item:
            sanctions_lev = nengyuan_item.get("sanctions_lev", "无风险")
            print(f"🔍 {title}: 第三方判断 = {sanctions_lev}")
        else:
            # 如果没有找到第三方判断逻辑，默认为无风险
            print(f"⚠️ {title}: 未找到第三方判断逻辑，默认为无风险")
            sanctions_lev = "无风险"
        
        # 收集所有非能源公司判断的检查项到数组中
        check_items_list = []
        for item_name, item in items.items():
            # 跳过能源公司判断逻辑项
            if not item_name.startswith("nengyuan_"):
                # 确保每个检查项都有完整的字段
                enhanced_item = item.copy()
                enhanced_item["title"] = title  # 确保标题一致
                check_items_list.append(enhanced_item)
        
        # 获取数组字段名
        array_field_name = title_to_array_field.get(title, f"risk_{title.replace('风险情况', '').replace('情况', '')}")
        
        # 创建新的分组项结构 - 按照您的示例格式
        group_item = {
            "title": title,  # 中文标题
            "sanctions_lev": sanctions_lev,  # 能源公司判断的风险等级
            array_field_name: check_items_list  # 所有相关的检查项数组
        }
        
        # 根据风险等级分组
        if sanctions_lev == "高风险":
            risk_groups_by_title["high_risk"].append(group_item)
        elif sanctions_lev == "中风险":
            risk_groups_by_title["mid_risk"].append(group_item)
        else:  # 无风险或其他
            risk_groups_by_title["no_risk"].append(group_item)
    
    # 统计分组结果
    high_count = len(risk_groups_by_title["high_risk"])
    mid_count = len(risk_groups_by_title["mid_risk"])
    no_count = len(risk_groups_by_title["no_risk"])
    
    print(f"🔍 风险等级分组完成（新格式）:")
    print(f"  高风险: {high_count} 个检查项组")
    print(f"  中风险: {mid_count} 个检查项组")
    print(f"  无风险: {no_count} 个检查项组")
    
    return risk_groups_by_title

def generate_third_party_name(title, items):
    """
    根据title和检查项生成第三方判断逻辑名称
    """
    # 定义title到第三方名称的映射
    title_mapping = {
        "AIS信号缺失风险情况": "nengyuan_ais_gap",
        "AIS信号伪造及篡改风险情况": "nengyuan_ais_spoof", 
        "STS转运不合规风险情况": "nengyuan_sts",
        "船舶相关方涉制裁风险情况": "nengyuan_compliance",
        "挂靠高风险港口风险情况": "nengyuan_port_calls",
        "隐蔽STS事件风险情况": "nengyuan_dark_sts",
        "暗港访问风险情况": "nengyuan_dark_port",
        "船舶涉UANI清单风险情况": "nengyuan_uani_check",
        "船舶运输受制裁货物情况": "nengyuan_cargo_risk",
        "船舶涉及受制裁贸易风险情况": "nengyuan_trades_risk",
        "船舶涉制裁名单风险情况": "nengyuan_lloyds_sanctions",
        "船舶历史涉制裁名单风险情况": "nengyuan_lloyds_sanctions",
        "可疑徘徊风险情况": "nengyuan_loitering_behavior"
    }
    
    # 如果title在映射中，直接使用
    if title in title_mapping:
        return title_mapping[title]
    
    # 否则根据第一个检查项的名称生成
    first_item_name = items[0].get('name', 'unknown')
    name_prefix = first_item_name.replace('_risk', '').split('_')[0]
    return f"nengyuan_{name_prefix}"

def test_new_risk_groups_structure():
    """
    测试新的risk_groups_by_title数据结构
    """
    print("🧪 开始测试新的risk_groups_by_title数据结构...")
    
    # 模拟check_items_by_title数据
    mock_check_items_by_title = {
        "挂靠高风险港口风险情况": {
            "high_risk_port": {
                "name": "high_risk_port",
                "title": "挂靠高风险港口风险情况",
                "description": "劳氏挂靠高风险港口风险情况",
                "sanctions_lev": "无风险",
                "sanctions_list": [
                    {
                        "VoyageId": None,
                        "VoyageStartTime": "2025-09-01T19:45:58+03:00",
                        "VoyageEndTime": None,
                        "VoyageRiskRating": "Red",
                        "StartPlace": {
                            "Name": "Ust-Luga",
                            "CountryName": "Russia",
                            "IsHighRiskPort": True
                        },
                        "EndPlace": {
                            "Name": None,
                            "CountryName": None,
                            "IsHighRiskPort": False
                        },
                        "RiskTypes": ["High Risk Port Calling"]
                    }
                ]
            },
            "has_port_calls_risk": {
                "name": "has_port_calls_risk",
                "title": "挂靠高风险港口风险情况",
                "description": "开普勒挂靠高风险港口风险情况",
                "sanctions_lev": "高风险",
                "sanctions_list": [
                    {
                        "volume": "-21213.0",
                        "endDate": 1741455780,
                        "portName": "Felton",
                        "zoneName": "Felton",
                        "startDate": 1741422663,
                        "shipToShip": "",
                        "countryName": "Cuba",
                        "sanctionedCargo": "True",
                        "sanctionedVessel": "True",
                        "sanctionedOwnership": "True"
                    }
                ]
            },
            "nengyuan_port_calls": {
                "name": "nengyuan_port_calls",
                "title": "挂靠高风险港口风险情况",
                "description": "能源公司挂靠高风险港口风险情况",
                "sanctions_lev": "高风险"
            }
        }
    }
    
    # 调用新的分组函数
    result = group_check_items_by_risk_level(mock_check_items_by_title)
    
    # 验证结果结构
    print("🧪 测试结果:")
    print(f"  高风险组数量: {len(result['high_risk'])}")
    print(f"  中风险组数量: {len(result['mid_risk'])}")
    print(f"  无风险组数量: {len(result['no_risk'])}")
    
    if result['high_risk']:
        high_risk_item = result['high_risk'][0]
        print(f"  高风险组示例:")
        print(f"    title: {high_risk_item.get('title')}")
        print(f"    sanctions_lev: {high_risk_item.get('sanctions_lev')}")
        print(f"    包含的字段: {[k for k in high_risk_item.keys() if k not in ['title', 'sanctions_lev']]}")
        
        # 检查数组字段
        if 'risk_items' in high_risk_item:
            risk_items = high_risk_item['risk_items']
            print(f"    risk_items 数组长度: {len(risk_items)}")
            for i, item in enumerate(risk_items):
                print(f"      检查项 {i+1}: {item.get('name')} (sanctions_lev: {item.get('sanctions_lev')})")
    
    print("✅ 测试完成")
    return result

def extract_sts_data(data: Dict[str, Any], queried_imo: str) -> List[Dict[str, Any]]:
    """
    从STS数据中提取所需字段，主体船舶字段放在Activity字段前面
    
    参数:
        data: 包含STS数据的字典
        
    返回:
        仅包含与当前查询船舶(queried_imo)进行STS的“对方船舶”的字典列表。
    """
    extracted_data = []
    
    if not data.get("IsSuccess", False) or "Data" not in data:
        return extracted_data
    
    for item in data["Data"]["Items"]:
        vessel_pairings = item.get("VesselPairings", [])
        if not vessel_pairings:
            # 无配对船舶则不输出（没有“对方船舶”可展示）
            continue

        main_vessel = vessel_pairings[0]
        main_is_queried = str(main_vessel.get("Imo", "")) == str(queried_imo)

        # 活动通用字段
        activity_fields = {
            "ActivityStartDate": item.get("ActivityStartDate"),
            "ActivityEndDate": item.get("ActivityEndDate"),
            "ActivityAreaName": item.get("ActivityAreaName"),
            "ComplianceRiskScore": item.get("ActivityRiskRating", {}).get("ComplianceRiskScore"),
            "ComplianceRiskReason": item.get("ActivityRiskRating", {}).get("ComplianceRiskReason"),
            "NearestPlaceName": item.get("NearestPlace", {}).get("name"),
            "NearestPlaceCountry": item.get("NearestPlace", {}).get("countryName")
        }

        def build_vessel_entry(v: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "VesselImo": v.get("Imo"),
                "VesselName": v.get("VesselName"),
                "VesselRiskRating": v.get("RiskRating"),
                "VesselFlag": v.get("Flag"),
                "VesselDwtTonnage": v.get("DwtTonnage"),
                "VesselType": v.get("VesselType"),
                "StsType": v.get("StsType"),
                "VesselDraftStart": v.get("DraftStart"),
                "VesselDraftEnd": v.get("DraftEnd"),
                "VesselSogStart": v.get("SogStart"),
                "VesselSogEnd": v.get("SogEnd")
            } | activity_fields

        if main_is_queried:
            # 只展示对方船（非主体）。如有多个非主体，则每艘输出一条。
            if len(vessel_pairings) > 1:
                for counter_v in vessel_pairings[1:]:
                    extracted_data.append(build_vessel_entry(counter_v))
            # 只有主体（且主体即查询船），没有对方则不输出
        else:
            # 主体不是查询船，则主体即为“对方船”，输出主体信息
            extracted_data.append(build_vessel_entry(main_vessel))
    
    return extracted_data

def check_uani_imo_from_database(vessel_imo: str) -> tuple[bool, dict]:
    """从Kingbase数据库查询UANI数据"""
    try:
        # 获取数据库配置
        db_config = get_kingbase_config()
        
        # 连接数据库
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 查询UANI数据
        cursor.execute("""
            SELECT imo, vessel_name, date_added, current_flag, former_flags
            FROM lng.uani_list 
            WHERE imo = %s
            ORDER BY date_added DESC
        """, (vessel_imo,))
        
        record = cursor.fetchone()
        
        # 关闭连接
        cursor.close()
        connection.close()
        
        if record:
            # 使用字典访问方式，兼容RealDictCursor
            if isinstance(record, dict):
                return True, {
                    "imo": record.get("imo"),
                    "vessel_name": record.get("vessel_name"), 
                    "date_added": record.get("date_added"),
                    "current_flag": record.get("current_flag"),
                    "former_flags": record.get("former_flags")
                }
            else:
                # 兼容普通cursor
                return True, {
                    "imo": record[0],
                    "vessel_name": record[1], 
                    "date_added": record[2],
                    "current_flag": record[3],
                    "former_flags": record[4]
                }
        else:
            return False, {}
            
    except Exception as e:
        print(f"❌ 查询UANI数据库失败: {str(e)}")
        return False, {}

def get_sts_data_from_api(vessel_imo: str) -> List[Dict[str, Any]]:
    """
    从API获取STS数据并提取所需字段
    
    参数:
        vessel_imo: 船舶IMO号
        
    返回:
        包含提取数据的字典列表
    """
    headers = {
        "accept": "application/json",
        "Authorization": STS_API_CONFIG["token"]
    }
    
    params = {
        "vesselImo": vessel_imo
    }
    
    try:
        response = requests.get(STS_API_CONFIG["base_url"], headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        return extract_sts_data(data, queried_imo=vessel_imo)
    except requests.exceptions.RequestException as e:
        print(f"请求STS API时出错: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"解析STS API JSON响应时出错: {e}")
        return []

@maritime_app.get("/")
async def root():
    """API根路径"""
    return {
        "message": "船舶信息综合API服务",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs"
    }

@maritime_app.post("/api/execute_full_analysis")
async def execute_full_analysis(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为今天")
):
    """执行完整分析流程"""
    try:
        processor = get_processor()
        processor.execute_full_analysis(vessel_imo, start_date, end_date)
        
        return {
            "status": "success",
            "message": "完整分析执行完成",
            "vessel_imo": vessel_imo,
            "date_range": f"{start_date} - {end_date}",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析执行失败: {str(e)}")

@maritime_app.get("/api/lloyds_compliance")
async def get_lloyds_compliance(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为今天")
):
    """获取劳氏船舶公司制裁信息"""
    try:
        processor = get_processor()
        result = processor.process_lloyds_compliance_data(vessel_imo, start_date, end_date)
        
        if result:
            return {
                "status": "success",
                "data": result,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "no_data",
                "message": "未找到合规数据",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取合规数据失败: {str(e)}")

@maritime_app.get("/api/lloyds_sanctions")
async def get_lloyds_sanctions(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取劳氏船舶制裁数据"""
    try:
        processor = get_processor()
        sanctions_df = processor.fetch_lloyds_sanctions(vessel_imo)
        
        if not sanctions_df.empty:
            processed_data = processor.transform_lloyds_sanctions_data(sanctions_df)
            return {
                "status": "success",
                "data": processed_data,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "no_data",
                "message": "未找到制裁数据",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取制裁数据失败: {str(e)}")

@maritime_app.get("/api/uani_check")
async def check_uani_imo(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """检查船舶是否在UANI清单中"""
    try:
        exists, data = check_uani_imo_from_database(vessel_imo)
        
        return {
            "status": "success",
            "vessel_imo": vessel_imo,
            "exists_in_uani": exists,
            "data": data if exists else None,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"UANI检查失败: {str(e)}")

@maritime_app.post("/api/kpler_analysis")
async def kpler_analysis_post(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为今天")
):
    """执行Kpler数据分析 (POST方法)"""
    try:
        processor = get_processor()
        result = processor.process_kpler([int(vessel_imo)], start_date, end_date)
        
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kpler分析失败: {str(e)}")

@maritime_app.get("/api/kpler_analysis")
async def kpler_analysis_get(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为今天")
):
    """执行Kpler数据分析 (GET方法)"""
    try:
        processor = get_processor()
        result = processor.process_kpler([int(vessel_imo)], start_date, end_date)
        
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kpler分析失败: {str(e)}")

@maritime_app.get("/api/voyage_risks")
async def get_voyage_risks(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为今天")
):
    """获取航次风险分析"""
    try:
        processor = get_processor()
        processor.analyze_voyage_risks(vessel_imo, start_date, end_date)
        
        # 转换航次风险数据格式（DataFrame -> 字典）
        voyage_risks_data = {}
        risk_types = ['high_risk_port', 'possible_dark_port', 'suspicious_ais_gap', 
                     'dark_sts', 'sanctioned_sts', 'loitering_behavior']
        
        print(f"🔍 调试 - 开始处理航次风险数据...")
        
        for risk_type in risk_types:
            if risk_type in processor.results['voyage_risks']:
                risk_data = processor.results['voyage_risks'][risk_type]
                print(f"🔍 调试 - {risk_type}: 数据类型 = {type(risk_data)}")
                
                # 处理DataFrame格式的数据
                if hasattr(risk_data, 'shape'):  # 这是一个DataFrame
                    print(f"🔍 调试 - {risk_type}: DataFrame, 形状 = {risk_data.shape}")
                    if risk_data.shape[0] > 0 and 'raw_data' in risk_data.columns:
                        raw_data_list = risk_data['raw_data'].tolist()
                        print(f"🔍 调试 - {risk_type}: raw_data列数据 = {len(raw_data_list)} 项")
                        # 判断风险等级
                        if risk_type in ['high_risk_port', 'sanctioned_sts']:
                            sanctions_lev = '高风险' if len(raw_data_list) > 0 else '无风险'
                        else:
                            sanctions_lev = '中风险' if len(raw_data_list) > 0 else '无风险'
                        
                        vessel_imo = None
                        try:
                            if 'VesselImo' in risk_data.columns and risk_data.shape[0] > 0:
                                vessel_imo = risk_data['VesselImo']
                        except Exception as e:
                            print(f"🔍 调试 - {risk_type}: 获取vessel_imo时出错: {e}")
                        
                        voyage_risks_data[risk_type] = {
                            'sanctions_lev': sanctions_lev,
                            'raw_data': raw_data_list,
                            'vessel_imo': vessel_imo
                        }
                        print(f"🔍 调试 - {risk_type}: 处理完成，风险等级 = {sanctions_lev}")
                    else:
                        sanctions_lev = '无风险'
                        voyage_risks_data[risk_type] = {
                            'sanctions_lev': sanctions_lev,
                            'raw_data': [],
                            'message': f'{risk_type}没有数据'
                        }
                        print(f"🔍 调试 - {risk_type}: DataFrame为空或没有raw_data列")
                else:
                    # 如果已经是字典格式，直接使用
                    print(f"🔍 调试 - {risk_type}: 已经是字典格式，直接使用")
                    voyage_risks_data[risk_type] = risk_data
            else:
                print(f"🔍 调试 - {risk_type}: 不存在于结果中")
                voyage_risks_data[risk_type] = {
                    'sanctions_lev': '无风险',
                    'raw_data': [],
                    'message': f'{risk_type}数据不存在'
                }
        
        # 暂时不获取汇总信息，避免调用有问题的get_voyage_risk_summary方法
        # summary = processor.get_voyage_risk_summary()
        
        return {
            "status": "success",
            "vessel_imo": vessel_imo,
            "voyage_risks": voyage_risks_data,
            "summary": [],  # 暂时返回空列表
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"航次风险分析失败: {str(e)}")

@maritime_app.get("/api/vessel_status")
async def get_vessel_status(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取船舶综合状态"""
    try:
        processor = get_processor()
        status = processor.check_vessel_status(vessel_imo)
        
        return {
            "status": "success",
            "vessel_imo": vessel_imo,
            "vessel_status": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取船舶状态失败: {str(e)}")

@maritime_app.get("/api/vessel_list")
async def get_vessel_basic_info(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取船舶基础信息"""
    try:
        processor = get_processor()
        vessel_info = processor.get_vessel_basic_info(vessel_imo)
        
        if "error" in vessel_info:
            return {
                "status": "success",
                "message": "暂无数据",
                "vessel_imo": vessel_imo,
                "data": {},
                "timestamp": datetime.now().isoformat()
            }
        
        return {
            "status": "success",
            "data": vessel_info,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ 获取船舶信息失败: {str(e)}")
        return {
            "status": "error",
            "message": f"获取船舶信息失败: {str(e)}",
            "vessel_imo": vessel_imo,
            "timestamp": datetime.now().isoformat()
        }

@maritime_app.get("/api/get_vessel_all_data")
async def get_vessel_all_data(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为当前日期")
):
    """获取船舶所有相关数据（一次性调用所有接口）"""
    try:
        process_id = os.getpid()
        print(f"[PID:{process_id}] 🔍 调试 - 开始处理船舶IMO: {vessel_imo}", flush=True)
        print(f"[PID:{process_id}] 🔍 调试 - 原始日期参数: start_date={start_date}, end_date={end_date}", flush=True)
        print(f"[PID:{process_id}] 🚀 API调用开始 - get_vessel_all_data - IMO: {vessel_imo} - {datetime.now()}", flush=True)
        
        # 设置默认日期
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            # 计算1年前的今天
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime("%Y-%m-%d")
        
        print(f"🔍 调试 - 设置后的日期范围: {start_date} - {end_date}")
        
        processor = get_processor()
        
        # 存储所有数据
        all_data = {
            "vessel_imo": vessel_imo,
            "date_range": f"{start_date} - {end_date}",
            "timestamp": datetime.now().isoformat()
        }
        
        # 用于收集所有sanctions_lev值
        all_sanctions_levs = []
        
        # 1. 获取劳氏船舶公司制裁信息
        try:
            lloyds_compliance = processor.process_lloyds_compliance_data(vessel_imo, start_date, end_date)
            # 添加sanctions_lev字段
            if lloyds_compliance and 'RiskScores' in lloyds_compliance:
                risk_scores = lloyds_compliance['RiskScores']
                # 查找RiskPeriodInMonths=12的记录
                risk_12_months = None
                for risk in risk_scores:
                    if risk.get('RiskPeriodInMonths') == 12:
                        risk_12_months = risk
                        break
                
                if risk_12_months and 'TotalRiskScore' in risk_12_months:
                    total_score = risk_12_months['TotalRiskScore']
                    if total_score > 80:
                        sanctions_lev = '高风险'
                    elif 60 <= total_score <= 80:
                        sanctions_lev = '中风险'
                    else:
                        sanctions_lev = '无风险'
                else:
                    sanctions_lev = '无风险'
            else:
                sanctions_lev = '无风险'
            
            # 在VesselImo后面添加sanctions_lev字段
            if lloyds_compliance and 'VesselImo' in lloyds_compliance:
                lloyds_compliance['sanctions_lev'] = sanctions_lev
            
            all_data["lloyds_compliance"] = lloyds_compliance
            all_sanctions_levs.append(sanctions_lev)
        except Exception as e:
            all_data["lloyds_compliance"] = {"error": "劳氏合规接口调用失败，暂无数据", "details": str(e)}
        
        # 2. 获取劳氏船舶制裁数据
        try:
            sanctions_df = processor.fetch_lloyds_sanctions(vessel_imo)
            if not sanctions_df.empty:
                lloyds_sanctions = processor.transform_lloyds_sanctions_data(sanctions_df)
                
                # 分离当前制裁和历史制裁数据
                lloyds_sanctions_current = []
                lloyds_sanctions_his = []
                
                if isinstance(lloyds_sanctions, list) and len(lloyds_sanctions) > 0:
                    for item in lloyds_sanctions:
                        if isinstance(item, dict):
                            # 分离制裁记录
                            current_sanctions = []
                            his_sanctions = []
                            
                            sanctions_list = item.get('sanctions_list', [])
                            for sanction in sanctions_list:
                                if isinstance(sanction, dict):
                                    sanction_end_date = sanction.get('endDate', '')
                                    # 判断endDate是否为空
                                    if pd.isna(sanction_end_date) or str(sanction_end_date).strip() in ('', 'None'):
                                        current_sanctions.append(sanction)
                                    else:
                                        his_sanctions.append(sanction)
                            
                            # 创建当前制裁检查项
                            current_item = item.copy()
                            current_item['sanctions_list'] = current_sanctions
                            current_item['sanctions_lev'] = '高风险' if len(current_sanctions) > 0 else '无风险'
                            current_item['is_in_sanctions'] = '是' if len(current_sanctions) > 0 else '否'
                            current_item['is_in_sanctions_his'] = '是' if len(his_sanctions) > 0 else '否'
                            lloyds_sanctions_current.append(current_item)
                            
                            # 创建历史制裁检查项
                            his_item = item.copy()
                            his_item['sanctions_list'] = his_sanctions
                            his_item['sanctions_lev'] = '中风险' if len(his_sanctions) > 0 else '无风险'
                            his_item['is_in_sanctions'] = '是' if len(current_sanctions) > 0 else '否'
                            his_item['is_in_sanctions_his'] = '是' if len(his_sanctions) > 0 else '否'
                            lloyds_sanctions_his.append(his_item)
                
                # 存储分离后的数据
                all_data["lloyds_sanctions"] = lloyds_sanctions_current
                all_data["lloyds_sanctions_his"] = lloyds_sanctions_his
                
                # 收集风险等级用于整体判断
                current_risk_levels = [item.get('sanctions_lev', '无风险') for item in lloyds_sanctions_current]
                his_risk_levels = [item.get('sanctions_lev', '无风险') for item in lloyds_sanctions_his]
                
                # 整体风险等级判断：当前制裁 > 历史制裁 > 无风险
                overall_risk_level = '无风险'
                if '高风险' in current_risk_levels:
                    overall_risk_level = '高风险'
                elif '中风险' in his_risk_levels:
                    overall_risk_level = '中风险'
                
                all_sanctions_levs.append(overall_risk_level)
            else:
                all_data["lloyds_sanctions"] = {"message": "未找到制裁数据", "sanctions_lev": "无风险"}
                all_data["lloyds_sanctions_his"] = {"message": "未找到历史制裁数据", "sanctions_lev": "无风险"}
                all_sanctions_levs.append('无风险')
        except Exception as e:
            all_data["lloyds_sanctions"] = {"error": "劳氏制裁接口调用失败，暂无数据", "details": str(e), "sanctions_lev": "无风险"}
            all_data["lloyds_sanctions_his"] = {"error": "劳氏制裁接口调用失败，暂无数据", "details": str(e), "sanctions_lev": "无风险"}
            all_sanctions_levs.append('无风险')
        
        # 3. 检查UANI数据
        try:
            exists, uani_data = check_uani_imo_from_database(vessel_imo)
            # 添加sanctions_lev字段
            sanctions_lev = '高风险' if exists else '无风险'
            all_data["uani_check"] = {
                "exists_in_uani": exists,
                "sanctions_lev": sanctions_lev,
                "data": uani_data if exists else None
            }
            all_sanctions_levs.append(sanctions_lev)
        except Exception as e:
            all_data["uani_check"] = {"error": str(e)}
        
        # 4. 获取Kpler数据分析并拆分成独立检查项
        kpler_risk_items = []
        try:
            print(f"🔍 调试 - 开始调用Kpler分析...")
            print(f"🔍 调试 - Kpler参数: vessel_imo={vessel_imo}, start_date='{start_date}', end_date='{end_date}'")
            kpler_data = processor.process_kpler([int(vessel_imo)], start_date, end_date)
            print(f"🔍 调试 - Kpler分析完成，数据类型: {type(kpler_data)}")
            print(f"🔍 调试 - Kpler分析完成，数据内容: {kpler_data}")
            
            if kpler_data and int(vessel_imo) in kpler_data:
                vessel_data = kpler_data[int(vessel_imo)]
                print(f"🔍 调试 - 找到船舶 {vessel_imo} 的Kpler数据: {vessel_data}")
                
                # 定义kpler风险检查项映射
                kpler_risk_mapping = {
                    "has_sanctioned_cargo_risk": {
                        "title": "船舶运输受制裁货物情况",
                        "description": "开普勒船舶运输受制裁货物情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_sanctioned_cargo_list"
                    },
                    "has_sanctioned_trades_risk": {
                        "title": "船舶涉及受制裁贸易风险情况", 
                        "description": "开普勒船舶涉及受制裁贸易风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_sanctioned_trades_list"
                    },
                    # "has_sanctioned_flag_risk": {
                    #     "title": "船旗受制裁风险情况",
                    #     "description": "开普勒船旗受制裁风险情况", 
                    #     "list_field": "sanctions_list",
                    #     "original_list_field": "has_sanctioned_flag_list"
                    # },
                    "has_port_calls_risk": {
                        "title": "挂靠高风险港口风险情况",
                        "description": "开普勒挂靠高风险港口风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_port_calls_list"
                    },
                    "has_sts_events_risk": {
                        "title": "STS转运不合规风险情况",
                        "description": "开普勒STS转运不合规风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_sts_events_list"
                    },
                    "has_ais_gap_risk": {
                        "title": "AIS信号缺失风险情况",
                        "description": "开普勒AIS信号缺失风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_ais_gap_list"
                    },
                    "has_ais_spoofs_risk": {
                        "title": "AIS信号伪造及篡改风险情况",
                        "description": "开普勒AIS信号伪造及篡改风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_ais_spoofs_list"
                    },
                    "has_dark_sts_risk": {
                        "title": "隐蔽STS事件风险情况",
                        "description": "开普勒隐蔽STS事件风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_dark_sts_list"
                    },
                    "has_sanctioned_companies_risk": {
                        "title": "船舶相关方涉制裁风险情况",
                        "description": "开普勒船舶相关方涉制裁风险情况",
                        "list_field": "sanctions_list",
                        "original_list_field": "has_sanctioned_companies_list",
                        "vessel_companies_table": "vessel_companies_table",
                        "sanctioned_companies_table": "sanctioned_companies_table"
                    }
                }
                
                # 为每个风险项创建独立的检查项
                for risk_field, config in kpler_risk_mapping.items():
                    if risk_field in vessel_data:
                        risk_value = vessel_data[risk_field]

                        # 转换风险等级
                        if risk_value == "高风险":
                            sanctions_lev = "高风险"
                        elif risk_value == "中风险":
                            sanctions_lev = "中风险"
                        else:
                            sanctions_lev = "无风险"
                        
                        # 构建数据对象
                        risk_data = {
                            "imo": vessel_data.get("imo"),
                            risk_field: risk_value
                        }
                        
                        # 添加对应的列表字段
                        list_field = config["list_field"]
                        original_list_field = config["original_list_field"]
                        
                        # 初始化 sanctions_list 为空数组
                        risk_data[list_field] = []
                        
                        # 特殊处理 has_sanctioned_companies_risk，添加表格字段
                        if risk_field == "has_sanctioned_companies_risk":
                            vessel_table_field = config.get("vessel_companies_table")
                            sanctioned_table_field = config.get("sanctioned_companies_table")
                            
                        
                        if original_list_field in vessel_data:
                            # 将原来的列表数据复制到sanctions_list字段
                            original_data = vessel_data[original_list_field]
                            
                            # 解析数据为JSON KV结构
                            if isinstance(original_data, str) and original_data.strip():
                                # 如果是字符串，尝试解析为KV结构
                                try:
                                    # 按 || 分割多个记录
                                    records = original_data.split(" || ")
                                    parsed_records = []
                                    
                                    for record in records:
                                        if record.strip():
                                            # 按 , 分割键值对
                                            kv_pairs = record.split(", ")
                                            record_dict = {}
                                            
                                            for kv in kv_pairs:
                                                if ":" in kv:
                                                    key, value = kv.split(":", 1)
                                                    record_dict[key.strip()] = value.strip()
                                            
                                            if record_dict:
                                                parsed_records.append(record_dict)
                                    
                                    # 存储解析后的数据
                                    risk_data[list_field] = parsed_records
                                except Exception as e:
                                    risk_data[list_field] = original_data
                            else:
                                # 如果不是字符串或为空，直接使用原数据
                                risk_data[list_field] = original_data
                        
                        # 特殊处理 has_sanctioned_companies_risk，只保留单独的制裁记录，删除重复的合并记录
                        if risk_field == "has_sanctioned_companies_risk":
                            # 获取制裁公司数据
                            sanctioned_table_field = config.get("sanctioned_companies_table")
                            sanctioned_companies_data = []
                            if sanctioned_table_field and sanctioned_table_field in vessel_data:
                                sanctioned_companies_data = vessel_data[sanctioned_table_field]
                            
                            # 获取船舶公司数据用于获取公司名称
                            vessel_table_field = config.get("vessel_companies_table")
                            vessel_companies_data = []
                            if vessel_table_field and vessel_table_field in vessel_data:
                                vessel_companies_data = vessel_data[vessel_table_field]
                            
                            # 创建公司名称和类型名称映射
                            company_name_map = {}
                            type_name_map = {}
                            for vessel_company in vessel_companies_data:
                                type_code = vessel_company.get("type")
                                company_name = vessel_company.get("name", "")
                                type_name = vessel_company.get("typeName", "")
                                if type_code is not None:
                                    company_name_map[type_code] = company_name
                                    type_name_map[type_code] = type_name
                            
                            # 只保留单独的制裁记录，不生成合并记录
                            individual_sanctions = []
                            for sanctioned_company in sanctioned_companies_data:
                                sanction_type = sanctioned_company.get("type")
                                company_name = company_name_map.get(sanction_type, "")
                                type_name = type_name_map.get(sanction_type, "")
                                
                                individual_sanctions.append({
                                    "name": company_name,
                                    "type": sanction_type,
                                    "typeName": type_name,
                                    "source": {
                                        "name": sanctioned_company.get("sourceName", ""),
                                        "startDate": sanctioned_company.get("startDate", ""),
                                        "endDate": ""
                                    }
                                })
                            
                            # 替换 sanctions_list 为单独的制裁记录
                            if individual_sanctions:
                                risk_data[list_field] = individual_sanctions
                        
                        # 创建检查项 - 直接展开数据，不使用data字段包装
                        risk_item = {
                            "name": risk_field,
                            "title": config["title"],
                            "description": config["description"],
                            "sanctions_lev": sanctions_lev
                        }
                        
                        # 将risk_data中的数据直接展开到外层
                        for key, value in risk_data.items():
                            if key not in ["name", "title", "description", "sanctions_lev"]:
                                risk_item[key] = value
                        
                        # 统一规则：无风险则清空 sanctions_list（若存在对应字段）
                        try:
                            if risk_item.get("sanctions_lev", "无风险") == "无风险":
                                if "sanctions_list" in risk_item and isinstance(risk_item["sanctions_list"], list):
                                    risk_item["sanctions_list"] = []
                        except Exception:
                            pass

                        kpler_risk_items.append(risk_item)
                        all_sanctions_levs.append(sanctions_lev)
            
                print(f"🔍 调试 - Kpler风险检查项创建完成，共 {len(kpler_risk_items)} 个")
                all_data["kpler_risk_items"] = kpler_risk_items
            else:
                print(f"⚠️ 警告 - Kpler数据为空或未找到船舶 {vessel_imo} 的数据")
                print(f"🔍 调试 - Kpler数据键: {list(kpler_data.keys()) if kpler_data else 'None'}")
                all_data["kpler_risk_items"] = []
        except Exception as e:
            print(f"❌ Kpler分析异常: {str(e)}")
            print(f"❌ Kpler异常类型: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            all_data["kpler_risk_items"] = {"error": str(e), "error_type": type(e).__name__}
        
        # 5. 获取VesselAisManipulation数据
        ais_manipulation_success = False
        try:
            ais_manipulation_data = processor.process_vessel_ais_manipulation(vessel_imo)
            
            # 检查是否有错误信息
            if 'error' in ais_manipulation_data:
                print(f"❌ 劳氏AIS操纵接口调用失败: {ais_manipulation_data['error']}")
                ais_manipulation_success = False
            else:
                ais_manipulation_success = True
            
            # 将risks字段改为sanctions_list，同时保持原有字段名
            if isinstance(ais_manipulation_data, dict) and 'risks' in ais_manipulation_data:
                ais_manipulation_data['sanctions_list'] = ais_manipulation_data['risks']
                
                # 根据sanctions_list中的ComplianceRiskScore值判断风险等级
                risks_data = ais_manipulation_data['risks']
                # 额外提取前端需要的4个字段（取首个非空值，兼容多种嵌套）
                try:
                    def _get_first_non_empty(getters):
                        for getter in getters:
                            try:
                                value = getter()
                            except Exception:
                                value = None
                            if value not in (None, "", []):
                                return value
                        return ""

                    extracted_start_dt = ""
                    extracted_end_dt = ""
                    extracted_eez = ""
                    extracted_voyage_risk_rating = ""

                    if isinstance(risks_data, list):
                        for risk in risks_data:
                            if not isinstance(risk, dict):
                                continue
                            # AisGapStartDateTime
                            if not extracted_start_dt:
                                extracted_start_dt = _get_first_non_empty([
                                    lambda: risk.get('AisGapStartDateTime'),
                                    lambda: (risk.get('AISGap') or {}).get('AisGapStartDateTime'),
                                    lambda: (risk.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartDateTime')
                                ])
                            # AisGapEndDateTime
                            if not extracted_end_dt:
                                extracted_end_dt = _get_first_non_empty([
                                    lambda: risk.get('AisGapEndDateTime'),
                                    lambda: (risk.get('AISGap') or {}).get('AisGapEndDateTime'),
                                    lambda: (risk.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapEndDateTime')
                                ])
                            # AisGapStartEezName
                            if not extracted_eez:
                                extracted_eez = _get_first_non_empty([
                                    lambda: risk.get('AisGapStartEezName'),
                                    lambda: (risk.get('AISGap') or {}).get('AisGapStartEezName'),
                                    lambda: (risk.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartEezName')
                                ])
                            # VoyageRiskRating
                            if not extracted_voyage_risk_rating:
                                extracted_voyage_risk_rating = _get_first_non_empty([
                                    lambda: risk.get('VoyageRiskRating'),
                                    lambda: (risk.get('VoyageInfo') or {}).get('VoyageRiskRating')
                                ])
                            # 如果四个都已提取到，提前结束
                            if all([extracted_start_dt != "", extracted_end_dt != "", extracted_eez != "", extracted_voyage_risk_rating != ""]):
                                break

                    ais_manipulation_data['AisGapStartDateTime'] = extracted_start_dt
                    ais_manipulation_data['AisGapEndDateTime'] = extracted_end_dt
                    ais_manipulation_data['AisGapStartEezName'] = extracted_eez
                    ais_manipulation_data['VoyageRiskRating'] = extracted_voyage_risk_rating
                except Exception as _extract_err:
                    # 提取失败不影响主体逻辑
                    print(f"⚠️ AIS Manipulation 字段提取失败: {_extract_err}")
                if isinstance(risks_data, list) and len(risks_data) > 0:
                    # 检查ComplianceRiskScore值
                    high_risk_count = 0
                    medium_risk_count = 0
                    
                    print(f"🔍 调试 - 开始检查ComplianceRiskScore值...")
                    print(f"   risks_data长度: {len(risks_data)}")
                    
                    for i, risk in enumerate(risks_data):
                        compliance_risk_score = risk.get('ComplianceRiskScore', '')
                        print(f"   风险{i+1}: ComplianceRiskScore = '{compliance_risk_score}'")
                        if compliance_risk_score == 'High':
                            high_risk_count += 1
                            print(f"     → 识别为High风险")
                        elif compliance_risk_score == 'Medium':
                            medium_risk_count += 1
                            print(f"     → 识别为Medium风险")
                        else:
                            print(f"     → 识别为其他风险等级")
                    
                    print(f"🔍 调试 - 统计结果:")
                    print(f"   High风险数量: {high_risk_count}")
                    print(f"   Medium风险数量: {medium_risk_count}")
                    
                    # 根据风险等级判断sanctions_lev
                    if high_risk_count > 0:
                        print(f"🔍 调试 - 判断结果: 有High风险 → 高风险")
                        ais_manipulation_data['sanctions_lev'] = '高风险'
                        all_sanctions_levs.append('高风险')
                    elif medium_risk_count > 0:
                        print(f"🔍 调试 - 判断结果: 有Medium风险 → 中风险")
                        ais_manipulation_data['sanctions_lev'] = '中风险'
                        all_sanctions_levs.append('中风险')
                    else:
                        print(f"🔍 调试 - 判断结果: 无High/Medium风险 → 无风险")
                        # 如果没有High或Medium，但有其他数据，可能是Low或无评分
                        ais_manipulation_data['sanctions_lev'] = '无风险'
                        all_sanctions_levs.append('无风险')
                    
                    print(f"🔍 调试 - 最终设置的sanctions_lev: {ais_manipulation_data['sanctions_lev']}")
                else:
                    print(f"🔍 调试 - risks_data为空或不是列表")
                    ais_manipulation_data['sanctions_lev'] = '无风险'
                    all_sanctions_levs.append('无风险')
            else:
                ais_manipulation_data['sanctions_lev'] = '无风险'
                all_sanctions_levs.append('无风险')
            
            all_data["ais_manipulation"] = ais_manipulation_data
        except Exception as e:
            print(f"❌ 劳氏AIS操纵接口调用失败: {e}")
            all_data["ais_manipulation"] = {"error": "劳氏AIS操纵接口调用失败，暂无数据", "details": str(e), "sanctions_lev": "无风险"}
            all_sanctions_levs.append("无风险")
            ais_manipulation_success = False
        
        # 6. 获取航次风险分析（只有在关键接口调用成功时才执行）
        if ais_manipulation_success:
            try:
                print(f"🔍 调试 - 开始调用航次风险分析...")
                processor.analyze_voyage_risks(vessel_imo, start_date, end_date)
                print(f"🔍 调试 - 航次风险分析完成，开始获取摘要...")
                summary = processor.get_voyage_risk_summary()
                print(f"🔍 调试 - 航次风险摘要获取完成")
                
                # 处理各种风险数据的sanctions_lev字段
                voyage_risks = processor.results.get('voyage_risks', {})
                print(f"🔍 调试 - 原始航次风险数据: {list(voyage_risks.keys())}")
                
                # 处理每个航次风险类型
                risk_types = ['high_risk_port', 'possible_dark_port', 'suspicious_ais_gap', 'dark_sts', 'sanctioned_sts', 'loitering_behavior']
                
                for risk_type in risk_types:
                    if risk_type in voyage_risks:
                        risk_data = voyage_risks[risk_type]
                        print(f"🔍 调试 - {risk_type} 数据类型: {type(risk_data)}")
                        
                        # 处理DataFrame格式的数据
                        if hasattr(risk_data, 'shape'):  # 这是一个DataFrame
                            print(f"🔍 调试 - {risk_type} 是DataFrame，行数: {risk_data.shape[0]}")
                            
                            if risk_data.shape[0] > 0 and 'raw_data' in risk_data.columns:
                                # 提取raw_data列的数据
                                raw_data_list = risk_data['raw_data'].tolist()
                                print(f"🔍 调试 - {risk_type} raw_data列数据: {len(raw_data_list)} 项")
                                
                                # 判断风险等级
                                if risk_type == 'high_risk_port':
                                    # 特殊处理high_risk_port：检查是否包含"没有发现高风险港口航次"消息
                                    if len(raw_data_list) > 0:
                                        # 检查第一个元素是否包含"没有发现高风险港口航次"消息
                                        first_item = raw_data_list[0]
                                        if isinstance(first_item, dict) and first_item.get('message') == '没有发现高风险港口航次':
                                            sanctions_lev = '无风险'
                                        else:
                                            sanctions_lev = '高风险'
                                    else:
                                        sanctions_lev = '无风险'
                                elif risk_type == 'sanctioned_sts':
                                    sanctions_lev = '高风险' if len(raw_data_list) > 0 else '无风险'
                                else:
                                    sanctions_lev = '中风险' if len(raw_data_list) > 0 else '无风险'
                                
                                # 为所有航次风险字段创建扁平化数据结构
                                flattened_raw_data = []
                                for item in raw_data_list:
                                    if isinstance(item, dict):
                                        flattened_item = {}
                                        # 提取VesselInfo
                                        if 'VesselInfo' in item:
                                            vessel_info = item['VesselInfo']
                                            flattened_item.update({
                                                'VesselImo': vessel_info.get('VesselImo'),
                                                'VesselName': vessel_info.get('VesselName'),
                                                'VesselType': vessel_info.get('VesselType'),
                                                'Flag': vessel_info.get('Flag')
                                            })
                                        # 提取VoyageInfo
                                        if 'VoyageInfo' in item:
                                            voyage_info = item['VoyageInfo']
                                            flattened_item.update({
                                                'VoyageStartTime': voyage_info.get('VoyageStartTime'),
                                                'VoyageEndTime': voyage_info.get('VoyageEndTime'),
                                                'VoyageRiskRating': voyage_info.get('VoyageRiskRating'),
                                                'RiskTypes': voyage_info.get('RiskTypes', [])
                                            })
                                            # 特殊处理 dark_sts：展开AISGap关键字段
                                            if risk_type == 'dark_sts':
                                                ais_gap = voyage_info.get('AISGap', {}) if isinstance(voyage_info, dict) else {}
                                                flattened_item.update({
                                                    'AisGapStartDateTime': ais_gap.get('AisGapStartDateTime'),
                                                    'AisGapEndDateTime': ais_gap.get('AisGapEndDateTime'),
                                                    'AisGapStartEezName': ais_gap.get('AisGapStartEezName'),
                                                    'is_sanctioned_eez': ais_gap.get('is_sanctioned_eez'),
                                                    'OneWayDarkSts': ais_gap.get('1Way'),
                                                    'TwoWayDarkSts': ais_gap.get('2Way')
                                                })
                                            # 特殊处理 suspicious_ais_gap：展开AISGap关键字段
                                            elif risk_type == 'suspicious_ais_gap':
                                                ais_gap = voyage_info.get('AISGap', {}) if isinstance(voyage_info, dict) else {}
                                                flattened_item.update({
                                                    'AisGapStartDateTime': ais_gap.get('AisGapStartDateTime'),
                                                    'AisGapEndDateTime': ais_gap.get('AisGapEndDateTime'),
                                                    'AisGapStartEezName': ais_gap.get('AisGapStartEezName')
                                                })
                                                # 若有航次风险评级，补充
                                                if isinstance(item, dict):
                                                    flattened_item['VoyageRiskRating'] = item.get('VoyageRiskRating', flattened_item.get('VoyageRiskRating'))
                                            # 特殊处理 loitering_behavior：展开LoiteringEvent关键字段
                                            elif risk_type == 'loitering_behavior':
                                                loitering_event = voyage_info.get('LoiteringEvent', {}) if isinstance(voyage_info, dict) else {}
                                                flattened_item.update({
                                                    'LoiteringStart': loitering_event.get('LoiteringStart'),
                                                    'LoiteringEnd': loitering_event.get('LoiteringEnd'),
                                                    'LoiteringRiskTypes': loitering_event.get('RiskTypes', []),
                                                    'VoyageRiskRating': voyage_info.get('VoyageRiskRating')
                                                })
                                        # 提取其他可能的信息
                                        for key, value in item.items():
                                            if key not in ['VesselInfo', 'VoyageInfo']:
                                                flattened_item[key] = value
                                        flattened_raw_data.append(flattened_item)
                                
                                # 特殊处理sanctioned_sts：扁平化CounterpartVessels结构
                                if risk_type == 'sanctioned_sts':
                                    flattened_sanctions_list = []
                                    
                                    print(f"🔍 调试 - sanctioned_sts 开始扁平化处理，原始数据: {flattened_raw_data}")
                                    
                                    for voyage_item in flattened_raw_data:
                                        if isinstance(voyage_item, dict):
                                            print(f"🔍 调试 - 处理航次: {voyage_item.get('VoyageStartTime')}")
                                            
                                            # 提取航次基本信息
                                            voyage_info = {
                                                'VoyageStartTime': voyage_item.get('VoyageStartTime'),
                                                'VoyageEndTime': voyage_item.get('VoyageEndTime'),
                                                'STSEvent': voyage_item.get('STSEvent', {})
                                            }
                                            
                                            # 处理每个航次的CounterpartVessels
                                            counterpart_vessels = voyage_item.get('CounterpartVessels', [])
                                            print(f"🔍 调试 - CounterpartVessels数量: {len(counterpart_vessels)}")
                                            print(f"🔍 调试 - CounterpartVessels内容: {counterpart_vessels}")
                                            
                                            # 如果CounterpartVessels是空的，尝试从STSEvent中获取CounterpartVessel
                                            if not counterpart_vessels and 'STSEvent' in voyage_item:
                                                sts_event = voyage_item.get('STSEvent', {})
                                                if isinstance(sts_event, dict) and 'CounterpartVessel' in sts_event:
                                                    counterpart_vessel = sts_event.get('CounterpartVessel')
                                                    if counterpart_vessel:
                                                        counterpart_vessels = [counterpart_vessel]
                                                        print(f"🔍 调试 - 从STSEvent中找到CounterpartVessel: {counterpart_vessel}")
                                            
                                            for counterpart in counterpart_vessels:
                                                if isinstance(counterpart, dict):
                                                    # 直接从CounterpartVessels数组中提取字段
                                                    flattened_item = {
                                                        **voyage_info,
                                                        'IsVesselSanctioned': counterpart.get('IsVesselSanctioned'),
                                                        'IsVesselOwnershipSanctioned': counterpart.get('IsVesselOwnershipSanctioned'),
                                                        'IsVesselOwnershipLinkedToSanctionedEntities': counterpart.get('IsVesselOwnershipLinkedToSanctionedEntities'),
                                                        'VesselImo': counterpart.get('VesselImo'),
                                                        'VesselName': counterpart.get('VesselName'),
                                                        'VesselType': counterpart.get('VesselType'),
                                                        'RiskIndicators': counterpart.get('RiskIndicators'),
                                                        'RiskScore': counterpart.get('RiskScore')
                                                    }
                                                    flattened_sanctions_list.append(flattened_item)
                                                    print(f"🔍 调试 - 添加船舶记录: {counterpart.get('VesselImo')}")
                                                else:
                                                    print(f"🔍 调试 - CounterpartVessels元素不是字典: {type(counterpart)}")
                                    
                                    print(f"🔍 调试 - 扁平化完成，总记录数: {len(flattened_sanctions_list)}")
                                    
                                    # 去重：基于VesselImo和VoyageStartTime去重
                                    unique_sanctions_list = []
                                    seen_combinations = set()
                                    
                                    for item in flattened_sanctions_list:
                                        key = (item.get('VesselImo'), item.get('VoyageStartTime'))
                                        if key not in seen_combinations:
                                            unique_sanctions_list.append(item)
                                            seen_combinations.add(key)
                                        else:
                                            print(f"🔍 调试 - 发现重复记录，跳过: {key}")
                                    
                                    print(f"🔍 调试 - 去重后记录数: {len(unique_sanctions_list)}")
                                    flattened_raw_data = unique_sanctions_list
                            
                                voyage_risks[risk_type] = {
                                    'sanctions_lev': sanctions_lev,
                                    'raw_data': flattened_raw_data,
                                    'vessel_imo': risk_data['VesselImo'] if hasattr(risk_data, 'columns') and 'VesselImo' in risk_data.columns and risk_data.shape[0] > 0 else None
                                }
                                
                                all_sanctions_levs.append(sanctions_lev)
                                print(f"🔍 调试 - {risk_type} 处理完成，风险等级: {sanctions_lev}")
                            else:
                                # DataFrame为空或没有raw_data列
                                sanctions_lev = '无风险'
                                voyage_risks[risk_type] = {
                                    'sanctions_lev': sanctions_lev,
                                    'raw_data': [],
                                    'message': f'{risk_type}没有数据'
                                }
                                all_sanctions_levs.append(sanctions_lev)
                                print(f"🔍 调试 - {risk_type} DataFrame为空")
                        else:
                            # 处理字典格式的数据（兼容旧格式）
                            print(f"🔍 调试 - {risk_type} 是字典格式")
                            if isinstance(risk_data, dict) and 'raw_data' in risk_data:
                                raw_data = risk_data['raw_data']
                                if isinstance(raw_data, dict) and 'VesselImo' in raw_data:
                                    vessel_count = len([k for k, v in raw_data['VesselImo'].items() if v])
                                    if risk_type in ['high_risk_port', 'sanctioned_sts']:
                                        sanctions_lev = '高风险' if vessel_count > 0 else '无风险'
                                    else:
                                        sanctions_lev = '中风险' if vessel_count > 0 else '无风险'
                                else:
                                    sanctions_lev = '无风险'
                            else:
                                sanctions_lev = '无风险'
                            
                            # 更新数据结构
                            risk_data_new = {'sanctions_lev': sanctions_lev}
                            for key, value in risk_data.items():
                                risk_data_new[key] = value
                            voyage_risks[risk_type] = risk_data_new
                            all_sanctions_levs.append(sanctions_lev)
                            print(f"🔍 调试 - {risk_type} 字典格式处理完成，风险等级: {sanctions_lev}")
                    else:
                        # 该风险类型不存在
                        sanctions_lev = '无风险'
                        voyage_risks[risk_type] = {
                            'sanctions_lev': sanctions_lev,
                            'raw_data': [],
                            'message': f'{risk_type}数据不存在'
                        }
                        all_sanctions_levs.append(sanctions_lev)
                        print(f"🔍 调试 - {risk_type} 不存在，设置为无风险")
            
                # 保持原有的voyage_risks结构，包含risks和summary
                all_data["voyage_risks"] = {
                    "risks": voyage_risks,
                    "summary": summary.to_dict('records') if not summary.empty else []
                }
                print(f"🔍 调试 - 航次风险数据处理完成，sanctions_levs: {all_sanctions_levs}")
            except Exception as e:
                print(f"❌ 航次风险数据处理异常: {str(e)}")
                import traceback
                traceback.print_exc()
                all_data["voyage_risks"] = {"error": "劳氏航次风险接口调用失败，暂无数据", "details": str(e)}
                # 添加默认的无风险等级
                for _ in range(6):  # 6个航次风险类型
                    all_sanctions_levs.append("无风险")
        else:
            # 当劳氏AIS操纵接口调用失败时，不执行航次风险分析
            print(f"⚠️ 警告 - 劳氏AIS操纵接口调用失败，跳过航次风险分析")
            all_data["voyage_risks"] = {
                "risks": {},
                "summary": [],
                "message": "劳氏AIS操纵接口调用失败，无法执行航次风险分析"
            }
            # 添加默认的无风险等级
            for _ in range(6):  # 6个航次风险类型
                all_sanctions_levs.append("无风险")
        
        # 如果外部API调用有错误，优先返回失败结构
        if hasattr(processor, 'api_errors') and processor.api_errors:
            # 只返回简单的失败状态，不返回具体的状态码和异常情况
            return {
                "status": "Failure"
            }

        # 6. 获取船舶综合状态
        try:
            vessel_status = processor.check_vessel_status(vessel_imo)
            all_data["vessel_status"] = vessel_status
        except Exception as e:
            all_data["vessel_status"] = {"error": str(e)}
        
        # 综合判定sanctions_lev_all
        sanctions_lev_all = '无风险'  # 默认值
        if '高风险' in all_sanctions_levs:
            sanctions_lev_all = '高风险'
        elif '中风险' in all_sanctions_levs:
            sanctions_lev_all = '中风险'
        
        # 在外层vessel_imo后面添加sanctions_lev_all字段
        all_data["sanctions_lev_all"] = sanctions_lev_all
        
        # 重新设计数据结构：根据sanctions_lev等级分组 - 已注释，使用新的risk_groups_by_title结构
        # risk_groups = {
        #     "high_risk": [],
        #     "mid_risk": [],
        #     "no_risk": []
        # }
        
        # 单独提取lloyds_compliance检查项
        lloyds_compliance_item = None
        
        # 创建所有检查项
        all_check_items = []
        
        # 1. 基础检查项
        for name, config in CHECK_CONFIG.items():
            if name in ["high_risk_port", "possible_dark_port", "suspicious_ais_gap", "dark_sts", "sanctioned_sts", "loitering_behavior"]:
                continue  # 跳过航次风险检查项，后面单独处理
            
            data = all_data.get(name, {})
            if data is None:
                data = {}
            
            # 如果是lloyds_compliance，单独处理并使用已计算的风险等级
            if name == "lloyds_compliance":
                # 使用已经计算好的sanctions_lev（基于TotalRiskScore）
                sanctions_lev = data.get("sanctions_lev", "无风险")
                print(f"🔍 调试 - lloyds_compliance使用已计算的风险等级: {sanctions_lev}")
                
                check_item = create_check_item(name, config["title"], config["description"], data, sanctions_lev)
                lloyds_compliance_item = check_item
            else:
                # 使用数据中已有的sanctions_lev，如果没有则默认为"无风险"
                sanctions_lev = data.get("sanctions_lev", "无风险") if isinstance(data, dict) else "无风险"
                check_item = create_check_item(name, config["title"], config["description"], data, sanctions_lev)
                all_check_items.append(check_item)
        
        # 2. 航次风险检查项
        voyage_risks = all_data.get("voyage_risks", {})
        if voyage_risks is None:
            voyage_risks = {}
        voyage_risks_data = voyage_risks.get("risks", {})
        if voyage_risks_data is None:
            voyage_risks_data = {}
        print(f"🔍 调试 - voyage_risks_data keys: {list(voyage_risks_data.keys()) if voyage_risks_data else 'None'}")
        
        for name in ["high_risk_port", "possible_dark_port", "suspicious_ais_gap", "dark_sts", "sanctioned_sts", "loitering_behavior"]:
            if name in CHECK_CONFIG:
                data = voyage_risks_data.get(name, {})
                if data is None:
                    data = {}
                print(f"🔍 调试 - {name} data: {data}")
                
                # 处理航次风险数据：正确处理DataFrame格式
                if hasattr(data, 'shape'):  # 这是一个DataFrame
                    print(f"🔍 调试 - {name} 是DataFrame，行数: {data.shape[0]}")
                    
                    if data.shape[0] > 0 and 'raw_data' in data.columns:
                        # 提取raw_data列的数据
                        raw_data_list = data['raw_data'].tolist()
                        print(f"🔍 调试 - {name} raw_data列数据: {len(raw_data_list)} 项")
                        
                        # 创建包含sanctions_list的字典（默认使用 raw_data_list，后续根据无风险情况置空）
                        processed_data = {
                            "sanctions_list": raw_data_list,
                            "vessel_imo": data['VesselImo'] if hasattr(data, 'columns') and 'VesselImo' in data.columns and data.shape[0] > 0 else None
                        }
                        
                        # 特殊处理sanctioned_sts：扁平化CounterpartVessels结构
                        if name == 'sanctioned_sts':
                            flattened_sanctions_list = []
                            
                            for voyage_item in raw_data_list:
                                if isinstance(voyage_item, dict):
                                    # 提取航次基本信息
                                    voyage_info = {
                                        'VoyageStartTime': voyage_item.get('VoyageStartTime'),
                                        'VoyageEndTime': voyage_item.get('VoyageEndTime'),
                                        'STSEvent': voyage_item.get('STSEvent', {})
                                    }
                                    
                                    # 处理每个航次的CounterpartVessels
                                    counterpart_vessels = voyage_item.get('CounterpartVessels', [])
                                    for counterpart in counterpart_vessels:
                                        if isinstance(counterpart, dict):
                                            # 处理VesselSanctions
                                            vessel_sanctions = counterpart.get('VesselSanctions', [])
                                            for sanction in vessel_sanctions:
                                                if isinstance(sanction, dict):
                                                    flattened_item = {
                                                        **voyage_info,
                                                        'IsVesselSanctioned': sanction.get('IsVesselSanctioned'),
                                                        'IsVesselOwnershipSanctioned': sanction.get('IsVesselOwnershipSanctioned'),
                                                        'IsVesselOwnershipLinkedToSanctionedEntities': sanction.get('IsVesselOwnershipLinkedToSanctionedEntities'),
                                                        'VesselImo': sanction.get('VesselImo'),
                                                        'VesselName': sanction.get('VesselName'),
                                                        'VesselType': sanction.get('VesselType'),
                                                        'RiskIndicators': sanction.get('RiskIndicators'),
                                                        'RiskScore': sanction.get('RiskScore')
                                                    }
                                                    flattened_sanctions_list.append(flattened_item)
                                            
                                            # 处理SanctionedOwners
                                            sanctioned_owners = counterpart.get('SanctionedOwners', [])
                                            for owner in sanctioned_owners:
                                                if isinstance(owner, dict):
                                                    flattened_item = {
                                                        **voyage_info,
                                                        'IsVesselSanctioned': owner.get('IsVesselSanctioned'),
                                                        'IsVesselOwnershipSanctioned': owner.get('IsVesselOwnershipSanctioned'),
                                                        'IsVesselOwnershipLinkedToSanctionedEntities': owner.get('IsVesselOwnershipLinkedToSanctionedEntities'),
                                                        'VesselImo': owner.get('VesselImo'),
                                                        'VesselName': owner.get('VesselName'),
                                                        'VesselType': owner.get('VesselType'),
                                                        'RiskIndicators': owner.get('RiskIndicators'),
                                                        'RiskScore': owner.get('RiskScore')
                                                    }
                                                    flattened_sanctions_list.append(flattened_item)
                            
                            processed_data["sanctions_list"] = flattened_sanctions_list
                        
                        print(f"🔍 调试 - {name} 处理后的数据: {processed_data}")
                        
                        # 根据风险类型设置正确的风险等级
                        if name == 'high_risk_port':
                            # 检查是否包含"没有发现高风险港口航次"消息
                            if len(raw_data_list) > 0:
                                first_item = raw_data_list[0]
                                if isinstance(first_item, dict) and first_item.get('message') == '没有发现高风险港口航次':
                                    risk_level = '无风险'
                                else:
                                    risk_level = '高风险'
                            else:
                                risk_level = '无风险'
                        elif name == 'sanctioned_sts':
                            risk_level = '高风险' if len(raw_data_list) > 0 else '无风险'
                        elif name == 'possible_dark_port':
                            # 单独处理possible_dark_port：有数据时设为中风险
                            risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'
                        elif name == 'suspicious_ais_gap':
                            # 特殊处理suspicious_ais_gap：无风险时sanctions_list为空数组
                            risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'
                            if risk_level == '无风险':
                                processed_data["sanctions_list"] = []
                        else:
                            # 其他航次风险类型保持原有逻辑
                            risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'

                        # 高风险港口：在无风险时，sanctions_list 必须是空数组
                        if name == 'high_risk_port' and risk_level == '无风险':
                            processed_data["sanctions_list"] = []

                        # suspicious_ais_gap：提取4个字段（首个非空，兼容多层嵌套）
                        if name == 'suspicious_ais_gap':
                            try:
                                def _first_non_empty(getters):
                                    for getter in getters:
                                        try:
                                            v = getter()
                                        except Exception:
                                            v = None
                                        if v not in (None, "", []):
                                            return v
                                    return ""

                                gap_start_dt = ""
                                gap_end_dt = ""
                                gap_eez = ""
                                voyage_risk_rating = ""
                                gap_is_sanctioned_eez = ""

                                for item in raw_data_list:
                                    if not isinstance(item, dict):
                                        continue
                                    # 直接层级
                                    if not gap_start_dt:
                                        gap_start_dt = _first_non_empty([
                                            lambda: item.get('AisGapStartDateTime'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapStartDateTime'),
                                            lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartDateTime')
                                        ])
                                    if not gap_end_dt:
                                        gap_end_dt = _first_non_empty([
                                            lambda: item.get('AisGapEndDateTime'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapEndDateTime'),
                                            lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapEndDateTime')
                                        ])
                                    if not gap_eez:
                                        gap_eez = _first_non_empty([
                                            lambda: item.get('AisGapStartEezName'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapStartEezName'),
                                            lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartEezName')
                                        ])
                                    if not gap_is_sanctioned_eez:
                                        gap_is_sanctioned_eez = _first_non_empty([
                                            lambda: item.get('is_sanctioned_eez'),
                                            lambda: (item.get('AISGap') or {}).get('is_sanctioned_eez'),
                                            lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('is_sanctioned_eez')
                                        ])
                                    if not voyage_risk_rating:
                                        voyage_risk_rating = _first_non_empty([
                                            lambda: item.get('VoyageRiskRating'),
                                            lambda: (item.get('VoyageInfo') or {}).get('VoyageRiskRating')
                                        ])
                                    if all([gap_start_dt, gap_end_dt, gap_eez, voyage_risk_rating]):
                                        break

                                processed_data['AisGapStartDateTime'] = gap_start_dt
                                processed_data['AisGapEndDateTime'] = gap_end_dt
                                processed_data['AisGapStartEezName'] = gap_eez
                                processed_data['VoyageRiskRating'] = voyage_risk_rating
                                processed_data['is_sanctioned_eez'] = gap_is_sanctioned_eez
                            except Exception as _e:
                                print(f"⚠️ suspicious_ais_gap 字段提取失败: {_e}")
                        # possible_dark_port：提取Start/End/EezName/is_sanctioned_eez
                        if name == 'possible_dark_port':
                            try:
                                def _first_non_empty(getters):
                                    for getter in getters:
                                        try:
                                            v = getter()
                                        except Exception:
                                            v = None
                                        if v not in (None, "", []):
                                            return v
                                    return ""

                                gap_start_dt = ""
                                gap_end_dt = ""
                                gap_eez = ""
                                gap_is_sanctioned_eez = ""
                                voyage_risk_rating = ""

                                for item in raw_data_list:
                                    if not isinstance(item, dict):
                                        continue
                                    
                                    # 从VoyageInfo.AISGap中提取字段
                                    voyage_info = item.get('VoyageInfo', {})
                                    ais_gap = voyage_info.get('AISGap', {})
                                    
                                    if not gap_start_dt:
                                        gap_start_dt = _first_non_empty([
                                            lambda: ais_gap.get('AisGapStartDateTime'),
                                            lambda: item.get('AisGapStartDateTime'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapStartDateTime')
                                        ])
                                    if not gap_end_dt:
                                        gap_end_dt = _first_non_empty([
                                            lambda: ais_gap.get('AisGapEndDateTime'),
                                            lambda: item.get('AisGapEndDateTime'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapEndDateTime')
                                        ])
                                    if not gap_eez:
                                        gap_eez = _first_non_empty([
                                            lambda: ais_gap.get('AisGapStartEezName'),
                                            lambda: item.get('AisGapStartEezName'),
                                            lambda: (item.get('AISGap') or {}).get('AisGapStartEezName')
                                        ])
                                    if not gap_is_sanctioned_eez:
                                        gap_is_sanctioned_eez = _first_non_empty([
                                            lambda: ais_gap.get('is_sanctioned_eez'),
                                            lambda: item.get('is_sanctioned_eez'),
                                            lambda: (item.get('AISGap') or {}).get('is_sanctioned_eez')
                                        ])
                                    if not voyage_risk_rating:
                                        voyage_risk_rating = _first_non_empty([
                                            lambda: voyage_info.get('VoyageRiskRating'),
                                            lambda: item.get('VoyageRiskRating')
                                        ])
                                    
                                    # 如果找到了所有字段，就跳出循环
                                    if all([gap_start_dt, gap_end_dt, gap_eez, gap_is_sanctioned_eez]):
                                        break

                                processed_data['AisGapStartDateTime'] = gap_start_dt
                                processed_data['AisGapEndDateTime'] = gap_end_dt
                                processed_data['AisGapStartEezName'] = gap_eez
                                processed_data['is_sanctioned_eez'] = gap_is_sanctioned_eez
                                processed_data['VoyageRiskRating'] = voyage_risk_rating
                            except Exception as _e:
                                print(f"⚠️ possible_dark_port 字段提取失败: {_e}")
                        
                        voyage_item = create_check_item(
                            name, 
                            CHECK_CONFIG[name]["title"], 
                            CHECK_CONFIG[name]["description"], 
                            processed_data,
                            risk_level
                        )
                    else:
                        print(f"🔍 调试 - {name} DataFrame为空或没有raw_data列")
                        # 创建空的检查项
                        sanctions_list = []
                        # 特殊处理suspicious_ais_gap：无风险时sanctions_list为空数组
                        if name == 'suspicious_ais_gap':
                            sanctions_list = []
                        
                        voyage_item = create_check_item(
                            name, 
                            CHECK_CONFIG[name]["title"], 
                            CHECK_CONFIG[name]["description"], 
                            {"sanctions_list": sanctions_list},
                            "无风险"
                        )
                elif isinstance(data, dict) and "raw_data" in data:
                    # 处理字典格式的数据
                    data["sanctions_list"] = data["raw_data"]
                    print(f"🔍 调试 - {name} 已重命名 raw_data -> sanctions_list")
                    
                    # 特殊处理sanctioned_sts：数据已经在第一个位置扁平化过了，这里不需要再次处理
                    if name == 'sanctioned_sts':
                        print(f"🔍 调试 - sanctioned_sts 数据已在第一个位置扁平化，跳过重复处理")
                    
                    # 根据风险类型设置正确的风险等级
                    raw_data_list = data["raw_data"]
                    if name == 'high_risk_port':
                        # 检查是否包含"没有发现高风险港口航次"消息
                        if len(raw_data_list) > 0:
                            first_item = raw_data_list[0]
                            if isinstance(first_item, dict) and first_item.get('message') == '没有发现高风险港口航次':
                                risk_level = '无风险'
                            else:
                                risk_level = '高风险'
                        else:
                            risk_level = '无风险'
                    elif name == 'sanctioned_sts':
                        risk_level = '高风险' if len(raw_data_list) > 0 else '无风险'
                    elif name == 'possible_dark_port':
                        # 单独处理possible_dark_port：有数据时设为中风险
                        risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'
                    elif name == 'suspicious_ais_gap':
                        # 特殊处理suspicious_ais_gap：无风险时sanctions_list为空数组
                        risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'
                        if risk_level == '无风险':
                            data["sanctions_list"] = []
                    else:
                        # 其他航次风险类型保持原有逻辑
                        risk_level = '中风险' if len(raw_data_list) > 0 else '无风险'

                    # 高风险港口：在无风险时，sanctions_list 必须是空数组
                    if name == 'high_risk_port' and risk_level == '无风险':
                        data["sanctions_list"] = []

                    # suspicious_ais_gap：提取4个字段（首个非空，兼容多层嵌套）
                    if name == 'suspicious_ais_gap':
                        try:
                            def _first_non_empty(getters):
                                for getter in getters:
                                    try:
                                        v = getter()
                                    except Exception:
                                        v = None
                                    if v not in (None, "", []):
                                        return v
                                return ""

                            gap_start_dt = ""
                            gap_end_dt = ""
                            gap_eez = ""
                            voyage_risk_rating = ""
                            gap_is_sanctioned_eez = ""

                            for item in raw_data_list:
                                if not isinstance(item, dict):
                                    continue
                                if not gap_start_dt:
                                    gap_start_dt = _first_non_empty([
                                        lambda: item.get('AisGapStartDateTime'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapStartDateTime'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartDateTime')
                                    ])
                                if not gap_end_dt:
                                    gap_end_dt = _first_non_empty([
                                        lambda: item.get('AisGapEndDateTime'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapEndDateTime'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapEndDateTime')
                                    ])
                                if not gap_eez:
                                    gap_eez = _first_non_empty([
                                        lambda: item.get('AisGapStartEezName'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapStartEezName'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartEezName')
                                    ])
                                if not gap_is_sanctioned_eez:
                                    gap_is_sanctioned_eez = _first_non_empty([
                                        lambda: item.get('is_sanctioned_eez'),
                                        lambda: (item.get('AISGap') or {}).get('is_sanctioned_eez'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('is_sanctioned_eez')
                                    ])
                                if not voyage_risk_rating:
                                    voyage_risk_rating = _first_non_empty([
                                        lambda: item.get('VoyageRiskRating'),
                                        lambda: (item.get('VoyageInfo') or {}).get('VoyageRiskRating')
                                    ])
                                if all([gap_start_dt, gap_end_dt, gap_eez, voyage_risk_rating]):
                                    break

                            data['AisGapStartDateTime'] = gap_start_dt
                            data['AisGapEndDateTime'] = gap_end_dt
                            data['AisGapStartEezName'] = gap_eez
                            data['VoyageRiskRating'] = voyage_risk_rating
                            data['is_sanctioned_eez'] = gap_is_sanctioned_eez
                        except Exception as _e:
                            print(f"⚠️ suspicious_ais_gap 字段提取失败: {_e}")
                    # possible_dark_port：提取Start/End/EezName/is_sanctioned_eez
                    if name == 'possible_dark_port':
                        try:
                            def _first_non_empty(getters):
                                for getter in getters:
                                    try:
                                        v = getter()
                                    except Exception:
                                        v = None
                                    if v not in (None, "", []):
                                        return v
                                return ""

                            gap_start_dt = ""
                            gap_end_dt = ""
                            gap_eez = ""
                            gap_is_sanctioned_eez = ""
                            voyage_risk_rating = ""

                            for item in raw_data_list:
                                if not isinstance(item, dict):
                                    continue
                                if not gap_start_dt:
                                    gap_start_dt = _first_non_empty([
                                        lambda: item.get('AisGapStartDateTime'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapStartDateTime'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartDateTime')
                                    ])
                                if not gap_end_dt:
                                    gap_end_dt = _first_non_empty([
                                        lambda: item.get('AisGapEndDateTime'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapEndDateTime'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapEndDateTime')
                                    ])
                                if not gap_eez:
                                    gap_eez = _first_non_empty([
                                        lambda: item.get('AisGapStartEezName'),
                                        lambda: (item.get('AISGap') or {}).get('AisGapStartEezName'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('AisGapStartEezName')
                                    ])
                                if not gap_is_sanctioned_eez:
                                    gap_is_sanctioned_eez = _first_non_empty([
                                        lambda: item.get('is_sanctioned_eez'),
                                        lambda: (item.get('AISGap') or {}).get('is_sanctioned_eez'),
                                        lambda: (item.get('VoyageInfo') or {}).get('AISGap', {}).get('is_sanctioned_eez')
                                    ])
                                if not voyage_risk_rating:
                                    voyage_risk_rating = _first_non_empty([
                                        lambda: item.get('VoyageRiskRating'),
                                        lambda: (item.get('VoyageInfo') or {}).get('VoyageRiskRating')
                                    ])
                                if all([gap_start_dt, gap_end_dt, gap_eez, gap_is_sanctioned_eez]):
                                    break

                            data['AisGapStartDateTime'] = gap_start_dt
                            data['AisGapEndDateTime'] = gap_end_dt
                            data['AisGapStartEezName'] = gap_eez
                            data['is_sanctioned_eez'] = gap_is_sanctioned_eez
                            data['VoyageRiskRating'] = voyage_risk_rating

                            # 将关键字段同步到每条 sanctions_list 条目中
                            if isinstance(data.get('sanctions_list'), list):
                                for _entry in data['sanctions_list']:
                                    if isinstance(_entry, dict):
                                        # 从VoyageInfo.AISGap中提取字段
                                        voyage_info = _entry.get('VoyageInfo', {})
                                        ais_gap = voyage_info.get('AISGap', {})
                                        
                                        if 'AisGapStartDateTime' not in _entry:
                                            _entry['AisGapStartDateTime'] = (
                                                ais_gap.get('AisGapStartDateTime')
                                                or _entry.get('AisGapStartDateTime')
                                                or gap_start_dt
                                            )
                                        if 'AisGapEndDateTime' not in _entry:
                                            _entry['AisGapEndDateTime'] = (
                                                ais_gap.get('AisGapEndDateTime')
                                                or _entry.get('AisGapEndDateTime')
                                                or gap_end_dt
                                            )
                                        if 'AisGapStartEezName' not in _entry:
                                            _entry['AisGapStartEezName'] = (
                                                ais_gap.get('AisGapStartEezName')
                                                or _entry.get('AisGapStartEezName')
                                                or gap_eez
                                            )
                                        if 'is_sanctioned_eez' not in _entry:
                                            _entry['is_sanctioned_eez'] = (
                                                ais_gap.get('is_sanctioned_eez')
                                                or _entry.get('is_sanctioned_eez')
                                                or gap_is_sanctioned_eez
                                            )
                                        if 'VoyageRiskRating' not in _entry:
                                            _entry['VoyageRiskRating'] = (
                                                voyage_info.get('VoyageRiskRating')
                                                or _entry.get('VoyageRiskRating')
                                                or voyage_risk_rating
                                            )
                        except Exception as _e:
                            print(f"⚠️ possible_dark_port 字段提取失败: {_e}")
                    
                    voyage_item = create_check_item(
                        name, 
                        CHECK_CONFIG[name]["title"], 
                        CHECK_CONFIG[name]["description"], 
                        data,
                        risk_level
                    )
                else:
                    print(f"🔍 调试 - {name} 没有 raw_data 字段或数据格式不正确")
                    # 创建空的检查项
                    voyage_item = create_check_item(
                        name, 
                        CHECK_CONFIG[name]["title"], 
                        CHECK_CONFIG[name]["description"], 
                        {"sanctions_list": []},
                        "无风险"
                    )
                
                print(f"🔍 调试 - {name} 创建的检查项: {voyage_item}")
                all_check_items.append(voyage_item)
        
        # 3. Kpler风险检查项
        kpler_risk_items = all_data.get("kpler_risk_items", [])
        print(f"🔍 调试 - 获取到的Kpler风险检查项: {len(kpler_risk_items)} 个")
        if isinstance(kpler_risk_items, list):
            print(f"🔍 调试 - Kpler风险检查项详情: {kpler_risk_items}")
            all_check_items.extend(kpler_risk_items)
        else:
            print(f"🔍 调试 - Kpler风险检查项不是列表类型: {type(kpler_risk_items)}")
        
        # 4. 统一根据sanctions_list判断所有检查项的sanctions_lev
        # 注意：已经单独判断过的检查项不再重复判断，避免覆盖正确的风险等级
        for item in all_check_items:
            item_name = item.get("name", "")
            
            # 已经单独判断过的检查项，跳过统一判断逻辑
            already_judged_items = [
                "lloyds_compliance",  # 基于TotalRiskScore判断
                "lloyds_sanctions",   # 基于制裁数据存在性判断
                "uani_check",         # 基于UANI数据库查询结果判断
                "ais_manipulation",   # 基于ComplianceRiskScore判断
                # Kpler的8个检查项：基于API返回的风险等级判断
                "has_sanctioned_cargo_risk", "has_sanctioned_trades_risk", 
                "has_port_calls_risk", "has_sts_events_risk", "has_ais_gap_risk",
                "has_ais_spoofs_risk", "has_dark_sts_risk", "has_sanctioned_companies_risk",
                # 航次风险的6个检查项：基于raw_data存在性判断
                "high_risk_port", "possible_dark_port", "suspicious_ais_gap", 
                "dark_sts", "sanctioned_sts", "loitering_behavior"
            ]
            
            if item_name in already_judged_items:
                # 这些检查项已经在前面单独判断过，保持原有的sanctions_lev
                print(f"🔍 调试 - {item_name} 已单独判断过，跳过统一判断逻辑")
                continue
            
            # 对于其他检查项，使用统一判断逻辑
            sanctions_list = item.get("sanctions_list")
            sanctions_lev = "无风险"  # 默认无风险
            
            if sanctions_list:
                # 检查sanctions_list是否有数据
                has_data = False
                if isinstance(sanctions_list, list) and len(sanctions_list) > 0:
                    has_data = True
                elif isinstance(sanctions_list, dict):
                    # 对于字典类型，检查是否有非空值
                    for key, value in sanctions_list.items():
                        if value and (isinstance(value, str) and value.strip() or 
                                    isinstance(value, (list, dict)) and len(value) > 0 or
                                    value is not None and value != ""):
                            has_data = True
                            break
                
                if has_data:
                    # 默认有数据就是高风险，除非有特殊说明
                    sanctions_lev = "高风险"
                    print(f"🔍 调试 - {item_name} 统一判断：有数据 → {sanctions_lev}")
            
            # 更新检查项的风险等级
            item["sanctions_lev"] = sanctions_lev
        
        # 根据sanctions_lev分组数据 - 已注释，使用新的risk_groups_by_title结构
        # for item in all_check_items:
        #     # 现在所有检查项都已经有了sanctions_lev字段，直接使用
        #     if "sanctions_lev" in item:
        #         sanctions_lev = item["sanctions_lev"]
        #     else:
        #         # 如果没有sanctions_lev字段，设置为默认值
        #         sanctions_lev = "无风险"
        #         item["sanctions_lev"] = sanctions_lev
        #     
        #     # 根据风险等级分组
        #     if sanctions_lev == "高风险":
        #         risk_groups["high_risk"].append(item)
        #     elif sanctions_lev == "中风险":
        #         risk_groups["mid_risk"].append(item)
        #     else:  # 无风险或其他
        #         risk_groups["no_risk"].append(item)
        
        # 检查是否有任何有效数据
        has_valid_data = False
        
        # 检查各个数据源是否有有效数据
        if all_data.get("lloyds_compliance") and not isinstance(all_data["lloyds_compliance"], dict):
            has_valid_data = True
        if all_data.get("lloyds_sanctions") and not isinstance(all_data["lloyds_sanctions"], dict):
            has_valid_data = True
        if all_data.get("uani_check") and all_data["uani_check"].get("exists_in_uani"):
            has_valid_data = True
        if all_data.get("kpler_risk_items") and len(all_data["kpler_risk_items"]) > 0:
            has_valid_data = True
        if all_data.get("ais_manipulation") and not isinstance(all_data["ais_manipulation"], dict):
            has_valid_data = True
        if all_data.get("voyage_risks"):
            voyage_risks = all_data["voyage_risks"]
            for risk_type in ['high_risk_port', 'possible_dark_port', 'suspicious_ais_gap', 'dark_sts', 'sanctioned_sts', 'loitering_behavior']:
                if risk_type in voyage_risks:
                    risk_data = voyage_risks[risk_type]
                    if hasattr(risk_data, 'shape') and risk_data.shape[0] > 0:
                        has_valid_data = True
                        break
                    elif isinstance(risk_data, dict) and risk_data.get('raw_data'):
                        has_valid_data = True
                        break
        
        # 如果没有有效数据，返回"暂无数据"
        if not has_valid_data:
            # 即使没有有效数据，也要进行数据重构以保持数据结构一致性
            empty_check_items = []
            if lloyds_compliance_item:
                empty_check_items.append(lloyds_compliance_item)
            empty_check_items.extend(all_check_items)
            
            # 按title重新组织数据
            restructured_by_title = restructure_check_items_by_title(empty_check_items)
            
            # 根据第三方能源公司判断进行风险等级分组
            risk_groups_by_title = group_check_items_by_risk_level(restructured_by_title)
            
            return {
                "status": "success",
                "data": {
                    "vessel_imo": vessel_imo,
                    "date_range": f"{start_date} - {end_date}",
                    "timestamp": datetime.now().isoformat(),
                    "message": "暂无数据",
                    "sanctions_lev_all": "无风险",
                    "risk_groups": risk_groups_by_title,  # 使用新的按第三方判断分组的风险等级
                    "vessel_status": {}
                    # "check_items_by_title": restructured_by_title  # 按title重新组织的数据 - 已注释，数据已包含在risk_groups中
                },
                "database_saved": False
            }
        
        # 启用数据库增强功能，添加risk_desc_info和info字段
        print("🔍 正在为船舶数据添加risk_desc_info和info字段...")
        
        # 按title重新组织检查项数据结构
        print("🔍 开始按title重新组织检查项数据结构...")
        all_check_items_for_restructure = []
        
        # 收集所有检查项（包括lloyds_compliance）
        if lloyds_compliance_item:
            all_check_items_for_restructure.append(lloyds_compliance_item)
        all_check_items_for_restructure.extend(all_check_items)
        
        # 按title重新组织数据
        restructured_by_title = restructure_check_items_by_title(all_check_items_for_restructure)
        
        # 根据第三方能源公司判断进行风险等级分组
        risk_groups_by_title = group_check_items_by_risk_level(restructured_by_title)
        
        # 重新构建返回数据结构
        restructured_data = {
            "vessel_imo": vessel_imo,
            "date_range": f"{start_date} - {end_date}",
            "timestamp": datetime.now().isoformat(),
            "sanctions_lev_all": sanctions_lev_all,
            "lloyds_compliance": {},  # 先初始化为空，后面会重新生成
            "risk_groups": risk_groups_by_title,  # 使用新的按第三方判断分组的风险等级替换原来的risk_groups
            "vessel_status": all_data.get("vessel_status", {})
            # "check_items_by_title": restructured_by_title,  # 按title重新组织的数据 - 已注释，数据已包含在risk_groups中
        }
        
        # 特殊处理：将risk_groups中的"船舶相关方涉制裁风险情况"移动到lloyds_compliance
        print("🔍 开始处理船舶相关方涉制裁风险情况的数据移动...")
        
        # 查找risk_groups中的"船舶相关方涉制裁风险情况"检查项
        compliance_risk_item = None
        compliance_risk_index = None
        compliance_risk_group = None
        
        # 遍历所有风险组查找"船舶相关方涉制裁风险情况"
        risk_groups_to_check = ["high_risk", "mid_risk", "no_risk"]
        
        for group_name in risk_groups_to_check:
            if group_name in restructured_data["risk_groups"]:
                group_items = restructured_data["risk_groups"][group_name]
                for i, item in enumerate(group_items):
                    if item.get("title") == "船舶相关方涉制裁风险情况":
                        compliance_risk_item = item
                        compliance_risk_index = i
                        compliance_risk_group = group_name
                        print(f"🔍 在 {group_name} 组中找到船舶相关方涉制裁风险情况检查项，索引: {i}")
                        break
                if compliance_risk_item is not None:
                    break
        
        if compliance_risk_item:
            print(f"🔍 找到船舶相关方涉制裁风险情况检查项，风险等级: {compliance_risk_item.get('sanctions_lev')}")
            
            # 将整个检查项移动到lloyds_compliance字段
            restructured_data["lloyds_compliance"] = compliance_risk_item
            print(f"🔍 已将整个船舶相关方涉制裁风险情况检查项移动到lloyds_compliance字段")
            
            # 从risk_groups中移除这个检查项
            if compliance_risk_index is not None and compliance_risk_group is not None:
                del restructured_data["risk_groups"][compliance_risk_group][compliance_risk_index]
                print(f"🔍 已从 {compliance_risk_group} 组中移除船舶相关方涉制裁风险情况检查项")
        else:
            print("⚠️ 未找到船舶相关方涉制裁风险情况检查项")
        
        enhanced_data = enhance_vessel_data_with_risk_desc(restructured_data)
        
        # 转换时间戳字段
        print("🕒 开始转换时间戳字段...")
        enhanced_data = convert_timestamps_in_data(enhanced_data)
        print("✅ 时间戳字段转换完成")
        
        # 启用数据库插入功能
        print("📝 开始保存数据到Kingbase数据库...")
        db_save_success = insert_vessel_data_to_kingbase(enhanced_data)
        
        if db_save_success:
            print("✅ 数据已成功保存到Kingbase数据库")
            enhanced_data["database_saved"] = True
        else:
            print("⚠️ 数据保存到Kingbase数据库失败，但API调用仍然成功")
            enhanced_data["database_saved"] = False
        
        print(f"[PID:{process_id}] ✅ API调用成功完成 - get_vessel_all_data - IMO: {vessel_imo} - {datetime.now()}", flush=True)
        return {
            "status": "success",
            "data": enhanced_data,
            "database_saved": db_save_success
        }
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"❌ 获取船舶所有数据失败: {str(e)}")
        print(f"❌ 详细错误信息: {error_details}")
        raise HTTPException(status_code=500, detail=f"获取船舶所有数据失败: {str(e)}")

# 兼容性路由 - 支持 /maritime/api/ 路径
@maritime_app.get("/maritime/api/get_vessel_all_data")
async def get_vessel_all_data_compatible(
    vessel_imo: str = Query(..., description="船舶IMO号（必需参数）"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为当前日期")
):
    """获取船舶所有相关数据（兼容性路由）
    
    必需参数:
    - vessel_imo: 船舶IMO号
    
    示例:
    GET /maritime/api/get_vessel_all_data?vessel_imo=9842190
    """
    # 直接调用原函数
    return await get_vessel_all_data(vessel_imo, start_date, end_date)

@maritime_app.post("/api/save_results")
async def save_results(
    output_dir: str = Query("results", description="输出目录")
):
    """保存所有分析结果"""
    try:
        processor = get_processor()
        processor.save_all_results(output_dir)
        
        return {
            "status": "success",
            "message": f"结果已保存到 {output_dir} 目录",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存结果失败: {str(e)}")

@maritime_app.get("/api/available_endpoints")
async def get_available_endpoints():
    """获取可用的API端点"""
    return {
        "endpoints": [
            {
                "path": "/api/execute_full_analysis",
                "method": "POST",
                "description": "执行完整分析流程",
                "params": ["vessel_imo", "start_date", "end_date"]
            },
            {
                "path": "/api/lloyds_compliance",
                "method": "GET",
                "description": "获取劳氏船舶公司制裁信息",
                "params": ["vessel_imo", "start_date", "end_date"]
            },
            {
                "path": "/api/lloyds_sanctions",
                "method": "GET",
                "description": "获取劳氏船舶制裁数据",
                "params": ["vessel_imo"]
            },
            {
                "path": "/api/uani_check",
                "method": "GET",
                "description": "检查船舶是否在UANI清单中",
                "params": ["vessel_imo"]
            },
            {
                "path": "/api/kpler_analysis",
                "method": "POST",
                "description": "执行Kpler数据分析 (POST方法)",
                "params": ["vessel_imo", "start_date", "end_date"]
            },
            {
                "path": "/api/kpler_analysis",
                "method": "GET",
                "description": "执行Kpler数据分析 (GET方法)",
                "params": ["vessel_imo", "start_date", "end_date"]
            },
            {
                "path": "/api/voyage_risks",
                "method": "GET",
                "description": "获取航次风险分析",
                "params": ["vessel_imo", "start_date", "end_date"]
            },
            {
                "path": "/api/vessel_status",
                "method": "GET",
                "description": "获取船舶综合状态",
                "params": ["vessel_imo"]
            },
            {
                "path": "/api/save_results",
                "method": "POST",
                "description": "保存所有分析结果",
                "params": ["output_dir"]
            },
            {
                "path": "/api/get_vessel_all_data",
                "method": "GET",
                "description": "获取船舶所有相关数据（一次性调用所有接口）",
                "params": ["vessel_imo", "start_date(可选)", "end_date(可选)"]
            },
            {
                "path": "/api/sts_data",
                "method": "GET",
                "description": "获取船舶STS（船对船转运）数据 - 独立接口，实时获取",
                "params": ["vessel_imo"]
            },
            {
                "path": "/api/database/query_vessel_data",
                "method": "GET",
                "description": "从Kingbase数据库查询已保存的船舶风险数据",
                "params": ["vessel_imo"]
            },
            {
                "path": "/api/database/list_vessels",
                "method": "GET",
                "description": "从Kingbase数据库获取船舶列表",
                "params": ["limit(可选)", "offset(可选)"]
            }
        ],
        "timestamp": datetime.now().isoformat()
    }

@maritime_app.get("/api/sts_data")
async def get_sts_data(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取船舶STS（船对船转运）数据"""
    try:
        # 调用STS数据处理函数
        sts_data = get_sts_data_from_api(vessel_imo)
        
        if sts_data:
            return {
                "status": "success",
                "vessel_imo": vessel_imo,
                "data": sts_data,
                "count": len(sts_data),
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "success",
                "message": "暂无数据",
                "vessel_imo": vessel_imo,
                "data": [],
                "count": 0,
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        print(f"❌ 获取STS数据失败: {str(e)}")
        return {
            "status": "error",
            "message": f"获取STS数据失败: {str(e)}",
            "vessel_imo": vessel_imo,
            "timestamp": datetime.now().isoformat()
        }

@maritime_app.get("/api/database/query_vessel_data")
async def query_vessel_data_from_database(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """从Kingbase数据库查询已保存的船舶风险数据"""
    try:
        # 获取数据库配置
        db_config = get_kingbase_config()
        
        # 连接数据库
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 查询船舶数据
        cursor.execute("""
            SELECT id, vessel_name, vessel_imo, date_range, sanctions_lev_all, 
                   high_risk, mid_risk, no_risk, voyage_risks_summary, vessel_status, 
                   lloyds_compliance, serch_time, create_time
            FROM lng.sanction_risk_vessel_result_his 
            WHERE vessel_imo = %s AND is_delete = '0'
            ORDER BY create_time DESC
            LIMIT 1
        """, (vessel_imo,))
        
        record = cursor.fetchone()
        
        # 关闭连接
        cursor.close()
        connection.close()
        
        if record:
            # 解析JSON数据
            def safe_json_parse(json_str, default_value=None):
                """安全解析JSON字符串，处理可能的格式错误"""
                if not json_str or json_str in ("null", "None"):
                    return default_value
                try:
                    return json.loads(json_str)
                except (json.JSONDecodeError, TypeError):
                    return default_value
            
            high_risk = safe_json_parse(record['high_risk'], [])
            mid_risk = safe_json_parse(record['mid_risk'], [])
            no_risk = safe_json_parse(record['no_risk'], [])
            voyage_risks_summary = safe_json_parse(record['voyage_risks_summary'], [])
            vessel_status = safe_json_parse(record['vessel_status'], {})
            lloyds_compliance = safe_json_parse(record['lloyds_compliance'], {})
            
            # 重构数据结构
            reconstructed_data = {
                "vessel_imo": record['vessel_imo'],
                "vessel_name": record['vessel_name'],
                "date_range": record['date_range'],
                "sanctions_lev_all": record['sanctions_lev_all'],
                "risk_groups": {
                    "high_risk": high_risk,
                    "mid_risk": mid_risk,
                    "no_risk": no_risk
                },
                "voyage_risks": {
                    "summary": voyage_risks_summary
                },
                "vessel_status": vessel_status,
                "lloyds_compliance": lloyds_compliance
            }
            
            return {
                "status": "success",
                "vessel_imo": record['vessel_imo'],
                "vessel_name": record['vessel_name'],
                "date_range": record['date_range'],
                "sanctions_lev_all": record['sanctions_lev_all'],
                "data": reconstructed_data,
                "serch_time": record['serch_time'],
                "create_time": record['create_time'].isoformat() if record['create_time'] else None,
                "message": "从数据库成功获取船舶风险数据"
            }
        else:
            return {
                "status": "no_data",
                "message": f"船舶 {vessel_imo} 在数据库中没有找到记录",
                "vessel_imo": vessel_imo,
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        print(f"❌ 查询数据库失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": f"查询数据库失败: {str(e)}",
            "vessel_imo": vessel_imo,
            "timestamp": datetime.now().isoformat()
        }

@maritime_app.get("/api/database/list_vessels")
async def list_vessels_from_database(
    limit: int = Query(10, description="返回记录数量限制", ge=1, le=100),
    offset: int = Query(0, description="偏移量", ge=0)
):
    """从Kingbase数据库获取船舶列表"""
    try:
        # 获取数据库配置
        db_config = get_kingbase_config()
        
        # 连接数据库
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 查询船舶列表
        cursor.execute("""
            SELECT id, vessel_name, vessel_imo, date_range, sanctions_lev_all, 
                   serch_time, create_time
            FROM lng.sanction_risk_vessel_result_his 
            WHERE is_delete = '0'
            ORDER BY create_time DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        
        records = cursor.fetchall()
        
        # 获取总记录数
        cursor.execute("SELECT COUNT(*) as total FROM lng.sanction_risk_vessel_result_his WHERE is_delete = '0'")
        total_count = cursor.fetchone()['total']
        
        # 关闭连接
        cursor.close()
        connection.close()
        
        # 格式化数据
        vessels = []
        for record in records:
            vessels.append({
                "id": record['id'],
                "vessel_name": record['vessel_name'],
                "vessel_imo": record['vessel_imo'],
                "date_range": record['date_range'],
                "sanctions_lev_all": record['sanctions_lev_all'],
                "serch_time": record['serch_time'],
                "create_time": record['create_time'].isoformat() if record['create_time'] else None
            })
        
        return {
            "status": "success",
            "data": vessels,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count
            },
            "timestamp": datetime.now().isoformat()
        }
            
    except Exception as e:
        print(f"❌ 查询数据库失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": f"查询数据库失败: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

def insert_vessel_data_to_kingbase(data: Dict[str, Any]) -> bool:
    """将船舶数据插入到Kingbase数据库"""
    try:
        # 获取数据库配置
        db_config = get_kingbase_config()
        
        # 连接数据库
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        print(f"📝 开始将船舶 {data.get('vessel_imo', 'N/A')} 的数据插入Kingbase数据库...")
        
        # 准备插入的数据
        vessel_imo = data.get('vessel_imo', '')
        date_range = data.get('date_range', '')
        sanctions_lev_all = data.get('sanctions_lev_all', '无风险')
        current_time = datetime.now()
        
        # 从各个数据源中提取船舶名称
        vessel_name = ''
        
        # 1. 优先从lloyds_compliance中获取
        lloyds_compliance_data = data.get('lloyds_compliance', {})
        if isinstance(lloyds_compliance_data, dict):
            vessel_name = lloyds_compliance_data.get('VesselName', '')
            # 确保vessel_name是字符串
            if hasattr(vessel_name, 'to_dict'):  # pandas Series
                vessel_name = str(vessel_name.iloc[0]) if len(vessel_name) > 0 else ''
            elif not isinstance(vessel_name, str):
                vessel_name = str(vessel_name) if vessel_name else ''
        
        # 2. 如果lloyds_compliance中没有，从risk_groups中获取
        if not vessel_name:
            risk_groups = data.get('risk_groups', {})
            # 遍历所有风险组查找船舶名称
            for risk_level in ['high_risk', 'mid_risk', 'no_risk']:
                if risk_level in risk_groups:
                    risk_items = risk_groups[risk_level]
                    for risk_item in risk_items:
                        if isinstance(risk_item, dict) and 'risk_items' in risk_item:
                            for item in risk_item['risk_items']:
                                if isinstance(item, dict):
                                    # 查找VesselName字段
                                    if 'VesselName' in item:
                                        vessel_name = item['VesselName']
                                        # 确保vessel_name是字符串
                                        if hasattr(vessel_name, 'to_dict'):  # pandas Series
                                            vessel_name = str(vessel_name.iloc[0]) if len(vessel_name) > 0 else ''
                                        elif not isinstance(vessel_name, str):
                                            vessel_name = str(vessel_name) if vessel_name else ''
                                        break
                                    # 查找vesselName字段
                                    elif 'vesselName' in item:
                                        vessel_name = item['vesselName']
                                        # 确保vessel_name是字符串
                                        if hasattr(vessel_name, 'to_dict'):  # pandas Series
                                            vessel_name = str(vessel_name.iloc[0]) if len(vessel_name) > 0 else ''
                                        elif not isinstance(vessel_name, str):
                                            vessel_name = str(vessel_name) if vessel_name else ''
                                        break
                                    # 查找sanctions_list中的VesselName
                                    elif 'sanctions_list' in item and isinstance(item['sanctions_list'], list):
                                        for sanction_item in item['sanctions_list']:
                                            if isinstance(sanction_item, dict) and 'VesselName' in sanction_item:
                                                vessel_name = sanction_item['VesselName']
                                                # 确保vessel_name是字符串
                                                if hasattr(vessel_name, 'to_dict'):  # pandas Series
                                                    vessel_name = str(vessel_name.iloc[0]) if len(vessel_name) > 0 else ''
                                                elif not isinstance(vessel_name, str):
                                                    vessel_name = str(vessel_name) if vessel_name else ''
                                                break
                                        if vessel_name:
                                            break
                                if vessel_name:
                                    break
                        if vessel_name:
                            break
                if vessel_name:
                    break
        
        # 3. 如果还没有，尝试从vessel_status中获取
        if not vessel_name:
            vessel_status_data = data.get('vessel_status', {})
            if isinstance(vessel_status_data, dict):
                vessel_name = vessel_status_data.get('vessel_name', '')
                # 确保vessel_name是字符串
                if hasattr(vessel_name, 'to_dict'):  # pandas Series
                    vessel_name = str(vessel_name.iloc[0]) if len(vessel_name) > 0 else ''
                elif not isinstance(vessel_name, str):
                    vessel_name = str(vessel_name) if vessel_name else ''
        
        # 4. 如果还是没有，使用IMO作为默认名称
        if not vessel_name:
            vessel_name = f"Vessel_{vessel_imo}"
        
        print(f"🔍 提取到的船舶名称: {vessel_name}")
        
        # 从数据中提取各个风险组的数据
        risk_groups = data.get('risk_groups', {})
        
        # 安全地序列化风险组数据，处理pandas Series等特殊类型
        def safe_json_serialize(obj):
            """安全地序列化对象，处理pandas Series等特殊类型，确保使用双引号"""
            try:
                if hasattr(obj, 'to_dict'):  # pandas Series/DataFrame
                    return json.dumps(obj.to_dict(), ensure_ascii=False, separators=(',', ':'))
                elif hasattr(obj, 'tolist'):  # numpy array
                    return json.dumps(obj.tolist(), ensure_ascii=False, separators=(',', ':'))
                else:
                    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))
            except (TypeError, ValueError):
                # 如果序列化失败，转换为字符串
                return json.dumps(str(obj), ensure_ascii=False, separators=(',', ':'))
        
        # 递归清理数据中的pandas Series对象
        def clean_pandas_objects(obj):
            """递归清理数据中的pandas Series对象"""
            if hasattr(obj, 'to_dict'):  # pandas Series/DataFrame
                return obj.to_dict()
            elif hasattr(obj, 'tolist'):  # numpy array
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: clean_pandas_objects(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [clean_pandas_objects(item) for item in obj]
            else:
                return obj
        
        # 先清理pandas对象，再序列化
        cleaned_high_risk = clean_pandas_objects(risk_groups.get('high_risk', []))
        cleaned_mid_risk = clean_pandas_objects(risk_groups.get('mid_risk', []))
        cleaned_no_risk = clean_pandas_objects(risk_groups.get('no_risk', []))
        
        high_risk = safe_json_serialize(cleaned_high_risk)
        mid_risk = safe_json_serialize(cleaned_mid_risk)
        no_risk = safe_json_serialize(cleaned_no_risk)
        
        # 提取航次风险摘要
        voyage_risks = data.get('voyage_risks', {})
        cleaned_voyage_risks_summary = clean_pandas_objects(voyage_risks.get('summary', []))
        voyage_risks_summary = safe_json_serialize(cleaned_voyage_risks_summary)
        
        # 提取船舶状态
        cleaned_vessel_status = clean_pandas_objects(data.get('vessel_status', {}))
        vessel_status = safe_json_serialize(cleaned_vessel_status)
        
        # 提取劳氏合规数据
        cleaned_lloyds_compliance = clean_pandas_objects(data.get('lloyds_compliance', {}))
        lloyds_compliance = safe_json_serialize(cleaned_lloyds_compliance)
        
        # 直接插入新记录，不检查是否存在（允许多次查询记录）
        print(f"📝 插入船舶 {vessel_imo} ({vessel_name}) 的新记录")
        
        cursor.execute("""
            INSERT INTO lng.sanction_risk_vessel_result_his (
                vessel_imo, date_range, sanctions_lev_all, high_risk, mid_risk,
                no_risk, vessel_status, serch_time, is_delete, create_time,
                lloyds_compliance, vessel_name, voyage_risks_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            vessel_imo,
            date_range,
            sanctions_lev_all,
            high_risk,
            mid_risk,
            no_risk,
            vessel_status,
            current_time.strftime("%Y-%m-%d %H:%M:%S"),
            '0',
            current_time,
            lloyds_compliance,
            vessel_name,
            voyage_risks_summary
        ))
        print(f"✅ 成功插入船舶 {vessel_imo} ({vessel_name}) 的新记录")
        
        # 提交事务
        connection.commit()
        
        # 额外插入到制裁风险结果历史表（使用默认用户信息）
        try:
            print(f"📝 额外插入到制裁风险结果历史表...")
            
            # 使用默认用户信息
            default_user_info = {
                "user_id": "system",
                "user_name": "系统用户",
                "depart_id": "system",
                "depart_name": "系统部门"
            }
            
            # 构建历史记录
        except Exception as e:
            print(f"⚠️ 船舶结果保存后处理发生警告: {str(e)}")
            # 不影响主记录保存，只记录警告
        
        # 关闭连接
        cursor.close()
        connection.close()
        
        print(f"✅ 船舶 {vessel_imo} ({vessel_name}) 的数据已成功保存到Kingbase数据库表 sanction_risk_vessel_result_his")
        return True
        
    except Exception as e:
        print(f"❌ Kingbase数据库插入失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(maritime_app, host="0.0.0.0", port=8000)
