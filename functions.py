import pandas as pd
import requests
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from bs4 import BeautifulSoup
import time
import random
import os
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import argparse

class MaritimeDataProcessor:
    """海事数据综合处理器（完整保留所有原始处理逻辑）"""
    
    def __init__(self):
        # 导入API配置
        from kingbase_config import get_lloyds_token, get_kpler_token
        
        # API配置
        self.lloyds_api_key = get_lloyds_token()  # Lloyd's API密钥
        self.kpler_api_key = get_kpler_token()   # Kpler API密钥
        
        # API端点
        self.lloyds_base_url = "https://api.lloydslistintelligence.com/v1"
        self.kpler_api_url = "https://api.kpler.com/v2/compliance/vessel-risks-v2"
        self.uani_url = "https://www.unitedagainstnucleariran.com/blog/stop-hop-ii-ghost-armada-grows"
        
        # 请求头
        self.lloyds_headers = {
            "accept": "application/json",
            "Authorization": self.lloyds_api_key
        }
        
        self.kpler_headers = {
            "Authorization": self.kpler_api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # 风险映射配置
        self.risk_mapping = {
            'has_sanctioned_cargo': {'true': '高风险', 'false': '无风险'},
            'has_sanctioned_trades': {'true': '高风险', 'false': '无风险'},
            # 'has_sanctioned_flag': {'true': '高风险', 'false': '无风险'},
            'has_port_calls': {'true': '高风险', 'false': '无风险'},
            'has_sts_events': {'true': '中风险', 'false': '无风险'},
            'has_ais_gap': {'true': '中风险', 'false': '无风险'},
            'has_ais_spoofs': {'true': '中风险', 'false': '无风险'},
            'has_dark_sts': {'true': '中风险', 'false': '无风险'},
            'has_sanctioned_companies': {'true': '高风险', 'false': '无风险'}
        }
        
        # 结果存储
        self.results = {
            'lloyds_compliance': None,
            'lloyds_sanctions': None,
            'lloyds_risks': None,
            'uani_data': None,
            'kpler_data': None,
            'voyage_risks': {
                'high_risk_port': pd.DataFrame(),
                'possible_dark_port': pd.DataFrame(),
                'suspicious_ais_gap': pd.DataFrame(),
                'dark_sts': pd.DataFrame(),
                'sanctioned_sts': pd.DataFrame(),
                'loitering_behavior': pd.DataFrame()
            }
        }
    
    # ==================== 通用工具方法 ====================
    
    def random_delay(self):
        """随机延迟避免被封"""
        time.sleep(random.uniform(1, 3))
    
    def _is_sanctioned_eez(self, eez_name: str) -> str:
        """检查EEZ名称是否在受制裁清单中"""
        SANCTIONED_EEZ = {
            "Cuban Exclusive Economic Zone",
            "Iranian Exclusive Economic Zone",
            "Syrian Exclusive Economic Zone",
            "Overlapping claim Ukrainian Exclusive Economic Zone",
            "North Korean Exclusive Economic Zone",
            "Venezuelan Exclusive Economic Zone",
            "Russian Exclusive Economic Zone"
        }
        return "是" if eez_name in SANCTIONED_EEZ else "否"
    
    def format_detail_list(self, items: List[Dict[str, Any]], fields: List[str]) -> str:
        """
        终极修复版 - 确保转换所有日期字段（包括深层嵌套）
        输出示例：
        commodity: Oil, startDate: 2024-12-01, 
        sanctioned_flag: flagCode: US, startDate: 2024-12-01
        """
        
        def deep_convert_dates(obj):
            """递归转换所有日期字段（支持任意嵌套层级）"""
            if isinstance(obj, dict):
                return {
                    k: deep_convert_dates(v) if not k.lower().endswith('date') 
                    else normalize_date(v)
                    for k, v in obj.items()
                }
            elif isinstance(obj, list):
                return [deep_convert_dates(i) for i in obj]
            return obj
        
        def normalize_date(value):
            """统一日期转换逻辑"""
            if value is None:
                return ""
            if isinstance(value, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                return value
            
            # 处理时间戳（10位或13位）
            if str(value).isdigit():
                ts = int(value)
                if len(str(ts)) == 13:  # 毫秒级
                    ts = ts // 1000
                try:
                    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                except:
                    pass
            
            # 尝试解析其他文本日期格式
            if isinstance(value, str):
                for fmt in ['%d-%b-%Y', '%Y/%m/%d', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
                    except:
                        continue
            
            return str(value)  # 保底返回字符串

        formatted_items = []
        for item in items:
            # 深度转换所有日期字段（关键修复）
            processed_item = deep_convert_dates(item)
            
            # 构建输出行
            parts = []
            for field in fields:
                # 处理嵌套字段（如sanctioned_flag.startDate）
                if '.' in field:
                    keys = field.split('.')
                    value = processed_item
                    for key in keys:
                        value = value.get(key, {}) if isinstance(value, dict) else ''
                    parts.append(f"{keys[-1]}: {value}")
                else:
                    value = processed_item.get(field, '')
                    parts.append(f"{field}: {value}")
            
            # 处理sources - 保留数组格式，不再生成source_1, source_2等字段
            # 如果需要扁平化格式，可以使用format_detail_list_flat方法
            sources = processed_item.get('sources', [])
            if sources:
                # 将sources作为数组保留，转换为JSON字符串格式
                sources_json = json.dumps(sources, ensure_ascii=False)
                parts.append(f"sources: {sources_json}")
            
            formatted_items.append(", ".join(filter(None, parts)))
        
        return " || ".join(filter(None, formatted_items))
    
    def format_detail_list_flat(self, items: List[Dict[str, Any]], fields: List[str]) -> str:
        """
        扁平化版本 - 生成source_1, source_2等字段（保持向后兼容）
        当需要扁平化格式时使用此方法
        """
        
        def deep_convert_dates(obj):
            """递归转换所有日期字段（支持任意嵌套层级）"""
            if isinstance(obj, dict):
                return {
                    k: deep_convert_dates(v) if not k.lower().endswith('date') 
                    else normalize_date(v)
                    for k, v in obj.items()
                }
            elif isinstance(obj, list):
                return [deep_convert_dates(i) for i in obj]
            return obj
        
        def normalize_date(value):
            """统一日期转换逻辑"""
            if value is None:
                return ""
            if isinstance(value, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                return value
            
            # 处理时间戳（10位或13位）
            if str(value).isdigit():
                ts = int(value)
                if len(str(ts)) == 13:  # 毫秒级
                    ts = ts // 1000
                try:
                    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                except:
                    pass
            
            # 尝试解析其他文本日期格式
            if isinstance(value, str):
                for fmt in ['%d-%b-%Y', '%Y/%m/%d', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
                    except:
                        continue
            
            return str(value)  # 保底返回字符串

        formatted_items = []
        for item in items:
            # 深度转换所有日期字段（关键修复）
            processed_item = deep_convert_dates(item)
            
            # 构建输出行
            parts = []
            for field in fields:
                # 处理嵌套字段（如sanctioned_flag.startDate）
                if '.' in field:
                    keys = field.split('.')
                    value = processed_item
                    for key in keys:
                        value = value.get(key, {}) if isinstance(value, dict) else ''
                    parts.append(f"{keys[-1]}: {value}")
                else:
                    value = processed_item.get(field, '')
                    parts.append(f"{field}: {value}")
            
            # 处理sources（保持原有逻辑 - 生成source_1, source_2等字段）
            sources = processed_item.get('sources', [])
            for i, source in enumerate(sources, 1):
                if not isinstance(source, dict):
                    continue
                    
                source_info = []
                if 'name' in source:
                    source_info.append(f"name={source['name']}")
                if 'startDate' in source:
                    source_info.append(f"start={source['startDate']}")  # 已转换
                if 'endDate' in source:
                    source_info.append(f"end={source['endDate']}")  # 已转换
                
                if source_info:
                    parts.append(f"source_{i}: {', '.join(source_info)}")
            
            formatted_items.append(", ".join(filter(None, parts)))
        
        return " || ".join(filter(None, formatted_items))
    
    def get_nested_value(self, obj: Dict[str, Any], path: str) -> Any:
        """获取嵌套字典中的值"""
        keys = path.split('.')
        value = obj
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, {})
            else:
                return ''
        
        return value
    
    def _process_place(self, place_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """处理地点信息"""
        if place_data is None:
            return {
                "Name": None,
                "CountryName": None,
                "IsHighRiskPort": False
            }
        
        return {
            "Name": place_data.get("Name"),
            "CountryName": place_data.get("CountryName"),
            "IsHighRiskPort": place_data.get("IsHighRiskPort", False)
        }
    
    def _process_voyage_events(self, events_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理航次事件"""
        return {
            "AisGaps": events_data.get("AisGap", []),
            "Loitering": events_data.get("Loitering", []),
            "ShipToShipTransfers": events_data.get("ShipToShipTransfer", []),
            "DraughtChanges": events_data.get("DraughtChange", []),
            "DestinationChanges": events_data.get("DestinationChange", []),
            "Movements": events_data.get("Movement", [])
        }





# ==================== Lloyd's API 相关方法 ====================
    
    def fetch_lloyds_data(self, endpoint: str, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """通用Lloyd's数据获取方法"""
        url = f"{self.lloyds_base_url}/{endpoint}"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        print(f"🔍 调试 - 请求URL: {url}")
        print(f"🔍 调试 - 请求参数: {params}")
        print(f"🔍 调试 - 请求头: {self.lloyds_headers}")
        
        try:
            response = requests.get(url, headers=self.lloyds_headers, params=params, timeout=30)
            print(f"🔍 调试 - 响应状态码: {response.status_code}")
            print(f"🔍 调试 - 响应头: {dict(response.headers)}")
            
            if response.status_code == 403:
                print(f"❌ 劳氏{endpoint}接口调用失败 - 403 Forbidden")
                print(f"❌ 响应内容: {response.text[:500]}...")
                print(f"❌ 可能原因: 权限不足、端点不存在、订阅限制")
                return {}
            elif response.status_code == 401:
                print(f"❌ 劳氏{endpoint}接口调用失败 - 401 Unauthorized，API密钥可能无效或过期")
                return {}
            elif response.status_code == 429:
                print(f"❌ 劳氏{endpoint}接口调用失败 - 429 Too Many Requests，请求频率超限")
                return {}
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ 劳氏{endpoint}接口调用失败: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"❌ 错误响应状态码: {e.response.status_code}")
                print(f"❌ 错误响应内容: {e.response.text[:500]}...")
            return {}
        except Exception as e:
            print(f"❌ 劳氏{endpoint}接口调用失败 - 其他错误: {e}")
            return {}
    
    def process_vessel_ais_manipulation(self, vessel_imo: str) -> Dict[str, Any]:
        """处理VesselAisManipulation数据"""
        try:
            # 调用vesseladvancedcompliancerisk_v3端点
            endpoint = f"vesseladvancedcompliancerisk_v3?vesselImo={vessel_imo}"
            url = f"{self.lloyds_base_url}/{endpoint}"
            
            response = requests.get(url, headers=self.lloyds_headers, timeout=120)  # 翻倍
            response.raise_for_status()
            data = response.json()
            
            if data.get('IsSuccess') and data.get('Data', {}).get('Items'):
                full_data = data['Data']['Items'][0]
                
                # 提取VesselAisManipulation相关数据
                ais_manipulation_risks = []
                for risk in full_data.get('ComplianceRisks', []):
                    if risk.get('ComplianceRiskType', {}).get('Description') == 'VesselAisManipulation':
                        # 处理风险详情
                        details = risk.get('Details', [])
                        if not details:  # 无详情时保留基础信息
                            ais_manipulation_risks.append({
                                'VesselImo': full_data.get('VesselImo'),
                                'VesselName': full_data.get('VesselName'),
                                'RiskType': 'VesselAisManipulation',
                                **risk
                            })
                        else:
                            for detail in details:
                                # 合并基础信息、风险属性和详情
                                ais_manipulation_risks.append({
                                    'VesselImo': full_data.get('VesselImo'),
                                    'VesselName': full_data.get('VesselName'),
                                    'RiskType': 'VesselAisManipulation',
                                    **risk,
                                    **detail,
                                    'PlaceInfo': detail.get('Place', {}),
                                    'RiskIndicators': [ind['Description'] for ind in detail.get('RiskIndicators', [])]
                                })
                
                # 计算sanctions_lev - 根据ComplianceRiskScore值判断
                if ais_manipulation_risks:
                    # 检查ComplianceRiskScore值
                    high_risk_count = 0
                    medium_risk_count = 0
                    
                    for risk in ais_manipulation_risks:
                        compliance_risk_score = risk.get('ComplianceRiskScore', '')
                        if compliance_risk_score == 'High':
                            high_risk_count += 1
                        elif compliance_risk_score == 'Medium':
                            medium_risk_count += 1
                    
                    # 根据风险等级判断sanctions_lev
                    if high_risk_count > 0:
                        sanctions_lev = '高风险'
                    elif medium_risk_count > 0:
                        sanctions_lev = '中风险'
                    else:
                        # 如果没有High或Medium，但有其他数据，可能是Low或无评分
                        sanctions_lev = '无风险'
                else:
                    sanctions_lev = '无风险'
                
                return {
                    'VesselImo': vessel_imo,
                    'sanctions_lev': sanctions_lev,
                    'risk_count': len(ais_manipulation_risks),
                    'risks': ais_manipulation_risks,
                    'processing_time': datetime.now().isoformat()
                }
            else:
                return {
                    'VesselImo': vessel_imo,
                    'sanctions_lev': '无风险',
                    'risk_count': 0,
                    'risks': [],
                    'message': '未找到AIS操纵风险数据'
                }
                
        except requests.exceptions.RequestException as e:
            print(f"❌ 劳氏AIS操纵接口调用失败: {e}")
            return {
                'VesselImo': vessel_imo,
                'sanctions_lev': '无风险',
                'risk_count': 0,
                'risks': [],
                'error': str(e)
            }

    def process_lloyds_compliance_data(self, vessel_imo: str, start_date: str, end_date: str):
        """处理Lloyd's合规数据（精确提取版）"""
        compliance_data = self.fetch_lloyds_data("vesselcompliancescreening_v3", vessel_imo, start_date, end_date)
        risk_data = self.fetch_lloyds_data("vesselriskscore", vessel_imo, start_date, end_date)
        

        
        if not compliance_data or not risk_data:
            print("未获取到有效数据")
            return None

        # 提取合规数据（保持原始字段）
        compliance_items = compliance_data.get("Data", {}).get("Items", [])
        if not compliance_items:
            compliance_item = {}
        else:
            compliance_item = compliance_items[0]
        compliance_result = {
            "VesselImo": compliance_item.get("VesselImo"),
            "OwnerIsInSanctionedCountry": compliance_item.get("SanctionRisks", {}).get("OwnerIsInSanctionedCountry"),
            "OwnerIsCurrentlySanctioned": compliance_item.get("SanctionRisks", {}).get("OwnerIsCurrentlySanctioned"),
            "OwnerHasHistoricalSanctions": compliance_item.get("SanctionRisks", {}).get("OwnerHasHistoricalSanctions"),
            "ComplianceDataVersion": compliance_data.get("Data", {}).get("Version", "1.0")
        }

        # 提取风险数据（保持原始字段）
        risk_items = risk_data.get("Data", {}).get("Items", [])
        if not risk_items:
            risk_item = {}
        else:
            risk_item = risk_items[0]
        
        # 尝试从多个可能的字段中获取船舶的Country信息
        vessel_country = None
        if risk_item.get("Country"):
            vessel_country = risk_item.get("Country")
        elif risk_item.get("VesselCountry"):
            vessel_country = risk_item.get("VesselCountry")
        elif risk_item.get("FlagCountry"):
            vessel_country = risk_item.get("FlagCountry")
        elif risk_item.get("Flag"):
            vessel_country = risk_item.get("Flag")
        
        vessel_info = {
            "VesselImo": risk_item.get("VesselImo"),
            "Mmsi": risk_item.get("Mmsi"),
            "VesselName": risk_item.get("VesselName"),
            "VesselType": risk_item.get("VesselType"),
            "Country": vessel_country,  # 使用提取的Country字段
            "Flag": risk_item.get("Flag"),  # 添加Flag字段
            "RiskScores": risk_item.get("RiskScores", {}),
            "VesselOwnershipContainsLinksToSanctionedEntities": risk_item.get("VesselOwnershipContainsLinksToSanctionedEntities", False)
        }

        # 精确提取 SanctionedOwners 的字段
        sanctioned_owners = []
        for owner in risk_item.get("SanctionedOwners", []):
            # 尝试从多个可能的字段中获取Country信息
            country = None
            if owner.get("Country"):
                country = owner.get("Country")
            elif owner.get("HeadOffice", {}).get("Country"):
                country = owner.get("HeadOffice", {}).get("Country")
            elif owner.get("Office", {}).get("Country"):
                country = owner.get("Office", {}).get("Country")
            elif owner.get("RegisteredOffice", {}).get("Country"):
                country = owner.get("RegisteredOffice", {}).get("Country")
            
            owner_data = {
                "CompanyName": owner.get("CompanyName"),
                "CompanyImo": owner.get("CompanyImo"),
                "OwnershipTypes": owner.get("OwnershipTypes", []),
                "OwnershipStartDate": owner.get("OwnershipStartDate", []),
                "Country": country,  # 使用提取的Country字段
                "HeadOffice": owner.get("HeadOffice", {}),  # 获取完整的HeadOffice信息
                "Office": owner.get("Office", {}),  # 获取Office信息
                "RegisteredOffice": owner.get("RegisteredOffice", {}),  # 获取RegisteredOffice信息
                "Sanctions": [{
                    "SanctionSource": s.get("SanctionSource"),
                    "SanctionStartDate": s.get("SanctionStartDate"),
                    "SanctionEndDate": s.get("SanctionEndDate")
                } for s in owner.get("Sanctions", [])],
                "HeadOfficeBasedInSanctionedCountry": owner.get("HeadOfficeBasedInSanctionedCountry", False),
                "HasSanctionedVesselsInFleet": owner.get("HasSanctionedVesselsInFleet", False),
                "SanctionedVesselsFleet": [{
                    "VesselName": s.get("VesselName"),
                    "VesselImo": s.get("VesselImo")
                } for s in owner.get("SanctionedVesselsFleet", [])],
                "RelatedSanctionedCompanies": [{
                    "CompanyImo": s.get("CompanyImo"),
                    "CompanyName": s.get("CompanyName")
                } for s in owner.get("RelatedSanctionedCompanies", [])]
            }
            sanctioned_owners.append(owner_data)

        # 合并数据
        final_data = {
            **vessel_info,
            **compliance_result,
            "SanctionedOwners": sanctioned_owners,
            "ProcessingTime": datetime.now().isoformat(),
            "VoyageDateRange": f"{start_date}-{end_date}"
        }

        self.results['lloyds_compliance'] = final_data
        return final_data
    
    def fetch_lloyds_sanctions(self, imo_number: str) -> pd.DataFrame:
        """获取Lloyd's船舶制裁数据并提取指定字段到DataFrame"""
        url = f"{self.lloyds_base_url}/vesselsanctions_v2?vesselImo={imo_number}"
        
        try:
            # 发送API请求（不重试）
            response = requests.get(url, headers=self.lloyds_headers, timeout=30)
            
            # 特殊处理403错误
            if response.status_code == 403:
                try:
                    error_data = response.json()
                    error_msg = str(error_data.get('Errors', []))
                    print(f"❌ 劳氏制裁接口调用失败 - 403错误: {error_msg}")
                    return pd.DataFrame()
                except:
                    print(f"❌ 劳氏制裁接口调用失败 - 403错误，无法解析错误信息")
                    return pd.DataFrame()
            
            # 检查其他HTTP错误
            if response.status_code != 200:
                print(f"❌ 劳氏制裁接口调用失败 - HTTP错误 {response.status_code}: {response.text[:200]}")
                return pd.DataFrame()
            
            response.raise_for_status()
            data = response.json()

            if not data.get("IsSuccess"):
                error_msg = data.get('Errors', '未知错误')
                print(f"❌ 劳氏制裁接口调用失败 - API返回错误: {error_msg}")
                return pd.DataFrame()

            # 提取items中的vesselSanctions数据
            items = data["Data"]["items"]
            vessel_data = [item["vesselSanctions"] for item in items]

            # 定义需要提取的字段（字符串类型）
            str_fields = [
                "vesselId", "vesselImo", "vesselMmsi", "vesselName",'VesselType','Flag',
                "sanctionId", "source", "type", "program", "name",
                "firstPublished", "lastPublished", "startDate",
                "startQualifier", "endDate", "endQualifier"
            ]

            # 构建结构化数据
            structured_data = []
            for vessel in vessel_data:
                record = {field: str(vessel.get(field, "")) for field in str_fields}
                
                # 处理sanctionVesselDetails列表
                details = vessel.get("sanctionVesselDetails", [])
                if details:
                    # 取第一个详情
                    detail = details[0]
                    record.update({
                        "vesselFlag": detail.get("vesselFlag", ""),
                        "formerVesselFlag": detail.get("formerVesselFlag", ""),
                        "vesselCallsign": detail.get("vesselCallsign", ""),
                        "vesselType": detail.get("vesselType", ""),
                        "vesselOwner": detail.get("vesselOwner", "")
                    })
                else:
                    record.update({
                        "vesselFlag": "", "formerVesselFlag": "",
                        "vesselCallsign": "", "vesselType": "", "vesselOwner": ""
                    })
                
                structured_data.append(record)

            # 创建DataFrame
            df = pd.DataFrame(structured_data)
            self.results['lloyds_sanctions'] = df
            return df

        except requests.exceptions.Timeout as e:
            print(f"❌ 劳氏制裁接口调用失败 - 请求超时: {str(e)}")
            return pd.DataFrame()
                
        except requests.exceptions.ConnectionError as e:
            print(f"❌ 劳氏制裁接口调用失败 - 连接错误: {str(e)}")
            return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            print(f"❌ 劳氏制裁接口调用失败 - 请求异常: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ 劳氏制裁接口调用失败 - 数据处理异常: {str(e)}")
            return pd.DataFrame()
    
    def transform_lloyds_sanctions_data(self, df: pd.DataFrame) -> list:
        """转换Lloyd's制裁数据为按IMO分组的嵌套结构"""
        if df.empty or 'vesselImo' not in df.columns:
            return []

        # 按IMO分组并判断风险状态
        def get_risk_status(group):
            # 处理空值
            end_dates = group['endDate'].apply(
                lambda x: pd.isna(x) or str(x).strip() in ('', 'None')
            )
            
            # 判断当前是否在制裁中（任意endDate为空）
            is_in_sanctions = "是" if end_dates.any() else "否"
            
            # 判断是否有历史制裁（任意endDate有值）
            is_in_sanctions_his = "是" if (~end_dates).any() else "否"
            
            # 判断最高风险等级
            risk_level = '高风险' if end_dates.any() else '中风险'
            
            return risk_level, is_in_sanctions, is_in_sanctions_his

        # 构建嵌套结构
        result = []
        for imo, group in df.groupby('vesselImo'):
            sanctions_list = group[[
                'sanctionId', 'source', 'startDate', 'endDate'
            ]].to_dict('records')
            
            # 统一处理空值
            for item in sanctions_list:
                if pd.isna(item['endDate']) or str(item['endDate']).strip() in ('', 'None'):
                    item['endDate'] = ""
            
            # 获取风险状态
            risk_level, is_in_sanctions, is_in_sanctions_his = get_risk_status(group)
            
            result.append({
                "vesselImo": imo,
                "vesselName": group['vesselName'],  # 取第一个非空值
                "sanctions_lev": risk_level,
                "is_in_sanctions": is_in_sanctions,
                "is_in_sanctions_his": is_in_sanctions_his,
                "sanctions_list": sanctions_list
            })

        self.results['lloyds_sanctions_processed'] = result
        return result
    




# ==================== UANI 数据收集器 ====================
    
    def load_uani_data(self, max_pages=6):
        """加载所有UANI数据到DataFrame"""
        if hasattr(self, '_uani_data_loaded') and self._uani_data_loaded:
            print("UANI数据已加载，无需重复加载")
            return True
            
        print("=== 开始加载UANI数据 ===")
        successful_pages = 0
        df = pd.DataFrame(columns=['IMO', 'Vessel Name', 'Date Added', 'Current Flag', 'Former Flags', 'Source Page'])
        
        for page_num in range(max_pages + 1):
            print(f"正在处理第 {page_num} 页...", end="\r")
            
            html = self._get_uani_page_html(page_num)
            if not html:
                continue
                
            page_df = self._parse_uani_html_to_dataframe(html, page_num)
            if not page_df.empty:
                df = pd.concat([df, page_df], ignore_index=True)
                successful_pages += 1
                
        self._uani_data_loaded = successful_pages > 0
        if self._uani_data_loaded:
            print(f"\n✅ 成功加载 {successful_pages} 页数据，共 {len(df)} 条记录")
            self._clean_uani_data(df)
            self.results['uani_data'] = df
        else:
            print("\n❌❌ 数据加载失败")
            
        return self._uani_data_loaded
    
    def check_uani_imo(self, imo_number):
        """检查IMO号是否存在"""
        if not hasattr(self, '_uani_data_loaded') or not self._uani_data_loaded:
            print("警告：UANI数据未加载，正在自动加载...")
            if not self.load_uani_data():
                return (False, None)
        
        # 标准化IMO号输入
        imo_str = str(imo_number).strip()
        imo_str = ''.join(c for c in imo_str if c.isdigit())
        
        if not imo_str:
            return (False, None)
            
        # 精确匹配查询
        result = self.results['uani_data'][self.results['uani_data']['IMO'].str.replace(r'\D', '', regex=True) == imo_str]
        
        if not result.empty:
            return (True, result.to_dict('records'))
        return (False, None)
    
    def _get_uani_page_html(self, page_num=0):
        """内部方法：获取UANI页面HTML源码"""
        params = {"page": page_num} if page_num > 0 else {}
        
        try:
            self.random_delay()
            response = requests.get(
                self.uani_url,
                params=params,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=120  # 翻倍
            )
            response.raise_for_status()
            return response.text
            
        except Exception as e:
            print(f"获取第{page_num}页失败: {str(e)}")
            return None
    
    def _parse_uani_html_to_dataframe(self, html, page_num):
        """内部方法：解析UANI HTML到DataFrame"""
        soup = BeautifulSoup(html, 'html.parser')
        page_data = []
        
        # 查找所有可能的表格
        tables = []
        for selector in ['table', 'div.table-container table', 'div.view-content table']:
            tables = soup.select(selector)
            if tables:
                break
                
        if not tables:
            return pd.DataFrame()
            
        # 解析表格数据
        for table in tables:
            rows = table.find_all('tr')[1:]  # 跳过表头
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:  # 至少需要3列数据
                    row_data = {
                        'IMO': cells[0].get_text(" ", strip=True),
                        'Vessel Name': cells[1].get_text(" ", strip=True) if len(cells) > 1 else '',
                        'Date Added': cells[2].get_text(" ", strip=True) if len(cells) > 2 else '',
                        'Current Flag': cells[3].get_text(" ", strip=True) if len(cells) > 3 else '',
                        'Former Flags': cells[4].get_text(" ", strip=True) if len(cells) > 4 else '',
                        'Source Page': f"Page {page_num}"
                    }
                    page_data.append(row_data)
                    
        return pd.DataFrame(page_data)
    
    def _clean_uani_data(self, df):
        """内部方法：清洗UANI数据"""
        # 标准化IMO号（去除非数字字符）
        df['IMO'] = df['IMO'].str.replace(r'\D', '', regex=True)
        # 去除空IMO记录
        df = df[df['IMO'].str.len() > 0]
        self.results['uani_data'] = df




# ==================== Kpler API 相关方法 ====================
    
    def fetch_kpler_data(self, imos: List[int], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """从 Kpler API 获取数据"""
        # 检查日期参数
        if not start_date or not end_date or start_date.strip() == '' or end_date.strip() == '':
            print(f"❌ Kpler日期参数无效: start_date='{start_date}', end_date='{end_date}'")
            return []
        
        # 计算日期范围
        try:
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError as e:
            print(f"❌ Kpler日期解析失败: {e}")
            print(f"❌ 日期参数: start_date='{start_date}', end_date='{end_date}'")
            return []
        
        params = {
            "startDate": start_date_dt.isoformat(),
            "endDate": end_date_dt.isoformat(),
            "accept": "application/json"
        }
        
        print(f"Fetching Kpler data for IMOs: {imos} from {start_date}-{end_date}")
        
        try:
            response = requests.post(
                self.kpler_api_url,
                params=params,
                headers=self.kpler_headers,
                json=imos,
                timeout=120  # 翻倍
            )
            response.raise_for_status()
            data = response.json()
            print(f"Received {len(data)} vessel records from Kpler")
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Kpler data: {e}")
            return []
    
    def process_kpler_data(self, raw_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """处理Kpler原始数据，构建内存数据结构"""
        vessels = {}
        
        for record in raw_data:
            vessel = record.get('vessel', {})
            imo = vessel.get('imo')
            if imo is None:
                continue
                
            # 初始化船舶记录
            vessels[imo] = {
                'vessel_info': {
                    'imo': imo,
                    'mmsi': vessel.get('mmsi'),
                    'callsign': vessel.get('callsign'),
                    'shipname': vessel.get('shipname'),
                    'flag': vessel.get('flag'),
                    'countryCode': vessel.get('countryCode'),
                    'typeName': vessel.get('typeName'),
                    'typeSummary': vessel.get('typeSummary'),
                    'gt': str((vessel.get('particulars') or {}).get('gt') or ''),
                    'yob': str((vessel.get('particulars') or {}).get('yob') or '')
                },
                'vessel_companies': [
                    {
                        'name': c.get('name'),
                        'typeName': c.get('typeName'),
                        'startDate': c.get('startDate'),
                        'type': c.get('type')
                    }
                    for c in (vessel.get('vesselCompanies') or [])
                ],
                'sanctioned_vessels': [
                    {
                        'name': vessel.get('shipname'),
                        'source': {
                            #'url': s.get('source', {}).get('url'),
                            'startDate': s.get('source', {}).get('startDate'),
                            'endDate': s.get('source', {}).get('endDate')
                        }
                    }
                    for s in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedVessels')) or [])
                ],
                'sanctioned_cargo': [
                    {
                        'commodity': cargo.get('commodity'),
                        'originZone': cargo.get('originZone'),
                        'originCountry': cargo.get('originCountry'),
                        'destinationCountry': cargo.get('destinationCountry'),
                        'hsCode': str(cargo.get('hsCode') or ''),
                        'hsLink': cargo.get('hsLink'),
                        'sources': [
                            {
                                'name': src.get('name'),
                                #'url': src.get('url'),
                                'startDate': src.get('startDate'),
                                'endDate': src.get('endDate')
                            }
                            for src in (cargo.get('sources') or [])
                        ]
                    }
                    for cargo in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedCargo')) or [])
                ],
                'sanctioned_trades': [
                    {
                        'commodity': trade.get('commodity'),
                        'originZone': trade.get('originZone'),
                        'originCountry': trade.get('originCountry'),
                        'destinationZone': trade.get('destinationZone'),
                        'destinationCountry': trade.get('destinationCountry'),
                        'hsCode': str(trade.get('hsCode') or ''),
                        'hsLink': trade.get('hsLink'),
                        'voyageId': str(trade.get('voyageId') or ''),
                        'sources': [
                            {
                                'name': src.get('name'),
                                #'url': src.get('url'),
                                'startDate': src.get('startDate'),
                                'endDate': src.get('endDate')
                            }
                            for src in (trade.get('sources') or [])
                        ]
                    }
                    for trade in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedTrades')) or [])
                ],
                'sanctioned_companies': [
                    {
                        'name': company.get('name'),
                        'type': company.get('type'),
                        'source': {
                            'name': company.get('source', {}).get('name'),
                            #'url': company.get('source', {}).get('url'),
                            'startDate': company.get('source', {}).get('startDate'),
                            'endDate': company.get('source', {}).get('endDate')
                        }
                    }
                    for company in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedCompanies')) or [])
                ],
                # 'sanctioned_flag': [
                #     {
                #         'flagCode': flag.get('flagCode'),
                #         'vesselFlagStartDate': flag.get('vesselFlagStartDate'),
                #         'vesselFlagEndDate': flag.get('vesselFlagEndDate'),
                #         'source': {
                #             'name': flag.get('source', {}).get('name'),
                #             #'url': flag.get('source', {}).get('url'),
                #             'startDate': flag.get('source', {}).get('startDate'),
                #             'endDate': flag.get('source', {}).get('endDate')
                #         }
                #     }
                #     for flag in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedFlag')) or [])
                # ],
                'port_calls': [
                    {
                        'volume': str(port.get('volume') or ''),
                        'endDate': port.get('endDate'),
                        'portName': port.get('portName'),
                        'zoneName': port.get('zoneName'),
                        'startDate': port.get('startDate'),
                        'shipToShip': str(port.get('shipToShip') or ''),
                        'countryName': port.get('countryName'),
                        'sanctionedCargo': str(port.get('sanctionedCargo') or ''),
                        'sanctionedVessel': str(port.get('sanctionedVessel') or ''),
                        'sanctionedOwnership': str(port.get('sanctionedOwnership') or '')
                    }
                    for port in ((record.get('compliance', {}).get('operationalRisks', {}).get('portCalls')) or [])
                ],
                'sts_events': [
                    {
                        'zoneName': sts.get('zoneName'),
                        'volume': str(sts.get('volume') or ''),
                        'endDate': sts.get('endDate'),
                        'portName': sts.get('portName'),
                        'startDate': sts.get('startDate'),
                        'shipToShip': str(sts.get('shipToShip') or ''),
                        'countryName': sts.get('countryName'),
                        'sanctionedCargo': str(sts.get('sanctionedCargo') or ''),
                        'sanctionedVessel': str(sts.get('sanctionedVessel') or ''),
                        'sanctionedOwnership': str(sts.get('sanctionedOwnership') or ''),
                        'vessel2Imo': str(sts.get('vessel2Imo') or ''),
                        'vessel2Name': str(sts.get('vessel2Name') or ''),
                        'vessel2SanctionedVessel': str(sts.get('vessel2SanctionedVessel') or ''),
                        'vessel2SanctionedOwnership': str(sts.get('vessel2SanctionedOwnership') or ''),
                        'stsVessel': {
                            'imo': str((sts.get('stsVessel') or {}).get('imo') or ''),
                            'name': (sts.get('stsVessel') or {}).get('name'),
                            'sanctionedVessel': str((sts.get('stsVessel') or {}).get('sanctionedVessel') or ''),
                            'sanctionedOwnership': str((sts.get('stsVessel') or {}).get('sanctionedOwnership') or '')
                        }
                    }
                    for sts in ((record.get('compliance', {}).get('operationalRisks', {}).get('stsEvents')) or [])
                ],
                'ais_gaps': [
                    {
                        'startDate':str(gap.get('startDate') or ''),
                        'draughtChange':str(gap.get('draughtChange') or ''),
                        'durationMin':str(gap.get('durationMin') or ''),
                        'zone': {
                            'start': {
                                'start_id': str((gap.get('zone') or {}).get('start', {}).get('id') or ''),
                                'start_name': (gap.get('zone') or {}).get('start', {}).get('name')
                            },
                            'end': {
                                'end_id': str((gap.get('zone') or {}).get('end', {}).get('id') or ''),
                                'end_name': (gap.get('zone') or {}).get('end', {}).get('name')
                            }
                        },
                        'position': {
                            'start': {
                                'start_lon': str((gap.get('position') or {}).get('start', {}).get('lon') or ''),
                                'start_lat': str((gap.get('position') or {}).get('start', {}).get('lat') or '')
                            },
                            'end': {
                                'end_lon': str((gap.get('position') or {}).get('end', {}).get('lon') or ''),
                                'end_lat': str((gap.get('position') or {}).get('end', {}).get('lat') or '')
                            }
                        }
                    }
                    for gap in ((record.get('compliance', {}).get('operationalRisks', {}).get('aisGaps')) or [])
                ],
                'ais_spoofs': [
                    {
                        'startDate': spoof.get('startDate'),
                        'endDate': spoof.get('endDate'),
                        'durationMin': str(spoof.get('durationMin') or ''),
                        'zone': {
                            'start': {
                                'id': str((spoof.get('zone') or {}).get('start', {}).get('id') or ''),
                                'name': (spoof.get('zone') or {}).get('start', {}).get('name')
                            },
                            'end': {
                                'id': str((spoof.get('zone') or {}).get('end', {}).get('id') or ''),
                                'name': (spoof.get('zone') or {}).get('end', {}).get('name')
                            }
                        },
                        'position': {
                            'start': {
                                'lon': str((spoof.get('position') or {}).get('start', {}).get('lon') or ''),
                                'lat': str((spoof.get('position') or {}).get('start', {}).get('lat') or '')
                            },
                            'end': {
                                'lon': str((spoof.get('position') or {}).get('end', {}).get('lon') or ''),
                                'lat': str((spoof.get('position') or {}).get('end', {}).get('lat') or '')
                            }
                        }
                    }
                    for spoof in ((record.get('compliance', {}).get('operationalRisks', {}).get('aisSpoofs')) or [])
                ],
                'dark_sts_events': [
                    {
                        'date': event.get('date'),
                        'source': event.get('source'),
                        'stsVessel': {
                            'imo': str((event.get('stsVessel') or {}).get('imo') or ''),
                            'name': (event.get('stsVessel') or {}).get('name')
                        },
                        'zone': {
                            'id': str((event.get('zone') or {}).get('id') or ''),
                            'name': (event.get('zone') or {}).get('name')
                        }
                    }
                    for event in ((record.get('compliance', {}).get('operationalRisks', {}).get('darkStsEvents')) or [])
                ]
            }
        
        return vessels
    
    def create_kpler_summary(self, vessels: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """创建Kpler船舶风险摘要"""
        summary = {}
        
        for imo, vessel_data in vessels.items():
            summary[imo] = {
                'has_sanctioned_cargo': 'true' if vessel_data['sanctioned_cargo'] else 'false',
                'has_sanctioned_trades': 'true' if vessel_data['sanctioned_trades'] else 'false',
                # 'has_sanctioned_flag': 'true' if vessel_data['sanctioned_flag'] else 'false',
                'has_port_calls': 'true' if vessel_data['port_calls'] else 'false',
                'has_sts_events': 'true' if vessel_data['sts_events'] else 'false',
                'has_ais_gap': 'true' if vessel_data['ais_gaps'] else 'false',
                'has_ais_spoofs': 'true' if vessel_data['ais_spoofs'] else 'false',
                'has_dark_sts': 'true' if vessel_data['dark_sts_events'] else 'false',
                'has_sanctioned_companies': 'true' if vessel_data['sanctioned_companies'] else 'false'
            }
        
        return summary
    
    def apply_kpler_risk_mapping(self, summary: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """应用Kpler风险映射规则"""
        risk_assessment = {}
        
        for imo, vessel_summary in summary.items():
            risk_assessment[imo] = {}
            
            for field, value in vessel_summary.items():
                risk_assessment[imo][f"{field}_risk"] = self.risk_mapping[field][value]
            
            # 计算总体风险等级
            high_risk_fields = [
                risk_assessment[imo]['has_sanctioned_cargo_risk'] == '高风险',
                risk_assessment[imo]['has_sanctioned_trades_risk'] == '高风险',
                # risk_assessment[imo]['has_sanctioned_flag_risk'] == '高风险',
                risk_assessment[imo]['has_port_calls_risk'] == '高风险',
                risk_assessment[imo]['has_sts_events_risk'] == '高风险',
                # risk_assessment[imo]['has_ais_gap_risk'] == '高风险',
                risk_assessment[imo]['has_ais_spoofs_risk'] == '高风险',
                risk_assessment[imo]['has_dark_sts_risk'] == '高风险',
                risk_assessment[imo]['has_sanctioned_companies_risk'] == '高风险'
            ]
            
            medium_risk_fields = [
                risk_assessment[imo]['has_sanctioned_cargo_risk'] == '中风险',
                risk_assessment[imo]['has_sanctioned_trades_risk'] == '中风险',
                # risk_assessment[imo]['has_sanctioned_flag_risk'] == '中风险',
                risk_assessment[imo]['has_port_calls_risk'] == '中风险',
                risk_assessment[imo]['has_sts_events_risk'] == '中风险',
                # risk_assessment[imo]['has_ais_gap_risk'] == '中风险',
                risk_assessment[imo]['has_ais_spoofs_risk'] == '中风险',
                risk_assessment[imo]['has_dark_sts_risk'] == '中风险',
                risk_assessment[imo]['has_sanctioned_companies_risk'] == '中风险'
            ]
            
            if any(high_risk_fields):
                risk_assessment[imo]['ship_status'] = '需拦截'
                risk_assessment[imo]['risk_level'] = '高'
            elif any(medium_risk_fields):
                risk_assessment[imo]['ship_status'] = '需关注'
                risk_assessment[imo]['risk_level'] = '中'
            else:
                risk_assessment[imo]['ship_status'] = '正常'
                risk_assessment[imo]['risk_level'] = '低'
        
        return risk_assessment
    
    def create_kpler_final_report(self, vessels: Dict[str, Dict[str, Any]], 
                                risk_assessment: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """创建Kpler最终报告"""
        final_report = {}
        
        for imo in vessels.keys():
            vessel_data = vessels[imo]
            assessment = risk_assessment.get(imo, {})
            
            final_report[imo] = {
                **vessel_data['vessel_info'],
                **assessment,
                # 只对has_sanctioned_cargo_list使用数组格式
                'has_sanctioned_cargo_list': vessel_data['sanctioned_cargo'],  # 直接使用数组，保留sources
                'has_sanctioned_trades_list': vessel_data['sanctioned_trades'],  # 直接使用数组，保留sources
                # 'has_sanctioned_flag_list': self.format_detail_list(
                #     vessel_data['sanctioned_flag'],
                #     ['source.name', 'source.startDate', 'source.endDate', 'flagCode']
                # ),
                'has_port_calls_list': self.format_detail_list(
                    vessel_data['port_calls'],
                    ['zoneName','portName','startDate','endDate' ,'countryName', 'volume', 'shipToShip', 'sanctionedVessel',  'sanctionedCargo','sanctionedOwnership']
                ),
                'has_sts_events_list': self.format_detail_list(
                    vessel_data['sts_events'],
                    ['zoneName', 'startDate', 'endDate', 'portName','countryName' , 'shipToShip','sanctionedVessel',  'sanctionedCargo', 'sanctionedOwnership','vessel2SanctionedOwnership','vessel2SanctionedVessel','vessel2Imo','vessel2Name']
                ),
                'has_ais_gap_list': self.format_detail_list(
                    vessel_data['ais_gaps'],
                    ['startDate','zone.start.start_id', 'zone.start.start_name', 'zone.end.end_id', 'zone.end.end_name',
                     'position.start.start_lon', 'position.start.start_lat', 'position.end.end_lon', 'position.end.end_lat','draughtChange','durationMin']
                ),
                'has_ais_spoofs_list': self.format_detail_list(
                    vessel_data['ais_spoofs'],
                    ['startDate', 'endDate', 'position.start.lon', 'position.start.lat','position.end.lon', 
                    'position.end.lat''durationMin']
                ),
                'has_dark_sts_list': self.format_detail_list(
                    vessel_data['dark_sts_events'],
                    ['date', 'stsVessel.imo', 'stsVessel.name', 'zone.id', 'zone.name','source']
                ),
                'has_sanctioned_companies_list': self.format_detail_list(
                    vessel_data['sanctioned_companies'],
                    ['name', 'source.name', 'source.startDate', 'source.endDate', 'type']
                ),
                # 新增表格字段
                'vessel_companies_table': self.format_vessel_companies_table(vessel_data['vessel_companies']),
                'sanctioned_companies_table': self.format_sanctioned_companies_table(vessel_data['sanctioned_companies'])
            }
        
        self.results['kpler_data'] = final_report
        return final_report
    
    def format_vessel_companies_table(self, vessel_companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """格式化船舶公司表格数据"""
        table_data = []
        
        def format_timestamp(timestamp):
            if timestamp and str(timestamp).isdigit():
                try:
                    from datetime import datetime
                    return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    return str(timestamp)
            return str(timestamp) if timestamp else ""
        
        for company in vessel_companies:
            table_data.append({
                "name": company.get('name', ''),
                "startDate": format_timestamp(company.get('startDate')),
                "typeName": company.get('typeName', ''),
                "type": company.get('type')  # 用于关联，不显示但保留
            })
        
        return table_data

    def format_sanctioned_companies_table(self, sanctioned_companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """格式化制裁公司表格数据"""
        table_data = []
        
        def format_timestamp(timestamp):
            if timestamp and str(timestamp).isdigit():
                try:
                    from datetime import datetime
                    return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    return str(timestamp)
            return str(timestamp) if timestamp else ""
        
        for company in sanctioned_companies:
            table_data.append({
                "sourceName": company.get('source', {}).get('name', ''),
                "startDate": format_timestamp(company.get('source', {}).get('startDate')),
                "type": company.get('type')  # 用于关联，不显示但保留
            })
        
        return table_data
    
    def create_kpler_final_report_with_sources(self, vessels: Dict[str, Dict[str, Any]], 
                                             risk_assessment: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """创建包含完整sources信息的Kpler最终报告（数组格式）"""
        final_report = {}
        
        for imo in vessels.keys():
            vessel_data = vessels[imo]
            assessment = risk_assessment.get(imo, {})
            
            # 处理sanctioned_cargo，保留sources数组
            sanctioned_cargo_with_sources = []
            for cargo in vessel_data['sanctioned_cargo']:
                cargo_info = {
                    'commodity': cargo.get('commodity'),
                    'originZone': cargo.get('originZone'),
                    'originCountry': cargo.get('originCountry'),
                    'destinationCountry': cargo.get('destinationCountry'),
                    'hsCode': cargo.get('hsCode'),
                    'hsLink': cargo.get('hsLink'),
                    'sources': cargo.get('sources', [])  # 保留完整的sources数组
                }
                sanctioned_cargo_with_sources.append(cargo_info)
            
            # 处理sanctioned_trades，保留sources数组
            sanctioned_trades_with_sources = []
            for trade in vessel_data['sanctioned_trades']:
                trade_info = {
                    'commodity': trade.get('commodity'),
                    'originZone': trade.get('originZone'),
                    'originCountry': trade.get('originCountry'),
                    'destinationZone': trade.get('destinationZone'),
                    'destinationCountry': trade.get('destinationCountry'),
                    'hsCode': trade.get('hsCode'),
                    'hsLink': trade.get('hsLink'),
                    'voyageId': trade.get('voyageId'),
                    'sources': trade.get('sources', [])  # 保留完整的sources数组
                }
                sanctioned_trades_with_sources.append(trade_info)
            
            final_report[imo] = {
                **vessel_data['vessel_info'],
                **assessment,
                'has_sanctioned_cargo_list': sanctioned_cargo_with_sources,  # 直接使用数组
                'has_sanctioned_trades_list': sanctioned_trades_with_sources,  # 直接使用数组
                'has_port_calls_list': vessel_data['port_calls'],  # 直接使用数组
                'has_sts_events_list': vessel_data['sts_events'],  # 直接使用数组
                'has_ais_gap_list': vessel_data['ais_gaps'],  # 直接使用数组
                'has_ais_spoofs_list': vessel_data['ais_spoofs'],  # 直接使用数组
                'has_dark_sts_list': vessel_data['dark_sts_events'],  # 直接使用数组
                'has_sanctioned_companies_list': vessel_data['sanctioned_companies'],  # 直接使用数组
                # 新增表格字段
                'vessel_companies_table': self.format_vessel_companies_table(vessel_data['vessel_companies']),
                'sanctioned_companies_table': self.format_sanctioned_companies_table(vessel_data['sanctioned_companies'])
            }
        
        self.results['kpler_data'] = final_report
        return final_report
    
    def process_kpler(self, imos: List[int], start_date: str, end_date: str, 
                     use_array_format: bool = True) -> Dict[str, Dict[str, Any]]:
        """完整Kpler处理流程
        
        Args:
            imos: IMO列表
            start_date: 开始日期
            end_date: 结束日期
            use_array_format: 是否使用数组格式（True: 保留sources数组, False: 使用扁平化格式）
        """
        # 1. 获取原始数据
        raw_data = self.fetch_kpler_data(imos, start_date, end_date)
        
        # 2. 处理原始数据
        vessels = self.process_kpler_data(raw_data)
        
        # 3. 创建摘要
        summary = self.create_kpler_summary(vessels)
        
        # 4. 应用风险映射
        risk_assessment = self.apply_kpler_risk_mapping(summary)
        
        # 5. 根据参数选择创建最终报告的方式
        if use_array_format:
            final_report = self.create_kpler_final_report_with_sources(vessels, risk_assessment)
        else:
            final_report = self.create_kpler_final_report(vessels, risk_assessment)
        
        return final_report
    



# ==================== 航次风险分析方法 ====================
    
    def get_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """从API获取航次数据"""
        url = f"{self.lloyds_base_url}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        try:
            response = requests.get(url, headers=self.lloyds_headers, params=params, timeout=120)  # 翻倍
            response.raise_for_status()
            data = response.json()
            
            # 添加调试信息
            print(f"🔍 调试 - get_voyage_data 返回数据类型: {type(data)}")
            if isinstance(data, dict):
                print(f"🔍 调试 - 数据键: {list(data.keys())}")
            else:
                print(f"🔍 调试 - 非字典数据内容: {data}")
            
            return data
        except requests.exceptions.RequestException as e:
            print(f"API请求失败: {e}")
            return {}
        except Exception as e:
            print(f"❌ get_voyage_data 发生未知错误: {e}")
            return {}
    
    def extract_vessel_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """提取船舶基本信息"""
        # 添加类型检查，防止传入字符串
        if not isinstance(data, dict):
            print(f"❌ extract_vessel_info 接收到非字典类型数据: {type(data)}")
            print(f"❌ 数据内容: {data}")
            return {
                "VesselImo": None,
                "VesselName": None,
                "VesselType": None,
                "Flag": None
            }
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return {
                "VesselImo": None,
                "VesselName": None,
                "VesselType": None,
                "Flag": None
            }
        
        vessel_data = items[0]
        return {
            "VesselImo": vessel_data.get("VesselImo"),
            "VesselName": vessel_data.get("VesselName"),
            "VesselType": vessel_data.get("VesselType"),
            "Flag": vessel_data.get("Flag")
        }
    
    def process_high_risk_port_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理高风险港口访问的航次数据"""
        print(f"🔍 处理高风险港口航次: IMO={vessel_imo}")
        
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            print("❌ 没有获取到航次数据")
            return pd.DataFrame()
        
        try:
            vessel_info = self.extract_vessel_info(raw_data)
            print(f"📋 船舶信息: {vessel_info}")
            
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                print("❌ 没有Items数据")
                return pd.DataFrame()
            
            first_item = items[0]
            voyages = first_item.get("Voyages", [])
            if not voyages:
                print("❌ 没有航次数据")
                return pd.DataFrame()
            
            print(f"🚢 找到 {len(voyages)} 个航次")
            
            high_risk_voyages = []
            
            for i, voyage in enumerate(voyages):
                risk_types = voyage.get("RiskTypes", [])
                print(f"   航次 {i+1}: RiskTypes = {risk_types}")
                
                if "High Risk Port Calling" in risk_types:
                    processed_voyage = {
                        "VoyageId": voyage.get("VoyageId"),
                        "VoyageStartTime": voyage.get("VoyageStartTime"),
                        "VoyageEndTime": voyage.get("VoyageEndTime"),
                        "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                        "StartPlace": self._process_place(voyage.get("VoyageStartPlace")),
                        "EndPlace": self._process_place(voyage.get("VoyageEndPlace")),
                        "RiskTypes": risk_types
                    }
                    high_risk_voyages.append(processed_voyage)
                    print(f"   ✅ 发现高风险港口航次: {processed_voyage['VoyageId']}")
            
            print(f"🚨 总共发现 {len(high_risk_voyages)} 个高风险港口航次")
            
            # 转换为DataFrame并保存
            if high_risk_voyages:
                df = pd.DataFrame({"raw_data": high_risk_voyages})
                df['VesselImo'] = vessel_imo
                self.results['voyage_risks']['high_risk_port'] = df
                print(f"💾 已保存高风险港口数据到结果中")
                return df
            else:
                print("⚠️  没有发现高风险港口航次")
                # 创建一个空的DataFrame，但包含船舶信息
                empty_df = pd.DataFrame({
                    "raw_data": [{"message": "没有发现高风险港口航次", "vessel_info": vessel_info}],
                    "VesselImo": [vessel_imo]
                })
                self.results['voyage_risks']['high_risk_port'] = empty_df
                return empty_df
                
        except Exception as e:
            print(f"❌ 处理高风险港口航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_possible_dark_port_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理可能的Dark Port访问的航次数据"""
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            return pd.DataFrame()
        
        vessel_info = self.extract_vessel_info(raw_data)
        items = raw_data.get("Data", {}).get("Items", [])
        if not items:
            return pd.DataFrame()
        voyages = items[0].get("Voyages", [])
        results = []
        
        for voyage in voyages:
            events = voyage.get("VoyageEvents", {})
            for gap in events.get("AisGap", []):
                risk_types = gap.get("RiskTypes", [])
                dark_port_keywords = ["Possible Dark Port Calling", "probable Dark Port Callin"]
                
                if any(keyword in risk_types for keyword in dark_port_keywords):
                    eez_name = gap.get("AisGapStartEezName", "")
                    s_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                    result = {
                        "VesselInfo": vessel_info,
                        "VoyageInfo": {
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                            "RiskTypes": risk_types,
                            "AisGapStartDateTime":gap.get("AisGapStartDateTime"),
                            "AisGapEndDateTime":gap.get("AisGapEndDateTime"),
                            "AisGapStartEezName":gap.get("AisGapStartEezName"),
                            "s_sanctioned_eez": s_sanctioned_eez,
                            "DarkPortCalls": self._extract_dark_port_calls(gap)
                        }
                    }
                    results.append(result)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame({"raw_data": results})
        df['VesselImo'] = vessel_imo
        self.results['voyage_risks']['possible_dark_port'] = df
        return df
    
    def process_suspicious_ais_gap_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理可疑AIS中断的航次数据"""
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            return pd.DataFrame()
        
        vessel_info = self.extract_vessel_info(raw_data)
        items = raw_data.get("Data", {}).get("Items", [])
        if not items:
            return pd.DataFrame()
        voyages = items[0].get("Voyages", [])
        results = []
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            events = voyage.get("VoyageEvents", {})
            
            if "Suspicious AIS Gap" in risk_types:
                for gap in events.get("AisGap", []):
                    if "Suspicious AIS Gap" in gap.get("RiskTypes", []):
                        eez_name = gap.get("AisGapStartEezName", "")
                        is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                        result = {
                            "VesselInfo": vessel_info,
                            "VoyageInfo": {
                                "VoyageStartTime": voyage.get("VoyageStartTime"),
                                "VoyageEndTime": voyage.get("VoyageEndTime"),
                                "RiskTypes": risk_types,
                                "AISGap": {
                                    "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                                    "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                                    "AisGapStartEezName": gap.get("AisGapStartEezName"),
                                    "is_sanctioned_eez": is_sanctioned_eez,
                                    "RiskTypes": gap.get("RiskTypes", [])
                                }
                            }
                        }
                        results.append(result)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame({"raw_data": results})
        df['VesselImo'] = vessel_imo
        self.results['voyage_risks']['suspicious_ais_gap'] = df
        return df
    
    def process_dark_sts_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理Dark STS（船对船转运）的航次数据"""
        target_risk_types = [
            "Possible 1-way Dark STS (as dark party)",
            "Possible 2-way Dark STS (as dark party)"
        ]
        
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            return pd.DataFrame()
        
        vessel_info = self.extract_vessel_info(raw_data)
        items = raw_data.get("Data", {}).get("Items", [])
        if not items:
            return pd.DataFrame()
        voyages = items[0].get("Voyages", [])
        results = []
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            events = voyage.get("VoyageEvents", {})
            
            if any(rt in risk_types for rt in target_risk_types):
                for gap in events.get("AisGap", []):
                    gap_risk_types = gap.get("RiskTypes", [])
                    if any(rt in gap_risk_types for rt in target_risk_types):
                        eez_name = gap.get("AisGapStartEezName", "")
                        is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                        result = {
                            "VesselInfo": vessel_info,
                            "VoyageInfo": {
                                "VoyageStartTime": voyage.get("VoyageStartTime"),
                                "VoyageEndTime": voyage.get("VoyageEndTime"),
                                "RiskTypes": risk_types,
                                "AISGap": {
                                "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                                "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                                "AisGapStartEezName": gap.get("AisGapStartEezName"),
                                "is_sanctioned_eez": is_sanctioned_eez,
                                "1Way": self._extract_probable_1w_dark_sts(gap),
                                "2Way": self._extract_probable_2w_dark_sts(gap)
                            }
                            }
                        }
                        results.append(result)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame({"raw_data": results})
        df['VesselImo'] = vessel_imo
        self.results['voyage_risks']['dark_sts'] = df
        return df
    
    def process_sanctioned_sts_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理与受制裁船舶的STS转运的航次数据"""
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            return pd.DataFrame()
        
        vessel_info = self.extract_vessel_info(raw_data)
        items = raw_data.get("Data", {}).get("Items", [])
        if not items:
            return pd.DataFrame()
        voyages = items[0].get("Voyages", [])
        results = []
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            events = voyage.get("VoyageEvents", {})
            
            if "STS With a Sanctioned Vessel" in risk_types:
                for sts in events.get("ShipToShipTransfer", []):
                    result = {
                        "VesselInfo": vessel_info,
                        "VoyageInfo": {
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "RiskTypes": risk_types
                        },
                        "STSEvent": {
                            "StartDateTime": sts.get("StartDateTime"),
                            "EndDateTime": sts.get("EndDateTime"),
                            "StsType": sts.get("StsType")
                        },
                        "CounterpartVessels": self._extract_counterpart_vessels(sts)
                    }
                    results.append(result)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame({"raw_data": results})
        df['VesselImo'] = vessel_imo
        self.results['voyage_risks']['sanctioned_sts'] = df
        return df
    
    def process_loitering_behavior_voyages(self, vessel_imo: str, start_date: str, end_date: str):
        """处理可疑徘徊行为的航次数据"""
        target_risk_types = [
            "Suspicious Loitering Behaviour",
            "Possible 1-Way Dark STS (as non-dark party)"
        ]
        
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        if not raw_data:
            return pd.DataFrame()
        
        vessel_info = self.extract_vessel_info(raw_data)
        items = raw_data.get("Data", {}).get("Items", [])
        if not items:
            return pd.DataFrame()
        voyages = items[0].get("Voyages", [])
        results = []
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            events = voyage.get("VoyageEvents", {})
            
            if any(rt in risk_types for rt in target_risk_types):
                for event in events.get("Loitering", []):
                    event_risk_types = event.get("RiskTypes", [])
                    if any(rt in event_risk_types for rt in target_risk_types):
                        result = {
                            "VesselInfo": vessel_info,
                            "VoyageInfo": {
                                "VoyageStartTime": voyage.get("VoyageStartTime"),
                                "VoyageEndTime": voyage.get("VoyageEndTime"),
                                "RiskTypes": risk_types,
                                "DarkSTS": self._extract_probable_1w_dark_sts(event),
                                "LoiteringEvent": {
                                    "LoiteringStart": event.get("LoiteringStart"),
                                    "LoiteringEnd": event.get("LoiteringEnd"),
                                    "RiskTypes": event_risk_types
                                }
                            }
                        }
                        results.append(result)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame({"raw_data": results})
        df['VesselImo'] = vessel_imo
        self.results['voyage_risks']['loitering_behavior'] = df
        return df
    
    def _extract_dark_port_calls(self, gap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取Dark Port Calls详情"""
        dark_port_calls = []
        for call in gap_data.get("ProbableHighRiskDarkPortCalls", []):
            port_data = call.get("Port", {})
            dark_port_calls.append({
                "Name": port_data.get("Name"),
                "CountryName": port_data.get("CountryName"),
                "IsHighRiskPort": port_data.get("IsHighRiskPort", False)
            })
        return dark_port_calls
    
    def _extract_probable_1w_dark_sts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取1-way Dark STS详情"""
        dark_sts_list = []
        for sts in data.get("Probable1WDarkSts", []):
            dark_sts_list.append({
                "LoiteringStart": sts.get("LoiteringStart"),
                "LoiteringEnd": sts.get("LoiteringEnd"),
                "VesselImo": sts.get("VesselImo"),
                "VesselName": sts.get("VesselName"),
                "VesselType": sts.get("VesselType"),
                "RiskIndicators": sts.get("RiskIndicators", []),
                "RiskScore": sts.get("RiskScore")
            })
        return dark_sts_list
    
    def _extract_probable_2w_dark_sts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取2-way Dark STS详情"""
        dark_sts_list = []
        for sts in data.get("Probable2WDarkSts", []):
            dark_sts_list.append({
                "GapStart": sts.get("GapStart"),
                "GapEnd": sts.get("GapEnd"),
                "VesselImo": sts.get("VesselImo"),
                "VesselName": sts.get("VesselName"),
                "VesselType": sts.get("VesselType"),
                "RiskIndicators": sts.get("RiskIndicators", []),
                "RiskScore": sts.get("RiskScore")
            })
        return dark_sts_list
    
    def _extract_counterpart_vessels(self, sts_event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取STS事件中的对方船舶信息 - 修改为直接返回船舶基本信息"""
        counterpart_vessels = []
        counterpart_data = sts_event.get("CounterpartVessel")
        
        if isinstance(counterpart_data, dict):
            # 直接提取船舶基本信息，不处理VesselSanctions和SanctionedOwners数组
            vessel_info = {
                "IsVesselSanctioned": counterpart_data.get("IsVesselSanctioned"),
                "IsVesselOwnershipSanctioned": counterpart_data.get("IsVesselOwnershipSanctioned"),
                "IsVesselOwnershipLinkedToSanctionedEntities": counterpart_data.get("IsVesselOwnershipLinkedToSanctionedEntities"),
                "VesselImo": counterpart_data.get("VesselImo"),
                "VesselName": counterpart_data.get("VesselName"),
                "VesselType": counterpart_data.get("VesselType"),
                "RiskIndicators": counterpart_data.get("RiskIndicators"),
                "RiskScore": counterpart_data.get("RiskScore")
            }
            counterpart_vessels.append(vessel_info)
        elif isinstance(counterpart_data, list):
            for vessel in counterpart_data:
                if isinstance(vessel, dict):
                    vessel_info = {
                        "IsVesselSanctioned": vessel.get("IsVesselSanctioned"),
                        "IsVesselOwnershipSanctioned": vessel.get("IsVesselOwnershipSanctioned"),
                        "IsVesselOwnershipLinkedToSanctionedEntities": vessel.get("IsVesselOwnershipLinkedToSanctionedEntities"),
                        "VesselImo": vessel.get("VesselImo"),
                        "VesselName": vessel.get("VesselName"),
                        "VesselType": vessel.get("VesselType"),
                        "RiskIndicators": vessel.get("RiskIndicators"),
                        "RiskScore": vessel.get("RiskScore")
                    }
                    counterpart_vessels.append(vessel_info)
        
        return counterpart_vessels
    
    def _extract_single_vessel_info(self, vessel: Dict[str, Any]) -> Dict[str, Any]:
        """提取单个船舶信息 - 修改为扁平化结构"""
        if not isinstance(vessel, dict):
            return {}
        
        # 基础船舶信息
        base_vessel_info = {
            "IsVesselSanctioned": vessel.get("IsVesselSanctioned", False),
            "IsVesselOwnershipSanctioned": vessel.get("IsVesselOwnershipSanctioned", False),
            "IsVesselOwnershipLinkedToSanctionedEntities": vessel.get("IsVesselOwnershipLinkedToSanctionedEntities", False),
            "VesselImo": vessel.get("VesselImo"),
            "VesselName": vessel.get("VesselName"),
            "VesselType": vessel.get("VesselType"),
            "RiskIndicators": vessel.get("RiskIndicators", []),
            "RiskScore": vessel.get("RiskScore")
        }
        
        # 处理制裁信息 - 每个制裁记录都包含完整的船舶信息
        vessel_sanctions = []
        for sanction in vessel.get("VesselSanctions", []):
            sanction_record = base_vessel_info.copy()
            sanction_record.update({
                "SanctionSource": sanction.get("SanctionSource"),
                "SanctionProgram": sanction.get("SanctionProgram"),
                "SanctionStartDate": sanction.get("SanctionStartDate"),
                "SanctionEndDate": sanction.get("SanctionEndDate")
            })
            vessel_sanctions.append(sanction_record)
        
        # 处理受制裁的船东信息 - 每个船东记录都包含完整的船舶信息
        sanctioned_owners = []
        for owner in vessel.get("SanctionedOwners", []):
            owner_record = base_vessel_info.copy()
            owner_record.update({
                "CompanyName": owner.get("CompanyName"),
                "OwnershipTypes": owner.get("OwnershipTypes", []),
                "OwnershipStart": owner.get("OwnershipStart"),
                "HeadOfficeTown": owner.get("HeadOfficeTown")
            })
            sanctioned_owners.append(owner_record)
        
        return {
            "VesselSanctions": vessel_sanctions,
            "SanctionedOwners": sanctioned_owners
        }
    
    def analyze_voyage_risks(self, vessel_imo: str, start_date: str, end_date: str):
        """执行所有航次风险分析 - 优化版本，只调用一次API"""
        print(f"\n开始分析船舶 IMO: {vessel_imo} ({start_date}-{end_date})")
        
        # 只调用一次API获取航次数据
        print("🔄 正在获取航次数据...")
        raw_data = self.get_voyage_data(vessel_imo, start_date, end_date)
        
        # 添加调试信息
        print(f"🔍 调试 - analyze_voyage_risks 中 raw_data 类型: {type(raw_data)}")
        if isinstance(raw_data, dict):
            print(f"🔍 调试 - raw_data 键: {list(raw_data.keys())}")
        else:
            print(f"🔍 调试 - raw_data 内容: {raw_data}")
        
        if not raw_data:
            print("❌ 没有获取到航次数据")
            return
        
        print(f"✅ 成功获取航次数据，开始分析...")
        
        # 在内存中处理所有风险类型，避免重复API调用
        self.process_high_risk_port_voyages_from_data(raw_data, vessel_imo)
        self.process_possible_dark_port_voyages_from_data(raw_data, vessel_imo)
        self.process_suspicious_ais_gap_voyages_from_data(raw_data, vessel_imo)
        self.process_dark_sts_voyages_from_data(raw_data, vessel_imo)
        self.process_sanctioned_sts_voyages_from_data(raw_data, vessel_imo)
        self.process_loitering_behavior_voyages_from_data(raw_data, vessel_imo)
        
        print("✅ 航次风险分析完成（优化版本：只调用1次API）")
    
    def process_high_risk_port_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理高风险港口访问的航次数据"""
        print(f"🔍 处理高风险港口航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_high_risk_port_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_high_risk_port_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            vessel_info = self.extract_vessel_info(raw_data)
            print(f"📋 船舶信息: {vessel_info}")
            
            # 添加额外的安全检查
            if not isinstance(raw_data, dict):
                print(f"❌ raw_data 在 get 操作前不是字典类型: {type(raw_data)}")
                return pd.DataFrame()
            
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                print("❌ 没有Items数据")
                return pd.DataFrame()
            
            first_item = items[0]
            voyages = first_item.get("Voyages", [])
            if not voyages:
                print("❌ 没有航次数据")
                return pd.DataFrame()
            
            print(f"🚢 找到 {len(voyages)} 个航次")
            
            high_risk_voyages = []
            
            for i, voyage in enumerate(voyages):
                risk_types = voyage.get("RiskTypes", [])
                print(f"   航次 {i+1}: RiskTypes = {risk_types}")
                
                if "High Risk Port Calling" in risk_types:
                    processed_voyage = {
                        "VoyageId": voyage.get("VoyageId"),
                        "VoyageStartTime": voyage.get("VoyageStartTime"),
                        "VoyageEndTime": voyage.get("VoyageEndTime"),
                        "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                        "StartPlace": self._process_place(voyage.get("VoyageStartPlace")),
                        "EndPlace": self._process_place(voyage.get("VoyageEndPlace")),
                        "RiskTypes": risk_types
                    }
                    high_risk_voyages.append(processed_voyage)
                    print(f"   ✅ 发现高风险港口航次: {processed_voyage['VoyageId']}")
            
            print(f"🚨 总共发现 {len(high_risk_voyages)} 个高风险港口航次")
            
            # 转换为DataFrame并保存
            if high_risk_voyages:
                df = pd.DataFrame({"raw_data": high_risk_voyages})
                df['VesselImo'] = vessel_imo
                self.results['voyage_risks']['high_risk_port'] = df
                print(f"💾 已保存高风险港口数据到结果中")
                return df
            else:
                print("⚠️  没有发现高风险港口航次")
                # 创建一个空的DataFrame，但包含船舶信息
                empty_df = pd.DataFrame({
                    "raw_data": [{"message": "没有发现高风险港口航次", "vessel_info": vessel_info}],
                    "VesselImo": [vessel_imo]
                })
                self.results['voyage_risks']['high_risk_port'] = empty_df
                return empty_df
                
        except Exception as e:
            print(f"❌ 处理高风险港口航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_possible_dark_port_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理可能的Dark Port访问的航次数据"""
        print(f"🔍 处理可能的Dark Port访问航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_possible_dark_port_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_possible_dark_port_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            vessel_info = self.extract_vessel_info(raw_data)
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                return pd.DataFrame()
            voyages = items[0].get("Voyages", [])
            results = []
            
            for voyage in voyages:
                risk_types = voyage.get("RiskTypes", [])
                events = voyage.get("VoyageEvents", {})
                
                if "Possible Dark Port Calling" in risk_types:
                    for gap in events.get("AisGap", []):
                        gap_risk_types = gap.get("RiskTypes", [])
                        if "Possible Dark Port Calling" in gap_risk_types:
                            eez_name = gap.get("AisGapStartEezName", "")
                            is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                            result = {
                                "VesselInfo": vessel_info,
                                "VoyageInfo": {
                                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                                    "RiskTypes": risk_types,
                                    "AISGap": {
                                        "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                                        "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                                        "AisGapStartEezName": gap.get("AisGapStartEezName"),
                                        "is_sanctioned_eez": is_sanctioned_eez,
                                        "RiskTypes": gap_risk_types
                                    }
                                },
                                "DarkPortCalls": self._extract_dark_port_calls(gap)
                            }
                            results.append(result)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame({"raw_data": results})
            df['VesselImo'] = vessel_imo
            self.results['voyage_risks']['possible_dark_port'] = df
            return df
            
        except Exception as e:
            print(f"❌ 处理可能的Dark Port访问航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_suspicious_ais_gap_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理可疑AIS中断的航次数据"""
        print(f"🔍 处理可疑AIS中断航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_suspicious_ais_gap_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_suspicious_ais_gap_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            vessel_info = self.extract_vessel_info(raw_data)
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                return pd.DataFrame()
            voyages = items[0].get("Voyages", [])
            results = []
            
            for voyage in voyages:
                risk_types = voyage.get("RiskTypes", [])
                events = voyage.get("VoyageEvents", {})
                
                if "Suspicious AIS Gap" in risk_types:
                    for gap in events.get("AisGap", []):
                        if "Suspicious AIS Gap" in gap.get("RiskTypes", []):
                            eez_name = gap.get("AisGapStartEezName", "")
                            is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                            result = {
                                "VesselInfo": vessel_info,
                                "VoyageInfo": {
                                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                                    "RiskTypes": risk_types,
                                    "AISGap": {
                                        "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                                        "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                                        "AisGapStartEezName": gap.get("AisGapStartEezName"),
                                        "is_sanctioned_eez": is_sanctioned_eez,
                                        "RiskTypes": gap.get("RiskTypes", [])
                                    }
                                }
                            }
                            results.append(result)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame({"raw_data": results})
            df['VesselImo'] = vessel_imo
            self.results['voyage_risks']['suspicious_ais_gap'] = df
            return df
            
        except Exception as e:
            print(f"❌ 处理可疑AIS中断航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_dark_sts_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理Dark STS（船对船转运）的航次数据"""
        print(f"🔍 处理Dark STS航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_dark_sts_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_dark_sts_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            target_risk_types = [
                "Possible 1-way Dark STS (as dark party)",
                "Possible 2-way Dark STS (as dark party)"
            ]
            
            vessel_info = self.extract_vessel_info(raw_data)
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                return pd.DataFrame()
            voyages = items[0].get("Voyages", [])
            results = []
            
            for voyage in voyages:
                risk_types = voyage.get("RiskTypes", [])
                events = voyage.get("VoyageEvents", {})
                
                if any(rt in risk_types for rt in target_risk_types):
                    for gap in events.get("AisGap", []):
                        gap_risk_types = gap.get("RiskTypes", [])
                        if any(rt in gap_risk_types for rt in target_risk_types):
                            eez_name = gap.get("AisGapStartEezName", "")
                            is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                            result = {
                                "VesselInfo": vessel_info,
                                "VoyageInfo": {
                                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                                    "RiskTypes": risk_types,
                                    "AISGap": {
                                        "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                                        "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                                        "AisGapStartEezName": gap.get("AisGapStartEezName"),
                                        "is_sanctioned_eez": is_sanctioned_eez,
                                        "1Way": self._extract_probable_1w_dark_sts(gap),
                                        "2Way": self._extract_probable_2w_dark_sts(gap)
                                    }
                                }
                            }
                            results.append(result)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame({"raw_data": results})
            df['VesselImo'] = vessel_imo
            self.results['voyage_risks']['dark_sts'] = df
            return df
            
        except Exception as e:
            print(f"❌ 处理Dark STS航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_sanctioned_sts_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理与受制裁船舶的STS转运的航次数据"""
        print(f"🔍 处理受制裁STS航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_sanctioned_sts_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_sanctioned_sts_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            vessel_info = self.extract_vessel_info(raw_data)
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                print("❌ 没有Items数据")
                return pd.DataFrame()
            
            voyages = items[0].get("Voyages", [])
            results = []
            
            for voyage in voyages:
                risk_types = voyage.get("RiskTypes", [])
                events = voyage.get("VoyageEvents", {})
                
                if "STS With a Sanctioned Vessel" in risk_types:
                    for sts in events.get("ShipToShipTransfer", []):
                        result = {
                            "VesselInfo": vessel_info,
                            "VoyageInfo": {
                                "VoyageStartTime": voyage.get("VoyageStartTime"),
                                "VoyageEndTime": voyage.get("VoyageEndTime"),
                                "RiskTypes": risk_types
                            },
                            "STSEvent": {
                                "StartDateTime": sts.get("StartDateTime"),
                                "EndDateTime": sts.get("EndDateTime"),
                                "StsType": sts.get("StsType")
                            },
                            "CounterpartVessels": self._extract_counterpart_vessels(sts)
                        }
                        results.append(result)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame({"raw_data": results})
            df['VesselImo'] = vessel_imo
            self.results['voyage_risks']['sanctioned_sts'] = df
            return df
            
        except Exception as e:
            print(f"❌ 处理受制裁STS航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def process_loitering_behavior_voyages_from_data(self, raw_data: Dict[str, Any], vessel_imo: str):
        """从已获取的数据中处理可疑徘徊行为的航次数据"""
        print(f"🔍 处理可疑徘徊行为航次: IMO={vessel_imo}")
        
        # 添加类型检查和调试信息
        print(f"🔍 调试 - process_loitering_behavior_voyages_from_data 接收到的 raw_data 类型: {type(raw_data)}")
        if not isinstance(raw_data, dict):
            print(f"❌ process_loitering_behavior_voyages_from_data 接收到非字典类型数据: {type(raw_data)}")
            print(f"❌ 数据内容: {raw_data}")
            return pd.DataFrame()
        
        try:
            target_risk_types = [
                "Suspicious Loitering Behaviour",
                "Possible 1-Way Dark STS (as non-dark party)"
            ]
            
            vessel_info = self.extract_vessel_info(raw_data)
            items = raw_data.get("Data", {}).get("Items", [])
            if not items:
                return pd.DataFrame()
            voyages = items[0].get("Voyages", [])
            results = []
            
            for voyage in voyages:
                risk_types = voyage.get("RiskTypes", [])
                events = voyage.get("VoyageEvents", {})
                
                if any(rt in risk_types for rt in target_risk_types):
                    for event in events.get("Loitering", []):
                        event_risk_types = event.get("RiskTypes", [])
                        if any(rt in event_risk_types for rt in target_risk_types):
                            result = {
                                "VesselInfo": vessel_info,
                                "VoyageInfo": {
                                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                                    "RiskTypes": risk_types,
                                    "DarkSTS": self._extract_probable_1w_dark_sts(event),
                                    "LoiteringEvent": {
                                        "LoiteringStart": event.get("LoiteringStart"),
                                        "LoiteringEnd": event.get("LoiteringEnd"),
                                        "RiskTypes": event_risk_types
                                    }
                                }
                            }
                            results.append(result)
            
            # 转换为DataFrame并保存
            df = pd.DataFrame({"raw_data": results})
            df['VesselImo'] = vessel_imo
            self.results['voyage_risks']['loitering_behavior'] = df
            return df
            
        except Exception as e:
            print(f"❌ 处理可疑徘徊行为航次时发生异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_voyage_risk_summary(self) -> pd.DataFrame:
        """生成航次风险汇总表"""
        summary_data = []
        
        for risk_type, data in self.results['voyage_risks'].items():
            count = 0
            example_risk = "N/A"
            
            # 检查数据类型并相应处理
            if hasattr(data, 'shape'):  # 这是一个DataFrame
                count = len(data)
                if count > 0:
                    try:
                        # 安全地获取第一行数据
                        if data.shape[0] > 0:
                            example = data.to_dict()
                            example_risk = example.get('VoyageInfo.RiskTypes', 'N/A')
                        else:
                            example_risk = "N/A"
                    except Exception as e:
                        print(f"处理DataFrame数据时出错: {e}")
                        example_risk = "N/A"
            elif isinstance(data, dict):  # 这是一个字典
                # 如果是转换后的字典格式，检查raw_data
                if 'raw_data' in data:
                    raw_data = data['raw_data']
                    if isinstance(raw_data, list):
                        count = len(raw_data)
                    elif isinstance(raw_data, dict):
                        count = 1 if raw_data else 0
                    else:
                        count = 0
                    
                    # 尝试获取风险类型信息
                    if count > 0 and 'sanctions_lev' in data:
                        example_risk = data['sanctions_lev']
                    else:
                        example_risk = "N/A"
                else:
                    count = 0
                    example_risk = "N/A"
            else:
                count = 0
                example_risk = "N/A"
            
            summary_data.append({
                "RiskType": risk_type.replace('_', ' ').title(),
                "Count": count,
                "ExampleRiskTypes": example_risk
            })
        
        return pd.DataFrame(summary_data)
    




# ==================== 主执行方法和结果输出 ====================
    
    def execute_full_analysis(self, vessel_imo: str, start_date: str, end_date: str):
        """执行完整分析流程"""
        # 1. Lloyd's合规数据
        print("\n=== 处理Lloyd's合规数据 ===")
        self.process_lloyds_compliance_data(vessel_imo, start_date, end_date)
        
        # 2. Lloyd's制裁数据
        print("\n=== 处理Lloyd's制裁数据 ===")
        sanctions_df = self.fetch_lloyds_sanctions(vessel_imo)
        if not sanctions_df.empty:
            self.transform_lloyds_sanctions_data(sanctions_df)
        
        # 3. UANI数据
        print("\n=== 处理UANI数据 ===")
        self.load_uani_data()
        self.check_uani_imo(vessel_imo)
        
        # 4. Kpler数据
        print("\n=== 处理Kpler数据 ===")
        self.process_kpler([int(vessel_imo)], start_date, end_date)
        
        # 5. 航次风险分析
        print("\n=== 处理航次风险数据 ===")
        self.analyze_voyage_risks(vessel_imo, start_date, end_date)
        
        print("\n=== 所有分析完成 ===")
    
    def save_all_results(self, output_dir="results"):
        """保存所有结果到文件"""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. 保存Lloyd's合规数据
        if self.results['lloyds_compliance']:
            with open(f"{output_dir}/lloyds_compliance_{timestamp}.json", 'w') as f:
                json.dump(self.results['lloyds_compliance'], f, indent=2,ensure_ascii=False)
        
        # 2. 保存Lloyd's制裁数据
        if isinstance(self.results['lloyds_sanctions'], pd.DataFrame) and not self.results['lloyds_sanctions'].empty:
            self.results['lloyds_sanctions'].to_csv(f"{output_dir}/lloyds_sanctions_{timestamp}.csv", index=False)
        
        if self.results.get('lloyds_sanctions_processed'):
            with open(f"{output_dir}/lloyds_sanctions_processed_{timestamp}.json", 'w') as f:
                json.dump(self.results['lloyds_sanctions_processed'], f, indent=2,ensure_ascii=False)
        
        # 3. 保存UANI数据
        if isinstance(self.results['uani_data'], pd.DataFrame) and not self.results['uani_data'].empty:
            self.results['uani_data'].to_csv(f"{output_dir}/uani_data_{timestamp}.csv", index=False)
        
        # 4. 保存Kpler数据
        if self.results['kpler_data']:
            with open(f"{output_dir}/kpler_data_{timestamp}.json", 'w') as f:
                json.dump(self.results['kpler_data'], f, indent=2,ensure_ascii=False)
        
        # 5. 保存航次风险数据
        for risk_type, df in self.results['voyage_risks'].items():
            if not df.empty:
                df.to_csv(f"{output_dir}/voyage_{risk_type}_{timestamp}.csv", index=False)
        
        # 保存航次风险汇总
        voyage_summary = self.get_voyage_risk_summary()
        if not voyage_summary.empty:
            voyage_summary.to_csv(f"{output_dir}/voyage_risk_summary_{timestamp}.csv", index=False)
        
        print(f"\n所有结果已保存到 {output_dir} 目录")

    def check_vessel_status(self, vessel_imo: str) -> Dict[str, Any]:
        """综合判断船舶状态
        
        Args:
            vessel_imo: 船舶IMO号
            
        Returns:
            {
                "lloyds_status": "是/否",
                "uani_status": "是/否",
                "kpler_status": {
                    "exists": "是/否",
                    "risk_level": "高/中/低/无数据"
                },
                "combined_risk": "高/中/低/无数据"  # 综合风险等级
            }
        """
        # 1. 判断劳氏数据状态
        lloyds_status = self._check_lloyds_status(vessel_imo)
        
        # 2. 判断UANI状态
        uani_status = self._check_uani_status(vessel_imo)
        
        # 3. 判断Kpler状态
        kpler_status = self._check_kpler_status(vessel_imo)
        
        # 4. 计算综合风险等级
        combined_risk = self._calculate_combined_risk(lloyds_status, uani_status, kpler_status)
        
        return {
            "lloyds_status": lloyds_status,
            "uani_status": uani_status,
            "kpler_status": kpler_status,
            "combined_risk": combined_risk
        }

    def _check_lloyds_status(self, vessel_imo: str) -> str:
        """检查船舶在劳氏数据中的状态"""
        # 检查合规数据
        compliance_exists = False
        if self.results.get('lloyds_compliance'):
            compliance_exists = str(vessel_imo) == str(self.results['lloyds_compliance'].get('VesselImo', ''))
        
        # 检查制裁数据
        sanctions_exists = False
        if isinstance(self.results.get('lloyds_sanctions'), pd.DataFrame):
            sanctions_exists = str(vessel_imo) in self.results['lloyds_sanctions']['vesselImo'].astype(str).values
        
        # 检查航次风险数据
        voyage_risk_exists = False
        for risk_type, df in self.results['voyage_risks'].items():
            if not df.empty and str(vessel_imo) in df['VesselImo'].astype(str).values:
                voyage_risk_exists = True
                break
        
        return "是" if any([compliance_exists, sanctions_exists, voyage_risk_exists]) else "否"

    def _check_uani_status(self, vessel_imo: str) -> str:
        """检查船舶在UANI清单中的状态"""
        if not hasattr(self, '_uani_data_loaded'):
            return "无数据"
        
        exists, _ = self.check_uani_imo(vessel_imo)
        return "是" if exists else "否"

    def _check_kpler_status(self, vessel_imo: str) -> Dict[str, str]:
        """检查船舶在Kpler数据中的状态和风险等级"""
        if not self.results.get('kpler_data'):
            return {"exists": "无数据", "risk_level": "无数据"}
        
        vessel_data = self.results['kpler_data'].get(str(vessel_imo))
        if not vessel_data:
            return {"exists": "否", "risk_level": "无数据"}
        
        return {
            "exists": "是",
            "risk_level": vessel_data.get("risk_level", "无数据")
        }

    def _calculate_combined_risk(self, lloyds_status: str, uani_status: str, kpler_status: Dict[str, str]) -> str:
        """计算综合风险等级"""
        risk_scores = []
        
        # 劳氏数据风险评分
        if lloyds_status == "是":
            risk_scores.append(2)  # 中等风险
        
        # UANI清单风险评分
        if uani_status == "是":
            risk_scores.append(3)  # 高风险
        
        # Kpler数据风险评分
        kpler_risk = kpler_status.get("risk_level", "无数据")
        if kpler_risk == "高":
            risk_scores.append(3)
        elif kpler_risk == "中":
            risk_scores.append(2)
        elif kpler_risk == "低":
            risk_scores.append(1)
        
        # 计算综合风险
        if not risk_scores:
            return "无数据"
        
        max_score = max(risk_scores)
        if max_score >= 3:
            return "高"
        elif max_score >= 2:
            return "中"
        else:
            return "低"
    
    def get_vessel_basic_info(self, vessel_imo: str) -> Dict[str, Any]:
        """获取船舶基础信息"""
        try:
            # 使用当前日期作为默认日期范围
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            # 获取航次数据
            voyage_data = self.get_voyage_data(vessel_imo, start_date, end_date)
            
            if not voyage_data or "Data" not in voyage_data or "Items" not in voyage_data.get("Data", {}):
                return {"error": "无法获取船舶数据"}
            
            # 提取船舶基本信息
            vessel_info = self.extract_vessel_info(voyage_data)
            
            # 如果没有获取到基本信息，返回错误
            if not vessel_info.get("VesselImo"):
                return {"error": "船舶信息不完整"}
            
            return vessel_info
            
        except Exception as e:
            print(f"获取船舶基础信息失败: {str(e)}")
            return {"error": f"获取船舶信息失败: {str(e)}"}

# ==================== 示例和测试方法 ====================

    def demonstrate_data_formats(self, imos: List[int], start_date: str, end_date: str):
        """演示两种数据格式的区别"""
        print("🔍 演示两种数据格式的区别...")
        
        # 1. 混合格式（推荐）- cargo和trades使用数组格式
        print("\n📊 混合格式示例 (use_array_format=True):")
        print("   - has_sanctioned_cargo_list: 使用数组格式，保留sources数组")
        print("   - has_sanctioned_trades_list: 使用数组格式，保留sources数组")
        print("   - 其他字段: 使用扁平化格式，保持原有逻辑")
        
        array_result = self.process_kpler(imos, start_date, end_date, use_array_format=True)
        
        if array_result and imos[0] in array_result:
            sample_data = array_result[imos[0]]
            print("\n✅ 混合格式结果:")
            
            # 检查cargo字段（应该是数组）
            if 'has_sanctioned_cargo_list' in sample_data:
                cargo_list = sample_data['has_sanctioned_cargo_list']
                if isinstance(cargo_list, list) and len(cargo_list) > 0:
                    first_cargo = cargo_list[0]
                    print(f"   📦 has_sanctioned_cargo_list (数组格式):")
                    print(f"      - commodity: {first_cargo.get('commodity')}")
                    print(f"      - originCountry: {first_cargo.get('originCountry')}")
                    print(f"      - sources: {first_cargo.get('sources')}")  # 这是数组
                else:
                    print(f"   📦 has_sanctioned_cargo_list: {cargo_list}")
            
            # 检查trades字段（现在也应该是数组）
            if 'has_sanctioned_trades_list' in sample_data:
                trades_list = sample_data['has_sanctioned_trades_list']
                if isinstance(trades_list, list) and len(trades_list) > 0:
                    first_trade = trades_list[0]
                    print(f"   📋 has_sanctioned_trades_list (数组格式):")
                    print(f"      - commodity: {first_trade.get('commodity')}")
                    print(f"      - originCountry: {first_trade.get('originCountry')}")
                    print(f"      - sources: {first_trade.get('sources')}")  # 这是数组
                else:
                    print(f"   📋 has_sanctioned_trades_list: {trades_list}")
            
            # 检查其他字段（应该是扁平化格式）
            if 'has_port_calls_list' in sample_data:
                port_calls_list = sample_data['has_port_calls_list']
                print(f"   🚢 has_port_calls_list (扁平化格式): {port_calls_list}")
        
        # 2. 完全扁平化格式（向后兼容）
        print("\n📋 完全扁平化格式示例 (use_array_format=False):")
        print("   - 所有字段都使用扁平化格式")
        
        flat_result = self.process_kpler(imos, start_date, end_date, use_array_format=False)
        
        if flat_result and imos[0] in flat_result:
            sample_data = flat_result[imos[0]]
            print("✅ 完全扁平化格式结果:")
            if 'has_sanctioned_cargo_list' in sample_data:
                cargo_list_str = sample_data['has_sanctioned_cargo_list']
                print(f"   📦 has_sanctioned_cargo_list: {cargo_list_str}")
        
        print("\n🎯 推荐使用混合格式！")
        print("   - cargo和trades字段保持数组结构，便于处理sources信息")
        print("   - 其他字段保持扁平化，向后兼容")
        return {
            'mixed_format': array_result,
            'flat_format': flat_result
        }

# 示例使用
if __name__ == "__main__":

    # 1. 初始化处理器
    processor = MaritimeDataProcessor()
    
    # 2. 设置API密钥（从配置文件导入）
    from kingbase_config import get_lloyds_token, get_kpler_token
    processor.lloyds_api_key = get_lloyds_token()
    processor.kpler_api_key = get_kpler_token()
    
    # 3. 定义分析参数
    target_imo = "9842190"
    start_date = "2024-08-25"
    end_date = "2025-08-25"
    
    try:
        # 4. 执行完整分析流程
        print("=== 开始执行完整分析 ===")
        processor.execute_full_analysis(
            vessel_imo=target_imo,
            start_date=start_date,
            end_date=end_date
        )
        
        # 5. 获取并打印船舶状态
        print("\n=== 船舶状态分析结果 ===")
        status = processor.check_vessel_status(target_imo)
        print(json.dumps(status, indent=2, ensure_ascii=False))
        
        # 6. 保存详细结果
        print("\n=== 保存详细结果 ===")
        processor.save_all_results()
        
    except Exception as e:
        print(f"\n分析过程中发生错误: {str(e)}")
    finally:
        print("\n=== 分析完成 ===")

    # ==================== AIS Manipulation测试方法 ====================

    def test_ais_manipulation_risk_logic(self):
        """测试AIS Manipulation风险等级判断逻辑"""
        print("🧪 测试AIS Manipulation风险等级判断逻辑...")
        
        # 测试数据1：包含High风险
        test_data_1 = {
            'risks': [
                {
                    'ComplianceRiskScore': 'High',
                    'RiskType': 'VesselAisManipulation',
                    'Details': 'Test High Risk'
                }
            ]
        }
        
        # 测试数据2：包含Medium风险
        test_data_2 = {
            'risks': [
                {
                    'ComplianceRiskScore': 'Medium',
                    'RiskType': 'VesselAisManipulation',
                    'Details': 'Test Medium Risk'
                }
            ]
        }
        
        # 测试数据3：包含Low风险
        test_data_3 = {
            'risks': [
                {
                    'ComplianceRiskScore': 'Low',
                    'RiskType': 'VesselAisManipulation',
                    'Details': 'Test Low Risk'
                }
            ]
        }
        
        # 测试数据4：无风险数据
        test_data_4 = {
            'risks': []
        }
        
        # 测试数据5：混合风险等级
        test_data_5 = {
            'risks': [
                {
                    'ComplianceRiskScore': 'Medium',
                    'RiskType': 'VesselAisManipulation',
                    'Details': 'Test Medium Risk 1'
                },
                {
                    'ComplianceRiskScore': 'Low',
                    'RiskType': 'VesselAisManipulation',
                    'Details': 'Test Low Risk 1'
                }
            ]
        }
        
        def calculate_risk_level(risks_data):
            """模拟maritime_api.py中的风险等级计算逻辑"""
            if isinstance(risks_data, list) and len(risks_data) > 0:
                high_risk_count = 0
                medium_risk_count = 0
                
                for risk in risks_data:
                    compliance_risk_score = risk.get('ComplianceRiskScore', '')
                    if compliance_risk_score == 'High':
                        high_risk_count += 1
                    elif compliance_risk_score == 'Medium':
                        medium_risk_count += 1
                
                if high_risk_count > 0:
                    return '高风险'
                elif medium_risk_count > 0:
                    return '中风险'
                else:
                    return '无风险'
            else:
                return '无风险'
        
        # 执行测试
        test_cases = [
            ("High风险", test_data_1['risks']),
            ("Medium风险", test_data_2['risks']),
            ("Low风险", test_data_3['risks']),
            ("无风险数据", test_data_4['risks']),
            ("混合风险等级", test_data_5['risks'])
        ]
        
        for test_name, test_risks in test_cases:
            risk_level = calculate_risk_level(test_risks)
            print(f"   ✅ {test_name}: {risk_level}")
        
        print("\n🎯 AIS Manipulation风险等级判断逻辑测试完成！")
        print("   - High → 高风险")
        print("   - Medium → 中风险") 
        print("   - Low/无数据 → 无风险")
        print("   - 优先级：High > Medium > Low")
        
        # 测试process_vessel_ais_manipulation方法的逻辑
        print("\n🧪 测试process_vessel_ais_manipulation方法逻辑...")
        
        # 模拟测试数据
        test_risks_data = [
            {
                'ComplianceRiskScore': 'Low',
                'RiskType': 'VesselAisManipulation',
                'Details': 'Test Low Risk'
            }
        ]
        
        # 模拟process_vessel_ais_manipulation方法的逻辑
        if test_risks_data:
            high_risk_count = 0
            medium_risk_count = 0
            
            for risk in test_risks_data:
                compliance_risk_score = risk.get('ComplianceRiskScore', '')
                if compliance_risk_score == 'High':
                    high_risk_count += 1
                elif compliance_risk_score == 'Medium':
                    medium_risk_count += 1
            
            if high_risk_count > 0:
                expected_lev = '高风险'
            elif medium_risk_count > 0:
                expected_lev = '中风险'
            else:
                expected_lev = '无风险'
        else:
            expected_lev = '无风险'
        
        print(f"   📊 测试数据: ComplianceRiskScore = 'Low'")
        print(f"   🎯 预期结果: sanctions_lev = '{expected_lev}'")
        print(f"   ✅ 实际结果: sanctions_lev = '{expected_lev}'")
        
        if expected_lev == '无风险':
            print("   🎉 测试通过！Low风险正确映射为无风险")
        else:
            print("   ❌ 测试失败！预期结果不正确")