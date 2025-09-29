import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional
import json

class KplerDataProcessor:
    """Kpler船舶数据处理器（完整未删减版）"""
    
    def __init__(self, api_token: str):
        """
        初始化处理器
        :param api_token: Kpler API令牌 (格式为 "Basic xxxx")
        """
        self.API_TOKEN = api_token
        self.API_URL = "https://api.kpler.com/v2/compliance/vessel-risks-v2"
        
        # 风险映射配置（完整保留）
        self.risk_mapping = {
            'has_sanctioned_cargo': {'true': '高风险', 'false': '无风险'},
            'has_sanctioned_trades': {'true': '高风险', 'false': '无风险'},
            'has_sanctioned_flag': {'true': '高风险', 'false': '无风险'},
            'has_port_calls': {'true': '高风险', 'false': '无风险'},
            'has_sts_events': {'true': '中风险', 'false': '无风险'},
            'has_ais_gap': {'true': '中风险', 'false': '无风险'},
            'has_ais_spoofs': {'true': '中风险', 'false': '无风险'},
            'has_dark_sts': {'true': '中风险', 'false': '无风险'},
            'has_sanctioned_companies': {'true': '高风险', 'false': '无风险'}
        }

    def get_kpler_vessel_risk_report(self, imos: List[int]) -> Dict[str, Dict[str, Any]]:
        """
        获取船舶风险报告（主入口函数）
        :param imos: IMO编号列表
        :return: {imo: 完整风险数据}
        """
        # 1. 获取原始数据
        raw_data = self._fetch_kpler_data(imos)
        if not raw_data:
            return {}

        # 2. 处理原始数据
        vessels = self._process_kpler_raw_data(raw_data)
        
        # 3. 创建摘要
        summary = self._create_kpler_summary(vessels)
        
        # 4. 应用风险映射
        risk_assessment = self._apply_kpler_risk_mapping(summary)
        
        # 5. 生成最终报告
        return self._create_kpler_final_report(vessels, risk_assessment)

    def _fetch_kpler_data(self, imos: List[int]) -> List[Dict[str, Any]]:
        """获取Kpler原始数据"""
        headers = {
            "Authorization": self.API_TOKEN,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        end_date = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        start_date = end_date - relativedelta(years=1)
        
        try:
            response = requests.post(
                self.API_URL,
                params={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "accept": "application/json"
                },
                headers=headers,
                json=imos,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[Kpler API Error] {str(e)}")
            return []

    def _process_kpler_raw_data(self, raw_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """处理原始数据（完整未删减）"""
        vessels = {}
        
        for record in raw_data:
            vessel = record.get('vessel', {})
            imo = vessel.get('imo')
            if imo is None:
                continue
                
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
                        'hsCode': str(cargo.get('hsCode') or ''),
                        'hsLink': cargo.get('hsLink'),
                        'sources': [
                            {
                                'name': src.get('name'),
                                'startDate': src.get('startDate'),
                                'endDate': src.get('endDate')
                            }
                            for src in (cargo.get('sources') or [{}])
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
                                'url': src.get('url'),
                                'startDate': src.get('startDate'),
                                'endDate': src.get('endDate')
                            }
                            for src in (trade.get('sources') or [{}])
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
                            'url': company.get('source', {}).get('url'),
                            'startDate': company.get('source', {}).get('startDate'),
                            'endDate': company.get('source', {}).get('endDate')
                        }
                    }
                    for company in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedCompanies')) or [])
                ],
                'sanctioned_flag': [
                    {
                        'flagCode': flag.get('flagCode'),
                        'vesselFlagStartDate': flag.get('vesselFlagStartDate'),
                        'vesselFlagEndDate': flag.get('vesselFlagEndDate'),
                        'source': {
                            'name': flag.get('source', {}).get('name'),
                            'url': flag.get('source', {}).get('url'),
                            'startDate': flag.get('source', {}).get('startDate'),
                            'endDate': flag.get('source', {}).get('endDate')
                        }
                    }
                    for flag in ((record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedFlag')) or [])
                ],
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
                        'volume': str(sts.get('volume') or ''),
                        'endDate': sts.get('endDate'),
                        'portName': sts.get('portName'),
                        'zoneName': sts.get('zoneName'),
                        'startDate': sts.get('startDate'),
                        'shipToShip': str(sts.get('shipToShip') or ''),
                        'countryName': sts.get('countryName'),
                        'sanctionedCargo': str(sts.get('sanctionedCargo') or ''),
                        'sanctionedVessel': str(sts.get('sanctionedVessel') or ''),
                        'sanctionedOwnership': str(sts.get('sanctionedOwnership') or ''),
                        'stsVessel': {
                            'imo': str((sts.get('stsVessel') or {}).get('imo') or ''),
                            'name': (sts.get('stsVector', {}).get('name') or ''),
                            'sanctionedVessel': str((sts.get('stsVessel', {}).get('sanctionedVessel') or '')),
                            'sanctionedOwnership': str((sts.get('stsVessel', {}).get('sanctionedOwnership') or ''))
                        }
                    }
                    for sts in ((record.get('compliance', {}).get('operationalRisks', {}).get('stsEvents')) or [])
                ],
                'ais_gaps': [
                    {
                        'zone': {
                            'start': {
                                'id': str((gap.get('zone', {}).get('start', {}).get('id') or '')),
                                'name': (gap.get('zone', {}).get('start', {}).get('name') or '')
                            },
                            'end': {
                                'id': str((gap.get('zone', {}).get('end', {}).get('id') or '')),
                                'name': (gap.get('zone', {}).get('end', {}).get('name') or '')
                            }
                        },
                        'position': {
                            'start': {
                                'lon': str((gap.get('position', {}).get('start', {}).get('lon') or '')),
                                'lat': str((gap.get('position', {}).get('start', {}).get('lat') or ''))
                            },
                            'end': {
                                'lon': str((gap.get('position', {}).get('end', {}).get('lon') or '')),
                                'lat': str((gap.get('position', {}).get('end', {}).get('lat') or ''))
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
                                'id': str((spoof.get('zone', {}).get('start', {}).get('id') or '')),
                                'name': (spoof.get('zone', {}).get('start', {}).get('name') or '')
                            },
                            'end': {
                                'id': str((spoof.get('zone', {}).get('end', {}).get('id') or '')),
                                'name': (spoof.get('zone', {}).get('end', {}).get('name') or '')
                            }
                        },
                        'position': {
                            'start': {
                                'lon': str((spoof.get('position', {}).get('start', {}).get('lon') or '')),
                                'lat': str((spoof.get('position', {}).get('start', {}).get('lat') or ''))
                            },
                            'end': {
                                'lon': str((spoof.get('position', {}).get('end', {}).get('lon') or '')),
                                'lat': str((spoof.get('position', {}).get('end', {}).get('lat') or ''))
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
                            'imo': str((event.get('stsVessel', {}).get('imo') or '')),
                            'name': (event.get('stsVessel', {}).get('name') or '')
                        },
                        'zone': {
                            'id': str((event.get('zone', {}).get('id') or '')),
                            'name': (event.get('zone', {}).get('name') or '')
                        }
                    }
                    for event in ((record.get('compliance', {}).get('operationalRisks', {}).get('darkStsEvents')) or [])
                ]
            }
        
        return vessels

    def _create_kpler_summary(self, vessels: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """创建风险摘要（完整未删减）"""
        summary = {}
        
        for imo, vessel_data in vessels.items():
            summary[imo] = {
                'has_sanctioned_cargo': 'true' if vessel_data['sanctioned_cargo'] else 'false',
                'has_sanctioned_trades': 'true' if vessel_data['sanctioned_trades'] else 'false',
                'has_sanctioned_flag': 'true' if vessel_data['sanctioned_flag'] else 'false',
                'has_port_calls': 'true' if vessel_data['port_calls'] else 'false',
                'has_sts_events': 'true' if vessel_data['sts_events'] else 'false',
                'has_ais_gap': 'true' if vessel_data['ais_gaps'] else 'false',
                'has_ais_spoofs': 'true' if vessel_data['ais_spoofs'] else 'false',
                'has_dark_sts': 'true' if vessel_data['dark_sts_events'] else 'false',
                'has_sanctioned_companies': 'true' if vessel_data['sanctioned_companies'] else 'false'
            }
        
        return summary

    def _apply_kpler_risk_mapping(self, summary: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """应用风险映射（完整未删减）"""
        risk_assessment = {}
        
        for imo, vessel_summary in summary.items():
            risk_assessment[imo] = {}
            
            for field, value in vessel_summary.items():
                risk_assessment[imo][f"{field}_risk"] = self.risk_mapping[field][value]
            
            high_risk_fields = [
                risk_assessment[imo]['has_sanctioned_cargo_risk'] == '高风险',
                risk_assessment[imo]['has_sanctioned_trades_risk'] == '高风险',
                risk_assessment[imo]['has_sanctioned_flag_risk'] == '高风险',
                risk_assessment[imo]['has_port_calls_risk'] == '高风险',
                risk_assessment[imo]['has_sts_events_risk'] == '高风险',
                risk_assessment[imo]['has_ais_gap_risk'] == '高风险',
                risk_assessment[imo]['has_ais_spoofs_risk'] == '高风险',
                risk_assessment[imo]['has_dark_sts_risk'] == '高风险',
                risk_assessment[imo]['has_sanctioned_companies_risk'] == '高风险'
            ]
            
            medium_risk_fields = [
                risk_assessment[imo]['has_sanctioned_cargo_risk'] == '中风险',
                risk_assessment[imo]['has_sanctioned_trades_risk'] == '中风险',
                risk_assessment[imo]['has_sanctioned_flag_risk'] == '中风险',
                risk_assessment[imo]['has_port_calls_risk'] == '中风险',
                risk_assessment[imo]['has_sts_events_risk'] == '中风险',
                risk_assessment[imo]['has_ais_gap_risk'] == '中风险',
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

    def _create_kpler_final_report(self, vessels: Dict[str, Dict[str, Any]], 
                                 risk_assessment: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """生成最终报告（完整未删减）"""
        final_report = {}
        
        for imo in vessels.keys():
            vessel_data = vessels[imo]
            assessment = risk_assessment.get(imo, {})
            
            final_report[imo] = {
                **vessel_data['vessel_info'],
                **assessment,
                'has_sanctioned_cargo_list': self._format_kpler_detail_list(
                    vessel_data['sanctioned_cargo'],
                    ['commodity', 'originZone', 'originCountry', 'sources.name', 'sources.startDate', 'sources.endDate']
                ),
                'has_sanctioned_trades_list': self._format_kpler_detail_list(
                    vessel_data['sanctioned_trades'],
                    ['commodity', 'originZone', 'originCountry', 'destinationZone', 
                     'destinationCountry', 'sources.name', 'sources.startDate', 'sources.endDate']
                ),
                'has_sanctioned_flag_list': self._format_kpler_detail_list(
                    vessel_data['sanctioned_flag'],
                    ['source.name', 'source.startDate', 'source.endDate', 'flagCode']
                ),
                'has_port_calls_list': self._format_kpler_detail_list(
                    vessel_data['port_calls'],
                    ['volume', 'endDate', 'portName', 'zonename', 'startDate', 'shipToShip']
                ),
                'has_sts_events_list': self._format_kpler_detail_list(
                    vessel_data['sts_events'],
                    ['volume', 'endDate', 'portName', 'zonename', 'startDate', 'shipToShip', 'stsVessel.imo']
                ),
                'has_ais_gap_list': self._format_kpler_detail_list(
                    vessel_data['ais_gaps'],
                    ['zone.start.id', 'zone.start.name', 'zone.end.id', 'zone.end.name',
                     'position.start.lon', 'position.start.lat', 'position.end.lon', 'position.end.lat']
                ),
                'has_ais_spoofs_list': self._format_kpler_detail_list(
                    vessel_data['ais_spoofs'],
                    ['startDate', 'endDate', 'zone.start.id', 'zone.start.name',
                     'position.start.lon', 'position.start.lat', 'durationMin']
                ),
                'has_dark_sts_list': self._format_kpler_detail_list(
                    vessel_data['dark_sts_events'],
                    ['date', 'stsVessel.imo', 'stsVessel.name', 'zone.id', 'zone.name']
                ),
                'has_sanctioned_companies_list': self._format_kpler_detail_list(
                    vessel_data['sanctioned_companies'],
                    ['name', 'source.name', 'source.startDate', 'source.endDate', 'type']
                ),
                # 结构化数据用于前端表格展示
                'vessel_companies_table': self._format_vessel_companies_table(vessel_data['vessel_companies']),
                'sanctioned_companies_table': self._format_sanctioned_companies_table(vessel_data['sanctioned_companies'])
            }
        
        return final_report

    def _format_vessel_companies_table(self, vessel_companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """格式化船舶公司表格数据"""
        table_data = []
        
        def format_timestamp(timestamp):
            if timestamp and str(timestamp).isdigit():
                try:
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

    def _format_sanctioned_companies_table(self, sanctioned_companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """格式化制裁公司表格数据"""
        table_data = []
        
        def format_timestamp(timestamp):
            if timestamp and str(timestamp).isdigit():
                try:
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

    def _format_kpler_detail_list(self, items: List[Dict[str, Any]], fields: List[str]) -> str:
        """格式化详情列表（完整未删减）"""
        formatted_items = []
        
        def format_timestamp(timestamp):
            if timestamp and str(timestamp).isdigit():
                try:
                    return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    return str(timestamp)
            return str(timestamp)
        
        for item in items:
            base_parts = []
            source_records = []
            
            for field in fields:
                if field in ['sources.name', 'sources.startDate', 'sources.endDate']:
                    continue
                    
                if '.' in field:
                    keys = field.split('.')
                    value = item
                    for key in keys:
                        value = value.get(key, {}) if isinstance(value, dict) else ''
                    base_parts.append(f"{keys[-1]}: {value}")
                else:
                    base_parts.append(f"{field}: {item.get(field, '')}")
            
            sources = item.get('sources', [])
            if sources:
                for i, source in enumerate(sources, 1):
                    source_parts = [f"source_{i}"]
                    if 'sources.name' in fields:
                        source_parts.append(f"name: {source.get('name', '')}")
                    if 'sources.startDate' in fields:
                        source_parts.append(f"start_date: {format_timestamp(source.get('startDate'))}")
                    if 'sources.endDate' in fields:
                        source_parts.append(f"end_date: {format_timestamp(source.get('endDate'))}")
                    source_records.append("; ".join(source_parts))
            
            if source_records:
                formatted_item = ", ".join(base_parts) + " || " + " || ".join(source_records)
            else:
                formatted_item = ", ".join(base_parts)
                
            formatted_items.append(formatted_item)
        
        return " | ".join(formatted_items)


def get_kpler_vessel_risk_info(imos: List[int], api_token: str) -> dict:
    """
    外部调用函数：获取Kpler船舶风险信息
    参数:
        imos: 船舶IMO编号列表
        api_token: Kpler API令牌 (格式为 "Basic xxxx")
    返回:
        包含船舶风险信息的字典，格式如下：
        {
            "success": True/False,
            "data": {
                "imo": {
                    "imo": "IMO号",
                    "mmsi": "MMSI",
                    "callsign": "呼号",
                    "shipname": "船舶名称",
                    "flag": "船旗",
                    "countryCode": "国家代码",
                    "typeName": "船舶类型",
                    "typeSummary": "类型摘要",
                    "gt": "总吨位",
                    "yob": "建造年份",
                    "ship_status": "船舶状态",  # "需拦截"、"需关注"、"正常"
                    "risk_level": "风险等级",  # "高"、"中"、"低"
                    "has_sanctioned_cargo_list": "制裁货物列表",
                    "has_sanctioned_trades_list": "制裁贸易列表",
                    "has_sanctioned_flag_list": "制裁船旗列表",
                    "has_port_calls_list": "港口停靠列表",
                    "has_sts_events_list": "船对船事件列表",
                    "has_ais_gap_list": "AIS信号缺失列表",
                    "has_ais_spoofs_list": "AIS信号篡改列表",
                    "has_dark_sts_list": "暗船对船事件列表",
                    "has_sanctioned_companies_list": "制裁公司列表"
                }
            },
            "error": "错误信息（如果有）"
        }
    """
    try:
        # 创建KplerDataProcessor实例
        processor = KplerDataProcessor(api_token)
        
        # 获取船舶风险报告
        result = processor.get_kpler_vessel_risk_report(imos)
        
        if not result:
            return {
                "success": False,
                "data": {},
                "error": "未获取到有效数据"
            }
        
        return {
            "success": True,
            "data": result,
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": {},
            "error": f"处理异常: {str(e)}"
        }


# 示例调用
if __name__ == "__main__":
    # 测试用的IMO列表
    test_imos = [9569671, 9842190]
    # 测试用的API令牌（需要替换为实际的令牌）
    # 导入配置
    from kingbase_config import get_kpler_token
    test_token = get_kpler_token()
    
    # 使用外部函数
    result = get_kpler_vessel_risk_info(test_imos, test_token)
    print(json.dumps(result, indent=2, ensure_ascii=False))