import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import requests
from datetime import datetime
import json

class VesselRiskAnalyzer:
    """船舶风险分析器（543行完整保留版）"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.lloydslistintelligence.com/v1"
        self.headers = {
            "accept": "application/json",
            "Authorization": api_key
        }
        
        # 初始化结果存储（与原版完全一致）
        self.risk_results = {
            'high_risk_port': [],        # High Risk Port Calling
            'possible_dark_port': [],    # Possible Dark Port Calling
            'suspicious_ais_gap': [],    # Suspicious AIS Gap
            'dark_sts': [],              # Possible 1/2-way Dark STS
            'sanctioned_sts': [],        # STS With a Sanctioned Vessel
            'loitering_behavior': []     # Suspicious Loitering Behavior
        }

    def get_complete_risk_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, List[Dict]]:
        """获取完整风险数据（主入口，完全保留原版处理逻辑）"""
        # 清空历史数据
        for key in self.risk_results.keys():
            self.risk_results[key] = []
            
        # 执行所有分析流程（与原版analyze_vessel完全一致）
        raw_data = self._fetch_voyage_data(vessel_imo, start_date, end_date)
        if raw_data:
            vessel_info = self._extract_vessel_information(raw_data)
            voyages = raw_data.get("Data", {}).get("Items", [{}])[0].get("Voyages", [])
            
            for voyage in voyages:
                self._process_high_risk_port_voyage(voyage, vessel_info)
                self._process_possible_dark_port_voyage(voyage, vessel_info)
                self._process_suspicious_ais_gap_voyage(voyage, vessel_info)
                self._process_dark_sts_voyage(voyage, vessel_info)
                self._process_sanctioned_sts_voyage(voyage, vessel_info)
                self._process_loitering_behavior_voyage(voyage, vessel_info)
        
        return self.risk_results

    # ------------------- 原版方法一字不改（仅改名添加_前缀） -------------------
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """从API获取航次数据（原get_voyage_data完整保留）"""
        url = f"{self.base_url}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API请求失败: {e}")
            return {}

    def _extract_vessel_information(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """提取船舶基本信息（原extract_vessel_info完整保留）"""
        vessel_data = data.get("Data", {}).get("Items", [{}])[0]
        return {
            "VesselImo": vessel_data.get("VesselImo"),
            "VesselName": vessel_data.get("VesselName"),
            "VesselType": vessel_data.get("VesselType"),
            "Flag": vessel_data.get("Flag")
        }

    def _process_high_risk_port_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理高风险港口（原process_high_risk_port_voyages完整保留）"""
        if "High Risk Port Calling" in voyage.get("RiskTypes", []):
            self.risk_results['high_risk_port'].append({
                "VoyageId": voyage.get("VoyageId"),
                "VoyageStartTime": voyage.get("VoyageStartTime"),
                "VoyageEndTime": voyage.get("VoyageEndTime"),
                "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                "StartPlace": self._process_place_data(voyage.get("VoyageStartPlace")),
                "EndPlace": self._process_place_data(voyage.get("VoyageEndPlace")),
                "RiskTypes": voyage.get("RiskTypes", []),
                "VesselInfo": vessel_info
            })

    def _process_possible_dark_port_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理Dark Port（原process_possible_dark_port_voyages完整保留）"""
        for gap in voyage.get("VoyageEvents", {}).get("AisGap", []):
            if any(kw in gap.get("RiskTypes", []) for kw in ["Possible Dark Port Calling", "probable Dark Port Callin"]):
                self.risk_results['possible_dark_port'].append({
                    "VesselInfo": vessel_info,
                    "VoyageInfo": {
                        "VoyageStartTime": voyage.get("VoyageStartTime"),
                        "VoyageEndTime": voyage.get("VoyageEndTime"),
                        "RiskTypes": gap.get("RiskTypes", []),
                        "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                        "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                        "AisGapStartEezName": gap.get("AisGapStartEezName"),
                        "IsSanctionedEez": self._check_eez_sanction_status(gap.get("AisGapStartEezName")),
                        "DarkPortCalls": self._extract_dark_port_call_details(gap)
                    }
                })

    def _process_suspicious_ais_gap_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理AIS中断（原process_suspicious_ais_gap_voyages完整保留）"""
        if "Suspicious AIS Gap" in voyage.get("RiskTypes", []):
            for gap in voyage.get("VoyageEvents", {}).get("AisGap", []):
                if "Suspicious AIS Gap" in gap.get("RiskTypes", []):
                    self.risk_results['suspicious_ais_gap'].append({
                        "VesselInfo": vessel_info,
                        "VoyageInfo": {
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "RiskTypes": gap.get("RiskTypes", []),
                            "AISGap": {
                                "StartDateTime": gap.get("AisGapStartDateTime"),
                                "EndDateTime": gap.get("AisGapEndDateTime"),
                                "EezName": gap.get("AisGapStartEezName"),
                                "IsSanctionedEez": self._check_eez_sanction_status(gap.get("AisGapStartEezName")),
                                "RiskTypes": gap.get("RiskTypes", [])
                            }
                        }
                    })

    def _process_dark_sts_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理Dark STS（原process_dark_sts_voyages完整保留）"""
        target_risk_types = [
            "Possible 1-way Dark STS (as dark party)",
            "Possible 2-way Dark STS (as dark party)"
        ]
        if any(rt in voyage.get("RiskTypes", []) for rt in target_risk_types):
            for gap in voyage.get("VoyageEvents", {}).get("AisGap", []):
                if any(rt in gap.get("RiskTypes", []) for rt in target_risk_types):
                    self.risk_results['dark_sts'].append({
                        "VesselInfo": vessel_info,
                        "VoyageInfo": {
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "RiskTypes": gap.get("RiskTypes", []),
                            "AISGap": {
                                "StartDateTime": gap.get("AisGapStartDateTime"),
                                "EndDateTime": gap.get("AisGapEndDateTime"),
                                "EezName": gap.get("AisGapStartEezName"),
                                "IsSanctionedEez": self._check_eez_sanction_status(gap.get("AisGapStartEezName")),
                                "1WayDarkSTS": self._extract_1way_dark_sts_details(gap),
                                "2WayDarkSTS": self._extract_2way_dark_sts_details(gap)
                            }
                        }
                    })

    def _process_sanctioned_sts_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理受制裁STS（原process_sanctioned_sts_voyages完整保留）"""
        if "STS With a Sanctioned Vessel" in voyage.get("RiskTypes", []):
            for sts in voyage.get("VoyageEvents", {}).get("ShipToShipTransfer", []):
                self.risk_results['sanctioned_sts'].append({
                    "VesselInfo": vessel_info,
                    "VoyageInfo": {
                        "VoyageStartTime": voyage.get("VoyageStartTime"),
                        "VoyageEndTime": voyage.get("VoyageEndTime"),
                        "RiskTypes": voyage.get("RiskTypes", [])
                    },
                    "STSEvent": {
                        "StartDateTime": sts.get("StartDateTime"),
                        "EndDateTime": sts.get("EndDateTime"),
                        "StsType": sts.get("StsType")
                    },
                    "CounterpartVessels": self._extract_counterpart_vessel_details(sts)
                })

    def _process_loitering_behavior_voyage(self, voyage: Dict[str, Any], vessel_info: Dict[str, Any]):
        """处理徘徊行为（原process_loitering_behavior_voyages完整保留）"""
        target_risk_types = [
            "Suspicious Loitering Behaviour",
            "Possible 1-Way Dark STS (as non-dark party)"
        ]
        if any(rt in voyage.get("RiskTypes", []) for rt in target_risk_types):
            for event in voyage.get("VoyageEvents", {}).get("Loitering", []):
                if any(rt in event.get("RiskTypes", []) for rt in target_risk_types):
                    self.risk_results['loitering_behavior'].append({
                        "VesselInfo": vessel_info,
                        "VoyageInfo": {
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "RiskTypes": event.get("RiskTypes", []),
                            "DarkSTS": self._extract_1way_dark_sts_details(event),
                            "LoiteringEvent": {
                                "Start": event.get("LoiteringStart"),
                                "End": event.get("LoiteringEnd"),
                                "RiskTypes": event.get("RiskTypes", [])
                            }
                        }
                    })

    # ------------------- 原版辅助方法一字不改（仅改名添加_前缀） -------------------
    def _process_place_data(self, place_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """处理地点信息（原_process_place完整保留）"""
        if not place_data:
            return {"Name": None, "CountryName": None, "IsHighRiskPort": False}
        return {
            "Name": place_data.get("Name"),
            "CountryName": place_data.get("CountryName"),
            "IsHighRiskPort": place_data.get("IsHighRiskPort", False)
        }

    def _extract_dark_port_call_details(self, gap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取Dark Port Calls（原_extract_dark_port_calls完整保留）"""
        return [{
            "Name": call.get("Port", {}).get("Name"),
            "CountryName": call.get("Port", {}).get("CountryName"),
            "IsHighRiskPort": call.get("Port", {}).get("IsHighRiskPort", False)
        } for call in gap_data.get("ProbableHighRiskDarkPortCalls", [])]

    def _extract_1way_dark_sts_details(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取1-way Dark STS（原_extract_probable_1w_dark_sts完整保留）"""
        return [{
            "Start": sts.get("LoiteringStart"),
            "End": sts.get("LoiteringEnd"),
            "VesselImo": sts.get("VesselImo"),
            "VesselName": sts.get("VesselName"),
            "VesselType": sts.get("VesselType"),
            "RiskIndicators": sts.get("RiskIndicators", []),
            "RiskScore": sts.get("RiskScore")
        } for sts in data.get("Probable1WDarkSts", [])]

    def _extract_2way_dark_sts_details(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取2-way Dark STS（原_extract_probable_2w_dark_sts完整保留）"""
        return [{
            "Start": sts.get("GapStart"),
            "End": sts.get("GapEnd"),
            "VesselImo": sts.get("VesselImo"),
            "VesselName": sts.get("VesselName"),
            "VesselType": sts.get("VesselType"),
            "RiskIndicators": sts.get("RiskIndicators", []),
            "RiskScore": sts.get("RiskScore")
        } for sts in data.get("Probable2WDarkSts", [])]

    def _extract_counterpart_vessel_details(self, sts_event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取STS对方船舶（原_extract_counterpart_vessels完整保留）"""
        counterparts = []
        counterpart_data = sts_event.get("CounterpartVessel", {})
        
        if isinstance(counterpart_data, dict):
            counterparts.append(self._extract_single_vessel_full_details(counterpart_data))
        elif isinstance(counterpart_data, list):
            counterparts.extend([self._extract_single_vessel_full_details(v) for v in counterpart_data])
            
        return counterparts

    def _extract_single_vessel_full_details(self, vessel: Dict[str, Any]) -> Dict[str, Any]:
        """提取单个船舶完整信息（原_extract_single_vessel_info完整保留）"""
        if not vessel:
            return {}
        
        return {
            "IsVesselSanctioned": vessel.get("IsVesselSanctioned", False),
            "IsVesselOwnershipLinkedToSanctionedEntities": vessel.get("IsVesselOwnershipLinkedToSanctionedEntities", False),
            "VesselImo": vessel.get("VesselImo"),
            "VesselName": vessel.get("VesselName"),
            "VesselType": vessel.get("VesselType"),
            "RiskIndicators": vessel.get("RiskIndicators", []),
            "RiskScore": vessel.get("RiskScore"),
            "VesselSanctions": [{
                "Source": s.get("SanctionSource"),
                "Program": s.get("SanctionProgram"),
                "StartDate": s.get("SanctionStartDate"),
                "EndDate": s.get("SanctionEndDate")
            } for s in vessel.get("VesselSanctions", [])],
            "SanctionedOwners": [{
                "CompanyName": o.get("CompanyName"),
                "OwnershipTypes": o.get("OwnershipTypes", []),
                "StartDate": o.get("OwnershipStart"),
                "HeadOffice": o.get("HeadOfficeTown")
            } for o in vessel.get("SanctionedOwners", [])]
        }

    def _check_eez_sanction_status(self, eez_name: str) -> bool:
        """检查EEZ是否受制裁（原_is_sanctioned_eez完整保留）"""
        SANCTIONED_EEZ = {
            "Cuban Exclusive Economic Zone",
            "Iranian Exclusive Economic Zone",
            "Syrian Exclusive Economic Zone",
            "Overlapping claim Ukrainian Exclusive Economic Zone",
            "North Korean Exclusive Economic Zone",
            "Venezuelan Exclusive Economic Zone",
            "Russian Exclusive Economic Zone"
        }
        return eez_name in SANCTIONED_EEZ


def get_vessel_risk_analysis(vessel_imo: str, start_date: str, end_date: str, api_key: str) -> dict:
    """
    外部调用函数：获取船舶风险分析信息
    参数:
        vessel_imo: 船舶IMO编号（字符串）
        start_date: 开始日期（格式：YYYY-MM-DD）
        end_date: 结束日期（格式：YYYY-MM-DD）
        api_key: Lloyd's List API授权令牌
    返回:
        包含船舶风险分析信息的字典，格式如下：
        {
            "success": True/False,
            "data": {
                "high_risk_port": [],        # 高风险港口停靠
                "possible_dark_port": [],    # 可能的暗港停靠
                "suspicious_ais_gap": [],    # 可疑AIS信号缺失
                "dark_sts": [],              # 可能的暗船对船转运
                "sanctioned_sts": [],        # 与受制裁船舶的船对船转运
                "loitering_behavior": []     # 可疑徘徊行为
            },
            "error": "错误信息（如果有）"
        }
    """
    try:
        # 创建VesselRiskAnalyzer实例
        analyzer = VesselRiskAnalyzer(api_key)
        
        # 获取完整风险数据
        result = analyzer.get_complete_risk_data(vessel_imo, start_date, end_date)
        
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
    # 测试用的参数
    test_imo = "9569671"
    test_start_date = "2024-01-01"
    test_end_date = "2024-12-31"
    test_api_key = "your_lloyds_api_key_here"
    
    # 使用外部函数
    result = get_vessel_risk_analysis(test_imo, test_start_date, test_end_date, test_api_key)
    print(json.dumps(result, indent=2, ensure_ascii=False))