"""
风险检查框架 - 模块化设计
支持配置驱动的风险检查项管理
"""

import pandas as pd
import requests
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== 统一缓存与默认日期工具 ====================

class ApiDataCache:
    """简单的内存缓存，用于避免重复外部API调用"""
    def __init__(self):
        self._store: Dict[Any, Any] = {}

    def get(self, key: Any) -> Any:
        return self._store.get(key)

    def set(self, key: Any, value: Any) -> None:
        self._store[key] = value

def get_default_date_range() -> Tuple[str, str]:
    """返回默认日期范围：近一年，格式 YYYY-MM-DD"""
    end_dt = datetime.now().date()
    start_dt = end_dt - timedelta(days=365)
    return start_dt.isoformat(), end_dt.isoformat()

# 全局缓存实例
CACHE = ApiDataCache()

def _normalize_params(params: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, Any], ...]:
    if not params:
        return tuple()
    # 将列表、字典等可变对象转换为不可变表示
    def _freeze(v: Any) -> Any:
        if isinstance(v, dict):
            return tuple(sorted((k, _freeze(val)) for k, val in v.items()))
        if isinstance(v, list):
            return tuple(_freeze(i) for i in v)
        return v
    return tuple(sorted((k, _freeze(v)) for k, v in params.items()))

def cached_request(method: str, url: str, headers: Optional[Dict[str, str]] = None,
                   params: Optional[Dict[str, Any]] = None,
                   json_body: Optional[Any] = None,
                   timeout: int = 120) -> Any:
    """带缓存的HTTP请求，按(method,url,params,json)作为key缓存响应JSON"""
    key = (method.upper(), url, _normalize_params(params), json.dumps(json_body, sort_keys=True, ensure_ascii=False) if json_body is not None else None)
    cached = CACHE.get(key)
    if cached is not None:
        return cached
    resp = requests.request(method=method.upper(), url=url, headers=headers, params=params, json=json_body, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    CACHE.set(key, data)
    return data

class RiskLevel(Enum):
    """风险等级枚举"""
    HIGH = "高风险"
    MEDIUM = "中风险"
    NO_RISK = "无风险"

class CheckItemType(Enum):
    """检查项类型枚举"""
    LLOYDS = "lloyds"
    KPLER = "kpler"
    UANI = "uani"
    VOYAGE = "voyage"

@dataclass
class CheckItemConfig:
    """检查项配置"""
    id: str
    business_module: str
    compliance_check_module: str
    compliance_check_type: str
    entity_cn: str
    entity_en: str
    entity_type: str
    risk_desc: str
    risk_type: str
    used_flag: str
    time_flag: str
    time_period: str
    area_flag: str
    area: str
    risk_flag: str
    risk_flag_type: str

@dataclass
class CheckResult:
    """检查结果 - 固定格式输出"""
    risk_type: str  # 检查项的英文名
    risk_desc: str  # 检查项的中文名
    risk_value: str  # 检查项的风险判定等级
    info: str  # 从sanctions_des_info表匹配的info字段信息
    risk_desc_info: str  # 从sanctions_des_info表匹配的risk_desc_info字段信息
    tab: List[Dict[str, Any]]  # 该检查项的详情数据
    vessel_imo: Optional[Dict[str, str]] = None  # 船舶IMO信息（可选）
    # 道琼斯制裁风险检查的额外字段
    risk_screening_time: Optional[str] = None  # 风险筛查时间
    risk_status_change_content: Optional[str] = None  # 风险状态变更内容
    risk_status_change_time: Optional[str] = None  # 风险状态变更时间
    risk_status_reason: Optional[Dict[str, Any]] = None  # 风险状态原因
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "risk_type": self.risk_type,
            "risk_desc": self.risk_desc,
            "risk_value": self.risk_value,
            "info": self.info,
            "risk_desc_info": self.risk_desc_info,
            "tab": self.tab
        }
        
        if self.vessel_imo:
            result["vessel_imo"] = self.vessel_imo
        
        # 添加道琼斯制裁风险检查的额外字段
        if self.risk_screening_time is not None:
            result["risk_screening_time"] = self.risk_screening_time
        if self.risk_status_change_content is not None:
            result["risk_status_change_content"] = self.risk_status_change_content
        if self.risk_status_change_time is not None:
            result["risk_status_change_time"] = self.risk_status_change_time
        if self.risk_status_reason is not None:
            result["risk_status_reason"] = self.risk_status_reason
        
        return result

class BaseCheckItem(ABC):
    """检查项基类 - 固定格式输出"""
    
    def __init__(self, config: CheckItemConfig, api_config: Dict[str, Any], info_manager=None):
        self.config = config
        self.api_config = api_config
        self.info_manager = info_manager
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
    
    @abstractmethod
    def check(self, entity_id: str, **kwargs) -> CheckResult:
        """执行检查 - 返回固定格式"""
        pass
    
    @abstractmethod
    def get_required_params(self) -> List[str]:
        """获取必需参数"""
        pass
    
    def validate_params(self, **kwargs) -> bool:
        """验证参数"""
        required_params = self.get_required_params()
        for param in required_params:
            if param not in kwargs or kwargs[param] is None:
                self.logger.error(f"缺少必需参数: {param}")
                return False
        return True
    
    def calculate_risk_level(self, data: Any) -> RiskLevel:
        """计算风险等级 - 子类可重写"""
        if not data:
            return RiskLevel.NO_RISK
        
        # 默认逻辑：根据数据存在性判断
        if isinstance(data, (list, dict)) and len(data) > 0:
            return RiskLevel.MEDIUM
        return RiskLevel.NO_RISK
    
    def get_risk_info(self, risk_level: RiskLevel) -> Dict[str, str]:
        """获取风险描述信息"""
        if self.info_manager:
            return self.info_manager.get_info_by_risk_type(self.config.id, risk_level.value)
        else:
            return {
                "info": f"风险判定为: {risk_level.value}",
                "risk_desc_info": f"风险描述: {self.config.risk_desc}"
            }
    
    def create_check_result(self, risk_level: RiskLevel, tab_data: List[Dict[str, Any]], 
                          vessel_imo: Optional[Dict[str, str]] = None) -> CheckResult:
        """创建固定格式的检查结果"""
        risk_info = self.get_risk_info(risk_level)
        
        return CheckResult(
            risk_type=self.config.id,
            risk_desc=self.config.risk_desc,
            risk_value=risk_level.value,
            info=risk_info["info"],
            risk_desc_info=risk_info["risk_desc_info"],
            tab=tab_data,
            vessel_imo=vessel_imo
        )

# ==================== Lloyd's 检查项实现 ====================

class LloydsComplianceCheckItem(BaseCheckItem):
    """劳氏合规检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用劳氏API
            data = self._fetch_lloyds_compliance_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_compliance_risk(data)
            
            # 构建tab数据
            tab_data = self._build_compliance_tab_data(data, vessel_imo)
            
            # 构建vessel_imo数据
            vessel_imo_data = {"0": vessel_imo}
            
            return self.create_check_result(risk_level, tab_data, vessel_imo_data)
            
        except Exception as e:
            self.logger.error(f"劳氏合规检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_lloyds_compliance_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取劳氏合规数据（同时调用两个接口）"""
        # 调用vesselcompliancescreening_v3接口
        compliance_url = f"{self.api_config['lloyds_base_url']}/vesselcompliancescreening_v3"
        compliance_params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        compliance_data = cached_request("GET", compliance_url, headers=self.api_config['lloyds_headers'], params=compliance_params, timeout=30)
        
        # 调用vesselriskscore接口
        risk_url = f"{self.api_config['lloyds_base_url']}/vesselriskscore"
        risk_params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        risk_data = cached_request("GET", risk_url, headers=self.api_config['lloyds_headers'], params=risk_params, timeout=30)
        
        # 合并两个接口的数据，模拟functions.py中的逻辑
        return self._merge_compliance_and_risk_data(compliance_data, risk_data, vessel_imo, start_date, end_date)
    
    def _merge_compliance_and_risk_data(self, compliance_data: Dict[str, Any], risk_data: Dict[str, Any], 
                                      vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """合并合规数据和风险数据，模拟functions.py中的process_lloyds_compliance_data逻辑"""
        if not compliance_data or not risk_data:
            return {"IsSuccess": False}
        
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
            "Country": vessel_country,
            "Flag": risk_item.get("Flag"),
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
                "Country": country,
                "HeadOffice": owner.get("HeadOffice", {}),
                "Office": owner.get("Office", {}),
                "RegisteredOffice": owner.get("RegisteredOffice", {}),
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
        
        # 合并数据，模拟functions.py中的最终数据结构
        merged_data = {
            **vessel_info,
            **compliance_result,
            "SanctionedOwners": sanctioned_owners,
            "ProcessingTime": datetime.now().isoformat(),
            "VoyageDateRange": f"{start_date}-{end_date}",
            "IsSuccess": True,
            "Data": {
                "Items": [{
                    **vessel_info,
                    **compliance_result,
                    "SanctionedOwners": sanctioned_owners,
                    # 添加其他必要的字段以保持兼容性
                    "SanctionRisks": compliance_item.get("SanctionRisks", {}),
                    "OwnershipAndRegistryRisks": compliance_item.get("OwnershipAndRegistryRisks", {}),
                    "VoyageRisks": compliance_item.get("VoyageRisks", {})
                }]
            }
        }
        
        return merged_data
    
    def _calculate_compliance_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算合规风险等级"""
        if not data.get("IsSuccess"):
            return RiskLevel.NO_RISK
        
        # 直接从合并后的数据中获取信息（不再需要从Data.Items中获取）
        # 检查制裁风险
        if (data.get("OwnerIsCurrentlySanctioned") or 
            data.get("VesselIsCurrentlySanctioned") or
            data.get("FlagIsCurrentlySanctioned")):
            return RiskLevel.HIGH
        
        # 如果有历史制裁，返回中风险
        if (data.get("OwnerHasHistoricalSanctions") or
            data.get("VesselHasHistoricalSanctions") or
            data.get("FlagHasHistoricalSanctions")):
            return RiskLevel.MEDIUM
        
        # 检查航程风险（从Data.Items中获取）
        items = data.get("Data", {}).get("Items", [])
        if items:
            item = items[0]
        voyage_risks = item.get("VoyageRisks", {})
        if voyage_risks:
                # 如果有高风险港口调用或STS转移，返回中风险
                if (voyage_risks.get("HighRiskPortCallingCount", 0) > 0 or
                    voyage_risks.get("StsWithASanctionedVesselCount", 0) > 0):
                    return RiskLevel.MEDIUM
        
        return RiskLevel.NO_RISK
    
    def _build_compliance_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建合规检查的tab数据 - 只输出用户需要的字段"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        # 从Data.Items中获取详细信息
        items = data.get("Data", {}).get("Items", [])
        if items:
            item = items[0]
            
            # 添加SanctionedOwners信息（只包含用户需要的字段）
            sanctioned_owners = data.get("SanctionedOwners", [])
        for owner in sanctioned_owners:
            owner_info = {
                    # vesselcompliancescreening_v3接口字段
                "CompanyName": owner.get("CompanyName"),
                "OwnershipTypes": owner.get("OwnershipTypes", []),
                "OwnershipStartDate": owner.get("OwnershipStartDate"),
                    "HeadOffice.Country": owner.get("HeadOffice", {}).get("Country"),
                "HeadOfficeBasedInSanctionedCountry": owner.get("HeadOfficeBasedInSanctionedCountry"),
                "HasSanctionedVesselsInFleet": owner.get("HasSanctionedVesselsInFleet"),
                    "LinkedToSanctionedCompanies": owner.get("LinkedToSanctionedCompanies"),
                    
                    # vesselriskscore接口字段
                    "Sanctions.SanctionSource": [s.get("SanctionSource") for s in owner.get("Sanctions", [])],
                    "Sanctions.SanctionProgram": [s.get("SanctionProgram") for s in owner.get("Sanctions", [])],
                    "Sanctions.SanctionStartDate": [s.get("SanctionStartDate") for s in owner.get("Sanctions", [])],
                    "Sanctions.SanctionEndDate": [s.get("SanctionEndDate") for s in owner.get("Sanctions", [])],
                    "SanctionedVesselsFleet.VesselName": [v.get("VesselName") for v in owner.get("SanctionedVesselsFleet", [])],
                    "SanctionedVesselsFleet.VesselImo": [v.get("VesselImo") for v in owner.get("SanctionedVesselsFleet", [])],
                    "RelatedSanctionedCompanies.CompanyName": [c.get("CompanyName") for c in owner.get("RelatedSanctionedCompanies", [])]
            }
            tab_data.append(owner_info)
        
        return tab_data


class LloydsFlagSanctionsCheckItem(BaseCheckItem):
    """劳氏船期制裁检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用劳氏vesselriskscore接口
            data = self._fetch_lloyds_flag_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_flag_risk(data)
            
            # 构建tab数据
            tab_data = self._build_flag_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"劳氏船期制裁检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_lloyds_flag_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取劳氏船期制裁数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselriskscore"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def _calculate_flag_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算船期制裁风险等级"""
        if not data or not data.get("IsSuccess"):
            return RiskLevel.NO_RISK
        
        # 从vesselriskscore接口数据中提取Flag信息
        vessel_data = data.get("Data", {})
        items = vessel_data.get("Items", [])
        
        if not items:
            return RiskLevel.NO_RISK
        
        # 获取第一个船舶的Flag信息
        first_vessel = items[0]
        flag_info = first_vessel.get("Flag", {})
        
        if not flag_info:
            return RiskLevel.NO_RISK
        
        # 检查是否有船期变更风险
        flag_start_date = flag_info.get("FlagStartDate")
        if flag_start_date:
            # 检查是否在一年内更换过船旗
            from datetime import datetime, timedelta
            try:
                # 处理ISO格式的时间戳
                if 'T' in flag_start_date:
                    flag_date = datetime.fromisoformat(flag_start_date.replace('Z', '+00:00'))
                else:
                    flag_date = datetime.strptime(flag_start_date, "%Y-%m-%d")
                one_year_ago = datetime.now() - timedelta(days=365)
                
                if flag_date > one_year_ago:
                    # 一年内更换过船旗，可能存在风险
                    return RiskLevel.MEDIUM
            except (ValueError, TypeError):
                pass
        
        return RiskLevel.NO_RISK
    
    def _build_flag_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建船期制裁检查的tab数据"""
        tab_data = []
        
        if not data or not data.get("IsSuccess"):
            return tab_data
        
        vessel_data = data.get("Data", {})
        items = vessel_data.get("Items", [])
        
        if not items:
            return tab_data
        
        # 获取第一个船舶的Flag信息
        first_vessel = items[0]
        flag_info = first_vessel.get("Flag", {})
        
        if not flag_info:
            return tab_data
        
        # 判断是否一年内更换过船旗
        flag_start_date = flag_info.get("FlagStartDate")
        is_flag_changed_within_year = "否"
        
        if flag_start_date:
            from datetime import datetime, timedelta
            try:
                # 处理ISO格式的时间戳
                if 'T' in flag_start_date:
                    flag_date = datetime.fromisoformat(flag_start_date.replace('Z', '+00:00'))
                else:
                    flag_date = datetime.strptime(flag_start_date, "%Y-%m-%d")
                one_year_ago = datetime.now() - timedelta(days=365)
                
                if flag_date > one_year_ago:
                    is_flag_changed_within_year = "是"
            except (ValueError, TypeError):
                pass
        
        tab_data.append({
            "VesselImo": vessel_imo,
            "FlagName": flag_info.get("FlagName"),
            "FlagStartDate": flag_info.get("FlagStartDate"),
            "ParisMouStatus": flag_info.get("ParisMouStatus"),
            "ParisMouStartDate": flag_info.get("ParisMouStartDate"),
            "是否一年内更换过船旗": is_flag_changed_within_year
        })
        
        return tab_data


class LloydsSanctionsCheckItem(BaseCheckItem):
    """劳氏制裁检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo_or_list, **kwargs) -> CheckResult:
        """检查劳氏制裁风险
        
        Args:
            vessel_imo_or_list: 单个船舶IMO(str)或船舶IMO列表(List[str])
        """
        # 判断输入类型
        if isinstance(vessel_imo_or_list, str):
            # 单个船舶IMO
            return self._check_single_vessel(vessel_imo_or_list)
        elif isinstance(vessel_imo_or_list, list):
            # 船舶IMO列表
            return self._check_multiple_vessels(vessel_imo_or_list)
        else:
            # 无效输入
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(vessel_imo_or_list)})
    
    def _check_single_vessel(self, vessel_imo: str) -> CheckResult:
        """检查单个船舶的劳氏制裁风险"""
        if not self.validate_params(vessel_imo=vessel_imo):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_lloyds_sanctions_data(vessel_imo)
            risk_level = self._calculate_sanctions_risk(data)
            
            # 构建tab数据
            tab_data = self._build_sanctions_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"劳氏制裁检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _check_multiple_vessels(self, vessel_imos: List[str]) -> CheckResult:
        """检查多个船舶的劳氏制裁风险"""
        if not vessel_imos:
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": ""})
        
        try:
            # 存储所有船舶的检查结果
            all_results = []
            highest_risk_level = RiskLevel.NO_RISK
            combined_tab_data = []
            
            # 逐个检查每个船舶
            for vessel_imo in vessel_imos:
                if not vessel_imo or not str(vessel_imo).strip():
                    continue
                    
                # 调用单个船舶检查
                data = self._fetch_lloyds_sanctions_data(str(vessel_imo).strip())
                risk_level = self._calculate_sanctions_risk(data)
                
                # 更新最高风险级别
                if risk_level == RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.HIGH
                elif risk_level == RiskLevel.MEDIUM and highest_risk_level != RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.MEDIUM
                
                # 构建该船舶的tab数据
                vessel_tab_data = self._build_sanctions_tab_data(data, vessel_imo)
                for tab_item in vessel_tab_data:
                    tab_item["船舶IMO"] = vessel_imo
                combined_tab_data.extend(vessel_tab_data)
                
                all_results.append({
                    "vessel_imo": vessel_imo,
                    "risk_level": risk_level,
                    "sanctions_data": data
                })
            
            # 构建复合结果
            composite_result = {
                "risk_type": "lloyds_sanctions_multiple",
                "risk_desc": "劳氏制裁检查（多船舶）",
                "risk_value": highest_risk_level.value,
                "info": f"检查了{len(vessel_imos)}个船舶，最高风险等级为{highest_risk_level.value}",
                "risk_desc_info": f"多船舶劳氏制裁风险检查",
                "tab": combined_tab_data,
                "vessel_count": len(vessel_imos),
                "vessel_imos": vessel_imos,
                "individual_results": all_results
            }
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"劳氏制裁检查（多船舶）失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(vessel_imos)})
    
    def _fetch_lloyds_sanctions_data(self, vessel_imo: str) -> Dict[str, Any]:
        """获取劳氏制裁数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselsanctions_v2?vesselImo={vessel_imo}"
        return cached_request("GET", url, headers=self.api_config['lloyds_headers'], timeout=30)
    
    def _calculate_sanctions_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算制裁风险等级"""
        if not data.get("IsSuccess"):
            return RiskLevel.NO_RISK
        
        items = data.get("Data", {}).get("items", [])
        if not items:
            return RiskLevel.NO_RISK
        
        # 检查是否有当前制裁（endDate为空）
        has_current_sanctions = False
        has_current_priority_source = False  # 当前制裁是否来自优先来源
        has_historical_sanctions = False
        
        priority_sources = {"OFAC", "EU", "HM", "UN"}
        for item in items:
            vessel_sanctions = item.get("vesselSanctions", {})
            # 直接检查制裁记录的endDate
            end_date = vessel_sanctions.get("endDate")
            if not end_date:
                has_current_sanctions = True
                source = vessel_sanctions.get("source")
                if isinstance(source, str) and source.strip().upper() in priority_sources:
                    has_current_priority_source = True
            else:
                has_historical_sanctions = True
        
        # 有当前制裁（endDate为空）
        if has_current_sanctions:
            # 当前制裁且来源属于优先列表（OFAC/EU/HM/UN）→ 高风险，否则中风险
            return RiskLevel.HIGH if has_current_priority_source else RiskLevel.MEDIUM
        
        # 如果数据里面所有的endDate不为空，则返回MEDIUM（历史制裁）
        if has_historical_sanctions:
            return RiskLevel.MEDIUM
        
        return RiskLevel.NO_RISK
    
    def _build_sanctions_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建制裁检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            self.logger.warning(f"劳氏制裁API返回失败: {data}")
            return tab_data
        
        items = data.get("Data", {}).get("items", [])
        if not items:
            self.logger.warning(f"劳氏制裁API返回空items: {data}")
            return tab_data
        
        self.logger.info(f"劳氏制裁数据items数量: {len(items)}")
        
        for i, item in enumerate(items):
            vessel_sanctions = item.get("vesselSanctions", {})
            
            # 劳氏制裁API的数据结构：每个item就是一个制裁记录
            if vessel_sanctions:
                tab_data.append({
                    "VesselImo": vessel_sanctions.get("vesselImo"),
                    "VesselName": vessel_sanctions.get("vesselName"),
                    "VesselMmsi": vessel_sanctions.get("vesselMmsi"),
                    "SanctionId": vessel_sanctions.get("sanctionId"),
                    "Source": vessel_sanctions.get("source"),
                    "Type": vessel_sanctions.get("type"),
                    "Program": vessel_sanctions.get("program"),
                    "Name": vessel_sanctions.get("name"),
                    "FirstPublished": vessel_sanctions.get("firstPublished"),
                    "LastPublished": vessel_sanctions.get("lastPublished"),
                    "StartDate": vessel_sanctions.get("startDate"),
                    "EndDate": vessel_sanctions.get("endDate"),
                    "SanctionVesselDetails": vessel_sanctions.get("sanctionVesselDetails", []),
                    "Aliases": vessel_sanctions.get("aliases", [])
                })
        
        self.logger.info(f"构建的制裁tab数据数量: {len(tab_data)}")
        return tab_data

class AisManipulationCheckItem(BaseCheckItem):
    """AIS信号伪造及篡改检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_ais_manipulation_data(vessel_imo)
            risk_level = self._calculate_ais_manipulation_risk(data)
            
            # 构建tab数据
            tab_data = self._build_ais_manipulation_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"AIS信号伪造及篡改检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_ais_manipulation_data(self, vessel_imo: str) -> Dict[str, Any]:
        """获取AIS信号伪造及篡改数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesseladvancedcompliancerisk_v3?vesselImo={vessel_imo}"
        response = requests.get(url, headers=self.api_config['lloyds_headers'], timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_ais_manipulation_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算AIS信号伪造及篡改风险等级"""
        if not data.get("IsSuccess"):
            return RiskLevel.NO_RISK
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return RiskLevel.NO_RISK
        
        # 检查ComplianceRiskScore
        compliance_risks = items[0].get("ComplianceRisks", [])
        for risk in compliance_risks:
            if risk.get("ComplianceRiskType", {}).get("Description") == "VesselAisManipulation":
                risk_score = risk.get("ComplianceRiskScore", "")
                if risk_score == "High":
                    return RiskLevel.HIGH
                elif risk_score == "Medium":
                    return RiskLevel.MEDIUM
                elif risk_score == "Low":
                    return RiskLevel.NO_RISK  # 劳氏的Low对应我们的无风险
        
        return RiskLevel.NO_RISK
    
    def _build_ais_manipulation_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建AIS信号伪造及篡改检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        for item in items:
            compliance_risks = item.get("ComplianceRisks", [])
            for risk in compliance_risks:
                if risk.get("ComplianceRiskType", {}).get("Description") == "VesselAisManipulation":
                    risk_score = risk.get("ComplianceRiskScore", "")
                    
                    # 如果风险等级为Low，不添加任何tab数据（返回空数组）
                    if risk_score == "Low":
                        continue
                    
                    # 处理风险详情
                    details = risk.get("Details", [])
                    if not details:  # 无详情时保留基础信息
                        tab_data.append({
                            "VesselImo": item.get("VesselImo"),
                            "VesselName": item.get("VesselName"),
                            "RiskType": "VesselAisManipulation",
                            "ComplianceRiskScore": risk.get("ComplianceRiskScore"),
                            "ComplianceRiskType": risk.get("ComplianceRiskType", {}).get("Description"),
                            "RiskIndicators": []
                        })
                    else:
                        for detail in details:
                            # 合并基础信息、风险属性和详情
                            tab_data.append({
                                "VesselImo": item.get("VesselImo"),
                                "VesselName": item.get("VesselName"),
                                "RiskType": "VesselAisManipulation",
                                "ComplianceRiskScore": risk.get("ComplianceRiskScore"),
                                "ComplianceRiskType": risk.get("ComplianceRiskType", {}).get("Description"),
                                "PlaceInfo": detail.get("Place", {}),
                                "RiskIndicators": [ind.get("Description") for ind in detail.get("RiskIndicators", [])],
                                "DetailInfo": detail
                            })
        
        return tab_data

# ==================== Kpler 检查项实现 ====================

class KplerSanctionedCargoCheckItem(BaseCheckItem):
    """Kpler受制裁货物检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data([int(vessel_imo)], kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_sanctioned_cargo_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_sanctioned_cargo_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"Kpler受制裁货物检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, imos: List[int], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取Kpler数据"""
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        return cached_request(
            "POST",
            self.api_config['kpler_api_url'],
            headers=self.api_config['kpler_headers'],
            params=params,
            json_body=imos,
            timeout=120
        )
    
    def _calculate_sanctioned_cargo_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算受制裁货物风险等级"""
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                sanctioned_cargo = record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedCargo', [])
                if sanctioned_cargo:
                    return RiskLevel.HIGH
                return RiskLevel.NO_RISK
        
        return RiskLevel.NO_RISK
    
    def _build_sanctioned_cargo_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建受制裁货物检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            sanction_risks = compliance.get('sanctionRisks', {})
            sanctioned_cargo = sanction_risks.get('sanctionedCargo', [])
            
            for cargo in sanctioned_cargo:
                tab_data.append({
                    "VesselImo": vessel.get('imo'),
                    "VesselName": vessel.get('shipname'),
                    "Commodity": cargo.get('commodity'),
                    "OriginZone": cargo.get('originZone'),
                    "OriginCountry": cargo.get('originCountry'),
                    "DestinationCountry": cargo.get('destinationCountry'),
                    "HsCode": cargo.get('hsCode'),
                    "HsLink": cargo.get('hsLink'),
                    "Sources": cargo.get('sources', [])
                })
        
        return tab_data

class KplerSanctionedTradesCheckItem(BaseCheckItem):
    """Kpler受制裁贸易检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data([int(vessel_imo)], kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_sanctioned_trades_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_sanctioned_trades_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"Kpler受制裁贸易检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, imos: List[int], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取Kpler数据"""
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        return cached_request(
            "POST",
            self.api_config['kpler_api_url'],
            headers=self.api_config['kpler_headers'],
            params=params,
            json_body=imos,
            timeout=120
        )
    
    def _calculate_sanctioned_trades_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算受制裁贸易风险等级"""
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                sanctioned_trades = record.get('compliance', {}).get('sanctionRisks', {}).get('sanctionedTrades', [])
                if sanctioned_trades:
                    return RiskLevel.HIGH
                return RiskLevel.NO_RISK
        
        return RiskLevel.NO_RISK
    
    def _build_sanctioned_trades_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建受制裁贸易检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            sanction_risks = compliance.get('sanctionRisks', {})
            sanctioned_trades = sanction_risks.get('sanctionedTrades', [])
            
            for trade in sanctioned_trades:
                tab_data.append({
                    "VesselImo": vessel.get('imo'),
                    "VesselName": vessel.get('shipname'),
                    "Commodity": trade.get('commodity'),
                    "OriginZone": trade.get('originZone'),
                    "OriginCountry": trade.get('originCountry'),
                    "DestinationZone": trade.get('destinationZone'),
                    "DestinationCountry": trade.get('destinationCountry'),
                    "HsCode": trade.get('hsCode'),
                    "HsLink": trade.get('hsLink'),
                    "VoyageId": trade.get('voyageId'),
                    "Sources": trade.get('sources', [])
                })
        
        return tab_data

# ==================== UANI 检查项实现 ====================

class UaniCheckItem(BaseCheckItem):
    """UANI检查项"""
    
    def __init__(self, config: CheckItemConfig, api_config: Dict[str, Any]):
        super().__init__(config, api_config)
        self._uani_data = None
        self._load_uani_data()
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            exists, data = self._check_uani_imo(vessel_imo)
            risk_level = RiskLevel.HIGH if exists else RiskLevel.NO_RISK
            
            # 构建tab数据
            tab_data = []
            if exists and data:
                tab_data = [data]
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"UANI检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _load_uani_data(self):
        """加载UANI数据"""
        # 这里实现UANI数据加载逻辑
        # 可以从数据库或文件加载
        pass
    
    def _check_uani_imo(self, imo_number: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """检查IMO号是否在UANI清单中"""
        try:
            # 导入maritime_api中的UANI检查函数
            from maritime_api import check_uani_imo_from_database
            exists, data = check_uani_imo_from_database(imo_number)
            return exists, data if exists else None
        except Exception as e:
            self.logger.error(f"UANI检查失败: {e}")
            return False, None

# ==================== 航次风险检查项实现 ====================

class HighRiskPortCheckItem(BaseCheckItem):
    """高风险港口检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_high_risk_port_risk(data)
            
            # 构建tab数据
            tab_data = self._build_high_risk_port_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"高风险港口检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        return cached_request("GET", url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
    
    def _calculate_high_risk_port_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算高风险港口风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "High Risk Port Calling" in risk_types:
                return RiskLevel.HIGH
        
        return RiskLevel.NO_RISK
    
    def _build_high_risk_port_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建高风险港口检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "High Risk Port Calling" in risk_types:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "VoyageId": voyage.get("VoyageId"),
                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                    "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                    "StartPlace": voyage.get("VoyageStartPlace", {}),
                    "EndPlace": voyage.get("VoyageEndPlace", {}),
                    "RiskTypes": risk_types
                })
        
        return tab_data

class SuspiciousAisGapCheckItem(BaseCheckItem):
    """可疑AIS中断检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_suspicious_ais_gap_risk(data)
            
            # 构建tab数据
            tab_data = self._build_suspicious_ais_gap_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"可疑AIS中断检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_suspicious_ais_gap_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算可疑AIS中断风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "Suspicious AIS Gap" in risk_types:
                return RiskLevel.MEDIUM
        
        return RiskLevel.NO_RISK
    
    def _build_suspicious_ais_gap_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建可疑AIS中断检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            events = voyage.get("VoyageEvents", {})
            
            if "Suspicious AIS Gap" in risk_types:
                for gap in events.get("AisGap", []):
                    if "Suspicious AIS Gap" in gap.get("RiskTypes", []):
                        # 判断AisGapStartEezName是否在受制裁地域中
                        eez_name = gap.get("AisGapStartEezName", "")
                        is_sanctioned_eez = self._is_sanctioned_eez(eez_name)
                        
                        tab_data.append({
                            "VesselImo": vessel_imo,
                            "VoyageStartTime": voyage.get("VoyageStartTime"),
                            "VoyageEndTime": voyage.get("VoyageEndTime"),
                            "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                            "AisGapStartDateTime": gap.get("AisGapStartDateTime"),
                            "AisGapEndDateTime": gap.get("AisGapEndDateTime"),
                            "AisGapStartEezName": eez_name,
                            "is_sanctioned_eez": is_sanctioned_eez,
                            "RiskTypes": gap.get("RiskTypes", [])
                        })
        
        return tab_data
    
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

        # 使用不区分大小写匹配，避免大小写差异导致的不一致
        sanctioned_eez_lower = {name.casefold() for name in SANCTIONED_EEZ}
        target = (eez_name or "").casefold()
        return "是" if target in sanctioned_eez_lower else "否"

class KplerAisGapCheckItem(BaseCheckItem):
    """开普勒AIS中断检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_ais_gap_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_ais_gap_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒AIS中断检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']  # 使用正确的API URL
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        # 开普勒API使用POST请求，参数是船舶IMO列表
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_ais_gap_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算AIS中断风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        # 查找目标船舶的数据
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                ais_gaps = operational_risks.get('aisGaps', [])
                
                if ais_gaps:
                    return RiskLevel.MEDIUM
                break
        
        return RiskLevel.NO_RISK
    
    def _build_ais_gap_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建AIS中断检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        # 查找目标船舶的数据
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                ais_gaps = operational_risks.get('aisGaps', [])
                
                for gap in ais_gaps:
                    tab_data.append({
                        "VesselImo": vessel_imo,
                        "StartDate": gap.get("startDate"),
                        "DraughtChange": gap.get("draughtChange"),
                        "DurationMin": gap.get("durationMin"),
                        "Zone": gap.get("zone", {}),
                        "Position": gap.get("position", {})
                    })
                break
        
        return tab_data

class KplerAisSpoofsCheckItem(BaseCheckItem):
    """开普勒AIS伪造检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_ais_spoofs_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_ais_spoofs_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒AIS伪造检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_ais_spoofs_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算AIS伪造风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                ais_spoofs = operational_risks.get('aisSpoofs', [])
                
                if ais_spoofs:
                    return RiskLevel.MEDIUM
                break
        
        return RiskLevel.NO_RISK
    
    def _build_ais_spoofs_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建AIS伪造检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            operational_risks = compliance.get('operationalRisks', {})
            ais_spoofs = operational_risks.get('aisSpoofs', [])
            
            for spoof in ais_spoofs:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "StartDate": spoof.get("startDate"),
                    "EndDate": spoof.get("endDate"),
                    "Position": spoof.get("position", {}),
                    "DurationMin": spoof.get("durationMin")
                })
        
        return tab_data

class KplerDarkStsCheckItem(BaseCheckItem):
    """开普勒暗STS检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_dark_sts_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_dark_sts_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒暗STS检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_dark_sts_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算暗STS风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                dark_sts_events = operational_risks.get('darkStsEvents', [])
                
                if dark_sts_events:
                    return RiskLevel.MEDIUM
                break
        
        return RiskLevel.NO_RISK
    
    def _build_dark_sts_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建暗STS检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            operational_risks = compliance.get('operationalRisks', {})
            dark_sts_events = operational_risks.get('darkStsEvents', [])
            
            for event in dark_sts_events:
                sts_vessel = event.get("stsVessel", {})
                zone = event.get("zone", {})
                
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "Date": event.get("date"),
                    "StsVesselImo": sts_vessel.get("imo"),
                    "StsVesselName": sts_vessel.get("name"),
                    "ZoneId": zone.get("id"),
                    "ZoneName": zone.get("name"),
                    "Source": event.get("source")
                })
        
        return tab_data

class KplerSanctionedCompaniesCheckItem(BaseCheckItem):
    """开普勒受制裁公司检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_sanctioned_companies_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_sanctioned_companies_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒受制裁公司检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_sanctioned_companies_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算受制裁公司风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                sanction_risks = compliance.get('sanctionRisks', {})
                sanctioned_companies = sanction_risks.get('sanctionedCompanies', [])
                
                if sanctioned_companies:
                    return RiskLevel.HIGH
                break
        
        return RiskLevel.NO_RISK
    
    def _build_sanctioned_companies_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建受制裁公司检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            # 获取船舶公司信息
            vessel_companies = vessel.get('vesselCompanies', [])
            
            # 获取受制裁公司信息
            compliance = record.get('compliance', {})
            sanction_risks = compliance.get('sanctionRisks', {})
            sanctioned_companies = sanction_risks.get('sanctionedCompanies', [])
            
            # 添加船舶公司信息
            for company in vessel_companies:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "CompanyName": company.get("name"),
                    "CompanyType": company.get("typeName"),
                    "StartDate": company.get("startDate"),
                    "CompanyTypeCode": company.get("type"),
                    "IsSanctioned": "是" if any(sc.get("name") == company.get("name") for sc in sanctioned_companies) else "否"
                })
            
            # 添加受制裁公司详细信息
            for company in sanctioned_companies:
                source = company.get("source", {})
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "CompanyName": company.get("name"),
                    "SanctionSource": source.get("name"),
                    "SanctionUrl": source.get("url"),
                    "SanctionStartDate": source.get("startDate"),
                    "CompanyTypeCode": company.get("type"),
                    "IsSanctioned": "是"
                })
        
        return tab_data

class KplerPortCallsCheckItem(BaseCheckItem):
    """开普勒港口调用检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_port_calls_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_port_calls_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒港口调用检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_port_calls_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算港口调用风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                port_calls = operational_risks.get('portCalls', [])
                
                if port_calls:
                    return RiskLevel.HIGH
                break
        
        return RiskLevel.NO_RISK
    
    def _build_port_calls_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建港口调用检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            operational_risks = compliance.get('operationalRisks', {})
            port_calls = operational_risks.get('portCalls', [])
            
            for call in port_calls:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "ZoneName": call.get("zoneName"),
                    "PortName": call.get("portName"),
                    "StartDate": call.get("startDate"),
                    "EndDate": call.get("endDate"),
                    "Volume": call.get("volume"),
                    "CountryName": call.get("countryName"),
                    "ShipToShip": call.get("shipToShip"),
                    "SanctionedVessel": call.get("sanctionedVessel"),
                    "SanctionedCargo": call.get("sanctionedCargo"),
                    "SanctionedOwnership": call.get("sanctionedOwnership")
                })
        
        return tab_data

class KplerStsEventsCheckItem(BaseCheckItem):
    """开普勒STS事件检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_sts_events_risk(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_sts_events_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒STS事件检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取开普勒数据"""
        url = self.api_config['kpler_api_url']
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)]
        
        response = requests.post(
            url,
            params=params,
            headers=self.api_config['kpler_headers'],
            json=imos,
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    
    def _calculate_sts_events_risk(self, data: List[Dict[str, Any]], vessel_imo: str) -> RiskLevel:
        """计算STS事件风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) == str(vessel_imo):
                compliance = record.get('compliance', {})
                operational_risks = compliance.get('operationalRisks', {})
                sts_events = operational_risks.get('stsEvents', [])
                
                if sts_events:
                    return RiskLevel.MEDIUM
                break
        
        return RiskLevel.NO_RISK
    
    def _build_sts_events_tab_data(self, data: List[Dict[str, Any]], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建STS事件检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        for record in data:
            vessel = record.get('vessel', {})
            if str(vessel.get('imo')) != str(vessel_imo):
                continue
                
            compliance = record.get('compliance', {})
            operational_risks = compliance.get('operationalRisks', {})
            sts_events = operational_risks.get('stsEvents', [])
            
            for event in sts_events:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "ZoneName": event.get("zoneName"),
                    "StartDate": event.get("startDate"),
                    "EndDate": event.get("endDate"),
                    "PortName": event.get("portName"),
                    "CountryName": event.get("countryName"),
                    "ShipToShip": event.get("shipToShip"),
                    "SanctionedVessel": event.get("sanctionedVessel"),
                    "SanctionedCargo": event.get("sanctionedCargo"),
                    "SanctionedOwnership": event.get("sanctionedOwnership"),
                    "Vessel2SanctionedOwnership": event.get("vessel2SanctionedOwnership"),
                    "Vessel2SanctionedVessel": event.get("vessel2SanctionedVessel"),
                    "Vessel2Imo": event.get("vessel2Imo"),
                    "Vessel2Name": event.get("vessel2Name")
                })
        
        return tab_data

class DarkPortCheckItem(BaseCheckItem):
    """暗港检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_dark_port_risk(data)
            
            # 构建tab数据
            tab_data = self._build_dark_port_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"暗港检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_dark_port_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算暗港风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if ("Possible Dark Port Calling" in risk_types or 
                "Probable Dark Port Calling" in risk_types):
                return RiskLevel.HIGH
        
        return RiskLevel.NO_RISK
    
    def _build_dark_port_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建暗港检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if ("Possible Dark Port Calling" in risk_types or 
                "Probable Dark Port Calling" in risk_types):
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "VoyageId": voyage.get("VoyageId"),
                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                    "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                    "StartPlace": voyage.get("VoyageStartPlace", {}),
                    "EndPlace": voyage.get("VoyageEndPlace", {}),
                    "RiskTypes": risk_types
                })
        
        return tab_data

class DarkStsCheckItem(BaseCheckItem):
    """暗STS检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_dark_sts_risk(data)
            
            # 构建tab数据
            tab_data = self._build_dark_sts_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"暗STS检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_dark_sts_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算暗STS风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if ("Possible 1-way Dark STS (as dark party)" in risk_types or 
                "Probable 2 way dark STS" in risk_types or
                "Possible 2-way Dark STS (as dark party)" in risk_types):
                return RiskLevel.HIGH
        
        return RiskLevel.NO_RISK
    
    def _build_dark_sts_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建暗STS检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if ("Possible 1-way Dark STS (as dark party)" in risk_types or 
                "Probable 2 way dark STS" in risk_types or
                "Possible 2-way Dark STS (as dark party)" in risk_types):
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "VoyageId": voyage.get("VoyageId"),
                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                    "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                    "StartPlace": voyage.get("VoyageStartPlace", {}),
                    "EndPlace": voyage.get("VoyageEndPlace", {}),
                    "RiskTypes": risk_types
                })
        
        return tab_data

class SanctionedStsCheckItem(BaseCheckItem):
    """受制裁STS检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_sanctioned_sts_risk(data)
            
            # 构建tab数据
            tab_data = self._build_sanctioned_sts_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"受制裁STS检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_sanctioned_sts_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算受制裁STS风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "STS With a Sanctioned Vessel" in risk_types:
                return RiskLevel.HIGH
        
        return RiskLevel.NO_RISK
    
    def _build_sanctioned_sts_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建受制裁STS检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "STS With a Sanctioned Vessel" in risk_types:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "VoyageId": voyage.get("VoyageId"),
                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                    "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                    "StartPlace": voyage.get("VoyageStartPlace", {}),
                    "EndPlace": voyage.get("VoyageEndPlace", {}),
                    "RiskTypes": risk_types
                })
        
        return tab_data

class LoiteringCheckItem(BaseCheckItem):
    """徘徊检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_voyage_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_loitering_risk(data)
            
            # 构建tab数据
            tab_data = self._build_loitering_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"徘徊检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_voyage_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取航次数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    
    def _calculate_loitering_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算徘徊风险等级"""
        if not data.get("Data", {}).get("Items"):
            return RiskLevel.NO_RISK
        
        items = data["Data"]["Items"]
        voyages = items[0].get("Voyages", [])
        
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "Loitering" in risk_types:
                return RiskLevel.MEDIUM
        
        return RiskLevel.NO_RISK
    
    def _build_loitering_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建徘徊检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        voyages = items[0].get("Voyages", [])
        for voyage in voyages:
            risk_types = voyage.get("RiskTypes", [])
            if "Loitering" in risk_types:
                tab_data.append({
                    "VesselImo": vessel_imo,
                    "VoyageId": voyage.get("VoyageId"),
                    "VoyageStartTime": voyage.get("VoyageStartTime"),
                    "VoyageEndTime": voyage.get("VoyageEndTime"),
                    "VoyageRiskRating": voyage.get("VoyageRiskRating"),
                    "StartPlace": voyage.get("VoyageStartPlace", {}),
                    "EndPlace": voyage.get("VoyageEndPlace", {}),
                    "RiskTypes": risk_types
                })
        
        return tab_data

# ==================== 检查项注册中心 ====================

class CheckItemRegistry:
    """检查项注册中心"""
    
    def __init__(self):
        self._registry: Dict[str, type] = {}
        self._register_default_items()
    
    def _register_default_items(self):
        """注册默认检查项"""
        # Lloyd's 检查项
        self.register("lloyds_compliance", LloydsComplianceCheckItem)
        self.register("lloyds_sanctions", LloydsSanctionsCheckItem)
        self.register("lloyds_flag_sanctions", LloydsFlagSanctionsCheckItem)
        self.register("ais_manipulation", AisManipulationCheckItem)
        self.register("lloydsRiskLevel", LloydsRiskLevelCheckItem)
        
        # Kpler 检查项
        self.register("has_sanctioned_cargo_risk", KplerSanctionedCargoCheckItem)
        self.register("has_sanctioned_trades_risk", KplerSanctionedTradesCheckItem)
        self.register("has_ais_gap_risk", KplerAisGapCheckItem)
        self.register("has_ais_spoofs_risk", KplerAisSpoofsCheckItem)
        self.register("has_dark_sts_risk", KplerDarkStsCheckItem)
        self.register("has_sanctioned_companies_risk", KplerSanctionedCompaniesCheckItem)
        self.register("has_port_calls_risk", KplerPortCallsCheckItem)
        self.register("has_sts_events_risk", KplerStsEventsCheckItem)
        self.register("kplerRiskLevel", KplerRiskLevelCheckItem)
        self.register("kpler_sanctions", KplerSanctionsCheckItem)
        
        # UANI 检查项
        self.register("uani_check", UaniCheckItem)
        
        # 航次风险检查项
        self.register("high_risk_port", HighRiskPortCheckItem)
        self.register("suspicious_ais_gap", SuspiciousAisGapCheckItem)
        self.register("possible_dark_port", DarkPortCheckItem)
        self.register("dark_sts", DarkStsCheckItem)
        self.register("sanctioned_sts", SanctionedStsCheckItem)
        self.register("loitering_behavior", LoiteringCheckItem)
        
        # 国家检查项
        self.register("cargo_country", CargoCountryCheckItem)
        self.register("port_country", PortCountryCheckItem)
        
        # 复合检查项
        self.register("Vessel_risk_level", VesselRiskLevelCheckItem)
        self.register("Vessel_is_sanction", VesselIsSanctionCheckItem)
        self.register("Vessel_flag_sanctions", VesselFlagSanctionsCheckItem)
        self.register("Vessel_in_uani", VesselInUaniCheckItem)
        self.register("Vessel_ais_gap", VesselAisGapCheckItem)
        self.register("Vessel_Manipulation", VesselManipulationCheckItem)
        self.register("Vessel_risky_port_call", VesselRiskyPortCallCheckItem)
        self.register("Vessel_dark_port_call", VesselDarkPortCallCheckItem)
        self.register("Vessel_cargo_sanction", VesselCargoSanctionCheckItem)
        self.register("Vessel_trade_sanction", VesselTradeSanctionCheckItem)
        self.register("Vessel_dark_sts_events", VesselDarkStsEventsCheckItem)
        self.register("Vessel_sts_transfer", VesselStsTransferCheckItem)
        self.register("Vessel_stakeholder_is_sanction", VesselStakeholderIsSanctionCheckItem)
        self.register("cargo_origin_from_sanctioned_country", CargoOriginFromSanctionedCountryCheckItem)
        self.register("port_origin_from_sanctioned_country", PortOriginFromSanctionedCountryCheckItem)
        self.register("Vessel_bunkering_sanctions", VesselBunkeringSanctionsCheckItem)
    
    def register(self, check_item_id: str, check_item_class: type):
        """注册检查项"""
        self._registry[check_item_id] = check_item_class
        logger.info(f"注册检查项: {check_item_id} -> {check_item_class.__name__}")
    
    def get_check_item_class(self, check_item_id: str) -> Optional[type]:
        """获取检查项类"""
        return self._registry.get(check_item_id)
    
    def list_registered_items(self) -> List[str]:
        """列出所有已注册的检查项"""
        return list(self._registry.keys())

# ==================== 配置管理器 ====================

class ConfigManager:
    """配置管理器"""
    
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
        self._config_cache: Dict[str, CheckItemConfig] = {}
    
    def load_config_from_db(self) -> Dict[str, CheckItemConfig]:
        """从数据库加载配置"""
        if not self.db_connection:
            logger.warning("数据库连接未配置，使用默认配置")
            return self._get_default_configs()
        
        try:
            # 从数据库查询配置
            query = """
            SELECT id, business_module, compliance_check_module, compliance_check_type,
                   entity_cn, entity_en, entity_type, risk_desc, risk_type,
                   used_flag, time_flag, time_period, area_flag, area, risk_flag, risk_flag_type
            FROM risk_fkpt_control_platform
            WHERE used_flag = 'Y'
            """
            
            # 这里需要根据你的数据库连接方式调整
            # df = pd.read_sql(query, self.db_connection)
            
            # 暂时返回默认配置
            return self._get_default_configs()
            
        except Exception as e:
            logger.error(f"从数据库加载配置失败: {e}")
            return self._get_default_configs()
    
    def _get_default_configs(self) -> Dict[str, CheckItemConfig]:
        """获取默认配置"""
        default_configs = {
            "lloyds_compliance": CheckItemConfig(
                id="lloyds_compliance",
                business_module="船舶风险筛查",
                compliance_check_module="劳氏API",
                compliance_check_type="合规检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="船舶相关方涉制裁风险情况（劳氏）",
                risk_type="compliance_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="30",
                area_flag="N",
                area="",
                risk_flag="高风险",
                risk_flag_type="compliance"
            ),
            "lloyds_sanctions": CheckItemConfig(
                id="lloyds_sanctions",
                business_module="船舶风险筛查",
                compliance_check_module="劳氏API",
                compliance_check_type="制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="船舶涉制裁名单风险情况（劳氏）",
                risk_type="sanctions_risk",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="高风险",
                risk_flag_type="sanctions"
            ),
            "ais_manipulation": CheckItemConfig(
                id="ais_manipulation",
                business_module="船舶风险筛查",
                compliance_check_module="劳氏API",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="AIS信号伪造及篡改风险情况（劳氏）",
                risk_type="ais_manipulation",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="中风险",
                risk_flag_type="ais"
            ),
            "has_sanctioned_cargo_risk": CheckItemConfig(
                id="has_sanctioned_cargo_risk",
                business_module="船舶风险筛查",
                compliance_check_module="Kpler API",
                compliance_check_type="货物检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="船舶运输受制裁货物情况（开普勒）",
                risk_type="cargo_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="30",
                area_flag="N",
                area="",
                risk_flag="高风险",
                risk_flag_type="cargo"
            ),
            "has_sanctioned_trades_risk": CheckItemConfig(
                id="has_sanctioned_trades_risk",
                business_module="船舶风险筛查",
                compliance_check_module="Kpler API",
                compliance_check_type="贸易检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="船舶涉及受制裁贸易风险情况（开普勒）",
                risk_type="trade_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="30",
                area_flag="N",
                area="",
                risk_flag="高风险",
                risk_flag_type="trade"
            ),
            "uani_check": CheckItemConfig(
                id="uani_check",
                business_module="船舶风险筛查",
                compliance_check_module="UANI",
                compliance_check_type="清单检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="船舶涉UANI清单风险情况",
                risk_type="uani_risk",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="高风险",
                risk_flag_type="uani"
            ),
            "suspicious_ais_gap": CheckItemConfig(
                id="suspicious_ais_gap",
                business_module="船舶风险筛查",
                compliance_check_module="劳氏API",
                compliance_check_type="航次检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="IMO",
                risk_desc="AIS信号缺失风险情况（劳氏）",
                risk_type="ais_gap_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="30",
                area_flag="N",
                area="",
                risk_flag="中风险",
                risk_flag_type="ais_gap"
            )
        }
        
        return default_configs
    
    def get_config(self, check_item_id: str) -> Optional[CheckItemConfig]:
        """获取检查项配置"""
        if not self._config_cache:
            self._config_cache = self.load_config_from_db()
        
        return self._config_cache.get(check_item_id)
    
    def is_enabled(self, check_item_id: str) -> bool:
        """检查检查项是否启用"""
        config = self.get_config(check_item_id)
        return config and config.used_flag == "Y"

# ==================== 风险检查编排器 ====================

class RiskCheckOrchestrator:
    """风险检查编排器"""
    
    def __init__(self, api_config: Dict[str, Any], info_manager=None):
        self.api_config = api_config
        self.info_manager = info_manager
        self.check_items: Dict[str, BaseCheckItem] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._register_check_items()
    
    def _register_check_items(self):
        """注册所有检查项"""
        # 创建默认配置
        default_configs = self._create_default_configs()
        
        # 注册检查项
        for config in default_configs:
            check_item_class = self._get_check_item_class(config.id)
            if check_item_class:
                check_item = check_item_class(config, self.api_config)
                # 设置info_manager
                check_item.info_manager = self.info_manager
                self.check_items[config.id] = check_item
                self.logger.info(f"注册检查项: {config.id} -> {check_item_class.__name__}")
    
    def _get_check_item_class(self, check_item_id: str):
        """获取检查项类"""
        class_mapping = {
            "lloyds_compliance": LloydsComplianceCheckItem,
            "lloyds_sanctions": LloydsSanctionsCheckItem,
            "lloyds_flag_sanctions": LloydsFlagSanctionsCheckItem,
            "ais_manipulation": AisManipulationCheckItem,
            "has_sanctioned_cargo_risk": KplerSanctionedCargoCheckItem,
            "has_sanctioned_trades_risk": KplerSanctionedTradesCheckItem,
            "uani_check": UaniCheckItem,
            "high_risk_port": HighRiskPortCheckItem,
            "suspicious_ais_gap": SuspiciousAisGapCheckItem,
            "has_ais_gap_risk": KplerAisGapCheckItem,
            "has_ais_spoofs_risk": KplerAisSpoofsCheckItem,
            "has_dark_sts_risk": KplerDarkStsCheckItem,
            "has_sanctioned_companies_risk": KplerSanctionedCompaniesCheckItem,
            "has_port_calls_risk": KplerPortCallsCheckItem,
            "has_sts_events_risk": KplerStsEventsCheckItem,
            "lloydsRiskLevel": LloydsRiskLevelCheckItem,
            "kplerRiskLevel": KplerRiskLevelCheckItem,
            "kpler_sanctions": KplerSanctionsCheckItem,
            "cargo_country": CargoCountryCheckItem,
            "port_country": PortCountryCheckItem,
            # 新增的检查项
            "possible_dark_port": DarkPortCheckItem,
            "dark_sts": DarkStsCheckItem,
            "sanctioned_sts": SanctionedStsCheckItem,
            "loitering_behavior": LoiteringCheckItem,
            # 复合检查项
            "Vessel_risk_level": VesselRiskLevelCheckItem,
            "Vessel_is_sanction": VesselIsSanctionCheckItem,
            "Vessel_flag_sanctions": VesselFlagSanctionsCheckItem,
            "Vessel_in_uani": VesselInUaniCheckItem,
            "Vessel_ais_gap": VesselAisGapCheckItem,
            "Vessel_Manipulation": VesselManipulationCheckItem,
            "Vessel_risky_port_call": VesselRiskyPortCallCheckItem,
            "Vessel_dark_port_call": VesselDarkPortCallCheckItem,
            "Vessel_cargo_sanction": VesselCargoSanctionCheckItem,
            "Vessel_trade_sanction": VesselTradeSanctionCheckItem,
            "Vessel_dark_sts_events": VesselDarkStsEventsCheckItem,
            "Vessel_sts_transfer": VesselStsTransferCheckItem,
            "Vessel_stakeholder_is_sanction": VesselStakeholderIsSanctionCheckItem,
            "cargo_origin_from_sanctioned_country": CargoOriginFromSanctionedCountryCheckItem,
            "port_origin_from_sanctioned_country": PortOriginFromSanctionedCountryCheckItem,
            "Vessel_bunkering_sanctions": VesselBunkeringSanctionsCheckItem,
            "dowjones_sanctions_risk": DowJonesSanctionsRiskCheckItem
            # 其他检查项类暂时未实现，可以后续添加
        }
        return class_mapping.get(check_item_id)
    
    def _create_default_configs(self) -> List[CheckItemConfig]:
        """创建默认配置"""
        return [
            CheckItemConfig(
                id="uani_check",
                business_module="船舶风险检查",
                compliance_check_module="UANI检查",
                compliance_check_type="清单检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶涉UANI清单风险情况",
                risk_type="uani_check",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_bunkering_sanctions",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="加油船舶制裁情况",
                risk_type="Vessel_bunkering_sanctions",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="lloyds_compliance",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="合规检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶相关方涉制裁风险情况",
                risk_type="lloyds_compliance",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="lloyds_sanctions",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶涉制裁名单风险情况",
                risk_type="lloyds_sanctions",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="lloyds_flag_sanctions",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="船期制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶船期制裁情况",
                risk_type="lloyds_flag_sanctions",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="中风险,无风险"
            ),
            CheckItemConfig(
                id="suspicious_ais_gap",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号缺失风险情况(劳氏)",
                risk_type="suspicious_ais_gap",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="ais_manipulation",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号伪造及篡改风险情况",
                risk_type="ais_manipulation",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="high_risk_port",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="港口检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="挂靠高风险港口风险情况",
                risk_type="high_risk_port",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_sanctioned_cargo_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="货物检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶运输受制裁货物情况",
                risk_type="has_sanctioned_cargo_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_sanctioned_trades_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="贸易检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶涉及受制裁贸易风险情况",
                risk_type="has_sanctioned_trades_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_ais_gap_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号缺失风险情况（开普勒）",
                risk_type="has_ais_gap_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_ais_spoofs_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号伪造及篡改风险情况（开普勒）",
                risk_type="has_ais_spoofs_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_dark_sts_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="隐蔽STS事件风险情况（开普勒）",
                risk_type="has_dark_sts_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_sanctioned_companies_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="公司检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶相关方涉制裁风险情况（开普勒）",
                risk_type="has_sanctioned_companies_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_port_calls_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="港口检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="挂靠高风险港口风险情况（开普勒）",
                risk_type="has_port_calls_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="has_sts_events_risk",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="STS转运不合规风险情况（开普勒）",
                risk_type="has_sts_events_risk",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="lloydsRiskLevel",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="风险等级检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶制裁合规结果(劳氏)",
                risk_type="lloydsRiskLevel",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="kplerRiskLevel",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="风险等级检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶制裁合规结果(开普勒)",
                risk_type="kplerRiskLevel",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            CheckItemConfig(
                id="kpler_sanctions",
                business_module="船舶风险检查",
                compliance_check_module="开普勒检查",
                compliance_check_type="制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶制裁情况(开普勒)",
                risk_type="kpler_sanctions",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="cargo_country",
                business_module="货物风险检查",
                compliance_check_module="国家检查",
                compliance_check_type="原产地检查",
                entity_cn="货物",
                entity_en="cargo",
                entity_type="cargo",
                risk_desc="货物原产地是否来源于高风险国家",
                risk_type="cargo_country",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            CheckItemConfig(
                id="port_country",
                business_module="港口风险检查",
                compliance_check_module="国家检查",
                compliance_check_type="港口检查",
                entity_cn="港口",
                entity_en="port",
                entity_type="port",
                risk_desc="港口是否来源于高风险国家",
                risk_type="port_country",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            # 复合检查项配置
            CheckItemConfig(
                id="Vessel_risk_level",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="风险等级检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶制裁合规结果",
                risk_type="Vessel_risk_level",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_is_sanction",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶是否在制裁名单中",
                risk_type="Vessel_is_sanction",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_flag_sanctions",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="船期制裁检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶船期制裁情况",
                risk_type="Vessel_flag_sanctions",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_in_uani",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="UANI检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶是否在UANI清单中",
                risk_type="Vessel_in_uani",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_ais_gap",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号缺失风险情况",
                risk_type="Vessel_ais_gap",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_Manipulation",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="AIS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="AIS信号伪造风险情况",
                risk_type="Vessel_Manipulation",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_risky_port_call",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="港口检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="挂靠高风险港口风险情况",
                risk_type="Vessel_risky_port_call",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_dark_port_call",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="港口检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="暗港访问风险情况",
                risk_type="Vessel_dark_port_call",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_cargo_sanction",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="货物检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶运输受制裁货物情况",
                risk_type="Vessel_cargo_sanction",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_trade_sanction",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="贸易检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶涉及受制裁贸易风险情况",
                risk_type="Vessel_trade_sanction",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_dark_sts_events",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="隐蔽STS事件风险情况",
                risk_type="Vessel_dark_sts_events",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_sts_transfer",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="STS转运不合规风险情况",
                risk_type="Vessel_sts_transfer",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="Vessel_stakeholder_is_sanction",
                business_module="船舶风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="相关方检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="船舶相关方涉制裁风险情况",
                risk_type="Vessel_stakeholder_is_sanction",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="cargo_origin_from_sanctioned_country",
                business_module="货物风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="原产地检查",
                entity_cn="货物",
                entity_en="cargo",
                entity_type="cargo",
                risk_desc="货物原产地是否来自于制裁国家",
                risk_type="cargo_origin_from_sanctioned_country",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            CheckItemConfig(
                id="port_origin_from_sanctioned_country",
                business_module="港口风险检查",
                compliance_check_module="复合检查",
                compliance_check_type="港口检查",
                entity_cn="港口",
                entity_en="port",
                entity_type="port",
                risk_desc="港口是否来自于制裁国家",
                risk_type="port_origin_from_sanctioned_country",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,无风险"
            ),
            CheckItemConfig(
                id="dark_sts",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="隐蔽STS事件风险情况（劳氏）",
                risk_type="dark_sts",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="possible_dark_port",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="港口检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="暗港访问风险情况（劳氏）",
                risk_type="possible_dark_port",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="sanctioned_sts",
                business_module="船舶风险检查",
                compliance_check_module="劳氏检查",
                compliance_check_type="STS检查",
                entity_cn="船舶",
                entity_en="vessel",
                entity_type="vessel",
                risk_desc="STS转运不合规风险情况（劳氏）",
                risk_type="sanctioned_sts",
                used_flag="Y",
                time_flag="Y",
                time_period="1年",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            ),
            CheckItemConfig(
                id="dowjones_sanctions_risk",
                business_module="制裁风险检查",
                compliance_check_module="道琼斯检查",
                compliance_check_type="制裁检查",
                entity_cn="实体",
                entity_en="entity",
                entity_type="entity",
                risk_desc="道琼斯制裁风险检查",
                risk_type="dowjones_sanctions_risk",
                used_flag="Y",
                time_flag="N",
                time_period="",
                area_flag="N",
                area="",
                risk_flag="Y",
                risk_flag_type="高风险,中风险,无风险"
            )
        ]
    
    # ==================== 具体执行方法 ====================
    
    def execute_uani_check(self, vessel_imo: str) -> CheckResult:
        """执行UANI检查"""
        check_item = self.check_items.get("uani_check")
        if not check_item:
            return self._create_error_result("uani_check", vessel_imo, "UANI检查项未注册")
        
        try:
            return check_item.check(vessel_imo)
        except Exception as e:
            self.logger.error(f"UANI检查失败: {e}")
            return self._create_error_result("uani_check", vessel_imo, str(e))
    
    def execute_lloyds_compliance_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行劳氏合规检查"""
        check_item = self.check_items.get("lloyds_compliance")
        if not check_item:
            return self._create_error_result("lloyds_compliance", vessel_imo, "劳氏合规检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"劳氏合规检查失败: {e}")
            return self._create_error_result("lloyds_compliance", vessel_imo, str(e))
    
    def execute_lloyds_sanctions_check(self, vessel_imo_or_list) -> CheckResult:
        """执行劳氏制裁检查
        
        Args:
            vessel_imo_or_list: 单个船舶IMO(str)或船舶IMO列表(List[str])
        """
        check_item = self.check_items.get("lloyds_sanctions")
        if not check_item:
            return self._create_error_result("lloyds_sanctions", str(vessel_imo_or_list), "劳氏制裁检查项未注册")
        
        try:
            return check_item.check(vessel_imo_or_list)
        except Exception as e:
            self.logger.error(f"劳氏制裁检查失败: {e}")
            return self._create_error_result("lloyds_sanctions", str(vessel_imo_or_list), str(e))
    
    def execute_kpler_sanctions_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行Kpler制裁检查
        
        Args:
            vessel_imo: 船舶IMO号码
            start_date: 开始日期
            end_date: 结束日期
        """
        check_item = self.check_items.get("kpler_sanctions")
        if not check_item:
            return self._create_error_result("kpler_sanctions", vessel_imo, "Kpler制裁检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"Kpler制裁检查失败: {e}")
            return self._create_error_result("kpler_sanctions", vessel_imo, str(e))
    
    def execute_lloyds_flag_sanctions_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行劳氏船期制裁检查
        
        Args:
            vessel_imo: 船舶IMO号码
            start_date: 开始日期
            end_date: 结束日期
        """
        check_item = self.check_items.get("lloyds_flag_sanctions")
        if not check_item:
            return self._create_error_result("lloyds_flag_sanctions", vessel_imo, "劳氏船期制裁检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"劳氏船期制裁检查失败: {e}")
            return self._create_error_result("lloyds_flag_sanctions", vessel_imo, str(e))
    
    def execute_suspicious_ais_gap_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行AIS中断检查（劳氏）"""
        check_item = self.check_items.get("suspicious_ais_gap")
        if not check_item:
            return self._create_error_result("suspicious_ais_gap", vessel_imo, "AIS中断检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"AIS中断检查失败: {e}")
            return self._create_error_result("suspicious_ais_gap", vessel_imo, str(e))
    
    def execute_kpler_ais_gap_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行AIS中断检查（开普勒）"""
        check_item = self.check_items.get("has_ais_gap_risk")
        if not check_item:
            return self._create_error_result("has_ais_gap_risk", vessel_imo, "开普勒AIS中断检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"开普勒AIS中断检查失败: {e}")
            return self._create_error_result("has_ais_gap_risk", vessel_imo, str(e))
    
    def execute_ais_manipulation_check(self, vessel_imo: str) -> CheckResult:
        """执行AIS信号伪造及篡改检查"""
        check_item = self.check_items.get("ais_manipulation")
        if not check_item:
            return self._create_error_result("ais_manipulation", vessel_imo, "AIS信号伪造及篡改检查项未注册")
        
        try:
            return check_item.check(vessel_imo)
        except Exception as e:
            self.logger.error(f"AIS信号伪造及篡改检查失败: {e}")
            return self._create_error_result("ais_manipulation", vessel_imo, str(e))
    
    def execute_high_risk_port_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行高风险港口检查"""
        check_item = self.check_items.get("high_risk_port")
        if not check_item:
            return self._create_error_result("high_risk_port", vessel_imo, "高风险港口检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"高风险港口检查失败: {e}")
            return self._create_error_result("high_risk_port", vessel_imo, str(e))
    
    def execute_dark_port_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行暗港检查"""
        check_item = self.check_items.get("possible_dark_port")
        if not check_item:
            return self._create_error_result("possible_dark_port", vessel_imo, "暗港检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"暗港检查失败: {e}")
            return self._create_error_result("possible_dark_port", vessel_imo, str(e))
    
    def execute_dark_sts_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行暗STS检查"""
        check_item = self.check_items.get("dark_sts")
        if not check_item:
            return self._create_error_result("dark_sts", vessel_imo, "暗STS检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"暗STS检查失败: {e}")
            return self._create_error_result("dark_sts", vessel_imo, str(e))
    
    def execute_sanctioned_sts_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行受制裁STS检查"""
        check_item = self.check_items.get("sanctioned_sts")
        if not check_item:
            return self._create_error_result("sanctioned_sts", vessel_imo, "受制裁STS检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"受制裁STS检查失败: {e}")
            return self._create_error_result("sanctioned_sts", vessel_imo, str(e))
    
    def execute_loitering_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行徘徊检查"""
        check_item = self.check_items.get("loitering_behavior")
        if not check_item:
            return self._create_error_result("loitering_behavior", vessel_imo, "徘徊检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"徘徊检查失败: {e}")
            return self._create_error_result("loitering_behavior", vessel_imo, str(e))
    
    def execute_sanctioned_cargo_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行受制裁货物检查（开普勒）"""
        check_item = self.check_items.get("has_sanctioned_cargo_risk")
        if not check_item:
            return self._create_error_result("has_sanctioned_cargo_risk", vessel_imo, "受制裁货物检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"受制裁货物检查失败: {e}")
            return self._create_error_result("has_sanctioned_cargo_risk", vessel_imo, str(e))
    
    def execute_sanctioned_trades_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行受制裁贸易检查（开普勒）"""
        check_item = self.check_items.get("has_sanctioned_trades_risk")
        if not check_item:
            return self._create_error_result("has_sanctioned_trades_risk", vessel_imo, "受制裁贸易检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"受制裁贸易检查失败: {e}")
            return self._create_error_result("has_sanctioned_trades_risk", vessel_imo, str(e))
    
    def execute_ais_spoofs_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行AIS伪造检查（开普勒）"""
        check_item = self.check_items.get("has_ais_spoofs_risk")
        if not check_item:
            return self._create_error_result("has_ais_spoofs_risk", vessel_imo, "AIS伪造检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"AIS伪造检查失败: {e}")
            return self._create_error_result("has_ais_spoofs_risk", vessel_imo, str(e))
    
    def execute_kpler_dark_sts_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行暗STS检查（开普勒）"""
        check_item = self.check_items.get("dark_sts_kpler_risk")
        if not check_item:
            return self._create_error_result("dark_sts_kpler_risk", vessel_imo, "开普勒暗STS检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"开普勒暗STS检查失败: {e}")
            return self._create_error_result("dark_sts_kpler_risk", vessel_imo, str(e))
    
    def execute_sanctioned_companies_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行受制裁公司检查（开普勒）"""
        check_item = self.check_items.get("has_sanctioned_companies_risk")
        if not check_item:
            return self._create_error_result("has_sanctioned_companies_risk", vessel_imo, "受制裁公司检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"受制裁公司检查失败: {e}")
            return self._create_error_result("has_sanctioned_companies_risk", vessel_imo, str(e))
    
    def execute_port_calls_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行港口调用检查（开普勒）"""
        check_item = self.check_items.get("has_port_calls_risk")
        if not check_item:
            return self._create_error_result("has_port_calls_risk", vessel_imo, "港口调用检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"港口调用检查失败: {e}")
            return self._create_error_result("has_port_calls_risk", vessel_imo, str(e))
    
    def execute_sts_events_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行STS事件检查（开普勒）"""
        check_item = self.check_items.get("has_sts_events_risk")
        if not check_item:
            return self._create_error_result("has_sts_events_risk", vessel_imo, "STS事件检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"STS事件检查失败: {e}")
            return self._create_error_result("has_sts_events_risk", vessel_imo, str(e))
    
    def execute_lloyds_risk_level_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行劳氏风险等级检查"""
        check_item = self.check_items.get("lloydsRiskLevel")
        if not check_item:
            return self._create_error_result("lloydsRiskLevel", vessel_imo, "劳氏风险等级检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"劳氏风险等级检查失败: {e}")
            return self._create_error_result("lloydsRiskLevel", vessel_imo, str(e))
    
    def execute_cargo_country_check(self, country_name_or_list) -> CheckResult:
        """执行货物原产地国家检查
        
        Args:
            country_name_or_list: 单个国家名称(str)或国家名称列表(List[str])
        """
        check_item = self.check_items.get("cargo_country")
        if not check_item:
            return self._create_error_result("cargo_country", str(country_name_or_list), "货物原产地国家检查项未注册")
        
        try:
            return check_item.check(country_name_or_list)
        except Exception as e:
            self.logger.error(f"货物原产地国家检查失败: {e}")
            return self._create_error_result("cargo_country", str(country_name_or_list), str(e))
    
    def execute_port_country_check(self, country_name: str) -> CheckResult:
        """执行港口国家检查"""
        check_item = self.check_items.get("port_country")
        if not check_item:
            return self._create_error_result("port_country", country_name, "港口国家检查项未注册")
        
        try:
            return check_item.check(country_name)
        except Exception as e:
            self.logger.error(f"港口国家检查失败: {e}")
            return self._create_error_result("port_country", country_name, str(e))
    
    def execute_kpler_risk_level_check(self, vessel_imo: str) -> CheckResult:
        """执行开普勒风险等级检查"""
        check_item = self.check_items.get("kplerRiskLevel")
        if not check_item:
            return self._create_error_result("kplerRiskLevel", vessel_imo, "开普勒风险等级检查项未注册")
        
        try:
            return check_item.check(vessel_imo)
        except Exception as e:
            self.logger.error(f"开普勒风险等级检查失败: {e}")
            return self._create_error_result("kplerRiskLevel", vessel_imo, str(e))
    
    # 复合检查项执行方法
    def execute_vessel_risk_level_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行船舶风险等级复合检查"""
        check_item = self.check_items.get("Vessel_risk_level")
        if not check_item:
            return self._create_error_result("Vessel_risk_level", vessel_imo, "船舶风险等级复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶风险等级复合检查失败: {e}")
            return self._create_error_result("Vessel_risk_level", vessel_imo, str(e))
    
    def execute_vessel_is_sanction_check(self, vessel_imo: str, start_date: str = None, end_date: str = None) -> CheckResult:
        """执行船舶是否在制裁名单中复合检查"""
        check_item = self.check_items.get("Vessel_is_sanction")
        if not check_item:
            return self._create_error_result("Vessel_is_sanction", vessel_imo, "船舶是否在制裁名单中复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶是否在制裁名单中复合检查失败: {e}")
            return self._create_error_result("Vessel_is_sanction", vessel_imo, str(e))
    
    def execute_vessel_flag_sanctions_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行船舶船期制裁情况复合检查"""
        check_item = self.check_items.get("Vessel_flag_sanctions")
        if not check_item:
            return self._create_error_result("Vessel_flag_sanctions", vessel_imo, "船舶船期制裁情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶船期制裁情况复合检查失败: {e}")
            return self._create_error_result("Vessel_flag_sanctions", vessel_imo, str(e))
    
    def execute_vessel_in_uani_check(self, vessel_imo: str) -> CheckResult:
        """执行船舶是否在UANI清单中复合检查"""
        check_item = self.check_items.get("Vessel_in_uani")
        if not check_item:
            return self._create_error_result("Vessel_in_uani", vessel_imo, "船舶是否在UANI清单中复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo)
        except Exception as e:
            self.logger.error(f"船舶是否在UANI清单中复合检查失败: {e}")
            return self._create_error_result("Vessel_in_uani", vessel_imo, str(e))
    
    def execute_vessel_ais_gap_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行AIS信号缺失风险情况复合检查"""
        check_item = self.check_items.get("Vessel_ais_gap")
        if not check_item:
            return self._create_error_result("Vessel_ais_gap", vessel_imo, "AIS信号缺失风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"AIS信号缺失风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_ais_gap", vessel_imo, str(e))
    
    def execute_vessel_manipulation_check(self, vessel_imo: str, start_date: str = None, end_date: str = None) -> CheckResult:
        """执行AIS信号伪造风险情况复合检查"""
        check_item = self.check_items.get("Vessel_Manipulation")
        if not check_item:
            return self._create_error_result("Vessel_Manipulation", vessel_imo, "AIS信号伪造风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"AIS信号伪造风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_Manipulation", vessel_imo, str(e))
    
    def execute_vessel_risky_port_call_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行挂靠高风险港口风险情况复合检查"""
        check_item = self.check_items.get("Vessel_risky_port_call")
        if not check_item:
            return self._create_error_result("Vessel_risky_port_call", vessel_imo, "挂靠高风险港口风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"挂靠高风险港口风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_risky_port_call", vessel_imo, str(e))
    
    def execute_vessel_dark_port_call_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行暗港访问风险情况复合检查"""
        check_item = self.check_items.get("Vessel_dark_port_call")
        if not check_item:
            return self._create_error_result("Vessel_dark_port_call", vessel_imo, "暗港访问风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"暗港访问风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_dark_port_call", vessel_imo, str(e))
    
    def execute_vessel_cargo_sanction_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行船舶运输受制裁货物情况复合检查"""
        check_item = self.check_items.get("Vessel_cargo_sanction")
        if not check_item:
            return self._create_error_result("Vessel_cargo_sanction", vessel_imo, "船舶运输受制裁货物情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶运输受制裁货物情况复合检查失败: {e}")
            return self._create_error_result("Vessel_cargo_sanction", vessel_imo, str(e))
    
    def execute_vessel_trade_sanction_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行船舶涉及受制裁贸易风险情况复合检查"""
        check_item = self.check_items.get("Vessel_trade_sanction")
        if not check_item:
            return self._create_error_result("Vessel_trade_sanction", vessel_imo, "船舶涉及受制裁贸易风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶涉及受制裁贸易风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_trade_sanction", vessel_imo, str(e))
    
    def execute_vessel_dark_sts_events_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行隐蔽STS事件风险情况复合检查"""
        check_item = self.check_items.get("Vessel_dark_sts_events")
        if not check_item:
            return self._create_error_result("Vessel_dark_sts_events", vessel_imo, "隐蔽STS事件风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"隐蔽STS事件风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_dark_sts_events", vessel_imo, str(e))
    
    def execute_vessel_sts_transfer_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行STS转运不合规风险情况复合检查"""
        check_item = self.check_items.get("Vessel_sts_transfer")
        if not check_item:
            return self._create_error_result("Vessel_sts_transfer", vessel_imo, "STS转运不合规风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"STS转运不合规风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_sts_transfer", vessel_imo, str(e))
    
    def execute_vessel_stakeholder_is_sanction_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行船舶相关方涉制裁风险情况复合检查"""
        check_item = self.check_items.get("Vessel_stakeholder_is_sanction")
        if not check_item:
            return self._create_error_result("Vessel_stakeholder_is_sanction", vessel_imo, "船舶相关方涉制裁风险情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"船舶相关方涉制裁风险情况复合检查失败: {e}")
            return self._create_error_result("Vessel_stakeholder_is_sanction", vessel_imo, str(e))
    
    def execute_vessel_bunkering_sanctions_check(self, vessel_imo: str, start_date: str, end_date: str) -> CheckResult:
        """执行加油船舶制裁情况复合检查"""
        check_item = self.check_items.get("Vessel_bunkering_sanctions")
        if not check_item:
            return self._create_error_result("Vessel_bunkering_sanctions", vessel_imo, "加油船舶制裁情况复合检查项未注册")
        
        try:
            return check_item.check(vessel_imo, start_date=start_date, end_date=end_date)
        except Exception as e:
            self.logger.error(f"加油船舶制裁情况复合检查失败: {e}")
            return self._create_error_result("Vessel_bunkering_sanctions", vessel_imo, str(e))

    def execute_cargo_origin_from_sanctioned_country_check(self, country_name: str) -> CheckResult:
        """执行货物原产地是否来自于制裁国家复合检查"""
        check_item = self.check_items.get("cargo_origin_from_sanctioned_country")
        if not check_item:
            return self._create_error_result("cargo_origin_from_sanctioned_country", country_name, "货物原产地是否来自于制裁国家复合检查项未注册")
        
        try:
            return check_item.check(country_name)
        except Exception as e:
            self.logger.error(f"货物原产地是否来自于制裁国家复合检查失败: {e}")
            return self._create_error_result("cargo_origin_from_sanctioned_country", country_name, str(e))
    
    def execute_port_origin_from_sanctioned_country_check(self, country_name: str) -> CheckResult:
        """执行港口是否来自于制裁国家复合检查"""
        check_item = self.check_items.get("port_origin_from_sanctioned_country")
        if not check_item:
            return self._create_error_result("port_origin_from_sanctioned_country", country_name, "港口是否来自于制裁国家复合检查项未注册")
        
        try:
            return check_item.check(country_name)
        except Exception as e:
            self.logger.error(f"港口是否来自于制裁国家复合检查失败: {e}")
            return self._create_error_result("port_origin_from_sanctioned_country", country_name, str(e))
    
    def execute_dowjones_sanctions_risk_check(self, entity_name_or_list) -> CheckResult:
        """执行道琼斯制裁风险检查
        
        Args:
            entity_name_or_list: 单个实体名称(str)或实体名称列表(List[str])
        """
        check_item = self.check_items.get("dowjones_sanctions_risk")
        if not check_item:
            return self._create_error_result("dowjones_sanctions_risk", str(entity_name_or_list), "道琼斯制裁风险检查项未注册")
        
        try:
            return check_item.check(entity_name_or_list)
        except Exception as e:
            self.logger.error(f"道琼斯制裁风险检查失败: {e}")
            return self._create_error_result("dowjones_sanctions_risk", str(entity_name_or_list), str(e))
    
    def _create_error_result(self, risk_type: str, vessel_imo: str, error_message: str) -> CheckResult:
        """创建错误结果"""
        return CheckResult(
            risk_type=risk_type,
            risk_desc="检查失败",
            risk_value="无风险",
            info=f"检查失败: {error_message}",
            risk_desc_info="检查过程中发生错误",
            tab=[],
            vessel_imo={"0": vessel_imo}
        )
    
    def execute_checks(self, entity_id: str, check_item_ids: List[str] = None, **kwargs) -> List[CheckResult]:
        """执行检查"""
        if check_item_ids is None:
            # 获取所有启用的检查项
            check_item_ids = self._get_enabled_check_items()
        
        # 自动设置默认日期参数
        if "start_date" not in kwargs or "end_date" not in kwargs:
            default_start, default_end = get_default_date_range()
            if "start_date" not in kwargs:
                kwargs["start_date"] = default_start
            if "end_date" not in kwargs:
                kwargs["end_date"] = default_end
        
        results = []
        
        for check_item_id in check_item_ids:
            try:
                # 检查是否启用
                if not self.config_manager.is_enabled(check_item_id):
                    self.logger.info(f"检查项 {check_item_id} 未启用，跳过")
                    continue
                
                # 获取配置
                config = self.config_manager.get_config(check_item_id)
                if not config:
                    self.logger.warning(f"未找到检查项 {check_item_id} 的配置")
                    continue
                
                # 获取检查项类
                check_item_class = self.registry.get_check_item_class(check_item_id)
                if not check_item_class:
                    self.logger.warning(f"未找到检查项 {check_item_id} 的实现")
                    continue
                
                # 创建检查项实例
                check_item = check_item_class(config, self.api_config)
                
                # 执行检查
                result = check_item.check(entity_id, **kwargs)
                results.append(result)
                
                self.logger.info(f"检查项 {check_item_id} 执行完成: {result.risk_level.value}")
                
            except Exception as e:
                self.logger.error(f"执行检查项 {check_item_id} 失败: {e}")
                # 添加错误结果
                error_result = CheckResult(
                    risk_type=check_item_id,
                    risk_desc="检查失败",
                    risk_value="无风险",
                    info=f"执行失败: {str(e)}",
                    risk_desc_info="检查过程中发生错误",
                    tab=[],
                    vessel_imo={"0": entity_id}
                )
                results.append(error_result)
        
        return results
    
    def _get_enabled_check_items(self) -> List[str]:
        """获取所有启用的检查项"""
        configs = self.config_manager.load_config_from_db()
        return [config.id for config in configs.values() if config.used_flag == "Y"]
    
    def get_check_summary(self, results: List[CheckResult]) -> Dict[str, Any]:
        """获取检查汇总"""
        summary = {
            "total_checks": len(results),
            "high_risk_count": 0,
            "medium_risk_count": 0,
            "no_risk_count": 0,
            "overall_risk_level": RiskLevel.NO_RISK.value,
            "check_details": []
        }
        
        for result in results:
            summary["check_details"].append(result.to_dict())
            
            if result.risk_level == RiskLevel.HIGH:
                summary["high_risk_count"] += 1
            elif result.risk_level == RiskLevel.MEDIUM:
                summary["medium_risk_count"] += 1
            else:
                summary["no_risk_count"] += 1
        
        # 计算总体风险等级
        if summary["high_risk_count"] > 0:
            summary["overall_risk_level"] = RiskLevel.HIGH.value
        elif summary["medium_risk_count"] > 0:
            summary["overall_risk_level"] = RiskLevel.MEDIUM.value
        else:
            summary["overall_risk_level"] = RiskLevel.NO_RISK.value
        
        return summary

# ==================== 使用示例 ====================

def create_api_config() -> Dict[str, Any]:
    """创建API配置"""
    from kingbase_config import get_lloyds_token, get_kpler_token
    
    return {
        "lloyds_base_url": "https://api.lloydslistintelligence.com/v1",
        "kpler_base_url": "https://api.kpler.com/v2",
        "kpler_api_url": "https://api.kpler.com/v2/compliance/vessel-risks-v2",
        "lloyds_headers": {
            "accept": "application/json",
            "Authorization": get_lloyds_token()
        },
        "kpler_headers": {
            "Authorization": get_kpler_token(),
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    }

def main():
    """主函数示例"""
    # 1. 创建API配置
    api_config = create_api_config()
    
    # 2. 创建编排器
    orchestrator = RiskCheckOrchestrator(api_config)
    
    # 3. 执行检查
    vessel_imo = "9842190"
    start_date = "2024-08-25"
    end_date = "2025-08-25"
    
    # 执行所有启用的检查项
    results = orchestrator.execute_checks(
        entity_id=vessel_imo,
        start_date=start_date,
        end_date=end_date
    )
    
    # 4. 获取汇总
    summary = orchestrator.get_check_summary(results)
    
    # 5. 输出结果
    print("=== 风险检查结果汇总 ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    
    # 6. 执行特定检查项
    specific_results = orchestrator.execute_checks(
        entity_id=vessel_imo,
        check_item_ids=["lloyds_compliance", "has_sanctioned_cargo_risk"],
        start_date=start_date,
        end_date=end_date
    )
    
    print("\n=== 特定检查项结果 ===")
    for result in specific_results:
        print(f"{result.check_item_id}: {result.risk_level.value}")

class LloydsRiskLevelCheckItem(BaseCheckItem):
    """劳氏风险等级检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_lloyds_risk_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_risk_level(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_risk_level_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"劳氏风险等级检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_lloyds_risk_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取劳氏风险数据"""
        url = f"{self.api_config['lloyds_base_url']}/vesselriskscore"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        response = requests.get(url, headers=self.api_config['lloyds_headers'], params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def _calculate_risk_level(self, data: Dict[str, Any], vessel_imo: str) -> RiskLevel:
        """计算风险等级"""
        if not data.get("IsSuccess"):
            return RiskLevel.NO_RISK
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return RiskLevel.NO_RISK
        
        # 查找目标船舶
        for item in items:
            if str(item.get("VesselImo")) == str(vessel_imo):
                risk_scores = item.get("RiskScores", [])
                
                # 只对RiskPeriodInMonths=12的记录进行判定
                for score in risk_scores:
                    if score.get("RiskPeriodInMonths") == 12:
                        total_risk_score = score.get("TotalRiskScore")
                        if total_risk_score == 100:
                            return RiskLevel.HIGH
                        elif total_risk_score is not None:
                            return RiskLevel.MEDIUM
                        else:
                            return RiskLevel.NO_RISK
                
                # 如果没有找到12个月的记录，返回无风险
                return RiskLevel.NO_RISK
        
        return RiskLevel.NO_RISK
    
    def _build_risk_level_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建风险等级检查的tab数据"""
        tab_data = []
        
        if not data.get("IsSuccess"):
            return tab_data
        
        items = data.get("Data", {}).get("Items", [])
        if not items:
            return tab_data
        
        # 查找目标船舶
        for item in items:
            if str(item.get("VesselImo")) == str(vessel_imo):
                risk_scores = item.get("RiskScores", [])
                
                # 只对RiskPeriodInMonths=12的记录进行判定生成Risktype字段
                risktype = "LOW"  # 默认值
                for score in risk_scores:
                    if score.get("RiskPeriodInMonths") == 12:
                        total_risk_score = score.get("TotalRiskScore")
                        if total_risk_score == 100:
                            risktype = "HIGH"
                        break
                
                tab_data.append({
                    "Risktype": risktype,
                    "RiskScores": risk_scores
                })
                break
        
        return tab_data

class CargoCountryCheckItem(BaseCheckItem):
    """货物原产地国家检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["country_name"]
    
    def check(self, country_name_or_list, **kwargs) -> CheckResult:
        """检查货物原产地国家风险
        
        Args:
            country_name_or_list: 单个国家名称(str)或国家名称列表(List[str])
        """
        # 判断输入类型
        if isinstance(country_name_or_list, str):
            # 单个国家名称
            return self._check_single_country(country_name_or_list)
        elif isinstance(country_name_or_list, list):
            # 国家名称列表
            return self._check_multiple_countries(country_name_or_list)
        else:
            # 无效输入
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(country_name_or_list)})
    
    def _check_single_country(self, country_name: str) -> CheckResult:
        """检查单个国家的货物原产地风险"""
        if not self.validate_params(country_name=country_name):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
        
        try:
            is_high_risk = self._check_cargo_country_risk(country_name)
            risk_level = RiskLevel.HIGH if is_high_risk else RiskLevel.NO_RISK
            
            # 构建tab数据
            tab_data = self._build_cargo_country_tab_data(country_name, is_high_risk)
            
            return self.create_check_result(risk_level, tab_data, {"0": country_name})
        except Exception as e:
            self.logger.error(f"货物原产地国家检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
    
    def _check_multiple_countries(self, country_names: List[str]) -> CheckResult:
        """检查多个国家的货物原产地风险"""
        if not country_names:
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": ""})
        
        try:
            # 存储所有国家的检查结果
            all_results = []
            highest_risk_level = RiskLevel.NO_RISK
            combined_tab_data = []
            
            # 逐个检查每个国家
            for country_name in country_names:
                if not country_name or not str(country_name).strip():
                    continue
                    
                # 调用单个国家检查
                is_high_risk = self._check_cargo_country_risk(str(country_name).strip())
                risk_level = RiskLevel.HIGH if is_high_risk else RiskLevel.NO_RISK
                
                # 更新最高风险级别
                if risk_level == RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.HIGH
                
                # 构建该国家的tab数据
                country_tab_data = self._build_cargo_country_tab_data(country_name, is_high_risk)
                for tab_item in country_tab_data:
                    tab_item["国家名称"] = country_name
                combined_tab_data.extend(country_tab_data)
                
                all_results.append({
                    "country_name": country_name,
                    "risk_level": risk_level,
                    "is_high_risk": is_high_risk
                })
            
            # 构建复合结果
            composite_result = {
                "risk_type": "cargo_country_multiple",
                "risk_desc": "货物原产地国家检查（多国家）",
                "risk_value": highest_risk_level.value,
                "info": f"检查了{len(country_names)}个国家，最高风险等级为{highest_risk_level.value}",
                "risk_desc_info": f"多国家货物原产地风险检查",
                "tab": combined_tab_data,
                "country_count": len(country_names),
                "country_names": country_names,
                "individual_results": all_results
            }
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"货物原产地国家检查（多国家）失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(country_names)})
    
    def _check_cargo_country_risk(self, country_name: str) -> bool:
        """检查货物原产地国家是否为高风险"""
        try:
            from kingbase_config import KINGBASE_CONFIG
            import psycopg2

            # 使用 ILIKE 做不区分大小写匹配，并限制返回 1 行以提高效率
            sql = """
                SELECT 1
                FROM lng.contry_cargo
                WHERE Countryname ILIKE %s
                LIMIT 1
            """

            with psycopg2.connect(**KINGBASE_CONFIG) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, (country_name,))
                    exists = cursor.fetchone() is not None
                    return exists
        except Exception as e:
            self.logger.error(f"查询货物原产地国家风险失败: {e}")
            return False
    
    def _build_cargo_country_tab_data(self, country_name: str, is_high_risk: bool) -> List[Dict[str, Any]]:
        """构建货物原产地国家检查的tab数据"""
        tab_data = []
        
        if is_high_risk:
            tab_data.append({
                "Countryname": country_name
            })
        
        return tab_data

class PortCountryCheckItem(BaseCheckItem):
    """港口国家检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["country_name"]
    
    def check(self, country_name: str, **kwargs) -> CheckResult:
        if not self.validate_params(country_name=country_name, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
        
        try:
            is_high_risk = self._check_port_country_risk(country_name)
            risk_level = RiskLevel.HIGH if is_high_risk else RiskLevel.NO_RISK
            
            # 构建tab数据
            tab_data = self._build_port_country_tab_data(country_name, is_high_risk)
            
            return self.create_check_result(risk_level, tab_data, {"0": country_name})
        except Exception as e:
            self.logger.error(f"港口国家检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
    
    def _check_port_country_risk(self, country_name: str) -> bool:
        """检查港口国家是否为高风险"""
        try:
            from kingbase_config import KINGBASE_CONFIG
            import psycopg2

            sql = """
                SELECT 1
                FROM lng.contry_port
                WHERE Countryname ILIKE %s
                LIMIT 1
            """

            with psycopg2.connect(**KINGBASE_CONFIG) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, (country_name,))
                    exists = cursor.fetchone() is not None
                    return exists
        except Exception as e:
            self.logger.error(f"查询港口国家风险失败: {e}")
            return False
    
    def _build_port_country_tab_data(self, country_name: str, is_high_risk: bool) -> List[Dict[str, Any]]:
        """构建港口国家检查的tab数据"""
        tab_data = []
        
        if is_high_risk:
            tab_data.append({
                "Countryname": country_name
            })
        
        return tab_data


class KplerRiskLevelCheckItem(BaseCheckItem):
    """开普勒风险等级检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_compliance_screening_data(vessel_imo)
            risk_level = self._calculate_risk_level(data, vessel_imo)
            
            # 构建tab数据
            tab_data = self._build_risk_level_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"开普勒风险等级检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_compliance_screening_data(self, vessel_imo: str) -> Dict[str, Any]:
        """获取开普勒合规筛查数据"""
        url = f"{self.api_config['kpler_base_url']}/compliance/compliance-screening"
        params = {
            "vessels": vessel_imo
        }
        
        response = requests.get(url, headers=self.api_config['kpler_headers'], params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def _calculate_risk_level(self, data: Dict[str, Any], vessel_imo: str) -> RiskLevel:
        """计算风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        metrics = data.get("metrics", {})
        fleet_status = metrics.get("fleetStatus", {})
        sanction_count = fleet_status.get("sanctionCount", 0)
        
        # 根据sanctionCount判断风险等级
        if sanction_count > 0:
            return RiskLevel.HIGH
        else:
            return RiskLevel.NO_RISK
    
    def _build_risk_level_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建风险等级检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        metrics = data.get("metrics", {})
        fleet_status = metrics.get("fleetStatus", {})
        
        # 判断风险指示器
        sanction_count = fleet_status.get("sanctionCount", 0)
        risk_indicator = "高风险" if sanction_count > 0 else "无风险"
        
        tab_data.append({
            "riskindicator": risk_indicator,
            "totalCount": fleet_status.get("totalCount", 0),
            "sanctionCount": sanction_count,
            "warningCount": fleet_status.get("warningCount", 0),
            "noRiskCount": fleet_status.get("noRiskCount", 0)
        })
        
        return tab_data


class KplerSanctionsCheckItem(BaseCheckItem):
    """Kpler制裁检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            data = self._fetch_kpler_sanctions_data(vessel_imo, start_date, end_date)
            risk_level = self._calculate_sanctions_risk(data)
            
            # 构建tab数据
            tab_data = self._build_sanctions_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
        except Exception as e:
            self.logger.error(f"Kpler制裁检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
    
    def _fetch_kpler_sanctions_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取Kpler制裁数据"""
        url = f"{self.api_config['kpler_base_url']}/compliance/vessel-risks-v2/{vessel_imo}"
        params = {
            "startDate": start_date,
            "endDate": end_date
        }
        
        response = requests.get(url, headers=self.api_config['kpler_headers'], params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def _calculate_sanctions_risk(self, data: Dict[str, Any]) -> RiskLevel:
        """计算制裁风险等级"""
        if not data:
            return RiskLevel.NO_RISK
        
        # 从compliance.sanctionRisks.sanctionedVessels数组中提取数据
        compliance = data.get("compliance", {})
        sanction_risks = compliance.get("sanctionRisks", {})
        sanctioned_vessels = sanction_risks.get("sanctionedVessels", [])
        
        if not sanctioned_vessels:
            return RiskLevel.NO_RISK
        
        # 检查是否有当前制裁（endDate为空或未来日期）
        has_current_sanctions = False
        has_historical_sanctions = False
        
        for vessel in sanctioned_vessels:
            end_date = vessel.get("endDate")
            if not end_date:
                # 没有结束日期，表示当前制裁
                has_current_sanctions = True
            else:
                # 有结束日期，表示历史制裁
                has_historical_sanctions = True
        
        # 风险判定逻辑
        if has_current_sanctions:
            return RiskLevel.HIGH
        elif has_historical_sanctions:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.NO_RISK
    
    def _build_sanctions_tab_data(self, data: Dict[str, Any], vessel_imo: str) -> List[Dict[str, Any]]:
        """构建制裁检查的tab数据"""
        tab_data = []
        
        if not data:
            return tab_data
        
        # 从compliance.sanctionRisks.sanctionedVessels数组中提取数据
        compliance = data.get("compliance", {})
        sanction_risks = compliance.get("sanctionRisks", {})
        sanctioned_vessels = sanction_risks.get("sanctionedVessels", [])
        
        if not sanctioned_vessels:
            return tab_data
        
        self.logger.info(f"Kpler制裁数据sanctionedVessels数量: {len(sanctioned_vessels)}")
        
        for i, vessel in enumerate(sanctioned_vessels):
            tab_data.append({
                "VesselImo": vessel.get("vesselImo"),
                "VesselName": vessel.get("vesselName"),
                "VesselMmsi": vessel.get("vesselMmsi"),
                "SanctionId": vessel.get("sanctionId"),
                "Source": vessel.get("source"),
                "Type": vessel.get("type"),
                "Program": vessel.get("program"),
                "Name": vessel.get("name"),
                "FirstPublished": vessel.get("firstPublished"),
                "LastPublished": vessel.get("lastPublished"),
                "StartDate": vessel.get("startDate"),
                "EndDate": vessel.get("endDate"),
                "SanctionVesselDetails": vessel.get("sanctionVesselDetails", []),
                "Aliases": vessel.get("aliases", []),
                "RiskLevel": vessel.get("riskLevel"),
                "Description": vessel.get("description")
            })
        
        self.logger.info(f"构建的Kpler制裁tab数据数量: {len(tab_data)}")
        return tab_data


class CompositeCheckItem(BaseCheckItem):
    """复合检查项基类"""
    
    def __init__(self, api_config: Dict[str, Any], logger=None):
        super().__init__(api_config, logger)
        # 延迟初始化orchestrator，避免递归
        self._orchestrator = None
    
    @property
    def orchestrator(self):
        """延迟获取orchestrator实例"""
        if self._orchestrator is None:
            self._orchestrator = RiskCheckOrchestrator(self.api_config, self.info_manager)
        return self._orchestrator
    
    def _get_current_time(self) -> str:
        """获取当前时间"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _calculate_composite_risk_level(self, check_results: List[CheckResult]) -> str:
        """计算复合风险等级"""
        risk_levels = [result.risk_value for result in check_results]
        
        # 取最高风险等级：高风险 > 中风险 > 无风险
        if "高风险" in risk_levels:
            return "高风险"
        elif "中风险" in risk_levels:
            return "中风险"
        else:
            return "无风险"
    
    def _build_composite_result(self, risk_type_number: str, risk_description: str, 
                              check_results: List[CheckResult]) -> Dict[str, Any]:
        """构建复合检查结果"""
        risk_value = self._calculate_composite_risk_level(check_results)
        current_time = self._get_current_time()
        
        # 构建sanctions_list
        sanctions_list = []
        for result in check_results:
            sanctions_item = {
                "risk_type": result.risk_type,
                "risk_desc": result.risk_desc,
                "risk_value": result.risk_value,
                "info": result.info,
                "risk_desc_info": result.risk_desc_info,
                "tab": result.tab
            }
            sanctions_list.append(sanctions_item)
        
        return {
            "risk_screening_status": risk_value,
            "risk_screening_time": current_time,
            "risk_status_change_content": "",
            "risk_status_change_time": "",
            "risk_type_number": risk_type_number,
            "risk_description": risk_description,
            "risk_info": "",
            "risk_status_reason": {
                "risk_description": risk_description,
                "risk_value": risk_value,
                "sanctions_list": sanctions_list
            }
        }


class MultipleEntityCheckItem(CompositeCheckItem):
    """多实体检查项基类"""
    
    def _process_multiple_entities(self, entities: List[str], single_check_func, 
                                 entity_type: str = "实体", **kwargs) -> CheckResult:
        """处理多个实体的通用方法"""
        if not entities:
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": ""})
        
        try:
            # 存储所有实体的检查结果
            all_results = []
            highest_risk_level = RiskLevel.NO_RISK
            combined_tab_data = []
            
            # 逐个检查每个实体
            for entity in entities:
                if not entity or not str(entity).strip():
                    continue
                    
                # 调用单个实体检查
                entity_result = single_check_func(str(entity).strip(), **kwargs)
                
                # 确定风险级别
                risk_value = entity_result.get('risk_value', '无风险')
                if risk_value == "高风险":
                    entity_risk_level = RiskLevel.HIGH
                elif risk_value == "中风险":
                    entity_risk_level = RiskLevel.MEDIUM
                else:
                    entity_risk_level = RiskLevel.NO_RISK
                
                # 更新最高风险级别
                if entity_risk_level == RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.HIGH
                elif entity_risk_level == RiskLevel.MEDIUM and highest_risk_level != RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.MEDIUM
                
                # 构建该实体的tab数据
                entity_tab_data = {
                    f"{entity_type}名称": entity,
                    "风险等级": risk_value,
                    "检查时间": entity_result.get('risk_screening_time', ''),
                    "风险描述": entity_result.get('risk_description', ''),
                    "风险信息": entity_result.get('info', '')
                }
                
                combined_tab_data.append(entity_tab_data)
                all_results.append({
                    f"{entity_type}_name": entity,
                    "risk_level": entity_risk_level,
                    "risk_status": risk_value,
                    "result_data": entity_result
                })
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "99", f"{entity_type}检查（多{entity_type}）", []
            )
            
            # 更新复合结果
            composite_result.update({
                "name": f"多{entity_type}检查({len(entities)}个{entity_type})",
                "risk_screening_status": highest_risk_level.value,
                "risk_screening_time": self._get_current_time(),
                "risk_description": f"检查了{len(entities)}个{entity_type}，最高风险等级为{highest_risk_level.value}",
                f"{entity_type}_count": len(entities),
                f"{entity_type}_names": entities,
                "individual_results": all_results
            })
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": f"多{entity_type}检查({len(entities)}个{entity_type})"}
            
            # 更新tab数据
            composite_result["tab"] = combined_tab_data
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"{entity_type}检查（多{entity_type}）失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(entities)})


class VesselRiskLevelCheckItem(CompositeCheckItem):
    """船舶风险等级复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用lloydsRiskLevel和kplerRiskLevel检查项
            lloyds_result = self.orchestrator.execute_lloyds_risk_level_check(vessel_imo, start_date, end_date)
            kpler_result = self.orchestrator.execute_kpler_risk_level_check(vessel_imo)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "4", "船舶制裁合规结果", [lloyds_result, kpler_result]
            )
            
            # 添加vessel_imo字段和英文名称
            composite_result["vessel_imo"] = {"0": vessel_imo}
            composite_result["risk_type"] = "Vessel_risk_level"
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶风险等级复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselIsSanctionCheckItem(CompositeCheckItem):
    """船舶是否在制裁名单中复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, start_date: str = None, end_date: str = None, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用lloyds_sanctions检查项
            lloyds_sanctions_result = self.orchestrator.execute_lloyds_sanctions_check(vessel_imo)
            
            # 调用kpler_sanctions检查项（如果提供了日期参数）
            kpler_sanctions_result = None
            if start_date and end_date:
                kpler_sanctions_result = self.orchestrator.execute_kpler_sanctions_check(vessel_imo, start_date, end_date)
            else:
                # 如果没有日期参数，创建一个无风险的结果
                kpler_sanctions_result = self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
                kpler_sanctions_result.risk_type = "kpler_sanctions"
                kpler_sanctions_result.risk_desc = "Kpler制裁检查（无日期参数）"
            
            # 计算复合风险等级
            risk_levels = [lloyds_sanctions_result.risk_value, kpler_sanctions_result.risk_value]
            if "高风险" in risk_levels:
                composite_risk_level = RiskLevel.HIGH
            elif "中风险" in risk_levels:
                composite_risk_level = RiskLevel.MEDIUM
            else:
                composite_risk_level = RiskLevel.NO_RISK
            
            # 构建复合tab数据
            composite_tab_data = []
            
            # 添加劳氏制裁数据
            if lloyds_sanctions_result.tab:
                for item in lloyds_sanctions_result.tab:
                    item["数据来源"] = "劳氏"
                    composite_tab_data.append(item)
            
            # 添加Kpler制裁数据
            if kpler_sanctions_result.tab:
                for item in kpler_sanctions_result.tab:
                    item["数据来源"] = "Kpler"
                    composite_tab_data.append(item)
            
            # 创建复合检查结果
            composite_result = self.create_check_result(
                composite_risk_level, 
                composite_tab_data, 
                {"0": vessel_imo}
            )
            
            # 设置复合检查的特殊字段
            composite_result.risk_type = "Vessel_is_sanction"
            composite_result.risk_desc = "船舶是否在制裁名单中"
            
            # 设置风险状态原因字段，避免下游访问时出现未定义错误
            composite_result.risk_status_reason = {}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶是否在制裁名单中复合检查失败: {e}")
            error_result = self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
            error_result.risk_status_reason = {}
            return error_result

 
class VesselFlagSanctionsCheckItem(CompositeCheckItem):
    """船舶船期制裁情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用lloyds_flag_sanctions检查项
            flag_sanctions_result = self.orchestrator.execute_lloyds_flag_sanctions_check(vessel_imo, start_date, end_date)
            
            # 创建复合检查结果
            composite_result = self.create_check_result(
                RiskLevel.MEDIUM if flag_sanctions_result.risk_value == "中风险" else RiskLevel.NO_RISK, 
                flag_sanctions_result.tab, 
                {"0": vessel_imo}
            )
            
            # 设置复合检查的特殊字段
            composite_result.risk_type = "Vessel_flag_sanctions"
            composite_result.risk_desc = "船舶船期制裁情况"
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶船期制裁情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselInUaniCheckItem(CompositeCheckItem):
    """船舶是否在UANI清单中复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用uani_check检查项
            uani_result = self.orchestrator.execute_uani_check(vessel_imo)
            
            # 创建复合检查结果
            composite_result = self.create_check_result(
                RiskLevel.HIGH if uani_result.risk_value == "高风险" else RiskLevel.NO_RISK,
                uani_result.tab,
                {"0": vessel_imo}
            )
            
            # 设置复合检查的特殊字段
            composite_result.risk_type = "Vessel_in_uani"
            composite_result.risk_desc = "船舶是否在UANI清单中"
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶是否在UANI清单中复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselAisGapCheckItem(CompositeCheckItem):
    """AIS信号缺失风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用suspicious_ais_gap和has_ais_gap_risk检查项
            suspicious_ais_gap_result = self.orchestrator.execute_suspicious_ais_gap_check(vessel_imo, start_date, end_date)
            has_ais_gap_result = self.orchestrator.execute_kpler_ais_gap_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "5", "AIS信号缺失风险情况", [suspicious_ais_gap_result, has_ais_gap_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"AIS信号缺失风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselManipulationCheckItem(CompositeCheckItem):
    """AIS信号伪造风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo"]
    
    def check(self, vessel_imo: str, start_date: str = None, end_date: str = None, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 如果没有提供日期参数，使用默认日期范围
            if not start_date or not end_date:
                default_start, default_end = get_default_date_range()
                start_date = start_date or default_start
                end_date = end_date or default_end
            
            # 调用ais_manipulation和has_ais_spoofs_risk检查项
            ais_manipulation_result = self.orchestrator.execute_ais_manipulation_check(vessel_imo)
            has_ais_spoofs_result = self.orchestrator.execute_ais_spoofs_check(vessel_imo, start_date, end_date)
            
            # 计算复合风险等级
            risk_levels = [ais_manipulation_result.risk_value, has_ais_spoofs_result.risk_value]
            if "高风险" in risk_levels:
                composite_risk_level = RiskLevel.HIGH
            elif "中风险" in risk_levels:
                composite_risk_level = RiskLevel.MEDIUM
            else:
                composite_risk_level = RiskLevel.NO_RISK
            
            # 构建复合tab数据
            composite_tab_data = []
            
            # 添加AIS操纵数据
            if ais_manipulation_result.tab:
                for item in ais_manipulation_result.tab:
                    item["数据来源"] = "劳氏"
                    composite_tab_data.append(item)
            
            # 添加AIS伪造数据
            if has_ais_spoofs_result.tab:
                for item in has_ais_spoofs_result.tab:
                    item["数据来源"] = "开普勒"
                    composite_tab_data.append(item)
            
            # 创建复合检查结果
            composite_result = self.create_check_result(
                composite_risk_level, 
                composite_tab_data, 
                {"0": vessel_imo}
            )
            
            # 设置复合检查的特殊字段
            composite_result.risk_type = "Vessel_Manipulation"
            composite_result.risk_desc = "AIS信号伪造风险情况"
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"AIS信号伪造风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselRiskyPortCallCheckItem(CompositeCheckItem):
    """挂靠高风险港口风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用high_risk_port和has_port_calls_risk检查项
            high_risk_port_result = self.orchestrator.execute_high_risk_port_check(vessel_imo, start_date, end_date)
            has_port_calls_result = self.orchestrator.execute_port_calls_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "6", "挂靠高风险港口风险情况", [high_risk_port_result, has_port_calls_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"挂靠高风险港口风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselDarkPortCallCheckItem(CompositeCheckItem):
    """暗港访问风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用possible_dark_port检查项
            dark_port_result = self.orchestrator.execute_dark_port_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "6", "暗港访问风险情况", [dark_port_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"暗港访问风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselCargoSanctionCheckItem(CompositeCheckItem):
    """船舶运输受制裁货物情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用has_sanctioned_cargo_risk检查项
            cargo_sanction_result = self.orchestrator.execute_sanctioned_cargo_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "10", "船舶运输受制裁货物情况", [cargo_sanction_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶运输受制裁货物情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselTradeSanctionCheckItem(CompositeCheckItem):
    """船舶涉及受制裁贸易风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用has_sanctioned_trades_risk检查项
            trade_sanction_result = self.orchestrator.execute_sanctioned_trades_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "10", "船舶涉及受制裁贸易风险情况", [trade_sanction_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶涉及受制裁贸易风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselDarkStsEventsCheckItem(CompositeCheckItem):
    """隐蔽STS事件风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用dark_sts和has_dark_sts_risk检查项
            dark_sts_result = self.orchestrator.execute_dark_sts_check(vessel_imo, start_date, end_date)
            has_dark_sts_result = self.orchestrator.execute_kpler_dark_sts_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "11", "隐蔽STS事件风险情况", [dark_sts_result, has_dark_sts_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"隐蔽STS事件风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselStsTransferCheckItem(CompositeCheckItem):
    """STS转运不合规风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用sanctioned_sts和has_sts_events_risk检查项
            sanctioned_sts_result = self.orchestrator.execute_sanctioned_sts_check(vessel_imo, start_date, end_date)
            has_sts_events_result = self.orchestrator.execute_sts_events_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "11", "STS转运不合规风险情况", [sanctioned_sts_result, has_sts_events_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"STS转运不合规风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselStakeholderIsSanctionCheckItem(CompositeCheckItem):
    """船舶相关方涉制裁风险情况复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})
        
        try:
            # 调用lloyds_compliance和has_sanctioned_companies_risk检查项
            lloyds_compliance_result = self.orchestrator.execute_lloyds_compliance_check(vessel_imo, start_date, end_date)
            has_sanctioned_companies_result = self.orchestrator.execute_sanctioned_companies_check(vessel_imo, start_date, end_date)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "8", "船舶相关方涉制裁风险情况", [lloyds_compliance_result, has_sanctioned_companies_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": vessel_imo}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"船舶相关方涉制裁风险情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})


class VesselBunkeringSanctionsCheckItem(CompositeCheckItem):
    """加油船舶制裁情况（复合检查项）
    组合：
    - 船舶是否在制裁名单中（Vessel_is_sanction）
    - 船舶是否在UANI清单中（Vessel_in_uani）
    """
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, start_date: str, end_date: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, start_date=start_date, end_date=end_date, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})

        try:
            # 分别调用两个已存在的复合检查项
            is_sanction_result = self.orchestrator.execute_vessel_is_sanction_check(vessel_imo, start_date, end_date)
            in_uani_result = self.orchestrator.execute_vessel_in_uani_check(vessel_imo)

            # 计算总体风险等级
            risk_levels = [is_sanction_result.risk_value, in_uani_result.risk_value]
            if "高风险" in risk_levels:
                composite_risk_level = RiskLevel.HIGH
            elif "中风险" in risk_levels:
                composite_risk_level = RiskLevel.MEDIUM
            else:
                composite_risk_level = RiskLevel.NO_RISK
            
            # 构建复合tab数据
            composite_tab_data = []
            
            # 添加制裁检查数据
            if is_sanction_result.tab:
                for item in is_sanction_result.tab:
                    item["数据来源"] = "制裁检查"
                    composite_tab_data.append(item)
            
            # 添加UANI检查数据
            if in_uani_result.tab:
                for item in in_uani_result.tab:
                    item["数据来源"] = "UANI检查"
                    composite_tab_data.append(item)
            
            # 创建复合检查结果
            composite_result = self.create_check_result(
                composite_risk_level, 
                composite_tab_data, 
                {"0": vessel_imo}
            )
            
            # 设置复合检查的特殊字段
            composite_result.risk_type = "Vessel_bunkering_sanctions"
            composite_result.risk_desc = "加油船舶制裁情况"
            
            return composite_result
        except Exception as e:
            self.logger.error(f"加油船舶制裁情况复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": vessel_imo})

class CargoOriginFromSanctionedCountryCheckItem(CompositeCheckItem):
    """货物原产地是否来自于制裁国家复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["country_name"]
    
    def check(self, country_name: str, **kwargs) -> CheckResult:
        if not self.validate_params(country_name=country_name, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
        
        try:
            # 调用cargo_country检查项
            cargo_country_result = self.orchestrator.execute_cargo_country_check(country_name)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "12", "货物原产地是否来自于制裁国家", [cargo_country_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": country_name}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"货物原产地是否来自于制裁国家复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})


class PortOriginFromSanctionedCountryCheckItem(CompositeCheckItem):
    """港口是否来自于制裁国家复合检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["country_name"]
    
    def check(self, country_name: str, **kwargs) -> CheckResult:
        if not self.validate_params(country_name=country_name, **kwargs):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})
        
        try:
            # 调用port_country检查项
            port_country_result = self.orchestrator.execute_port_country_check(country_name)
            
            # 构建复合结果
            composite_result = self._build_composite_result(
                "12", "港口是否来自于制裁国家", [port_country_result]
            )
            
            # 添加vessel_imo字段
            composite_result["vessel_imo"] = {"0": country_name}
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"港口是否来自于制裁国家复合检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": country_name})


class DowJonesSanctionsRiskCheckItem(CompositeCheckItem):
    """道琼斯制裁风险检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["entity_name"]
    
    def check(self, entity_name_or_list, **kwargs) -> CheckResult:
        """检查道琼斯制裁风险
        
        Args:
            entity_name_or_list: 单个实体名称(str)或实体名称列表(List[str])
        """
        # 判断输入类型
        if isinstance(entity_name_or_list, str):
            # 单个实体名称
            return self._check_single_entity(entity_name_or_list)
        elif isinstance(entity_name_or_list, list):
            # 实体名称列表
            return self._check_multiple_entities(entity_name_or_list)
        else:
            # 无效输入
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(entity_name_or_list)})
    
    def _check_single_entity(self, entity_name: str) -> CheckResult:
        """检查单个实体的道琼斯制裁风险"""
        if not self.validate_params(entity_name=entity_name):
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": entity_name})
        
        try:
            # 调用道琼斯制裁风险查询函数
            sanctions_data = query_sanctions_risk(entity_name)
            
            # 根据风险等级确定风险级别
            risk_status = sanctions_data.get("risk_screening_status", "无风险")
            if risk_status == "高风险":
                risk_level = RiskLevel.HIGH
            elif risk_status == "中风险":
                risk_level = RiskLevel.MEDIUM
            else:
                risk_level = RiskLevel.NO_RISK
            
            # 构建tab数据
            tab_data = []
            risk_status_reason = sanctions_data.get("risk_status_reason", {})
            
            # 添加制裁列表信息
            sanctions_list = risk_status_reason.get("sanctions_list", [])
            mid_sanctions_list = risk_status_reason.get("mid_sanctions_list", [])
            no_sanctions_list = risk_status_reason.get("no_sanctions_list", [])
            
            if sanctions_list:
                tab_data.append({
                    "制裁列表": sanctions_list,
                    "风险等级": "高风险"
                })
            
            if mid_sanctions_list:
                tab_data.append({
                    "中等制裁列表": mid_sanctions_list,
                    "风险等级": "中风险"
                })
            
            if no_sanctions_list:
                tab_data.append({
                    "无制裁列表": no_sanctions_list,
                    "风险等级": "无风险"
                })
            
            # 创建CheckResult对象
            check_result = CheckResult(
                risk_type="dowjones_sanctions_risk",
                risk_desc="道琼斯制裁风险检查",
                risk_value=risk_status,
                info=f"风险判定为: {risk_status}",
                risk_desc_info=f"风险描述: 道琼斯制裁风险检查",
                tab=tab_data,
                vessel_imo={"0": entity_name},
                risk_screening_time=sanctions_data.get("risk_screening_time", ""),
                risk_status_change_content=sanctions_data.get("risk_status_change_content", ""),
                risk_status_change_time=sanctions_data.get("risk_status_change_time", ""),
                risk_status_reason=risk_status_reason
            )
            
            return check_result
            
        except Exception as e:
            self.logger.error(f"道琼斯制裁风险检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": entity_name})
    
    def _check_multiple_entities(self, entity_names: List[str]) -> CheckResult:
        """检查多个实体的道琼斯制裁风险"""
        if not entity_names:
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": ""})
        
        try:
            # 存储所有实体的检查结果
            all_results = []
            highest_risk_level = RiskLevel.NO_RISK
            combined_tab_data = []
            
            # 逐个检查每个实体
            for entity_name in entity_names:
                if not entity_name or not entity_name.strip():
                    continue
                    
                # 调用单个实体检查
                sanctions_data = query_sanctions_risk(entity_name.strip())
                
                # 确定风险级别
                risk_status = sanctions_data.get("risk_screening_status", "无风险")
                if risk_status == "高风险":
                    entity_risk_level = RiskLevel.HIGH
                elif risk_status == "中风险":
                    entity_risk_level = RiskLevel.MEDIUM
                else:
                    entity_risk_level = RiskLevel.NO_RISK
                
                # 更新最高风险级别
                if entity_risk_level == RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.HIGH
                elif entity_risk_level == RiskLevel.MEDIUM and highest_risk_level != RiskLevel.HIGH:
                    highest_risk_level = RiskLevel.MEDIUM
                
                # 构建该实体的tab数据
                risk_status_reason = sanctions_data.get("risk_status_reason", {})
                sanctions_list = risk_status_reason.get("sanctions_list", [])
                mid_sanctions_list = risk_status_reason.get("mid_sanctions_list", [])
                no_sanctions_list = risk_status_reason.get("no_sanctions_list", [])
                
                entity_tab_data = {
                    "实体名称": entity_name,
                    "风险等级": risk_status,
                    "检查时间": sanctions_data.get("risk_screening_time", ""),
                    "风险描述": sanctions_data.get("risk_description", "")
                }
                
                if sanctions_list:
                    entity_tab_data["高风险制裁列表"] = sanctions_list
                if mid_sanctions_list:
                    entity_tab_data["中风险制裁列表"] = mid_sanctions_list
                if no_sanctions_list:
                    entity_tab_data["无风险制裁列表"] = no_sanctions_list
                
                combined_tab_data.append(entity_tab_data)
                all_results.append({
                    "entity_name": entity_name,
                    "risk_level": entity_risk_level,
                    "risk_status": risk_status,
                    "sanctions_data": sanctions_data
                })
            
            # 构建复合结果
            composite_result = CheckResult(
                risk_type="dowjones_sanctions_risk",
                risk_desc="道琼斯制裁风险检查（多实体）",
                risk_value=highest_risk_level.value,
                info=f"检查了{len(entity_names)}个实体，最高风险等级为{highest_risk_level.value}",
                risk_desc_info=f"风险描述: 道琼斯制裁风险检查（多实体）",
                tab=combined_tab_data,
                vessel_imo={"0": f"多实体检查({len(entity_names)}个实体)"},
                risk_screening_time=all_results[0]["sanctions_data"].get("risk_screening_time", "") if all_results else "",
                risk_status_change_content=all_results[0]["sanctions_data"].get("risk_status_change_content", "") if all_results else "",
                risk_status_change_time=all_results[0]["sanctions_data"].get("risk_status_change_time", "") if all_results else "",
                risk_status_reason={"all_results": all_results}
            )
            
            return composite_result
            
        except Exception as e:
            self.logger.error(f"道琼斯制裁风险检查（多实体）失败: {e}")
            return self.create_check_result(RiskLevel.NO_RISK, [], {"0": str(entity_names)})
    

def query_sanctions_risk(entity_name: str) -> dict:
    """
    查询实体制裁风险等级和制裁列表
    
    Args:
        entity_name: 实体名称
        
    Returns:
        dict: 包含制裁风险等级和制裁列表的字典
        {
            "name": str,  # 实体名称
            "risk_screening_status": str,  # 制裁风险等级
            "risk_screening_time": str,  # 当前时间
            "risk_status_change_content": str,  # 风险状态变更内容
            "risk_status_change_time": str,  # 风险状态变更时间
            "risk_type_number": int,  # 风险类型编号
            "risk_description": str,  # 风险描述
            "risk_info": None,  # 风险信息
            "risk_status_reason": dict  # 风险状态原因
        }
    """
    try:
        # 导入数据库配置
        from kingbase_config import KINGBASE_CONFIG
        import psycopg2
        from datetime import datetime
        
        # 建立KingBase数据库连接
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        
        with connection.cursor() as cursor:
            # 执行查询，获取制裁列表字段和新增的风险字段
            sql = """SELECT entity_id, ENTITYNAME1, sanctions_lev, 
                            sanctions_list, mid_sanctions_list, no_sanctions_list, unknown_risk_list,
                            is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries
                     FROM lng.sanctions_risk_result WHERE ENTITYNAME1 = %s"""
            cursor.execute(sql, (entity_name,))
            result = cursor.fetchone()
            
            # 获取当前时间
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if result:
                # 解析JSON字符串字段为对象
                def _parse_json_array(value):
                    try:
                        if value is None:
                            return []
                        if isinstance(value, list):
                            return value
                        if isinstance(value, str) and value.strip() == "":
                            return []
                        return json.loads(value)
                    except Exception:
                        return []

                parsed_sanctions_list = _parse_json_array(result.get('sanctions_list'))
                parsed_mid_sanctions_list = _parse_json_array(result.get('mid_sanctions_list'))
                parsed_no_sanctions_list = _parse_json_array(result.get('no_sanctions_list'))
                parsed_unknown_risk_list = _parse_json_array(result.get('unknown_risk_list'))
                # 固定 unknown_risk_list 的 tab 为 []
                if isinstance(parsed_unknown_risk_list, list):
                    for item in parsed_unknown_risk_list:
                        try:
                            if isinstance(item, dict):
                                item['tab'] = []
                        except Exception:
                            pass
                # 构建风险描述
                risk_description_parts = []
                if result.get('is_san'):
                    risk_description_parts.append(f"{result['is_san']}")
                if result.get('is_sco'):
                    risk_description_parts.append(f"{result['is_sco']}")
                if result.get('is_ool'):
                    risk_description_parts.append(f"{result['is_ool']}")
                
                risk_description = f"在道琼斯的判定为：{', '.join(risk_description_parts)}" if risk_description_parts else ""
                
                # 构建风险状态原因（直接使用数据库JSON字段）
                risk_status_reason = {
                    "sanctions_list": parsed_sanctions_list,
                    "mid_sanctions_list": parsed_mid_sanctions_list,
                    "no_sanctions_list": parsed_no_sanctions_list,
                    "unknown_risk_list": parsed_unknown_risk_list,
                    "is_san": result.get('is_san', ''),
                    "is_sco": result.get('is_sco', ''),
                    "is_ool": result.get('is_ool', ''),
                    "is_one_year": result.get('is_one_year', ''),
                    "is_sanctioned_countries": result.get('is_sanctioned_countries', '')
                }
                
                return {
                    "name": entity_name,
                    "risk_screening_status": result.get('sanctions_lev', '无风险'),
                    "risk_screening_time": current_time,
                    "risk_status_change_content": "",
                    "risk_status_change_time": "",
                    "risk_type_number": 1,
                    "risk_description": risk_description,
                    "risk_info": None,
                    "risk_status_reason": risk_status_reason
                }
            else:
                # 未找到记录时的默认返回
                return {
                    "name": entity_name,
                    "risk_screening_status": "无风险",
                    "risk_screening_time": current_time,
                    "risk_status_change_content": "",
                    "risk_status_change_time": "",
                    "risk_type_number": 1,
                    "risk_description": "",
                    "risk_info": None,
                    "risk_status_reason": {
                        "sanctions_list": [],
                        "mid_sanctions_list": [],
                        "no_sanctions_list": [],
                        "is_san": "",
                        "is_sco": "",
                        "is_ool": "",
                        "is_one_year": "",
                        "is_sanctioned_countries": ""
                    }
                }
                
    except Exception as e:
        print(f"查询制裁风险时出错: {e}")
        # 出错时的默认返回
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "name": entity_name,
            "risk_screening_status": "无风险",
            "risk_screening_time": current_time,
            "risk_status_change_content": "",
            "risk_status_change_time": "",
            "risk_type_number": 1,
            "risk_description": "",
            "risk_info": None,
            "risk_status_reason": {
                "sanctions_list": [],
                "mid_sanctions_list": [],
                "no_sanctions_list": [],
                "is_san": "",
                "is_sco": "",
                "is_ool": "",
                "is_one_year": "",
                "is_sanctioned_countries": ""
            }
        }
    finally:
        if 'connection' in locals():
            connection.close()


if __name__ == "__main__":
    main()
