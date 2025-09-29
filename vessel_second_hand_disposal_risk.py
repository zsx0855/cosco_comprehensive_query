from fastapi import APIRouter, HTTPException, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta
import pytz
import psycopg2
from psycopg2.extras import DictCursor, Json
from psycopg2 import OperationalError, IntegrityError
import logging
import json
import requests
import uvicorn
import os

# 导入外部API函数和配置生成器
from kingbase_config import get_kingbase_config
from functions_risk_check_framework import RiskCheckOrchestrator, create_api_config, CheckResult, RiskLevel
from functions_sanctions_des_info_manager import SanctionsDesInfoManager

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Vessel_Second_Hand_Disposal_Risk_Screen")
api_logger = logging.getLogger("API_Requests")

# 路由器
second_hand_router = APIRouter(prefix="/second_hand", tags=["船舶二手处置合规筛查"])

# 配置加载
DB_CONFIG_RAW = get_kingbase_config()
DB_CONFIG = {
    "host": DB_CONFIG_RAW.get("host"),
    "port": DB_CONFIG_RAW.get("port"),
    "user": DB_CONFIG_RAW.get("user"),
    "password": DB_CONFIG_RAW.get("password"),
    "dbname": DB_CONFIG_RAW.get("database")
}

API_CONFIG = create_api_config()

# 验证核心配置
REQUIRED_CONFIG_KEYS = [
    'lloyds_base_url', 
    'lloyds_headers', 
    'kpler_api_url', 
    'kpler_base_url', 
    'kpler_headers'
]
for key in REQUIRED_CONFIG_KEYS:
    if key not in API_CONFIG:
        raise ValueError(f"API配置缺失关键项: {key}")
if 'Authorization' not in API_CONFIG['lloyds_headers']:
    raise ValueError("劳氏接口配置缺少Authorization头信息")

# 表名定义
SCREENING_LOG_TABLE = "lng.vessel_second_hand_disposal_risk_log"
APPROVAL_RECORDS_TABLE = "lng.second_hand_approval_records"
RISK_RESULT_TABLE = "lng.vessel_second_hand_disposal_risk_result"
CHANGE_RECORDS_TABLE = "lng.second_hand_risk_change_records"

# 检查项类型映射关系
CHECK_ITEM_MAPPING = {
    "intent_transferee": "second_hand_intent_transferee",
    "intent_transferee_shareholder": "second_hand_transferee_shareholder",
    "intent_transferee_controller": "second_hand_transferee_controller",
    "intent_vessel_manager": "second_hand_vessel_manager",
    "intent_vessel_manager_shareholder": "second_hand_manager_shareholder",
    "intent_vessel_manager_controller": "second_hand_manager_controller",
    "vessel_operator": "second_hand_vessel_operator",
    "agent": "second_hand_agent",
    "vessel_owner_supplied": "second_hand_owner_supplied",
    "vessel_manager_supplied": "second_hand_manager_supplied"
}

# ---------------------- 1. 数据模型定义 ----------------------
class StakeholderRisk(BaseModel):
    """基础相关方风险模型"""
    name: str
    risk_screening_status: str  # 高风险/中风险/低风险/无风险
    risk_screening_time: str  # 筛查时间（YYYY-MM-DDTHH:MM:SSZ）
    risk_status_change_content: str = ""
    risk_status_change_time: str = ""
    risk_type_number: int  # 风险类型编号
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class VesselStakeholderSanction(BaseModel):
    """船舶相关方制裁模型"""
    Vessel_stakeholder_type: Optional[str] = None
    name: Optional[str] = None
    risk_screening_status: Optional[str] = None
    risk_screening_time: Optional[str] = None
    risk_status_change_content: Optional[str] = None
    risk_status_change_time: Optional[str] = None
    risk_type_number: int  # 风险类型编号
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class FleetRiskItem(BaseModel):
    """船队风险项模型"""
    Vessel_name: str
    Vessel_imo: str
    risk_screening_status: Optional[str] = None
    risk_screening_time: Optional[str] = None
    risk_status_change_content: Optional[str] = None
    risk_status_change_time: Optional[str] = None
    risk_type_number: int  # 风险类型编号
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class VesselRiskItem(BaseModel):
    """船舶风险项模型"""
    risk_screening_status: Optional[str] = None
    risk_screening_time: Optional[str] = None
    risk_status_change_content: Optional[str] = None
    risk_status_change_time: Optional[str] = None
    risk_type_number: int  # 风险类型编号
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class SecondHandDisposalRequest(BaseModel):
    """二手船处置接口请求模型"""
    Uuid: str = Field(..., alias="uuid")
    Process_id: Optional[str] = Field(None, alias="process_id")
    Process_operator_id: Optional[str] = Field(None, alias="process_operator_id")
    Process_operator: Optional[str] = Field(None, alias="process_operator_name")
    Process_start_time: Optional[datetime] = Field(None, alias="process_start_time")
    Process_end_time: Optional[datetime] = Field(None, alias="process_end_time")
    Process_status: Optional[str] = Field(None, alias="process_status")
    
    # 船舶基础信息
    Vessel_name: str = Field(..., alias="vessel_name")
    Vessel_imo: str = Field(..., alias="vessel_imo")
    
    # 相关方信息
    intent_transferee: str = Field(..., alias="intent_transferee")
    intent_transferee_shareholder: List[str] = Field(..., alias="intent_transferee_shareholder")
    intent_transferee_controller: str = Field(..., alias="intent_transferee_controller")
    intent_vessel_manager: str = Field(..., alias="intent_vessel_manager")
    intent_vessel_manager_shareholder: List[str] = Field(..., alias="intent_vessel_manager_shareholder")
    vessel_manager_controller: str = Field(..., alias="vessel_manager_controller")
    vessel_operator: str = Field(..., alias="vessel_operator")
    agent: List[str] = Field(..., alias="agent")
    intent_transferee_fleet_supplied: List[Dict[str, str]] = Field(..., alias="intent_transferee_fleet_supplied")
    vessel_owner_supplied: List[str] = Field(..., alias="vessel_owner_supplied")
    vessel_manager_supplied: List[str] = Field(..., alias="vessel_manager_supplied")

    @field_validator("Process_start_time", "Process_end_time", mode="before")
    def parse_time(cls, v):
        if v is None:
            return None
        try:
            return datetime.strptime(v, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                try:
                    return datetime.strptime(v, "%Y/%m/%d")
                except ValueError:
                    raise ValueError("时间格式必须为'YYYY/MM/DD HH:MM:SS'、'YYYY-MM-DD'或'YYYY/MM/DD'")

    class Config:
        populate_by_name = True


class SecondHandDisposalResponse(BaseModel):
    """二手船处置接口响应模型"""
    Uuid: str
    Process_id: Optional[str] = None
    Vessel_name: str
    Vessel_imo: str
    
    # 风控状态字段
    Project_risk_status: str  # 项目风控状态：拦截/关注/正常
    Vessel_risk_status: str   # 船舶风控状态：高风险/中风险/无风险
    Stakeholder_risk_status: str  # 相关方风控状态：高风险/中风险/无风险

    # 相关方风险结果
    intent_transferee: StakeholderRisk
    intent_transferee_shareholder: List[StakeholderRisk]
    intent_transferee_controller: StakeholderRisk
    intent_vessel_manager: StakeholderRisk
    intent_vessel_manager_shareholder: List[StakeholderRisk]
    intent_vessel_manager_controller: StakeholderRisk
    Vessel_operator: StakeholderRisk
    Agent: List[StakeholderRisk]

    # 船队风险筛查结果
    Vessel_seller_fleet_supplied_Lloyd: List[FleetRiskItem]
    Vessel_seller_fleet_supplied_kpler: List[FleetRiskItem]
    Vessel_seller_fleet_supplied_uani: List[FleetRiskItem]

    # 提供方制裁风险
    Vessel_owner_supplied_sanction: List[StakeholderRisk]
    Vessel_manager_supplied_sanction: List[StakeholderRisk]

    # 其他船舶风险项
    Vessel_is_sanction: Optional[VesselRiskItem] = None
    Vessel_history_is_sanction: Optional[VesselRiskItem] = None
    Vessel_in_uani: Optional[VesselRiskItem] = None
    Vessel_risk_level_lloyd: Optional[VesselRiskItem] = None
    Vessel_risk_level_kpler: Optional[VesselRiskItem] = None
    Vessel_ais_gap: Optional[VesselRiskItem] = None
    Vessel_Manipulation: Optional[VesselRiskItem] = None
    Vessel_risky_port_call: Optional[VesselRiskItem] = None
    Vessel_dark_port_call: Optional[VesselRiskItem] = None
    Vessel_cargo_sanction: Optional[VesselRiskItem] = None
    Vessel_trade_sanction: Optional[VesselRiskItem] = None
    Vessel_dark_sts_events: Optional[VesselRiskItem] = None
    Vessel_sts_transfer: Optional[VesselRiskItem] = None


class OptimizedRiskCheckOrchestrator(RiskCheckOrchestrator):
    """优化的风险检查编排器"""
    
    def __init__(self, api_config: Dict[str, Any], info_manager=None):
        super().__init__(api_config, info_manager)
        self._data_cache = {}
        if not self.api_config.get('lloyds_headers'):
            raise ValueError("API配置中缺少劳氏接口认证头(lloyds_headers)")
        if not self.api_config.get('lloyds_base_url'):
            raise ValueError("API配置中缺少劳氏基础URL(lloyds_base_url)")
    
    def fetch_all_data_once(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """一次性获取所有需要的数据"""
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"日期格式错误，应为YYYY-MM-DD，实际为{start_date}和{end_date}")
        
        logger.info(f"🔄 开始批量获取数据 - 船舶: {vessel_imo}, 时间: {start_date} - {end_date}")
        
        cache_key = f"{vessel_imo}_{start_date}_{end_date}"
        if cache_key in self._data_cache:
            logger.info("✅ 使用缓存数据")
            return self._data_cache[cache_key]
        
        all_data = {}
        
        # 获取劳氏数据
        logger.info("📡 正在获取劳氏数据...")
        try:
            lloyds_data = self._fetch_all_lloyds_data(vessel_imo, start_date, end_date)
            all_data['lloyds'] = lloyds_data
            logger.info("✅ 劳氏数据获取完成")
        except Exception as e:
            logger.error(f"❌ 劳氏数据获取失败: {str(e)}")
            all_data['lloyds'] = {}
        
        # 获取开普勒数据
        logger.info("📡 正在获取开普勒数据...")
        try:
            kpler_data = self._fetch_kpler_data(vessel_imo, start_date, end_date)
            all_data['kpler'] = kpler_data
            logger.info("✅ 开普勒数据获取完成")
        except Exception as e:
            logger.error(f"❌ 开普勒数据获取失败: {str(e)}")
            all_data['kpler'] = {}
        
        # 获取UANI数据
        logger.info("📡 正在获取UANI数据...")
        try:
            uani_data = self._fetch_uani_data(vessel_imo)
            all_data['uani'] = uani_data
            logger.info("✅ UANI数据获取完成")
        except Exception as e:
            logger.error(f"❌ UANI数据获取失败: {str(e)}")
            all_data['uani'] = {}
        
        # 缓存数据
        self._data_cache[cache_key] = all_data
        logger.info(f"✅ 所有数据获取完成")
        
        # 确保关键字段有默认值
        if not all_data.get('lloyds', {}).get('sanctions'):
            all_data['lloyds']['sanctions'] = {"is_sanctioned": False, "risk_level": "无风险"}
        if not all_data.get('kpler', {}).get('sanctions'):
            all_data['kpler']['sanctions'] = {"is_sanctioned": False, "risk_level": "无风险"}
        if not all_data.get('lloyds', {}).get('risk_score'):
            all_data['lloyds']['risk_score'] = {"risk_level": "无风险"}
        if not all_data.get('kpler', {}).get('risk_score'):
            all_data['kpler']['risk_score'] = {"risk_level": "无风险"}
        
        return all_data
    
    def _fetch_all_lloyds_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取所有劳氏数据"""
        lloyds_data = {}
        
        # 劳氏合规接口
        compliance_url = f"{self.api_config['lloyds_base_url']}/vesselcompliancescreening_v3"
        compliance_params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        try:
            api_logger.debug(
                f"劳氏合规接口请求 - URL: {compliance_url}, "
                f"参数: {compliance_params}, "
                f"请求头: { {k: v for k, v in self.api_config['lloyds_headers'].items() if k != 'Authorization'} }"
            )
            
            compliance_response = requests.get(
                compliance_url, 
                headers=self.api_config['lloyds_headers'],
                params=compliance_params, 
                timeout=60
            )
            
            api_logger.debug(f"劳氏合规接口响应 - 状态码: {compliance_response.status_code}")
            
            if compliance_response.status_code == 403:
                error_msg = (
                    f"劳氏接口权限拒绝(403) - 可能原因: 认证失效、IP限制、权限不足。"
                    f"响应内容: {compliance_response.text[:500]}"
                )
                api_logger.error(error_msg)
                raise Exception(error_msg)
            
            compliance_response.raise_for_status()
            lloyds_data['compliance'] = compliance_response.json()
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"劳氏合规接口HTTP错误: {str(e)}, 响应内容: {compliance_response.text[:500]}"
            api_logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            api_logger.error(f"劳氏合规接口调用失败: {str(e)}")
            lloyds_data['compliance'] = {}
        
        # 劳氏风险等级接口
        risk_url = f"{self.api_config['lloyds_base_url']}/vesselriskscore"
        risk_params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        try:
            risk_response = requests.get(
                risk_url, 
                headers=self.api_config['lloyds_headers'],
                params=risk_params, 
                timeout=60
            )
            risk_response.raise_for_status()
            lloyds_data['risk_score'] = risk_response.json()
        except Exception as e:
            logger.error(f"❌ 劳氏风险等级接口调用失败: {str(e)}")
            lloyds_data['risk_score'] = {}
        
        # 劳氏制裁接口
        sanctions_url = f"{self.api_config['lloyds_base_url']}/vesselsanctions_v2"
        sanctions_params = {"vesselImo": vessel_imo}
        
        try:
            sanctions_response = requests.get(
                sanctions_url, 
                headers=self.api_config['lloyds_headers'],
                params=sanctions_params, 
                timeout=60
            )
            sanctions_response.raise_for_status()
            lloyds_data['sanctions'] = sanctions_response.json()
        except Exception as e:
            logger.error(f"❌ 劳氏制裁接口调用失败: {str(e)}")
            lloyds_data['sanctions'] = {}
        
        # AIS信号伪造及篡改接口
        ais_manipulation_url = f"{self.api_config['lloyds_base_url']}/vesseladvancedcompliancerisk_v3"
        ais_manipulation_params = {"vesselImo": vessel_imo}
        
        try:
            ais_manipulation_response = requests.get(
                ais_manipulation_url, 
                headers=self.api_config['lloyds_headers'],
                params=ais_manipulation_params, 
                timeout=120
            )
            ais_manipulation_response.raise_for_status()
            lloyds_data['ais_manipulation'] = ais_manipulation_response.json()
        except Exception as e:
            logger.error(f"❌ 劳氏AIS信号接口调用失败: {str(e)}")
            lloyds_data['ais_manipulation'] = {}
        
        # 航次事件接口
        voyage_url = f"{self.api_config['lloyds_base_url']}/vesselvoyageevents"
        voyage_params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date}-{end_date}"
        }
        
        try:
            voyage_response = requests.get(
                voyage_url, 
                headers=self.api_config['lloyds_headers'],
                params=voyage_params, 
                timeout=120
            )
            voyage_response.raise_for_status()
            lloyds_data['voyage_events'] = voyage_response.json()
        except Exception as e:
            logger.error(f"❌ 劳氏航次事件接口调用失败: {str(e)}")
            lloyds_data['voyage_events'] = {}
        
        return lloyds_data
    
    def _fetch_kpler_data(self, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """获取开普勒数据"""
        kpler_data = {}
        
        # 开普勒综合数据
        vessel_risks_url = self.api_config['kpler_api_url']
        vessel_risks_params = {
            "startDate": start_date,
            "endDate": end_date,
            "accept": "application/json"
        }
        
        imos = [int(vessel_imo)] if vessel_imo and vessel_imo.isdigit() else []
        
        try:
            vessel_risks_response = requests.post(
                vessel_risks_url,
                params=vessel_risks_params,
                headers=self.api_config['kpler_headers'],
                json=imos,
                timeout=120
            )
            vessel_risks_response.raise_for_status()
            kpler_data['vessel_risks'] = vessel_risks_response.json()
        except Exception as e:
            logger.error(f"❌ 开普勒综合数据接口调用失败: {str(e)}")
            kpler_data['vessel_risks'] = []
        
        # 开普勒合规筛查数据
        compliance_screening_url = f"{self.api_config['kpler_base_url']}/compliance/compliance-screening"
        compliance_screening_params = {
            "vessels": vessel_imo
        }
        
        try:
            compliance_screening_response = requests.get(
                compliance_screening_url,
                params=compliance_screening_params,
                headers=self.api_config['kpler_headers'],
                timeout=60
            )
            compliance_screening_response.raise_for_status()
            kpler_data['compliance_screening'] = compliance_screening_response.json()
        except Exception as e:
            logger.error(f"❌ 开普勒合规筛查接口调用失败: {str(e)}")
            kpler_data['compliance_screening'] = {}
        
        return kpler_data
    
    def _fetch_uani_data(self, vessel_imo: str) -> Dict[str, Any]:
        """获取UANI数据（数据库查询）"""
        try:
            from maritime_api import check_uani_imo_from_database
            exists, data = check_uani_imo_from_database(vessel_imo)
            return {
                "found": exists,
                "data": data
            }
        except Exception as e:
            logger.error(f"❌ UANI数据查询失败: {str(e)}")
            return {"found": False, "data": {}}
    
    def execute_all_checks_optimized(self, vessel_imo: str, start_date: str, end_date: str,
                                    intent_transferee: str, intent_transferee_shareholder: List[str],
                                    intent_transferee_controller: str, intent_vessel_manager: str,
                                    intent_vessel_manager_shareholder: List[str], vessel_manager_controller: str,
                                    vessel_operator: str, agent: List[str], 
                                    intent_transferee_fleet_supplied: List[Dict[str, str]],
                                    vessel_owner_supplied: List[str], vessel_manager_supplied: List[str]) -> List[CheckResult]:
        """执行所有检查项"""
        logger.info(f"\n🚀 开始优化版本风险检查 - 船舶: {vessel_imo}")
        
        all_data = self.fetch_all_data_once(vessel_imo, start_date, end_date)
        
        results = []
        composite_results = self._execute_composite_checks(
            vessel_imo, start_date, end_date, all_data,
            intent_transferee, intent_transferee_shareholder,
            intent_transferee_controller, intent_vessel_manager,
            intent_vessel_manager_shareholder, vessel_manager_controller,
            vessel_operator, agent, intent_transferee_fleet_supplied,
            vessel_owner_supplied, vessel_manager_supplied
        )
        results.extend(composite_results)
        
        logger.info(f"\n✅ 所有复合检查完成，共 {len(results)} 个检查项")
        return results
    
    def _execute_composite_checks(self, vessel_imo: str, start_date: str, end_date: str, all_data: Dict[str, Any],
                                 intent_transferee: str, intent_transferee_shareholder: List[str],
                                 intent_transferee_controller: str, intent_vessel_manager: str,
                                 intent_vessel_manager_shareholder: List[str], vessel_manager_controller: str,
                                 vessel_operator: str, agent: List[str], 
                                 intent_transferee_fleet_supplied: List[Dict[str, str]],
                                 vessel_owner_supplied: List[str], vessel_manager_supplied: List[str]) -> List[CheckResult]:
        """执行复合检查项"""
        results = []
        
        composite_checks = [
            ("意向受让方制裁风险检查", self.execute_dowjones_sanctions_risk_check, [intent_transferee], "intent_transferee_sanctions_risk"),
            ("意向受让方股东制裁风险检查", self.execute_dowjones_sanctions_risk_check, [intent_transferee_shareholder], "intent_transferee_shareholder_sanctions_risk"),
            ("意向受让方实控人制裁风险检查", self.execute_dowjones_sanctions_risk_check, [intent_transferee_controller], "intent_transferee_controller_sanctions_risk"),
            ("意向船管方制裁风险检查", self.execute_dowjones_sanctions_risk_check, [intent_vessel_manager], "intent_vessel_manager_sanctions_risk"),
            ("意向船管方股东制裁风险检查", self.execute_dowjones_sanctions_risk_check, [intent_vessel_manager_shareholder], "intent_vessel_manager_shareholder_sanctions_risk"),
            ("意向船管方实控人制裁风险检查", self.execute_dowjones_sanctions_risk_check, [vessel_manager_controller], "intent_vessel_manager_controller_sanctions_risk"),
            ("船舶经营人制裁风险检查", self.execute_dowjones_sanctions_risk_check, [vessel_operator], "vessel_operator_sanctions_risk"),
            ("经纪人制裁风险检查", self.execute_dowjones_sanctions_risk_check, [agent], "agent_sanctions_risk"),
            ("船舶所有人提供方制裁风险检查", self.execute_dowjones_sanctions_risk_check, [vessel_owner_supplied], "vessel_owner_supplied_sanction"),
            ("船舶管理人提供方制裁风险检查", self.execute_dowjones_sanctions_risk_check, [vessel_manager_supplied], "vessel_manager_supplied_sanction"),
            ("船舶风险等级复合检查(劳氏)", self.execute_vessel_risk_level_check, [vessel_imo, start_date, end_date], "Vessel_risk_level_lloyd"),
            ("船舶风险等级复合检查(Kpler)", self.execute_vessel_risk_level_check, [vessel_imo, start_date, end_date], "Vessel_risk_level_kpler"),
            ("船舶涉制裁名单风险情况(当前)", self.execute_vessel_is_sanction_check, [vessel_imo], "Vessel_is_sanction_current"),
            ("船舶涉制裁名单风险情况(历史)", self.execute_vessel_is_sanction_check, [vessel_imo], "Vessel_is_sanction_history"),
            ("船舶涉UANI清单风险情况", self.execute_vessel_in_uani_check, [vessel_imo], "Vessel_in_uani"),
            ("船舶AIS信号缺失风险情况", self.execute_vessel_ais_gap_check, [vessel_imo, start_date, end_date], "Vessel_ais_gap"),
            ("船舶AIS信号伪造及篡改风险情况", self.execute_vessel_manipulation_check, [vessel_imo, start_date, end_date], "Vessel_Manipulation"),
            ("船舶挂靠高风险港口风险情况", self.execute_vessel_risky_port_call_check, [vessel_imo, start_date, end_date], "Vessel_risky_port_call"),
            ("船舶暗港访问风险情况", self.execute_vessel_dark_port_call_check, [vessel_imo, start_date, end_date], "Vessel_dark_port_call"),
            ("船舶运输受制裁货物风险情况", self.execute_vessel_cargo_sanction_check, [vessel_imo, start_date, end_date], "Vessel_cargo_sanction"),
            ("船舶相关方涉制裁风险情况", self.execute_vessel_stakeholder_is_sanction_check, [vessel_imo, start_date, end_date], "Vessel_stakeholder_is_sanction"),
            ("船舶暗STS事件风险情况", self.execute_vessel_dark_sts_events_check, [vessel_imo, start_date, end_date], "Vessel_dark_sts_events"),
            ("船舶STS转运风险情况", self.execute_vessel_sts_transfer_check, [vessel_imo, start_date, end_date], "Vessel_sts_transfer"),
            ("船舶涉及受制裁贸易风险情况", self.execute_vessel_trade_sanction_check, [vessel_imo, start_date, end_date], "Vessel_trade_sanction"),
        ]
        
        # 添加船队相关检查
        for idx, fleet_vessel in enumerate(intent_transferee_fleet_supplied):
            fleet_imo = fleet_vessel.get('Vessel_imo', '')
            fleet_name = fleet_vessel.get('Vessel_name', f'Fleet_Vessel_{idx+1}')
            
            composite_checks.extend([
                (f"船队船舶 {fleet_name} (IMO: {fleet_imo}) 劳氏风险检查", 
                 self.execute_vessel_risk_level_check, 
                 [fleet_imo, start_date, end_date], 
                 f"fleet_lloyd_risk_{idx}"),
                (f"船队船舶 {fleet_name} (IMO: {fleet_imo}) 开普勒风险检查", 
                 self.execute_vessel_risk_level_check, 
                 [fleet_imo, start_date, end_date], 
                 f"fleet_kpler_risk_{idx}"),
                (f"船队船舶 {fleet_name} (IMO: {fleet_imo}) UANI风险检查", 
                 self.execute_vessel_in_uani_check, 
                 [fleet_imo], 
                 f"fleet_uani_risk_{idx}")
            ])
        
        for check_name, execute_func, args, risk_type_name in composite_checks:
            try:
                result = execute_func(*args)
                if isinstance(result, dict):
                    result["risk_type"] = risk_type_name
                results.append(result)
                risk_value = result.get("risk_screening_status", "未知") if isinstance(result, dict) else result.risk_value
                logger.info(f"✅ {check_name}完成: {risk_value}")
            except Exception as e:
                logger.error(f"❌ {check_name}失败: {str(e)}")
        
        return results


# 初始化编排器
try:
    info_manager = SanctionsDesInfoManager()
    risk_orchestrator = OptimizedRiskCheckOrchestrator(API_CONFIG, info_manager)
    logger.info("风险检查编排器初始化成功")
except Exception as e:
    logger.error(f"风险检查编排器初始化失败: {str(e)}", exc_info=True)
    raise
    
# ---------------------- 2. 工具函数 ----------------------
def parse_json_safely(json_str: Optional[str]) -> Union[List, Dict, None]:
    """安全解析JSON字符串"""
    if not json_str or json_str in ("null", "None"):
        return None
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"JSON解析失败: {str(e)}, 原始数据: {str(json_str)[:100]}")
        return None


def get_sanction_info(name: str) -> Dict[str, Any]:
    """查询相关方制裁信息"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute("""
                           SELECT sanctions_lev,
                                  sanctions_list,
                                  mid_sanctions_list,
                                  no_sanctions_list,
                                  is_san,
                                  is_sco,
                                  is_one_year,
                                  is_sanctioned_countries
                           FROM lng.sanctions_risk_result
                           WHERE ENTITYNAME1 ILIKE %s
                           LIMIT 1
                           """, (f"%{name}%",))
            result = cursor.fetchone()
            if result:
                return {
                    "sanctions_lev": result[0] or "无风险",
                    "reason": {
                        "sanctions_list": parse_json_safely(result[1]),
                        "mid_sanctions_list": parse_json_safely(result[2]),
                        "no_sanctions_list": parse_json_safely(result[3]),
                        "is_san": result[4],
                        "is_sco": result[5],
                        "is_one_year": result[6],
                        "is_sanctioned_countries": result[7]
                    }
                }
            return {"sanctions_lev": "无风险", "reason": {}}
    except psycopg2.Error as e:
        logger.error(f"相关方制裁信息查询失败（{name}）: {str(e)}")
        return {"sanctions_lev": "无风险", "reason": {}}
    finally:
        if conn:
            conn.close()


def get_sanction_desc_and_info(
        check_item_keyword: str,
        risk_type: str,
        queried_risk_level: Optional[str] = None
) -> Dict[str, Any]:
    """从数据库查询风险描述和详情"""
    conn = None
    default_result = {"risk_desc_info": "", "info": None}

    try:
        if not risk_type:
            logger.warning("风险类型（risk_type）为空，无法查询风险描述")
            return default_result

        connect_params = DB_CONFIG.copy()
        connect_params["cursor_factory"] = DictCursor
        conn = psycopg2.connect(** connect_params)

        sql = """
              SELECT risk_desc_info, info
              FROM lng.sanctions_des_info
              WHERE risk_type = %s
            """
        sql_params = [risk_type]

        if queried_risk_level and queried_risk_level.strip():
            sql += " AND risk_level ILIKE %s"
            sql_params.append(f"%{queried_risk_level.strip()}%")

        sql += " LIMIT 1"

        with conn.cursor() as cursor:
            cursor.execute(sql, sql_params)
            query_result = cursor.fetchone()

            if query_result:
                return {
                    "risk_desc_info": query_result["risk_desc_info"] or "",
                    "info": parse_json_safely(query_result["info"])
                }
        return default_result

    except psycopg2.Error as e:
        logger.error(
            f"查询 lng.sanctions_des_info 失败："
            f"表中risk_type={risk_type}, 已查询的风险等级={queried_risk_level}, "
            f"错误={str(e)}"
        )
        return default_result
    finally:
        if conn:
            conn.close()


def get_current_local_time() -> str:
    """获取上海时区时间"""
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    local_time = datetime.now(shanghai_tz)
    return local_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_one_year_ago() -> datetime:
    """获取一年前的当前时间"""
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    return datetime.now(shanghai_tz) - timedelta(days=365)


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """解析时间戳"""
    try:
        timestamp = float(timestamp_str)
        if timestamp > 1e12:
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp, pytz.timezone('Asia/Shanghai'))
    except (ValueError, TypeError):
        return None


def parse_datetime_str(dt_str: str) -> Optional[datetime]:
    """解析ISO格式时间字符串"""
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00')).astimezone(pytz.timezone('Asia/Shanghai'))
    except (ValueError, TypeError):
        return None


def format_time(dt: Optional[datetime]) -> Optional[str]:
    """转换datetime为上海时区字符串"""
    if not dt:
        return None
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    local_time = dt.astimezone(shanghai_tz)
    return local_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_stakeholder_risk(risk_item: Union[Dict, Any], default_name: str = "") -> Dict:
    """标准化相关方风险结果"""
    if not isinstance(risk_item, dict):
        risk_item = risk_item.__dict__ if hasattr(risk_item, '__dict__') else {}
    
    name = risk_item.get("name", default_name)
    if isinstance(name, list):
        name = name[0] if name else default_name
        if isinstance(name, list):
            name = name[0] if name else default_name
    name = str(name) if name is not None else default_name
    
    current_time = get_current_local_time()
    risk_type_mapping = {
        "intent_transferee": 1,
        "intent_transferee_shareholder": 2,
        "intent_transferee_controller": 3,
        "intent_vessel_manager": 4,
        "intent_vessel_manager_shareholder": 5,
        "intent_vessel_manager_controller": 6,
        "vessel_operator": 7,
        "agent": 8,
        "vessel_owner_supplied": 9,
        "vessel_manager_supplied": 10
    }
    risk_type = risk_item.get("risk_type", "")
    risk_type_base = risk_type.split("_")[0] if "_" in risk_type else risk_type
    risk_type_number = risk_type_mapping.get(risk_type_base, 0)
    
    normalized = {
        "name": name.strip(),
        "risk_screening_status": risk_item.get("risk_screening_status", "无风险"),
        "risk_screening_time": risk_item.get("risk_screening_time", current_time),
        "risk_status_change_content": risk_item.get("risk_status_change_content", ""),
        "risk_status_change_time": risk_item.get("risk_status_change_time", current_time),
        "risk_type_number": risk_item.get("risk_type_number", risk_type_number),
        "risk_description": risk_item.get("risk_description", ""),
        "risk_info": risk_item.get("risk_info", None),
        "risk_status_reason": risk_item.get("risk_status_reason", {})
    }
    return normalized


def normalize_stakeholder_risk_list(risk_list: Union[List, Dict, Any], default_names: List[str] = [], risk_type: str = "") -> List[Dict]:
    """标准化列表类型的相关方风险结果"""
    if not isinstance(risk_list, list):
        risk_list = [risk_list] if risk_list else []
    
    normalized_list = []
    for idx, risk_item in enumerate(risk_list):
        default_name = default_names[idx] if (default_names and idx < len(default_names)) else ""
        if isinstance(risk_item, dict):
            risk_item["risk_type"] = risk_type
        normalized = normalize_stakeholder_risk(risk_item, default_name=default_name)
        normalized_list.append(normalized)
    
    if not normalized_list and default_names:
        for default_name in default_names:
            normalized_item = normalize_stakeholder_risk({}, default_name=default_name)
            normalized_item["risk_type"] = risk_type
            normalized_list.append(normalized_item)
    
    return normalized_list


def normalize_single_stakeholder_risk(risk_item: Union[Dict, Any], default_name: str = "", risk_type: str = "") -> Dict:
    """标准化单个相关方风险结果"""
    if not risk_item:
        risk_item = {}
    
    risk_item["risk_type"] = risk_type
    normalized = normalize_stakeholder_risk(risk_item, default_name=default_name)
    return normalized


def normalize_fleet_risk(fleet_vessel: Dict[str, str], risk_item: Union[Dict, Any], risk_type: str = "") -> Dict:
    """标准化船队风险结果"""
    if not risk_item:
        risk_item = {}
    
    risk_item_dict = risk_item.__dict__ if hasattr(risk_item, '__dict__') else risk_item
    current_time = get_current_local_time()
    
    risk_type_mapping = {
        "lloyd": 11,
        "kpler": 12,
        "uani": 13
    }
    risk_type_base = risk_type.split("_")[0] if "_" in risk_type else risk_type
    risk_type_number = risk_type_mapping.get(risk_type_base, 0)
    
    return {
        "Vessel_name": fleet_vessel.get("Vessel_name", ""),
        "Vessel_imo": fleet_vessel.get("Vessel_imo", ""),
        "risk_screening_status": risk_item_dict.get("risk_screening_status", "无风险"),
        "risk_screening_time": risk_item_dict.get("risk_screening_time", current_time),
        "risk_status_change_content": risk_item_dict.get("risk_status_change_content", ""),
        "risk_status_change_time": risk_item_dict.get("risk_status_change_time", current_time),
        "risk_type_number": risk_item_dict.get("risk_type_number", risk_type_number),
        "risk_description": risk_item_dict.get("risk_description", ""),
        "risk_info": risk_item_dict.get("risk_info", None),
        "risk_status_reason": risk_item_dict.get("risk_status_reason", {})
    }

# ---------------------- 3. 数据写入函数 ----------------------
def insert_second_hand_disposal_risk_log(request: SecondHandDisposalRequest, response: SecondHandDisposalResponse,
                                        project_risk_status: str, vessel_risk_status: str, stakeholder_risk_status):
    """将筛查结果写入数据库"""
    conn = None
    try:
        current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
        full_response_json = response.model_dump()

        def model_to_json(obj):
            if isinstance(obj, list):
                return [item.model_dump() for item in obj] if obj else []
            elif obj:
                return obj.model_dump()
            else:
                return {}

        insert_params = {
            "request_time": current_time,
            "response_time": current_time,
            "full_response": Json(full_response_json),
            "uuid": request.Uuid,
            "process_id": request.Process_id or "",
            "process_operator_id": request.Process_operator_id or "",
            "process_operator": request.Process_operator or "",
            "process_status": request.Process_status or "",
            
            # 船舶基础信息
            "vessel_name": request.Vessel_name.strip(),
            "vessel_imo": request.Vessel_imo.strip(),
            
            # 相关方风险结果
            "intent_transferee_risk": Json(model_to_json(response.intent_transferee)),
            "intent_transferee_shareholder_risk": Json(model_to_json(response.intent_transferee_shareholder)),
            "intent_transferee_controller_risk": Json(model_to_json(response.intent_transferee_controller)),
            "intent_vessel_manager_risk": Json(model_to_json(response.intent_vessel_manager)),
            "intent_vessel_manager_shareholder_risk": Json(model_to_json(response.intent_vessel_manager_shareholder)),
            "intent_vessel_manager_controller_risk": Json(model_to_json(response.intent_vessel_manager_controller)),
            "vessel_operator_risk": Json(model_to_json(response.Vessel_operator)),
            "agent_risk": Json(model_to_json(response.Agent)),
            
            # 船队风险筛查结果
            "vessel_seller_fleet_supplied_lloyd_risk": Json(model_to_json(response.Vessel_seller_fleet_supplied_Lloyd)),
            "vessel_seller_fleet_supplied_kpler_risk": Json(model_to_json(response.Vessel_seller_fleet_supplied_kpler)),
            "vessel_seller_fleet_supplied_uani_risk": Json(model_to_json(response.Vessel_seller_fleet_supplied_uani)),
            
            # 提供方制裁风险
            "vessel_owner_supplied_sanction_risk": Json(model_to_json(response.Vessel_owner_supplied_sanction)),
            "vessel_manager_supplied_sanction_risk": Json(model_to_json(response.Vessel_manager_supplied_sanction)),
            
            # 其他船舶风险项
            "vessel_is_sanction": Json(model_to_json(response.Vessel_is_sanction)),
            "vessel_history_is_sanction": Json(model_to_json(response.Vessel_history_is_sanction)),
            "vessel_in_uani": Json(model_to_json(response.Vessel_in_uani)),
            "vessel_risk_level_lloyd": Json(model_to_json(response.Vessel_risk_level_lloyd)),
            "vessel_risk_level_kpler": Json(model_to_json(response.Vessel_risk_level_kpler)),
            "vessel_ais_gap": Json(model_to_json(response.Vessel_ais_gap)),
            "vessel_manipulation": Json(model_to_json(response.Vessel_Manipulation)),
            "vessel_risky_port_call": Json(model_to_json(response.Vessel_risky_port_call)),
            "vessel_dark_port_call": Json(model_to_json(response.Vessel_dark_port_call)),
            "vessel_cargo_sanction": Json(model_to_json(response.Vessel_cargo_sanction)),
            "vessel_trade_sanction": Json(model_to_json(response.Vessel_trade_sanction)),
            "vessel_dark_sts_events": Json(model_to_json(response.Vessel_dark_sts_events)),
            "vessel_sts_transfer": Json(model_to_json(response.Vessel_sts_transfer)),
            
            # 风控状态字段
            "project_risk_status": project_risk_status,
            "vessel_risk_status": vessel_risk_status,
            "stakeholder_risk_status": stakeholder_risk_status,
            
            # 操作员信息
            "operator_id": request.Process_operator_id or "",
            "operator_name": request.Process_operator or "",
            "operator_time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "operator_department": ""

        }

        insert_sql = """
             INSERT INTO lng.vessel_second_hand_disposal_risk_log (
                 request_time, response_time, full_response, 
                 uuid, process_id, process_operator_id, process_operator, process_status,
                 vessel_name, vessel_imo,
                 intent_transferee_risk, intent_transferee_shareholder_risk, intent_transferee_controller_risk,
                 intent_vessel_manager_risk, intent_vessel_manager_shareholder_risk, intent_vessel_manager_controller_risk,
                 vessel_operator_risk, agent_risk,
                 vessel_seller_fleet_supplied_lloyd_risk, vessel_seller_fleet_supplied_kpler_risk, vessel_seller_fleet_supplied_uani_risk,
                 vessel_owner_supplied_sanction_risk, vessel_manager_supplied_sanction_risk,
                 project_risk_status, vessel_risk_status, stakeholder_risk_status,
                 operator_id, operator_name, operator_department, operator_time
             ) VALUES (
                 %(request_time)s, %(response_time)s, %(full_response)s,
                 %(uuid)s, %(process_id)s, %(process_operator_id)s, %(process_operator)s, %(process_status)s,
                 %(vessel_name)s, %(vessel_imo)s,
                 %(intent_transferee_risk)s, %(intent_transferee_shareholder_risk)s, %(intent_transferee_controller_risk)s,
                 %(intent_vessel_manager_risk)s, %(intent_vessel_manager_shareholder_risk)s, %(intent_vessel_manager_controller_risk)s,
                 %(vessel_operator_risk)s, %(agent_risk)s,
                 %(vessel_seller_fleet_supplied_lloyd_risk)s, %(vessel_seller_fleet_supplied_kpler_risk)s, %(vessel_seller_fleet_supplied_uani_risk)s,
                 %(vessel_owner_supplied_sanction_risk)s, %(vessel_manager_supplied_sanction_risk)s,
                 %(project_risk_status)s, %(vessel_risk_status)s, %(stakeholder_risk_status)s,
                 %(operator_id)s, %(operator_name)s, %(operator_department)s, %(operator_time)s
             )
             """
        conn = psycopg2.connect(** DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, insert_params)
            conn.commit()
        logger.info(f"船舶二手处置风险数据写入成功（UUID: {request.Uuid}, IMO: {request.Vessel_imo}）")
    except OperationalError as e:
        logger.error(f"数据库连接失败: {str(e)}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"数据库连接异常: {str(e)}")
    except IntegrityError as e:
        logger.error(f"数据完整性错误（UUID: {request.Uuid}）: {str(e)}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=400, detail=f"数据格式错误: {str(e)}")
    except psycopg2.Error as e:
        logger.error(f"数据库操作失败（UUID: {request.Uuid}）: {str(e)}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"数据写入异常: {str(e)}")
    finally:
        if conn:
            conn.close()


# ---------------------- 4. 核心接口函数 ----------------------
@second_hand_router.post("/vessel_disposal_risk", response_model=SecondHandDisposalResponse)
async def vessel_second_hand_disposal_risk_screen(request: SecondHandDisposalRequest):
    """船舶二手处置合规状态筛查主接口"""
    try:
        current_time = get_current_local_time()
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        vessel_imo = request.Vessel_imo.strip()
        vessel_name = request.Vessel_name.strip()
        logger.info(f"开始船舶二手处置合规筛查（UUID: {request.Uuid}, IMO: {vessel_imo}, 名称: {vessel_name}）")

        risk_check_orchestrator = OptimizedRiskCheckOrchestrator(API_CONFIG)

        # 执行所有检查项
        check_results = risk_check_orchestrator.execute_all_checks_optimized(
            vessel_imo, start_date, end_date,
            request.intent_transferee, request.intent_transferee_shareholder,
            request.intent_transferee_controller, request.intent_vessel_manager,
            request.intent_vessel_manager_shareholder, request.vessel_manager_controller,
            request.vessel_operator, request.agent, request.intent_transferee_fleet_supplied,
            request.vessel_owner_supplied, request.vessel_manager_supplied
        )
        
        # 处理检查结果
        check_results_dict = {}
        for result in check_results:
            if isinstance(result, dict) and "risk_type" in result:
                check_results_dict[result["risk_type"]] = result
            elif hasattr(result, "risk_type"):
                check_results_dict[result.risk_type] = result
            else:
                logger.warning(f"无效的风险检查结果，跳过存储: {type(result)} - {str(result)[:50]}")

        # 处理相关方风险结果
        intent_transferee_risk = normalize_single_stakeholder_risk(
            check_results_dict.get("intent_transferee_sanctions_risk", {}),
            default_name=request.intent_transferee,
            risk_type="intent_transferee_sanctions_risk"
        )
        
        intent_transferee_shareholder_risk = normalize_stakeholder_risk_list(
            check_results_dict.get("intent_transferee_shareholder_sanctions_risk", []),
            default_names=request.intent_transferee_shareholder,
            risk_type="intent_transferee_shareholder_sanctions_risk"
        )
        
        intent_transferee_controller_risk = normalize_single_stakeholder_risk(
            check_results_dict.get("intent_transferee_controller_sanctions_risk", {}),
            default_name=request.intent_transferee_controller,
            risk_type="intent_transferee_controller_sanctions_risk"
        )
        
        intent_vessel_manager_risk = normalize_single_stakeholder_risk(
            check_results_dict.get("intent_vessel_manager_sanctions_risk", {}),
            default_name=request.intent_vessel_manager,
            risk_type="intent_vessel_manager_sanctions_risk"
        )
        
        intent_vessel_manager_shareholder_risk = normalize_stakeholder_risk_list(
            check_results_dict.get("intent_vessel_manager_shareholder_sanctions_risk", []),
            default_names=request.intent_vessel_manager_shareholder,
            risk_type="intent_vessel_manager_shareholder_sanctions_risk"
        )
        
        intent_vessel_manager_controller_risk = normalize_single_stakeholder_risk(
            check_results_dict.get("intent_vessel_manager_controller_sanctions_risk", {}),
            default_name=request.vessel_manager_controller,
            risk_type="intent_vessel_manager_controller_sanctions_risk"
        )
        
        vessel_operator_risk = normalize_single_stakeholder_risk(
            check_results_dict.get("vessel_operator_sanctions_risk", {}),
            default_name=request.vessel_operator,
            risk_type="vessel_operator_sanctions_risk"
        )
        
        agent_risk = normalize_stakeholder_risk_list(
            check_results_dict.get("agent_sanctions_risk", []),
            default_names=request.agent,
            risk_type="agent_sanctions_risk"
        )
        
        # 处理船队风险筛查结果
        vessel_seller_fleet_supplied_Lloyd = []
        vessel_seller_fleet_supplied_kpler = []
        vessel_seller_fleet_supplied_uani = []
        
        for idx, fleet_vessel in enumerate(request.intent_transferee_fleet_supplied):
            lloyd_risk = check_results_dict.get(f"fleet_lloyd_risk_{idx}", {})
            kpler_risk = check_results_dict.get(f"fleet_kpler_risk_{idx}", {})
            uani_risk = check_results_dict.get(f"fleet_uani_risk_{idx}", {})
            
            vessel_seller_fleet_supplied_Lloyd.append(
                normalize_fleet_risk(fleet_vessel, lloyd_risk, risk_type="lloyd")
            )
            vessel_seller_fleet_supplied_kpler.append(
                normalize_fleet_risk(fleet_vessel, kpler_risk, risk_type="kpler")
            )
            vessel_seller_fleet_supplied_uani.append(
                normalize_fleet_risk(fleet_vessel, uani_risk, risk_type="uani")
            )
        
        # 处理提供方制裁风险
        vessel_owner_supplied_sanction = normalize_stakeholder_risk_list(
            check_results_dict.get("vessel_owner_supplied_sanction", []),
            default_names=request.vessel_owner_supplied,
            risk_type="vessel_owner_supplied_sanction"
        )
        
        vessel_manager_supplied_sanction = normalize_stakeholder_risk_list(
            check_results_dict.get("vessel_manager_supplied_sanction", []),
            default_names=request.vessel_manager_supplied,
            risk_type="vessel_manager_supplied_sanction"
        )

        # 处理船舶风险项
        def normalize_vessel_risk_item(item: Union[Dict, Any], risk_type_number: int = 0) -> Dict:
            if not item:
                item = {}
            item_dict = item.__dict__ if hasattr(item, '__dict__') else item
            # 确保risk_status_reason始终是字典类型
            risk_status_reason = item_dict.get("risk_status_reason", {})
            if risk_status_reason is None:
                risk_status_reason = {}
                
            return {
                "risk_screening_status": item_dict.get("risk_screening_status", ""),
                "risk_screening_time": item_dict.get("risk_screening_time", current_time),
                "risk_status_change_content": item_dict.get("risk_status_change_content", ""),
                "risk_status_change_time": item_dict.get("risk_status_change_time", current_time),
                "risk_type_number": item_dict.get("risk_type_number", risk_type_number),
                "risk_description": item_dict.get("risk_description", ""),
                "risk_info": item_dict.get("risk_info", None),
                "risk_status_reason": risk_status_reason
            }
        
        vessel_is_sanction = normalize_vessel_risk_item(check_results_dict.get("Vessel_is_sanction_current", {}), 14)
        vessel_history_sanction = normalize_vessel_risk_item(check_results_dict.get("Vessel_is_sanction_history", {}), 15)
        vessel_in_uani = normalize_vessel_risk_item(check_results_dict.get("Vessel_in_uani", {}), 16)
        vessel_risk_lloyd = normalize_vessel_risk_item(check_results_dict.get("Vessel_risk_level_lloyd", {}), 17)
        vessel_risk_kpler = normalize_vessel_risk_item(check_results_dict.get("Vessel_risk_level_kpler", {}), 18)
        vessel_ais_gap = normalize_vessel_risk_item(check_results_dict.get("Vessel_ais_gap", {}), 19)
        vessel_manipulation = normalize_vessel_risk_item(check_results_dict.get("Vessel_Manipulation", {}), 20)
        vessel_risky_port = normalize_vessel_risk_item(check_results_dict.get("Vessel_risky_port_call", {}), 21)
        vessel_dark_port = normalize_vessel_risk_item(check_results_dict.get("Vessel_dark_port_call", {}), 22)
        vessel_cargo_sanction = normalize_vessel_risk_item(check_results_dict.get("Vessel_cargo_sanction", {}), 23)
        vessel_trade_sanction = normalize_vessel_risk_item(check_results_dict.get("Vessel_trade_sanction", {}), 24)
        vessel_dark_sts = normalize_vessel_risk_item(check_results_dict.get("Vessel_dark_sts_events", {}), 25)
        vessel_sts_transfer = normalize_vessel_risk_item(check_results_dict.get("Vessel_sts_transfer", {}), 26)

        # 计算风控状态
        all_risk_levels = []
        vessel_risk_levels = []
        stakeholder_risk_levels = []

        # 相关方风险处理
        list_stakeholder_risks = intent_transferee_shareholder_risk + intent_vessel_manager_shareholder_risk + \
                                agent_risk + vessel_owner_supplied_sanction + vessel_manager_supplied_sanction
        single_stakeholder_risks = [intent_transferee_risk, intent_transferee_controller_risk,
                                  intent_vessel_manager_risk, intent_vessel_manager_controller_risk,
                                  vessel_operator_risk]
        all_stakeholder_risks = list_stakeholder_risks + single_stakeholder_risks

        for risk in all_stakeholder_risks:
            if not risk or isinstance(risk, str):
                continue
            level = risk.get('risk_screening_status', '') if isinstance(risk, dict) else getattr(risk, 'risk_screening_status', '')
            if level:
                all_risk_levels.append(level)
                stakeholder_risk_levels.append(level)

        # 船队风险处理
        fleet_risks = vessel_seller_fleet_supplied_Lloyd + vessel_seller_fleet_supplied_kpler + vessel_seller_fleet_supplied_uani
        for risk in fleet_risks:
            if not risk or isinstance(risk, str):
                continue
            level = risk.get('risk_screening_status', '') if isinstance(risk, dict) else getattr(risk, 'risk_screening_status', '')
            if level:
                all_risk_levels.append(level)
                stakeholder_risk_levels.append(level)

        # 船舶风险处理
        vessel_risk_items = [
            vessel_is_sanction, vessel_history_sanction, vessel_in_uani,
            vessel_risk_lloyd, vessel_risk_kpler, vessel_ais_gap,
            vessel_manipulation, vessel_risky_port, vessel_dark_port,
            vessel_cargo_sanction, vessel_trade_sanction, vessel_dark_sts,
            vessel_sts_transfer
        ]
        for item in vessel_risk_items:
            if not item or isinstance(item, str):
                continue
            level = item.get('risk_screening_status', '') if isinstance(item, dict) else getattr(item, 'risk_screening_status', '')
            if level:
                all_risk_levels.append(level)
                vessel_risk_levels.append(level)

        # 风险等级优先级
        risk_priority = {
            "高风险": 3, "中风险": 2, "Sanctioned": 3, 
            "Risks detected": 2, "high-risk": 3, "low-risk": 1, 
            "No risk": 1, "无风险": 1
        }

        # 项目风控状态
        if not all_risk_levels:
            max_project_risk = "无风险"
        else:
            max_project_risk = max(all_risk_levels, key=lambda x: risk_priority.get(x, 0))
        project_risk_status = "拦截" if risk_priority[max_project_risk] >= 3 else \
                             "关注" if risk_priority[max_project_risk] == 2 else "正常"

        # 船舶风控状态
        if not vessel_risk_levels:
            max_vessel_risk = "无风险"
        else:
            max_vessel_risk = max(vessel_risk_levels, key=lambda x: risk_priority.get(x, 0))
        vessel_risk_status = "高风险" if risk_priority[max_vessel_risk] >= 3 else \
                            "中风险" if risk_priority[max_vessel_risk] == 2 else "无风险"

        # 相关方风控状态
        if not stakeholder_risk_levels:
            max_stakeholder_risk = "无风险"
        else:
            max_stakeholder_risk = max(stakeholder_risk_levels, key=lambda x: risk_priority.get(x, 0))
        stakeholder_risk_status = "高风险" if risk_priority[max_stakeholder_risk] >= 3 else \
                                 "中风险" if risk_priority[max_stakeholder_risk] == 2 else "无风险"

        # 构造响应
        response = SecondHandDisposalResponse(
            Uuid=request.Uuid,
            Process_id=request.Process_id,
            Vessel_name=vessel_name,
            Vessel_imo=vessel_imo,
            Project_risk_status=project_risk_status,
            Vessel_risk_status=vessel_risk_status,
            Stakeholder_risk_status=stakeholder_risk_status,
            intent_transferee=intent_transferee_risk,
            intent_transferee_shareholder=intent_transferee_shareholder_risk,
            intent_transferee_controller=intent_transferee_controller_risk,
            intent_vessel_manager=intent_vessel_manager_risk,
            intent_vessel_manager_shareholder=intent_vessel_manager_shareholder_risk,
            intent_vessel_manager_controller=intent_vessel_manager_controller_risk,
            Vessel_operator=vessel_operator_risk,
            Agent=agent_risk,
            Vessel_seller_fleet_supplied_Lloyd=vessel_seller_fleet_supplied_Lloyd,
            Vessel_seller_fleet_supplied_kpler=vessel_seller_fleet_supplied_kpler,
            Vessel_seller_fleet_supplied_uani=vessel_seller_fleet_supplied_uani,
            Vessel_owner_supplied_sanction=vessel_owner_supplied_sanction,
            Vessel_manager_supplied_sanction=vessel_manager_supplied_sanction,
            Vessel_is_sanction=vessel_is_sanction,
            Vessel_history_is_sanction=vessel_history_sanction,
            Vessel_in_uani=vessel_in_uani,
            Vessel_risk_level_lloyd=vessel_risk_lloyd,
            Vessel_risk_level_kpler=vessel_risk_kpler,
            Vessel_ais_gap=vessel_ais_gap,
            Vessel_Manipulation=vessel_manipulation,
            Vessel_risky_port_call=vessel_risky_port,
            Vessel_dark_port_call=vessel_dark_port,
            Vessel_cargo_sanction=vessel_cargo_sanction,
            Vessel_trade_sanction=vessel_trade_sanction,
            Vessel_dark_sts_events=vessel_dark_sts,
            Vessel_sts_transfer=vessel_sts_transfer
        )

        # 写入数据库
        insert_second_hand_disposal_risk_log(request, response, project_risk_status, vessel_risk_status, stakeholder_risk_status)

        logger.info(f"船舶二手处置合规筛查完成（UUID: {request.Uuid}）")
        return response

    except Exception as e:
        logger.error(f"筛查接口异常（UUID: {request.Uuid}）: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"船舶二手处置合规筛查失败: {str(e)}")


# ---------------------- 5. 服务启动配置 ----------------------
def create_app():
    """创建FastAPI应用"""
    app = FastAPI(title="船舶二手处置合规状态筛查接口", version="1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(second_hand_router)

    @app.get("/health", tags=["系统"])
    async def health_check():
        return {
            "status": "healthy",
            "service": "船舶二手处置合规筛查接口",
            "timestamp": get_current_local_time(),
            "database": "kingbase (lng.vessel_second_hand_disposal_risk_log)"
        }

    @app.get("/", tags=["系统"])
    async def root():
        return {
            "message": "船舶二手处置合规状态筛查接口",
            "version": "1.0",
            "api": {
                "method": "POST",
                "url": "/second_hand/vessel_disposal_risk",
                "content_type": "application/json"
            }
        }

    return app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, log_level="info")
