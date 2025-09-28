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

# 导入外部API函数
from vessel_is_in_sanctions import get_vessel_sanctions_info
from KplerDataProcessor import get_kpler_vessel_risk_info
from VesselRiskAnalyzer import get_vessel_risk_analysis
from vessel_complance_risk import get_vessel_company_sanction_info
from ais_spoof import get_vessel_ais_manipulation_info
from kingbase_config import get_kingbase_config

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("STS_Bunkering_Risk_Screen")

# 路由器
sts_router = APIRouter(prefix="/sts", tags=["STS过驳作业合规筛查"])

# 配置加载
DB_CONFIG_RAW = get_kingbase_config()
DB_CONFIG = {
    "host": DB_CONFIG_RAW.get("host"),
    "port": DB_CONFIG_RAW.get("port"),
    "user": DB_CONFIG_RAW.get("user"),
    "password": DB_CONFIG_RAW.get("password"),
    "dbname": DB_CONFIG_RAW.get("database")
}
AUTH_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTc1ODcwMDMsImlhdCI6MTc1NzU4NzAwMywiZXhwIjoxNzYwMTc5MDAzLCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NzU4NzAwMywiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRS3VxalV2Wnd6V1cuRENmVC5fLmdjRGZVM2xDbDZsc3NscGhRQVNCRWpEcXowOE1LR1BoRzlhTDZoRjBQZldKYXlkeHNZejFzTy5DcE1HRkQxQUQ1TmhnQzU4bSIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJUcmlhbCI6WyJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTMwI1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTMwI1RydWUiLCJjb21wYW55c2FuY3Rpb25zI2NvbXBhbnlzYW5jdGlvbnMjMjAyNS0wOS0xMSNUcnVlIiwiY29tcGFueXJlcG9ydCNjb21wYW55cmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSNhaXNwb3NpdGlvbmdhcGhpc3RvcnkjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2VsY29tcGxpYW5jZXJpc2sjdmVzc2VsY29tcGxpYW5jZXJpc2sjMjAyNS0wOS0xMSNUcnVlIiwic2FuY3Rpb25zZWFyY2gjc2FuY3Rpb25zZWFyY2gjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc2FuY3Rpb25zI3Zlc3NlbHNhbmN0aW9ucyMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxyZXBvcnQjdmVzc2VscmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrcmVwb3J0YXVkaXQjdmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnRhdWRpdCMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCN2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCMyMDI1LTA5LTExI1RydWUiLCJjb21wYW55ZmxlZXRkZXRhaWxzI2NvbXBhbnlmbGVldGRldGFpbHMjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNi0wOC0zMCNUcnVlIiwiQWR2YW5jZWQgUlx1MDAyNkMgQVBJI2xsaWFyY2FwaSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWx2b3lhZ2VldmVudHMjdmVzc2Vsdm95YWdlZXZlbnRzIzIwMjYtMDgtMzAjVHJ1ZSIsIlJpc2sgXHUwMDI2IENvbXBsaWFuY2UgQVBJI2xsaXJjYXBpIzIwMjUtMDktMTEjVHJ1ZSIsIlNlYXNlYXJjaGVyI1NlYXNlYXJjaGVyIzIwMjUtMDktMTEjVHJ1ZSJdLCJ1c2VybmFtZSI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJ1c2VySWQiOiIwMDVOejAwMDAwQ2k5R25JQUoiLCJjb250YWN0QWNjb3VudElkIjoiMDAxTnowMDAwMEthQkpESUEzIiwidXNlclR5cGUiOiJDc3BMaXRlUG9ydGFsIiwiZW1haWwiOiJjaGFuZy54aW55dWFuQGNvc2Nvc2hpcHBpbmcuY29tIiwiZ2l2ZW5fbmFtZSI6Ilhpbnl1YW4iLCJmYW1pbHlfbmFtZSI6IkNoYW5nIiwic2hpcFRvIjoiIiwianRpIjoiNUIzQTZBMEU2OTgzRjM0MkRBMDA2NDVBRjc1OERFRDgifQ.QVdG_HrNDlT-OVVoJ1jKRYWvQJVLHRK3UJq8SZiMhi6uyzKvtIJEtzJDXYoWS_9lUg6QwgOO2aP-w6RSXxirtGKvwTQ3vu8zFdFdKK3iMQJljqJPhBwprpbNr3u1pXuTmwzQtcKFxyOqsKN5qirowKMpVrnuQCmGK7pJSi_1BlLSXysqnhNYOj9T67uAL7f2gl0aGs0YHcK4FxxtbXVYDpx7k3gPdtID8oBDv_b1YgpYbAGfdoEXI2V5-VGwQqs-DpqzsO_tpEhUqIuHBl-wq8lPuIkZaM70iskG1p4uagKons2MpzYSQi2VYbwBdWsVug5V4YVB-1mOgkndB0KOnQ"


KPLER_TOKEN = "Basic ejdXOEkzSGFKOEJWdno0ZzRIdEZJZzJZUzR1VmJQOVA6YWZEZ2d0NG9mZFJDX0Yyd1lQUlNhbXhMZFdjMVlJdnlsX1ctYW1QRnV3QmI2SFNaOWtwSFZ4NlpaYmVyaHJnbQ=="
VESSEL_ALL_DATA_URL = "http://10.18.66.38/crmp-python-api/api/get_vessel_all_data"


# ---------------------- 1. 数据模型定义 ----------------------
class StakeholderRisk(BaseModel):
    """基础相关方风险模型"""
    name: str
    risk_screening_status: str  # 高风险/中风险/低风险/无风险
    risk_screening_time: str  # 筛查时间（YYYY-MM-DDTHH:MM:SSZ）
    risk_status_change_content: str = ""
    risk_status_change_time: str = ""
    risk_type_number: int  # 风险类型编号（保持不变）
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
    risk_type_number: int  # 风险类型编号（保持不变）
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class VesselRiskItem(BaseModel):
    """船舶风险项模型"""
    risk_screening_status: Optional[str] = None
    risk_screening_time: Optional[str] = None
    risk_status_change_content: Optional[str] = None
    risk_status_change_time: Optional[str] = None
    risk_type_number: int  # 风险类型编号（保持不变）
    risk_description: str = ""  # 风险描述
    risk_info: Any = None  # 风险详情
    risk_status_reason: Dict[str, Any] = Field(default_factory=dict)  # 风险原因


class RiskScreenRequest(BaseModel):
    """请求模型"""
    Uuid: str = Field(..., alias="uuid")
    Process_id: Optional[str] = Field(None, alias="process_id")
    Process_operator_id: Optional[str] = Field(None, alias="process_operator_id")
    Process_operator: Optional[str] = Field(None, alias="process_operator_name")
    Process_start_time: Optional[datetime] = Field(None, alias="process_start_time")
    Process_end_time: Optional[datetime] = Field(None, alias="process_end_time")
    Process_status: Optional[str] = Field(None, alias="process_status")
    
    # 业务信息字段
    business_segment: str = Field(..., alias="business_segment")
    trade_type: Optional[str] = Field(None, alias="trade_type")
    business_model: str = Field(..., alias="business_model")
    operate_water_area: str = Field(..., alias="operate_water_area")
    expected_execution_date: datetime = Field(..., alias="expected_execution_date")
    
    # 船舶基础信息
    is_port_sts: str = Field(..., alias="is_port_sts")
    vessel_name: str = Field(..., alias="vessel_name")
    vessel_imo:  Optional[str] = Field(None, alias="vessel_imo")  # 改为非必填
    vessel_number: Optional[str] = Field(None, alias="vessel_number")  # 改为非必填
    vessel_transfer_imo: Optional[List[str]] = Field(None, alias="vessel_transfer_imo")  # 改为非必填
    vessel_transfer_name: Optional[List[str]] = Field(None, alias="vessel_transfer_name")  # 改为非必填
    
    # 相关方信息
    charterers: str = Field(..., alias="charterers")
    Consignee: List[str] = Field(..., alias="consignee")
    Consignor: List[str] = Field(..., alias="consignor")
    Agent: List[str] = Field(..., alias="agent")
    Vessel_broker: List[str] = Field(..., alias="vessel_broker")
    Vessel_owner: Union[str, List[str]] = Field(..., alias="vessel_owner")  # 支持字符串或列表
    Vessel_manager: Union[str, List[str]] = Field(..., alias="vessel_manager")  # 支持字符串或列表
    Vessel_operator: Union[str, List[str]] = Field(..., alias="vessel_operator")  # 支持字符串或列表

    @field_validator("Process_start_time", "Process_end_time", "expected_execution_date", mode="before")
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
                    return datetime.strptime(v, "%Y/%m/%d")  # 新增支持 YYYY/MM/DD
                except ValueError:
                    raise ValueError("时间格式必须为'YYYY/MM/DD HH:MM:SS'、'YYYY-MM-DD'或'YYYY/MM/DD'")

    class Config:
        populate_by_name = True



class RiskScreenResponse(BaseModel):
    """响应模型"""
    Uuid: str
    Process_id: Optional[str] = None
    Vessel_name: str
    Vessel_imo: str
    
    # 风控状态字段
    Project_risk_status: str  # 项目风控状态：拦截/关注/正常
    Vessel_risk_status: str   # 船舶风控状态：高风险/中风险/无风险
    Stakeholder_risk_status: str  # 相关方风控状态：高风险/中风险/无风险

    # 相关方风险结果
    Charterers: StakeholderRisk  # 租家
    Consignee: List[StakeholderRisk]  # 收货人
    Consignor: List[StakeholderRisk]  # 发货人
    Agent: List[StakeholderRisk]  # 代理
    Vessel_broker: List[StakeholderRisk]  # 租船经纪
    Vessel_owner: StakeholderRisk  # 注册船东
    Vessel_manager: StakeholderRisk  # 船舶管理人
    Vessel_operator: StakeholderRisk  # 船舶经营人

    # 船舶相关方制裁筛查
    Vessel_stakeholder_is_sanction_Lloyd: Optional[List[VesselStakeholderSanction]] = None  # 劳氏船舶相关方制裁筛查
    Vessel_stakeholder_is_sanction_kpler: Optional[List[VesselStakeholderSanction]] = None  # 开普勒船舶相关方制裁筛查

    # 船舶制裁风险字段
    Vessel_is_sanction: Optional[VesselRiskItem] = None  # 船舶是否被制裁（劳氏）
    Vessel_history_is_sanction: Optional[VesselRiskItem] = None  # 船舶历史是否被制裁（劳氏）
    Vessel_in_uani: Optional[VesselRiskItem] = None  # 船舶是否在UANI清单
    Vessel_risk_level_lloyd: Optional[VesselRiskItem] = None  # 船舶劳氏风险等级
    Vessel_risk_level_kpler: Optional[VesselRiskItem] = None  # 船舶开普勒风险等级
    Vessel_ais_gap: Optional[VesselRiskItem] = None  # 船舶AIS信号缺失风险
    Vessel_Manipulation: Optional[VesselRiskItem] = None  # 船舶人为伪造及操纵风险
    Vessel_risky_port_call: Optional[VesselRiskItem] = None  # 船舶挂靠高风险港口风险
    Vessel_dark_port_call: Optional[VesselRiskItem] = None  # 船舶Dark port call风险
    Vessel_cargo_sanction: Optional[VesselRiskItem] = None  # 船舶运过受制裁货物风险
    Vessel_trade_sanction: Optional[VesselRiskItem] = None  # 船舶运营受制裁贸易风险
    Cargo_origin_from_sanctioned: Optional[VesselRiskItem] = None  # 货物原产地是否为制裁国家
    Vessel_dark_sts_events: Optional[VesselRiskItem] = None  # 船舶Dark STS events风险
    Vessel_sts_transfer: Optional[VesselRiskItem] = None  # 船舶STS转运不合规风险


# ---------------------- 2. 工具函数 ----------------------
def parse_json_safely(json_str: Optional[str]) -> Union[List, Dict, str, None]:
    """安全解析JSON字符串：
    - 空/None 返回 None
    - 已是 list/dict 直接返回
    - 普通字符串（不以 { 或 [ 开头）直接原样返回（不告警）
    - 看起来像 JSON 再尝试解析，失败才告警
    """
    if json_str is None or json_str in ("null", "None"):
        return None
    if isinstance(json_str, (list, dict)):
        return json_str
    if isinstance(json_str, str):
        s = json_str.strip()
        if not s:
            return None
        # 明显不是JSON，直接返回原字符串，不告警
        if not (s.startswith('{') or s.startswith('[')):
            return json_str
        try:
            return json.loads(s)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"JSON解析失败: {str(e)}, 原始数据: {s[:100]}")
            return json_str
    # 其他类型兜底
    return None


def get_sanction_info(name: str) -> Dict[str, Any]:
    """查询相关方制裁信息（含等级和原因字段）- 模糊匹配"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(**connect_params)
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
                    "sanctions_lev": result["sanctions_lev"] or "无风险",
                    "reason": {
                        "sanctions_list": parse_json_safely(result["sanctions_list"]),
                        "mid_sanctions_list": parse_json_safely(result["mid_sanctions_list"]),
                        "no_sanctions_list": parse_json_safely(result["no_sanctions_list"]),
                        "is_san": result["is_san"],
                        "is_sco": result["is_sco"],
                        "is_one_year": result["is_one_year"],
                        "is_sanctioned_countries": result["is_sanctioned_countries"]
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
        risk_type: str,  # 表中实际的risk_type关键字（如"lloyds_sanctions"）
        queried_risk_level: Optional[str] = None  # 已查询到的风险等级（用于匹配表中risk_level）
) -> Dict[str, Any]:
    """
    从 lng.sanctions_des_info 表查询风险描述（risk_desc_info）和风险详情（info）
    匹配规则：
    - 精准匹配 risk_type 字段（表中实际的risk_type，如"lloyds_sanctions"）
    - 模糊匹配 risk_level 字段（用已查询到的风险等级进行匹配）

    :param risk_type: 表中实际的risk_type字段值（如"lloyds_sanctions"）
    :param queried_risk_level: 已查询到的风险等级（用于匹配表中risk_level字段）
    :return: 含 risk_desc_info（风险描述）和 info（风险详情）的字典
    """
    conn = None
    default_result = {"risk_desc_info": "", "info": None}

    try:
        # 校验必要参数（risk_type必须存在，风险等级可选）
        if not risk_type:
            logger.warning("风险类型（risk_type）为空，无法查询风险描述")
            return default_result

        connect_params = DB_CONFIG.copy()
        connect_params["cursor_factory"] = DictCursor
        conn = psycopg2.connect(** connect_params)

        # 构建查询条件：精准匹配risk_type，用已查询的风险等级匹配表中risk_level
        sql = """
              SELECT risk_desc_info, info
              FROM lng.sanctions_des_info
              WHERE risk_type = %s  -- 精准匹配表中risk_type
            """
        sql_params = [risk_type]

        # 若传入已查询的风险等级，则用该等级模糊匹配表中risk_level字段
        if queried_risk_level and queried_risk_level.strip():
            sql += " AND risk_level ILIKE %s"  # 核心修正：用risk_level字段匹配
            sql_params.append(f"%{queried_risk_level.strip()}%")  # 已查询的风险等级作为匹配值

        sql += " LIMIT 1"  # 只取第一条匹配结果

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
    """获取一年前的当前时间（上海时区）"""
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    return datetime.now(shanghai_tz) - timedelta(days=365)


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """解析时间戳（支持秒/毫秒）为datetime"""
    try:
        timestamp = float(timestamp_str)
        if timestamp > 1e12:
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp, pytz.timezone('Asia/Shanghai'))
    except (ValueError, TypeError):
        return None


def parse_datetime_str(dt_str: str) -> Optional[datetime]:
    """解析ISO格式时间字符串为datetime"""
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


def check_vessel_in_uani(vessel_name: str) -> bool:
    """查询船舶是否在UANI清单 - 模糊匹配"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(** DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM lng.uani_list WHERE vessel_name ILIKE %s LIMIT 1",
                (f"%{vessel_name.strip()}%",)
            )
            return cursor.fetchone() is not None
    except psycopg2.Error as e:
        logger.error(f"UANI清单查询失败（{vessel_name}）: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()


def check_cargo_origin_sanctioned(origin_country: str) -> bool:
    """查询货物原产地是否为制裁国家"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM lng.contry_port WHERE countryname ILIKE %s AND is_sanctioned = true LIMIT 1",
                (f"%{origin_country.strip()}%",)
            )
            return cursor.fetchone() is not None
    except psycopg2.Error as e:
        logger.error(f"货物原产地制裁查询失败（{origin_country}）: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()


def get_vessel_all_data(vessel_imo: str) -> Dict[str, Any]:
    """调用船舶明细数据接口，并保存数据到本地"""
    try:
        response = requests.get(
            VESSEL_ALL_DATA_URL,
            params={"vessel_imo": vessel_imo},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"获取船舶明细数据失败（IMO: {vessel_imo}）: {str(e)}")
        return {"success": False, "error": str(e)}


def find_risk_item_in_vessel_data(vessel_data: Dict[str, Any], keyword: str) -> Dict[str, Any]:
    """从船舶明细数据中查找包含关键字的风险项"""
    result = {"description": "", "sanctions_list": []}

    if not vessel_data.get("success", False) or "data" not in vessel_data:
        return result

    data = vessel_data["data"]

    # 检查所有风险组
    risk_groups = ["high_risk", "mid_risk", "no_risk"]
    for group in risk_groups:
        if group in data:
            for item in data[group]:
                if "title" in item and keyword in item["title"]:
                    result["description"] = item.get("description", "")
                    if "risk_items" in item and len(item["risk_items"]) > 0:
                        result["sanctions_list"] = item["risk_items"][0].get("sanctions_list", [])
                    return result

    # 检查sts_events
    if "sts_events" in data:
        for event in data["sts_events"]:
            if "zoneName" in event and keyword in event["zoneName"]:
                result["description"] = f"船对船转运事件: {event.get('zoneName', '')}"
                result["sanctions_list"] = [event]
                return result

    # 检查voyages
    if "voyages" in data:
        for voyage in data["voyages"]:
            if "RiskTypes" in voyage and any(keyword in rt for rt in voyage["RiskTypes"]):
                result["description"] = f"航行风险: {', '.join(voyage.get('RiskTypes', []))}"
                result["sanctions_list"] = [voyage]
                return result

    return result


# ---------------------- 3. 数据写入函数 ----------------------
def insert_sts_bunkering_risk_log(request: RiskScreenRequest, response: RiskScreenResponse,
                                  project_risk_status: str, vessel_risk_status: str, stakeholder_risk_status: str):
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
            
            # 业务信息字段
            "business_segment": request.business_segment,
            "trade_type": request.trade_type or "",
            "business_model": request.business_model,
            "operate_water_area": request.operate_water_area,
            "expected_execution_date": request.expected_execution_date,
            
            # 船舶基础信息
            "is_port_sts": request.is_port_sts,
            "vessel_name": request.vessel_name.strip(),
            "vessel_imo": request.vessel_imo.strip() if request.vessel_imo else "",
            "vessel_number": request.vessel_number.strip() if request.vessel_number else "",
            "vessel_transfer_imo": Json(request.vessel_transfer_imo),
            "vessel_transfer_name": Json(request.vessel_transfer_name),
            
            # 相关方风险结果
            "charterers_risk": Json(model_to_json(response.Charterers)),
            "consignee_risk": Json(model_to_json(response.Consignee)),
            "consignor_risk": Json(model_to_json(response.Consignor)),
            "agent_risk": Json(model_to_json(response.Agent)),
            "vessel_broker_risk": Json(model_to_json(response.Vessel_broker)),
            "vessel_owner_risk": Json(model_to_json(response.Vessel_owner)),
            "vessel_manager_risk": Json(model_to_json(response.Vessel_manager)),
            "vessel_operator_risk": Json(model_to_json(response.Vessel_operator)),
            
            # 船舶相关方制裁筛查
            "vessel_stakeholder_is_sanction_lloyd": Json(model_to_json(response.Vessel_stakeholder_is_sanction_Lloyd)),
            "vessel_stakeholder_is_sanction_kpler": Json(model_to_json(response.Vessel_stakeholder_is_sanction_kpler)),
            
            # 船舶制裁风险字段
            "vessel_is_sanction": Json(model_to_json(response.Vessel_is_sanction)),
            "vessel_history_is_sanction": Json(model_to_json(response.Vessel_history_is_sanction)),
            "vessel_in_uani": Json(model_to_json(response.Vessel_in_uani)),
            "vessel_risk_level_lloyd": Json(model_to_json(response.Vessel_risk_level_lloyd)),
            "vessel_risk_level_kpler": Json(model_to_json(response.Vessel_risk_level_kpler)),
            "vessel_ais_gap": Json(model_to_json(response.Vessel_ais_gap)),
            "vessel_manipulation": Json(model_to_json(response.Vessel_Manipulation)),
            "vessel_high_risk_port": Json(model_to_json(response.Vessel_risky_port_call)),
            "vessel_has_dark_port_call": Json(model_to_json(response.Vessel_dark_port_call)),
            "vessel_cargo_sanction": Json(model_to_json(response.Vessel_cargo_sanction)),
            "vessel_trade_sanction": Json(model_to_json(response.Vessel_trade_sanction)),
            "cargo_origin_from_sanctioned": Json(model_to_json(response.Cargo_origin_from_sanctioned)),
            "vessel_dark_sts_events": Json(model_to_json(response.Vessel_dark_sts_events)),
            "vessel_sts_transfer": Json(model_to_json(response.Vessel_sts_transfer)),
            
            # 风控状态字段
            "project_risk_status": project_risk_status,
            "vessel_risk_status": vessel_risk_status,
            "stakeholder_risk_status": stakeholder_risk_status
        }

        insert_sql = """
                     INSERT INTO lng.sts_bunkering_risk_log (
                         request_time, response_time, full_response, 
                         uuid, process_id, process_operator_id, process_operator, process_status,
                         business_segment, trade_type, business_model, operate_water_area, expected_execution_date,
                         is_port_sts, vessel_name, vessel_imo, vessel_number, vessel_transfer_imo, vessel_transfer_name,
                         charterers_risk, consignee_risk, consignor_risk, agent_risk, vessel_broker_risk,
                         vessel_owner_risk, vessel_manager_risk, vessel_operator_risk,
                         vessel_stakeholder_is_sanction_lloyd, vessel_stakeholder_is_sanction_kpler,
                         vessel_is_sanction, vessel_history_is_sanction, vessel_in_uani,
                         vessel_risk_level_lloyd, vessel_risk_level_kpler, vessel_ais_gap,
                         vessel_manipulation, vessel_high_risk_port, vessel_has_dark_port_call,
                         vessel_cargo_sanction, vessel_trade_sanction, cargo_origin_from_sanctioned,
                         vessel_dark_sts_events, vessel_sts_transfer,
                         project_risk_status, vessel_risk_status, stakeholder_risk_status
                     ) VALUES (
                         %(request_time)s, %(response_time)s, %(full_response)s,
                         %(uuid)s, %(process_id)s, %(process_operator_id)s, %(process_operator)s, %(process_status)s,
                         %(business_segment)s, %(trade_type)s, %(business_model)s, %(operate_water_area)s, %(expected_execution_date)s,
                         %(is_port_sts)s, %(vessel_name)s, %(vessel_imo)s, %(vessel_number)s, %(vessel_transfer_imo)s, %(vessel_transfer_name)s,
                         %(charterers_risk)s, %(consignee_risk)s, %(consignor_risk)s, %(agent_risk)s, %(vessel_broker_risk)s,
                         %(vessel_owner_risk)s, %(vessel_manager_risk)s, %(vessel_operator_risk)s,
                         %(vessel_stakeholder_is_sanction_lloyd)s, %(vessel_stakeholder_is_sanction_kpler)s,
                         %(vessel_is_sanction)s, %(vessel_history_is_sanction)s, %(vessel_in_uani)s,
                         %(vessel_risk_level_lloyd)s, %(vessel_risk_level_kpler)s, %(vessel_ais_gap)s,
                         %(vessel_manipulation)s, %(vessel_high_risk_port)s, %(vessel_has_dark_port_call)s,
                         %(vessel_cargo_sanction)s, %(vessel_trade_sanction)s, %(cargo_origin_from_sanctioned)s,
                         %(vessel_dark_sts_events)s, %(vessel_sts_transfer)s,
                         %(project_risk_status)s, %(vessel_risk_status)s, %(stakeholder_risk_status)s
                     )
                     """
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, insert_params)
            conn.commit()
        logger.info(f"STS过驳作业风险数据写入成功（UUID: {request.Uuid}, IMO: {request.vessel_imo}）")
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
@sts_router.post("/risk_screen", response_model=RiskScreenResponse)
async def sts_bunkering_risk_screen(request: RiskScreenRequest):
    """STS过驳作业合规状态筛查主接口"""
    try:
        current_time = get_current_local_time()
        one_year_ago = get_one_year_ago()
        vessel_imo = request.vessel_imo.strip() if request.vessel_imo else ""
        vessel_name = request.vessel_name.strip()
        logger.info(f"开始STS过驳作业合规筛查（UUID: {request.Uuid}, IMO: {vessel_imo}, 名称: {vessel_name}）")

        # 获取船舶明细数据
        vessel_all_data = get_vessel_all_data(vessel_imo) if vessel_imo else {"success": False}

        # ---------------------- 步骤1：构造基础相关方风险（4-12字段） ----------------------
        # 4. 租家（risk_type_number=4，表中risk_type="is_san"）
        charterers_info = get_sanction_info(request.charterers)
        charterers_desc_info = get_sanction_desc_and_info(
            check_item_keyword="租家涉制裁风险",
            risk_type="is_san",  # 对应表中risk_type
            queried_risk_level=charterers_info["sanctions_lev"]
        )
        charterers_risk = StakeholderRisk(
            name=request.charterers.strip(),
            risk_screening_status=charterers_info["sanctions_lev"],
            risk_screening_time=current_time,
            risk_type_number=4,  # 保持编号不变
            risk_description=charterers_desc_info["risk_desc_info"],
            risk_info=charterers_desc_info["info"],
            risk_status_reason=charterers_info["reason"]
        )

        # 5. 收货人（risk_type_number=5，表中risk_type="is_san"）
        consignee_risk = []
        for consignee in request.Consignee:
            consignee_name = consignee.strip()
            if not consignee_name:
                continue

            consignee_info = get_sanction_info(consignee_name)
            consignee_desc_info = get_sanction_desc_and_info(
                check_item_keyword="收货人权控风险",
                risk_type="is_san",  # 对应表中risk_type
                queried_risk_level=consignee_info["sanctions_lev"]
            )

            consignee_risk.append(StakeholderRisk(
                name=consignee_name,
                risk_screening_status=consignee_info["sanctions_lev"],
                risk_screening_time=current_time,
                risk_type_number=5,  # 保持编号不变
                risk_description=consignee_desc_info["risk_desc_info"],
                risk_info=consignee_desc_info["info"],
                risk_status_reason=consignee_info["reason"]
            ))

        # 6. 发货人（risk_type_number=6，表中risk_type="is_san"）
        consignor_risk = []
        for consignor in request.Consignor:
            consignor_name = consignor.strip()
            if not consignor_name:
                continue

            consignor_info = get_sanction_info(consignor_name)
            consignor_desc_info = get_sanction_desc_and_info(
                check_item_keyword="发货人权控风险",
                risk_type="is_san",  # 对应表中risk_type
                queried_risk_level=consignor_info["sanctions_lev"]
            )

            consignor_risk.append(StakeholderRisk(
                name=consignor_name,
                risk_screening_status=consignor_info["sanctions_lev"],
                risk_screening_time=current_time,
                risk_type_number=6,  # 保持编号不变
                risk_description=consignor_desc_info["risk_desc_info"],
                risk_info=consignor_desc_info["info"],
                risk_status_reason=consignor_info["reason"]
            ))

        # 7. 代理（risk_type_number=7，表中risk_type="is_san"）
        agent_risk = []
        for agent in request.Agent:
            agent_name = agent.strip()
            if not agent_name:
                continue

            agent_info = get_sanction_info(agent_name)
            agent_desc_info = get_sanction_desc_and_info(
                check_item_keyword="代理风控风险",
                risk_type="is_san",  # 对应表中risk_type
                queried_risk_level=agent_info["sanctions_lev"]
            )

            agent_risk.append(StakeholderRisk(
                name=agent_name,
                risk_screening_status=agent_info["sanctions_lev"],
                risk_screening_time=current_time,
                risk_type_number=7,  # 保持编号不变
                risk_description=agent_desc_info["risk_desc_info"],
                risk_info=agent_desc_info["info"],
                risk_status_reason=agent_info["reason"]
            ))

        # 8. 租船经纪（risk_type_number=8，表中risk_type="is_san"）
        broker_risk = []
        for broker in request.Vessel_broker:
            broker_name = broker.strip()
            if not broker_name:
                continue

            broker_info = get_sanction_info(broker_name)
            broker_desc_info = get_sanction_desc_and_info(
                check_item_keyword="租船经纪风控风险",
                risk_type="is_san",  # 对应表中risk_type
                queried_risk_level=broker_info["sanctions_lev"]
            )

            broker_risk.append(StakeholderRisk(
                name=broker_name,
                risk_screening_status=broker_info["sanctions_lev"],
                risk_screening_time=current_time,
                risk_type_number=8,  # 保持编号不变
                risk_description=broker_desc_info["risk_desc_info"],
                risk_info=broker_desc_info["info"],
                risk_status_reason=broker_info["reason"]
            ))

        # 9. 注册船东（risk_type_number=9，表中risk_type="is_san"）
        owner_info = get_sanction_info(request.Vessel_owner)
        owner_desc_info = get_sanction_desc_and_info(
            check_item_keyword="注册船东风控风险",
            risk_type="is_san",  # 对应表中risk_type
            queried_risk_level=owner_info["sanctions_lev"]
        )
        owner_risk = StakeholderRisk(
            name=request.Vessel_owner[0].strip() if isinstance(request.Vessel_owner, list) else request.Vessel_owner.strip(),
            risk_screening_status=owner_info["sanctions_lev"],
            risk_screening_time=current_time,
            risk_type_number=9,  # 保持编号不变
            risk_description=owner_desc_info["risk_desc_info"],
            risk_info=owner_desc_info["info"],
            risk_status_reason=owner_info["reason"]
        )

        # 11. 船舶管理人（risk_type_number=11，表中risk_type="is_san"）
        manager_info = get_sanction_info(request.Vessel_manager)
        manager_desc_info = get_sanction_desc_and_info(
            check_item_keyword="船舶管理人风控风险",
            risk_type="is_san",  # 对应表中risk_type
            queried_risk_level=manager_info["sanctions_lev"]
        )
        manager_risk = StakeholderRisk(
            name=request.Vessel_manager[0].strip() if isinstance(request.Vessel_manager, list) else request.Vessel_manager.strip(),
            risk_screening_status=manager_info["sanctions_lev"],
            risk_screening_time=current_time,
            risk_type_number=11,  # 保持编号不变
            risk_description=manager_desc_info["risk_desc_info"],
            risk_info=manager_desc_info["info"],
            risk_status_reason=manager_info["reason"]
        )

        # 12. 船舶经营人（risk_type_number=12，表中risk_type="is_san"）
        operator_info = get_sanction_info(request.Vessel_operator)
        operator_desc_info = get_sanction_desc_and_info(
            check_item_keyword="船舶经营人风控风险",
            risk_type="is_san",  # 对应表中risk_type
            queried_risk_level=operator_info["sanctions_lev"]
        )
        operator_risk = StakeholderRisk(
            name=request.Vessel_operator[0].strip() if isinstance(request.Vessel_operator, list) else request.Vessel_operator.strip(),
            risk_screening_status=operator_info["sanctions_lev"],
            risk_screening_time=current_time,
            risk_type_number=12,  # 保持编号不变
            risk_description=operator_desc_info["risk_desc_info"],
            risk_info=operator_desc_info["info"],
            risk_status_reason=operator_info["reason"]
        )

        # ---------------------- 步骤2：调用外部API获取船舶风险原始数据 ----------------------
        try:
            vessel_sanction_res = get_vessel_sanctions_info(vessel_imo, AUTH_TOKEN) if vessel_imo else {
                "success": False}
        except Exception as e:
            logger.error(f"调用船舶制裁信息API异常: {str(e)}")
            vessel_sanction_res = {"success": False, "error": str(e)}

        try:
            company_sanction_res = get_vessel_company_sanction_info(
                vessel_imo=vessel_imo,
                start_date=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                end_date=datetime.now().strftime("%Y-%m-%d"),
                api_key=AUTH_TOKEN
            ) if vessel_imo else {"success": False}
        except Exception as e:
            logger.error(f"调用公司制裁信息API异常: {str(e)}")
            company_sanction_res = {"success": False, "error": str(e)}

        try:
            risk_analysis_res = get_vessel_risk_analysis(
                vessel_imo=vessel_imo,
                start_date=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                end_date=datetime.now().strftime("%Y-%m-%d"),
                api_key=AUTH_TOKEN
            ) if vessel_imo else {"success": False}
        except Exception as e:
            logger.error(f"调用风险分析API异常: {str(e)}")
            risk_analysis_res = {"success": False, "error": str(e)}

        kpler_imo_list = [int(vessel_imo)] if (vessel_imo and vessel_imo.isdigit()) else []
        try:
            kpler_res = get_kpler_vessel_risk_info(kpler_imo_list, KPLER_TOKEN) if kpler_imo_list else {
                "success": False}
        except Exception as e:
            logger.error(f"调用Kpler API异常: {str(e)}")
            kpler_res = {"success": False, "error": str(e)}

        try:
            ais_manipulation_res = get_vessel_ais_manipulation_info(vessel_imo, AUTH_TOKEN) if vessel_imo else {
                "success": False}
        except Exception as e:
            logger.error(f"调用AIS操纵API异常: {str(e)}")
            ais_manipulation_res = {"success": False, "error": str(e)}

        # ---------------------- 步骤3：解析船舶风险字段（13-28） ----------------------
        # 13. 劳氏船舶相关方制裁（risk_type_number=13，表中risk_type="is_san"）
        vessel_stakeholder_lloyd = []
        if company_sanction_res.get("success"):
            sanctioned_owners = company_sanction_res["data"].get("SanctionedOwners", [])
            for owner in sanctioned_owners:
                ownership_types = owner.get("OwnershipTypes", ["未知类型"])
                company_name = str(owner.get("CompanyName", "")).strip()
                if not company_name:
                    continue

                risk_status = "高风险" if owner.get("OwnerIsCurrentlySanctioned", False) else "无风险"

                # 从数据库获取风险描述和详情
                desc_info = get_sanction_desc_and_info(
                    check_item_keyword="船舶相关方涉制裁风险情况",
                    risk_type="is_san",  # 对应表中risk_type
                    queried_risk_level=risk_status
                )

                risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船舶相关方涉制裁风险情况")
                sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

                for stake_type in ownership_types:
                    vessel_stakeholder_lloyd.append(VesselStakeholderSanction(
                        Vessel_stakeholder_type=stake_type,
                        name=company_name,
                        risk_screening_status=risk_status,
                        risk_screening_time=current_time,
                        risk_type_number=13,  # 保持编号不变
                        risk_description=desc_info["risk_desc_info"],
                        risk_info=desc_info["info"],
                        risk_status_reason=sanctions_dict
                    ))

        # 14. Kpler船舶相关方制裁（risk_type_number=14，表中risk_type="is_san"）
        vessel_stakeholder_kpler = []
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            risk_status = kpler_data.get("risk_level", "无风险")

            # 只有当风险等级为高/中风险时，才获取列表字段
            if risk_status in ["高风险", "中风险", "高", "中"]:
                sanction_comp_source = kpler_data.get("has_sanctioned_companies_list",
                                                      kpler_data.get("sanctionedCompanies", ""))
                if sanction_comp_source and str(sanction_comp_source).strip():
                    stake_type = kpler_data.get("typeName", "未知类型")

                    # 从数据库获取风险描述和详情
                    desc_info = get_sanction_desc_and_info(
                        check_item_keyword="船舶相关方涉制裁风险情况",
                        risk_type="is_san",  # 对应表中risk_type
                        queried_risk_level=risk_status
                    )

                    risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船舶相关方涉制裁风险情况")
                    sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

                    all_names = []
                    try:
                        # 检查是否是有效的JSON格式
                        if isinstance(sanction_comp_source, str) and sanction_comp_source.strip().startswith(('{', '[')):
                            sanction_json = json.loads(sanction_comp_source)
                            all_names = [item.get("name", "").strip() for item in sanction_json if
                                         item.get("name") and item.get("name").strip()]
                        else:
                            # 不是JSON格式，按字符串处理
                            split_parts = str(sanction_comp_source).replace("|", ",").split(",")
                            all_names = [
                                p.split("name:")[-1].strip() if "name:" in p else p.strip()
                                for p in split_parts if p.strip()
                            ]
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"JSON解析失败: {str(e)}, 原始数据: {str(sanction_comp_source)[:100]}")
                        # 解析失败时，按字符串处理
                        split_parts = str(sanction_comp_source).replace("|", ",").split(",")
                        all_names = [
                            p.split("name:")[-1].strip() if "name:" in p else p.strip()
                            for p in split_parts if p.strip()
                        ]

                    unique_names = list(filter(None, set(all_names)))
                    if unique_names:
                        joined_names = ", ".join(unique_names)

                        vessel_stakeholder_kpler.append(VesselStakeholderSanction(
                            Vessel_stakeholder_type=stake_type,
                            name=joined_names,
                            risk_screening_status=risk_status,
                            risk_screening_time=current_time,
                            risk_type_number=14,  # 保持编号不变
                            risk_description=desc_info["risk_desc_info"],
                            risk_info=desc_info["info"],
                            risk_status_reason=sanctions_dict
                        ))

        # 15. 船舶当前制裁（劳氏）（risk_type_number=15，表中risk_type="is_in_sanctions"）
        vessel_is_sanction = VesselRiskItem(risk_type_number=15)
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data = vessel_sanction_res["data"][0]
            is_sanctioned = str(vessel_data.get("is_in_sanctions", "false")).lower() == "true"
            risk_status = "high-risk" if is_sanctioned else "low-risk"

            # 从数据库获取风险描述和详情
            desc_info = get_sanction_desc_and_info(
                check_item_keyword="船舶涉制裁名单风险情况",
                risk_type="is_in_sanctions",  # 对应表中risk_type
                queried_risk_level=risk_status
            )

            # 只有高风险时才获取详情列表
            sanctions_dict = {}
            if risk_status == "high-risk":
                risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船舶涉制裁名单风险情况")
                sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

            vessel_is_sanction = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time,
                risk_type_number=15,  # 保持编号不变
                risk_description=desc_info["risk_desc_info"],
                risk_info=desc_info["info"],
                risk_status_reason=sanctions_dict
            )

        # 16. 船舶历史制裁（劳氏）（risk_type_number=16，表中risk_type="is_in_sanctions_his"）
        vessel_history_sanction = VesselRiskItem(risk_type_number=16)
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data1 = vessel_sanction_res["data"][0]
            has_history = str(vessel_data1.get("is_in_sanctions_his", "false")).lower() == "true"
            risk_status = "high-risk" if has_history else "low-risk"

            # 从数据库获取风险描述和详情
            desc_info = get_sanction_desc_and_info(
                check_item_keyword="船舶涉制裁名单风险情况",
                risk_type="is_in_sanctions_his",  # 对应表中risk_type
                queried_risk_level=risk_status
            )

            # 只有高风险时才获取详情列表
            sanctions_dict = {}
            if risk_status == "high-risk":
                risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船舶涉制裁名单风险情况")
                sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

            vessel_history_sanction = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time,
                risk_type_number=16,  # 保持编号不变
                risk_description=desc_info["risk_desc_info"],
                risk_info=desc_info["info"],
                risk_status_reason=sanctions_dict
            )

        # 17. 船舶UANI清单（risk_type_number=17，表中risk_type="uani_check"）
        in_uani = check_vessel_in_uani(vessel_name)
        risk_status = "高风险" if in_uani else "无风险"

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="船舶涉UANI清单风险情况",
            risk_type="uani_check",  # 对应表中risk_type
            queried_risk_level=risk_status
        )

        # 只有高风险时才获取详情列表
        sanctions_dict = {}
        if risk_status == "高风险":
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "UANI清单")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_in_uani = VesselRiskItem(
            risk_screening_status=risk_status,
            risk_screening_time=current_time,
            risk_type_number=17,  # 保持编号不变
            risk_description=desc_info["risk_desc_info"],
            risk_info=desc_info["info"],
            risk_status_reason=sanctions_dict
        )

        # 18. 船舶劳氏风险等级（risk_type_number=18，表中risk_type="lloyds_sanctions"）
        vessel_risk_lloyd = VesselRiskItem(risk_type_number=18)
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data = vessel_sanction_res["data"][0]
            risk_status = vessel_data.get("sanctions_lev", "无风险")

            # 从数据库获取风险描述和详情
            desc_info = get_sanction_desc_and_info(
                check_item_keyword="船舶涉制裁名单风险情况",
                risk_type="lloyds_sanctions",  # 对应表中risk_type
                queried_risk_level=risk_status
            )

            # 只有高/中风险时才获取详情列表
            sanctions_dict = {}
            if risk_status in ["高风险", "high-risk", "中风险"]:
                risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船舶相关方涉制裁风险情况")
                sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

            vessel_risk_lloyd = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time,
                risk_type_number=18,  # 保持编号不变
                risk_description=desc_info["risk_desc_info"],
                risk_info=desc_info["info"],
                risk_status_reason=sanctions_dict
            )

        # 19. 船舶Kpler风险等级（risk_type_number=19，表中risk_type="risk_level"）
        vessel_risk_kpler = VesselRiskItem(risk_type_number=19)
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            lev_map = {
                "高": "Sanctioned",
                "中": "Risks detected",
                "低": "No risk",
                "无风险": "No risk"
            }
            risk_status = lev_map.get(kpler_data.get("risk_level"), "No risk")

            # 从数据库获取风险描述和详情
            desc_info = get_sanction_desc_and_info(
                check_item_keyword="开普勒风险",
                risk_type="risk_level",  # 对应表中risk_type
                queried_risk_level=risk_status
            )

            # 只有高/中风险时才获取详情列表
            sanctions_dict = {}
            if risk_status in ["Sanctioned", "Risks detected"]:
                risk_data = find_risk_item_in_vessel_data(vessel_all_data, "开普勒风险")
                sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

            vessel_risk_kpler = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time,
                risk_type_number=19,  # 保持编号不变
                risk_description=desc_info["risk_desc_info"],
                risk_info=desc_info["info"],
                risk_status_reason=sanctions_dict
            )

        # 20. 船舶AIS信号缺失（risk_type_number=20，表中risk_type="has_ais_gap_risk"）
        vessel_ais_gap = VesselRiskItem(risk_type_number=20, risk_screening_status="无风险",
                                        risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_ais_gap_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_ais_gap.risk_screening_status = kpler_risk
        if risk_analysis_res.get("success") and vessel_ais_gap.risk_screening_status == "无风险":
            suspicious_ais = risk_analysis_res["data"].get("suspicious_ais_gap", [])
            has_suspicious = any("Suspicious AIS Gap" in item.get("RiskTypes", []) for item in suspicious_ais)
            if has_suspicious:
                vessel_ais_gap.risk_screening_status = "中风险"

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="AIS信号缺失风险",
            risk_type="has_ais_gap_risk",  # 对应表中risk_type
            queried_risk_level=vessel_ais_gap.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_ais_gap.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "AIS信号缺失风险")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_ais_gap.risk_description = desc_info["risk_desc_info"]
        vessel_ais_gap.risk_info = desc_info["info"]
        vessel_ais_gap.risk_status_reason = sanctions_dict

        # 21. 船舶人为伪造及操纵（risk_type_number=21，表中risk_type="has_ais_spoofs_risk"）
        vessel_manipulation = VesselRiskItem(risk_type_number=21, risk_screening_status="无风险",
                                             risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_ais_spoofs_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_manipulation.risk_screening_status = kpler_risk
        if ais_manipulation_res.get("success") and vessel_manipulation.risk_screening_status == "无风险":
            compliance_risks = ais_manipulation_res["data"].get("ComplianceRisks", [])
            has_manipulation = any(risks.get("RiskType") == "VesselAisManipulation" for risks in compliance_risks)
            if has_manipulation:
                vessel_manipulation.risk_screening_status = "中风险"

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="AIS信号篡改风险情况",
            risk_type="has_ais_spoofs_risk",  # 对应表中risk_type
            queried_risk_level=vessel_manipulation.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_manipulation.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "AIS信号伪造风险")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_manipulation.risk_description = desc_info["risk_desc_info"]
        vessel_manipulation.risk_info = desc_info["info"]
        vessel_manipulation.risk_status_reason = sanctions_dict

        # 22. 船舶挂靠高风险港口（risk_type_number=22，表中risk_type="has_port_calls_risk"）
        vessel_risky_port = VesselRiskItem(risk_type_number=22, risk_screening_status="无风险",
                                           risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_port_calls_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_risky_port.risk_screening_status = kpler_risk

                # 风险等级为高/中时，再获取列表字段详情
                port_calls_list = kpler_data.get("has_port_calls_list", "")
                if port_calls_list:
                    for entry in port_calls_list.split("|"):
                        if "startDate" in entry:
                            start_date_str = entry.split("startDate:")[-1].split(",")[0].strip()
                            start_date = parse_timestamp(start_date_str)
                            if start_date and start_date >= one_year_ago:
                                vessel_risky_port.risk_screening_status = "高风险"
                                break
        if risk_analysis_res.get("success") and vessel_risky_port.risk_screening_status == "无风险":
            risk_data_劳氏 = risk_analysis_res["data"].get("VoyageRisks", {})
            high_risk_ports = risk_data_劳氏.get("HighRiskPortCallings", [])
            for port in high_risk_ports:
                voyage_from = parse_datetime_str(port.get("VoyageFrom"))
                if voyage_from and voyage_from >= one_year_ago and port.get("VoyageRiskRating") == "Red":
                    vessel_risky_port.risk_screening_status = "高风险"
                    break

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="高风险港口停靠风险",
            risk_type="has_port_calls_risk",  # 对应表中risk_type
            queried_risk_level=vessel_risky_port.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_risky_port.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "高风险港口")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_risky_port.risk_description = desc_info["risk_desc_info"]
        vessel_risky_port.risk_info = desc_info["info"]
        vessel_risky_port.risk_status_reason = sanctions_dict

        # 23. 船舶Dark port call（risk_type_number=23，表中risk_type="possible_dark_port"）
        vessel_dark_port = VesselRiskItem(risk_type_number=23, risk_screening_status="无风险",
                                          risk_screening_time=current_time)

        if risk_analysis_res.get("success"):
            risk_data_劳氏 = risk_analysis_res["data"]
            # 先判断是否有风险
            dark_ports = risk_data_劳氏.get("possible_dark_port", [])
            for port in dark_ports:
                voyage_start = parse_datetime_str(port.get("VoyageInfo", {}).get("VoyageStartTime"))
                if voyage_start and voyage_start >= one_year_ago:
                    vessel_dark_port.risk_screening_status = "中风险"
                    break

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="暗港访问风险",
            risk_type="possible_dark_port",  # 对应表中risk_type
            queried_risk_level=vessel_dark_port.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_dark_port.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "暗港访问风险")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_dark_port.risk_description = desc_info["risk_desc_info"]
        vessel_dark_port.risk_info = desc_info["info"]
        vessel_dark_port.risk_status_reason = sanctions_dict

        # 24. 船舶运受制裁货物（risk_type_number=24，表中risk_type="has_sanctioned_cargo_risk"）
        vessel_cargo_sanction = VesselRiskItem(risk_type_number=24, risk_screening_status="无风险",
                                               risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_sanctioned_cargo_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_cargo_sanction.risk_screening_status = kpler_risk

                # 风险等级为高/中时，再获取列表字段详情
                cargo_list = kpler_data.get("has_sanctioned_cargo_list", "")
                if cargo_list:
                    if "start_date" in cargo_list:
                        start_date_str = cargo_list.split("start_date:")[-1].split("|")[0].strip()
                        start_date = parse_timestamp(start_date_str)
                        if start_date and start_date >= one_year_ago:
                            vessel_cargo_sanction.risk_screening_status = "高风险"

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="受制裁货物运输风险",
            risk_type="has_sanctioned_cargo_risk",  # 对应表中risk_type
            queried_risk_level=vessel_cargo_sanction.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_cargo_sanction.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "受制裁货物")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_cargo_sanction.risk_description = desc_info["risk_desc_info"]
        vessel_cargo_sanction.risk_info = desc_info["info"]
        vessel_cargo_sanction.risk_status_reason = sanctions_dict

        # 25. 船舶运营受制裁的贸易（risk_type_number=25，表中risk_type="has_sanctioned_trades_risk"）
        vessel_trade_sanction = VesselRiskItem(risk_type_number=25, risk_screening_status="无风险",
                                               risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_sanctioned_trades_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_trade_sanction.risk_screening_status = kpler_risk

                # 风险等级为高/中时，再获取列表字段详情
                trade_list = kpler_data.get("has_sanctioned_trades_list", "")
                if trade_list:
                    for entry in trade_list.split("|"):
                        if "startDate" in entry:
                            start_date_str = entry.split("startDate:")[-1].split(",")[0].strip()
                        elif "start_date" in entry:
                            start_date_str = entry.split("start_date:")[-1].split(",")[0].strip()
                        else:
                            continue
                        start_date = parse_timestamp(start_date_str)
                        if start_date and start_date >= one_year_ago:
                            vessel_trade_sanction.risk_screening_status = "高风险"
                            break

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="受制裁贸易运营风险",
            risk_type="has_sanctioned_trades_risk",  # 对应表中risk_type
            queried_risk_level=vessel_trade_sanction.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_trade_sanction.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "受制裁贸易")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_trade_sanction.risk_description = desc_info["risk_desc_info"]
        vessel_trade_sanction.risk_info = desc_info["info"]
        vessel_trade_sanction.risk_status_reason = sanctions_dict

        # 26. 货物原产地是否为制裁国家（risk_type_number=26，表中risk_type="has_sanctioned_cargo_risk"）
        # 假设从请求中获取货物原产地信息，这里简化处理
        cargo_origin_country = "未知"  # 实际应用中应从请求参数获取
        is_sanctioned_origin = check_cargo_origin_sanctioned(cargo_origin_country)
        risk_status = "高风险" if is_sanctioned_origin else "无风险"

        desc_info = get_sanction_desc_and_info(
            check_item_keyword="货物原产地制裁风险",
            risk_type="has_sanctioned_cargo_risk",  # 对应表中risk_type
            queried_risk_level=risk_status
        )

        sanctions_dict = {}
        if risk_status == "高风险":
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "货物原产地")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        cargo_origin_sanctioned = VesselRiskItem(
            risk_screening_status=risk_status,
            risk_screening_time=current_time,
            risk_type_number=26,  # 保持编号不变
            risk_description=desc_info["risk_desc_info"],
            risk_info=desc_info["info"],
            risk_status_reason={
                "origin_country": cargo_origin_country,
                "details": sanctions_dict
            }
        )

        # 27. 船舶Dark STS events（risk_type_number=27，表中risk_type="has_sts_events_risk"）
        vessel_dark_sts = VesselRiskItem(risk_type_number=27, risk_screening_status="无风险",
                                         risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_sts_events_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_dark_sts.risk_screening_status = kpler_risk

                # 风险等级为高/中时，再获取列表字段详情
                sts_events = kpler_data.get("has_sts_events_list", "")
                if sts_events:
                    for entry in sts_events.split("|"):
                        if "startDate" in entry:
                            start_date_str = entry.split("startDate:")[-1].split(",")[0].strip()
                            start_date = parse_timestamp(start_date_str)
                            if start_date and start_date >= one_year_ago:
                                vessel_dark_sts.risk_screening_status = "高风险"
                                break
        if risk_analysis_res.get("success") and vessel_dark_sts.risk_screening_status == "无风险":
            risk_data_劳氏 = risk_analysis_res["data"].get("dark_sts", [])
            for sts in risk_data_劳氏:
                voyage_start = parse_datetime_str(sts.get("VoyageInfo", {}).get("VoyageStartTime"))
                if voyage_start and voyage_start >= one_year_ago:
                    vessel_dark_sts.risk_screening_status = "高风险"
                    break

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="船对船转运风险",
            risk_type="has_sts_events_risk",  # 对应表中risk_type
            queried_risk_level=vessel_dark_sts.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_dark_sts.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船对船转运")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_dark_sts.risk_description = desc_info["risk_desc_info"]
        vessel_dark_sts.risk_info = desc_info["info"]
        vessel_dark_sts.risk_status_reason = sanctions_dict

        # 28. 船舶STS转运不合规（risk_type_number=28，表中risk_type="sanctioned_sts"）
        vessel_sts_transfer = VesselRiskItem(risk_type_number=28, risk_screening_status="无风险",
                                             risk_screening_time=current_time)

        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 先判断风险等级字段
            kpler_risk = kpler_data.get("has_sts_transfer_risk", "无风险")
            if kpler_risk in ["高风险", "中风险"]:
                vessel_sts_transfer.risk_screening_status = kpler_risk

                # 风险等级为高/中时，再获取列表字段详情
                sts_events = kpler_data.get("has_sts_events_list", "")
                if sts_events:
                    for entry in sts_events.split("|"):
                        if "startDate" in entry:
                            start_date_str = entry.split("startDate:")[-1].split(",")[0].strip()
                            start_date = parse_timestamp(start_date_str)
                            if start_date and start_date >= one_year_ago:
                                vessel_sts_transfer.risk_screening_status = "高风险"
                                break
        if risk_analysis_res.get("success") and vessel_sts_transfer.risk_screening_status == "无风险":
            risk_data_劳氏 = risk_analysis_res["data"].get("VoyageRisks", {})
            sts_sanctioned = risk_data_劳氏.get("StsWithASanctionedVessel", [])
            for sts in sts_sanctioned:
                voyage_from = parse_datetime_str(sts.get("VoyageFrom"))
                if voyage_from and voyage_from >= one_year_ago and sts.get("VoyageRiskRating") == "Red":
                    vessel_sts_transfer.risk_screening_status = "高风险"
                    break

        # 从数据库获取风险描述和详情
        desc_info = get_sanction_desc_and_info(
            check_item_keyword="船对船转运不合规风险",
            risk_type="sanctioned_sts",  # 对应表中risk_type
            queried_risk_level=vessel_sts_transfer.risk_screening_status
        )

        # 只有高/中风险时才获取详情列表
        sanctions_dict = {}
        if vessel_sts_transfer.risk_screening_status in ["高风险", "中风险"]:
            risk_data = find_risk_item_in_vessel_data(vessel_all_data, "船对船转运")
            sanctions_dict = risk_data["sanctions_list"][0] if risk_data["sanctions_list"] else {}

        vessel_sts_transfer.risk_description = desc_info["risk_desc_info"]
        vessel_sts_transfer.risk_info = desc_info["info"]
        vessel_sts_transfer.risk_status_reason = sanctions_dict

        # ---------------------- 步骤5：计算新增风控状态字段 ----------------------
        all_risk_levels = []
        vessel_risk_levels = []
        stakeholder_risk_levels = []

        # 相关方风险（4-12字段）
        stakeholder_risks = [
            charterers_risk, *consignee_risk, *consignor_risk, *agent_risk, *broker_risk,
            owner_risk, manager_risk, operator_risk
        ]
        for risk in stakeholder_risks:
            level = risk.risk_screening_status
            all_risk_levels.append(level)
            stakeholder_risk_levels.append(level)

        # 船舶风险（13-28字段）
        vessel_risk_items = [
            vessel_is_sanction, vessel_history_sanction, vessel_in_uani,
            vessel_risk_lloyd, vessel_risk_kpler, vessel_ais_gap,
            vessel_manipulation, vessel_risky_port, vessel_dark_port,
            vessel_cargo_sanction, vessel_trade_sanction, cargo_origin_sanctioned,
            vessel_dark_sts, vessel_sts_transfer
        ]
        for item in vessel_risk_items:
            if item.risk_screening_status:
                level = item.risk_screening_status
                all_risk_levels.append(level)
                vessel_risk_levels.append(level)
        # 相关方制裁风险（13-14字段）
        for s in vessel_stakeholder_lloyd + vessel_stakeholder_kpler:
            if s.risk_screening_status:
                level = s.risk_screening_status
                all_risk_levels.append(level)
                stakeholder_risk_levels.append(level)

        # 风险等级优先级
        risk_priority = {"高风险": 3, "中风险": 2, "Sanctioned": 3, "Risks detected": 2,
                         "high-risk": 3, "low-risk": 1, "No risk": 1, "无风险": 1}

        # 项目风控状态
        max_project_risk = max(all_risk_levels, key=lambda x: risk_priority.get(x, 0)) if all_risk_levels else "无风险"
        project_risk_status = "拦截" if risk_priority[max_project_risk] >= 3 else "关注" if risk_priority[
                                                                                                max_project_risk] == 2 else "正常"

        # 船舶风控状态
        max_vessel_risk = max(vessel_risk_levels,
                              key=lambda x: risk_priority.get(x, 0)) if vessel_risk_levels else "无风险"
        vessel_risk_status = "高风险" if risk_priority[max_vessel_risk] >= 3 else "中风险" if risk_priority[
                                                                                                  max_vessel_risk] == 2 else "无风险"

        # 相关方风控状态
        max_stakeholder_risk = max(stakeholder_risk_levels,
                                   key=lambda x: risk_priority.get(x, 0)) if stakeholder_risk_levels else "无风险"
        stakeholder_risk_status = "高风险" if risk_priority[max_stakeholder_risk] >= 3 else "中风险" if risk_priority[
                                                                                                            max_stakeholder_risk] == 2 else "无风险"

        # ---------------------- 步骤4：构造完整响应对象 ----------------------
        response = RiskScreenResponse(
            Uuid=request.Uuid,
            Process_id=request.Process_id,
            Vessel_name=vessel_name,
            Vessel_imo=vessel_imo,
            Project_risk_status=project_risk_status,
            Vessel_risk_status=vessel_risk_status,
            Stakeholder_risk_status=stakeholder_risk_status,
            Charterers=charterers_risk,
            Consignee=consignee_risk,
            Consignor=consignor_risk,
            Agent=agent_risk,
            Vessel_broker=broker_risk,
            Vessel_owner=owner_risk,
            Vessel_manager=manager_risk,
            Vessel_operator=operator_risk,
            Vessel_stakeholder_is_sanction_Lloyd=vessel_stakeholder_lloyd,
            Vessel_stakeholder_is_sanction_kpler=vessel_stakeholder_kpler,
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
            Cargo_origin_from_sanctioned=cargo_origin_sanctioned,
            Vessel_dark_sts_events=vessel_dark_sts,
            Vessel_sts_transfer=vessel_sts_transfer
        )

        # ---------------------- 步骤6：写入数据库日志 ----------------------
        insert_sts_bunkering_risk_log(request, response, project_risk_status, vessel_risk_status, stakeholder_risk_status)

        logger.info(f"STS过驳作业合规筛查完成（UUID: {request.Uuid}）")
        return response

    except Exception as e:
        logger.error(f"筛查接口异常（UUID: {request.Uuid}）: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"STS过驳合规筛查失败: {str(e)}")


# ---------------------- 5. 服务启动配置 ----------------------
def create_app():
    """创建FastAPI应用"""
    app = FastAPI(title="STS过驳作业合规状态筛查接口", version="1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(sts_router)

    @app.get("/health", tags=["系统"])
    async def health_check():
        return {
            "status": "healthy",
            "service": "STS过驳作业合规筛查接口",
            "timestamp": get_current_local_time(),
            "database": "kingbase (lng.sts_bunkering_risk_log)"
        }

    @app.get("/", tags=["系统"])
    async def root():
        return {
            "message": "STS过驳作业合规状态筛查接口",
            "version": "1.0",
            "api": {
                "method": "POST",
                "url": "/sts/risk_screen",
                "content_type": "application/json"
            }
        }

    return app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, log_level="info")

# 便捷包装：仅基于 IMO 运行一次风控并返回字典结果
async def run_sts_risk_by_imo(sts_vessel_imo: str) -> Dict[str, Any]:
    """基于 sts_vessel_imo 组装最小入参，调用风控函数并返回结果字典"""
    try:
        # 组装最小可用请求
        now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        req = RiskScreenRequest(
            uuid="auto-gen",
            process_id="",
            process_operator_id="",
            process_operator_name="",
            process_start_time=now,
            process_end_time=now,
            process_status="",
            business_segment="油轮",
            trade_type="",
            business_model="自营",
            operate_water_area="Singapore",
            expected_execution_date=now,
            is_port_sts="true",
            vessel_name=f"STS-{sts_vessel_imo}",
            vessel_imo=sts_vessel_imo,
            vessel_number="",
            vessel_transfer_imo=[],
            vessel_transfer_name=[],
            charterers="Unknown",
            consignee=[],
            consignor=[],
            agent=[],
            vessel_broker=[],
            vessel_owner="Unknown",
            vessel_manager="Unknown",
            vessel_operator="Unknown"
        )
        resp = await sts_bunkering_risk_screen(request=req)
        return resp.model_dump()
    except Exception as e:
        logger.error(f"run_sts_risk_by_imo 调用失败: {e}")
        return {
            "Uuid": "auto-gen",
            "Vessel_name": f"STS-{sts_vessel_imo}",
            "Vessel_imo": sts_vessel_imo,
            "Project_risk_status": "正常"
        }