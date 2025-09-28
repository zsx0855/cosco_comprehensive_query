from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
import pytz
import psycopg2
from psycopg2.extras import DictCursor, Json
from psycopg2 import OperationalError, IntegrityError
import logging
from kingbase_config import get_kingbase_config, get_lloyds_token, get_kpler_token
import json

# 导入外部API函数（复用STS逻辑，船旗变更接口需实际实现数据源）
from vessel_is_in_sanctions import get_vessel_sanctions_info
from KplerDataProcessor import get_kpler_vessel_risk_info
from VesselRiskAnalyzer import get_vessel_risk_analysis
from vessel_complance_risk import get_vessel_company_sanction_info
# 船舶近一年船旗变更查询（占位实现，需根据实际数据源完善）
def get_vessel_flag_change_info(vessel_imo: str, auth_token: str) -> dict:
    """占位：查询船舶近一年是否更换船旗（实际需对接船舶登记接口/数据库）"""
    try:
        # 示例逻辑：模拟返回（实际需替换为真实API调用）
        return {
            "success": True,
            "data": {
                "has_change": False,  # 无船旗变更→默认无风险
                "last_flag": "Panama",
                "change_date": None
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CharterIn_Risk_Screen")

# 路由器（与STS区分前缀，避免接口冲突）
charter_in_router = APIRouter(prefix="/charter_in", tags=["船舶租入合规筛查"])

# 配置加载（复用Kingbase数据库与API令牌）
DB_CONFIG_RAW = get_kingbase_config()
DB_CONFIG = {
    "host": DB_CONFIG_RAW.get("host"),
    "port": DB_CONFIG_RAW.get("port"),
    "user": DB_CONFIG_RAW.get("user"),
    "password": DB_CONFIG_RAW.get("password"),
    "dbname": DB_CONFIG_RAW.get("database")  # 与STS共用数据库
}
AUTH_TOKEN = get_lloyds_token()

KPLER_TOKEN = get_kpler_token()



# ---------------------- 1. 数据模型定义（严格对齐请求/响应参数） ----------------------
class StakeholderRisk(BaseModel):
    """基础相关方风险模型（租船方、管理人、经纪等通用）"""
    name: str
    risk_screening_status: str  # 高风险/中风险/低风险
    risk_screening_time: str    # 筛查时间（YYYY-MM-DDTHH:MM:SSZ）
    risk_status_change_content: str = ""  # 状态变更内容（默认空）
    risk_status_change_time: str = ""     # 状态变更时间（默认空）


class VesselStakeholderSanction(BaseModel):
    """船舶相关方制裁模型（劳氏/Kpler专用）"""
    Vessel_stakeholder_type: Optional[str] = None  # 相关方类型（如Registered Owner）
    name: Optional[str] = None                   # 相关方名称
    risk_screening_status: Optional[str] = None  # 风险状态（高/中/低风险）
    risk_screening_time: Optional[str] = None     # 筛查时间
    risk_status_change_content: Optional[str] = None  # 状态变更内容
    risk_status_change_time: Optional[str] = None     # 状态变更时间


class VesselRiskItem(BaseModel):
    """船舶风险项模型（15-28字段通用）"""
    risk_screening_status: Optional[str] = None  # 风险状态（按字段要求格式）
    risk_screening_time: Optional[str] = None     # 筛查时间
    risk_status_change_content: Optional[str] = None  # 状态变更内容
    risk_status_change_time: Optional[str] = None     # 状态变更时间


class RiskScreenRequest(BaseModel):
    """请求模型（完全匹配用户提供的请求Body参数）"""
    Uuid: str = Field(..., alias="uuid")  # 数据唯一标识（必传）
    Process_id: Optional[str] = Field(None, alias="process_id")  # 流程ID（非必传）
    Process_operator_id: Optional[str] = Field(None, alias="process_operator_id")  # 操作人ID（非必传）
    Process_operator: Optional[str] = Field(None, alias="process_operator_name")  # 操作人姓名（非必传）
    Process_start_time: Optional[datetime] = Field(None, alias="process_start_time")  # 流程发起时间（非必传）
    Process_end_time: Optional[datetime] = Field(None, alias="process_end_time")  # 流程结束时间（非必传）
    Process_status: Optional[str] = Field(None, alias="process_status")  # 流程状态（进行中/已通过/未通过，非必传）
    Vessel_name: str = Field(..., alias="vessel_name")  # 船舶名称（必传）
    Vessel_imo: str = Field(..., alias="vessel_imo")  # 船舶IMO号（必传）
    charterers: str = Field(..., alias="charterers")  # 租船方（必传）
    Vessel_manager: str = Field(..., alias="vessel_manager")  # 船舶管理人（必传）
    Vessel_owner: str = Field(..., alias="vessel_owner")  # 注册船东（必传）
    Vessel_final_beneficiary: str = Field(..., alias="vessel_final_beneficiary")  # 最终受益人（必传）
    Vessel_operator: str = Field(..., alias="vessel_operator")  # 船舶经营人（必传）
    Vessel_broker: List[str] = Field(..., alias="vessel_broker")  # 租船经纪（数组，必传）
    Second_vessel_owner: List[str] = Field(..., alias="second_vessel_owner")  # 船舶二船东（数组，必传）
    Vessel_insurer: List[str] = Field(..., alias="vessel_insurer")  # 船舶保险人（数组，必传）
    Lease_actual_controller: List[str] = Field(..., alias="lease_actual_controller")  # 租约实控人（数组，必传）

    # 时间格式验证（支持"YYYY/MM/DD HH:MM:SS"，如用户示例）
    @field_validator("Process_start_time", "Process_end_time", mode="before")
    def parse_time(cls, v):
        if v is None:
            return None
        try:
            return datetime.strptime(v, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            raise ValueError("时间格式必须为'YYYY/MM/DD HH:MM:SS'（示例：2025/08/20 15:13:25）")

    class Config:
        populate_by_name = True  # 允许通过JSON小写别名（如uuid）映射字段


class RiskScreenResponse(BaseModel):
    """响应模型（完全匹配用户提供的返回参数）"""
    # 0-3：基础标识字段
    uuid: str
    process_id: Optional[str] = None
    vessel_name: str
    vessel_imo: str

    # 4-12：相关方风险（Dict/Array<Dict>）
    charterers: StakeholderRisk  # 4. 租船方
    Vessel_manager: StakeholderRisk  # 5. 船舶管理人
    Vessel_owner: StakeholderRisk  # 6. 注册船东
    Vessel_final_beneficiary: StakeholderRisk  # 7. 船舶最终受益人
    Vessel_operator: StakeholderRisk  # 8. 船舶经营人
    Vessel_broker: List[StakeholderRisk]  # 9. 租船经纪（数组）
    Second_vessel_owner: List[StakeholderRisk]  # 10. 船舶二船东（数组）
    Vessel_insurer: List[StakeholderRisk]  # 11. 船舶保险人（数组）
    Lease_actual_controller: List[StakeholderRisk]  # 12. 租约相对方实控人（数组）

    # 13-28：船舶维度风险（Dict/Array<Dict>）
    Vessel_stakeholder_is_sanction_Lloyd: Optional[List[VesselStakeholderSanction]] = None  # 13. 劳氏相关方制裁
    Vessel_stakeholder_is_sanction_kpler: Optional[List[VesselStakeholderSanction]] = None  # 14. Kpler相关方制裁
    Vessel_is_sanction: Optional[VesselRiskItem] = None  # 15. 船舶当前制裁（劳氏）
    Vessel_history_is_sanction: Optional[VesselRiskItem] = None  # 16. 船舶历史制裁（劳氏）
    Vessel_in_uani: Optional[VesselRiskItem] = None  # 17. 船舶UANI清单
    Vessel_risk_level_Lloyd: Optional[VesselRiskItem] = None  # 18. 船舶劳氏风险等级
    Vessel_risk_level_kpler: Optional[VesselRiskItem] = None  # 19. 船舶Kpler风险等级
    Vessel_ais_gap: Optional[VesselRiskItem] = None  # 20. 船舶AIS信号缺失
    Vessel_Manipulation: Optional[VesselRiskItem] = None  # 21. 船舶人为伪造及操纵
    Vessel_risky_port_call: Optional[VesselRiskItem] = None  # 22. 船舶挂靠高风险港口
    Vessel_dark_port_call: Optional[VesselRiskItem] = None  # 23. 船舶Dark Port Call
    Vessel_change_flag: Optional[VesselRiskItem] = None  # 24. 船舶近一年换船旗
    Vessel_cargo_sanction: Optional[VesselRiskItem] = None  # 25. 船舶运受制裁货物
    Vessel_trade_sanction: Optional[VesselRiskItem] = None  # 26. 船舶运营受制裁贸易
    Vessel_dark_sts_events: Optional[VesselRiskItem] = None  # 27. 船舶Dark STS事件
    Vessel_sts_transfer: Optional[VesselRiskItem] = None  # 28. 船舶STS转运不合规


# ---------------------- 2. 工具函数（复用+专属逻辑） ----------------------
def get_sanction_level(name: str) -> str:
    """查询相关方制裁等级（从 lng.sanctions_risk_result 表获取）"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(**connect_params)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT sanctions_lev FROM lng.sanctions_risk_result WHERE ENTITYNAME1 LIKE %s LIMIT 1",
                (f"%{name}%",)  # 模糊匹配（兼容名称前缀/后缀差异）
            )
            result = cursor.fetchone()
            return result["sanctions_lev"] if (result and result["sanctions_lev"]) else "无风险"
    except psycopg2.Error as e:
        logger.error(f"相关方制裁等级查询失败（{name}）: {str(e)}")
        return "无风险"  # 异常时默认无风险
    finally:
        if conn:
            conn.close()


def get_current_local_time() -> str:
    """获取上海时区时间（统一格式：YYYY-MM-DDTHH:MM:SSZ）"""
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    local_time = datetime.now(shanghai_tz)
    return local_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_time(dt: Optional[datetime]) -> Optional[str]:
    """转换请求中的datetime为上海时区字符串"""
    if not dt:
        return None
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    local_time = dt.astimezone(shanghai_tz)
    return local_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def check_vessel_in_uani(vessel_name: str) -> bool:
    """查询船舶是否在UANI清单（lng.uani_list表）"""
    conn = None
    try:
        connect_params = DB_CONFIG.copy()
        connect_params['cursor_factory'] = DictCursor
        conn = psycopg2.connect(**connect_params)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM lng.uani_list WHERE vessel_name = %s LIMIT 1",
                (vessel_name.strip(),)  # 去空格匹配（避免名称前后空格问题）
            )
            return cursor.fetchone() is not None  # 存在则返回True
    except psycopg2.Error as e:
        logger.error(f"UANI清单查询失败（{vessel_name}）: {str(e)}")
        return False  # 异常时默认不在清单
    finally:
        if conn:
            conn.close()


# ---------------------- 3. 数据写入函数（适配租入表结构） ----------------------
def insert_charter_in_risk_log(request: RiskScreenRequest, response: RiskScreenResponse):
    """将筛查结果写入 lng.vessle_charter_in_risk_log 表（严格对齐表字段）"""
    conn = None
    try:
        current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
        full_response_json = response.model_dump()

        # 模型转JSONB（空值用默认结构填充，避免违反表NOT NULL约束）
        def model_to_json(obj):
            if isinstance(obj, list):
                return [item.model_dump() for item in obj] if obj else []  # 空数组
            elif obj:
                return obj.model_dump()  # 有数据则转Dict
            else:
                return {}  # 空对象

        # 构造插入参数（与表字段一一对应）
        insert_params = {
            # 1. 基础追溯字段
            "request_time": current_time,
            "response_time": current_time,
            "full_response": Json(full_response_json),
            # 2. 流程核心标识
            "uuid": request.Uuid,
            "process_id": request.Process_id or "",
            "process_operator_id": request.Process_operator_id or "",
            "process_operator": request.Process_operator or "",
            "process_status": request.Process_status or "",
            # 3. 船舶基础信息
            "vessel_name": request.Vessel_name.strip(),
            "vessel_imo": request.Vessel_imo.strip(),
            # 4. 相关方风险数据（4-12字段）
            "charterers_risk": Json(model_to_json(response.charterers)),
            "vessel_manager_risk": Json(model_to_json(response.Vessel_manager)),
            "vessel_owner_risk": Json(model_to_json(response.Vessel_owner)),
            "vessel_final_beneficiary_risk": Json(model_to_json(response.Vessel_final_beneficiary)),
            "vessel_operator_risk": Json(model_to_json(response.Vessel_operator)),
            "vessel_broker_risk": Json(model_to_json(response.Vessel_broker)),
            "second_vessel_owner_risk": Json(model_to_json(response.Second_vessel_owner)),
            "vessel_insurer_risk": Json(model_to_json(response.Vessel_insurer)),
            "lease_actual_controller_risk": Json(model_to_json(response.Lease_actual_controller)),
            # 5. 船舶维度风险数据（13-28字段）
            "vessel_stakeholder_is_sanction_Lloyd": Json(model_to_json(response.Vessel_stakeholder_is_sanction_Lloyd)),
            "vessel_stakeholder_is_sanction_kpler": Json(model_to_json(response.Vessel_stakeholder_is_sanction_kpler)),
            "vessel_is_sanction": Json(model_to_json(response.Vessel_is_sanction)),
            "vessel_history_is_sanction": Json(model_to_json(response.Vessel_history_is_sanction)),
            "vessel_in_uani": Json(model_to_json(response.Vessel_in_uani)),
            "vessel_risk_level_lloyd": Json(model_to_json(response.Vessel_risk_level_Lloyd)),
            "vessel_risk_level_kpler": Json(model_to_json(response.Vessel_risk_level_kpler)),
            "vessel_ais_gap": Json(model_to_json(response.Vessel_ais_gap)),
            "vessel_manipulation": Json(model_to_json(response.Vessel_Manipulation)),
            "vessel_risky_port_call": Json(model_to_json(response.Vessel_risky_port_call)),
            "vessel_dark_port_call": Json(model_to_json(response.Vessel_dark_port_call)),
            "vessel_change_flag": Json(model_to_json(response.Vessel_change_flag)),
            "vessel_cargo_sanction": Json(model_to_json(response.Vessel_cargo_sanction)),
            "vessel_trade_sanction": Json(model_to_json(response.Vessel_trade_sanction)),
            "vessel_dark_sts_events": Json(model_to_json(response.Vessel_dark_sts_events)),
            "vessel_sts_transfer": Json(model_to_json(response.Vessel_sts_transfer))
        }

        # 插入SQL（完全匹配表结构字段顺序）
        insert_sql = """
        INSERT INTO lng.vessle_charter_in_risk_log (
            request_time, response_time, full_response,
            uuid, process_id, process_operator_id, process_operator, process_status,
            vessel_name, vessel_imo,
            charterers_risk, vessel_manager_risk, vessel_owner_risk, vessel_final_beneficiary_risk,
            vessel_operator_risk, vessel_broker_risk, second_vessel_owner_risk, vessel_insurer_risk,
            lease_actual_controller_risk,
            vessel_stakeholder_is_sanction_Lloyd, vessel_stakeholder_is_sanction_kpler,
            vessel_is_sanction, vessel_history_is_sanction, vessel_in_uani,
            vessel_risk_level_lloyd, vessel_risk_level_kpler, vessel_ais_gap,
            vessel_manipulation, vessel_risky_port_call, vessel_dark_port_call,
            vessel_change_flag, vessel_cargo_sanction, vessel_trade_sanction,
            vessel_dark_sts_events, vessel_sts_transfer
        ) VALUES (
            %(request_time)s, %(response_time)s, %(full_response)s,
            %(uuid)s, %(process_id)s, %(process_operator_id)s, %(process_operator)s, %(process_status)s,
            %(vessel_name)s, %(vessel_imo)s,
            %(charterers_risk)s, %(vessel_manager_risk)s, %(vessel_owner_risk)s, %(vessel_final_beneficiary_risk)s,
            %(vessel_operator_risk)s, %(vessel_broker_risk)s, %(second_vessel_owner_risk)s, %(vessel_insurer_risk)s,
            %(lease_actual_controller_risk)s,
            %(vessel_stakeholder_is_sanction_Lloyd)s, %(vessel_stakeholder_is_sanction_kpler)s,
            %(vessel_is_sanction)s, %(vessel_history_is_sanction)s, %(vessel_in_uani)s,
            %(vessel_risk_level_lloyd)s, %(vessel_risk_level_kpler)s, %(vessel_ais_gap)s,
            %(vessel_manipulation)s, %(vessel_risky_port_call)s, %(vessel_dark_port_call)s,
            %(vessel_change_flag)s, %(vessel_cargo_sanction)s, %(vessel_trade_sanction)s,
            %(vessel_dark_sts_events)s, %(vessel_sts_transfer)s
        )
        """
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, insert_params)
            conn.commit()
        logger.info(f"船舶租入风险数据写入成功（UUID: {request.Uuid}, IMO: {request.Vessel_imo}）")
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


# ---------------------- 4. 核心接口函数（完整逻辑） ----------------------
@charter_in_router.post("/vessel_charter_risk", response_model=RiskScreenResponse)
async def charter_in_risk_screen(request: RiskScreenRequest):
    """船舶租入合规状态筛查主接口"""
    try:
        current_time = get_current_local_time()
        vessel_imo = request.Vessel_imo.strip()
        vessel_name = request.Vessel_name.strip()
        logger.info(f"开始船舶租入合规筛查（UUID: {request.Uuid}, IMO: {vessel_imo}, 名称: {vessel_name}）")

        # ---------------------- 步骤1：构造基础相关方风险（4-12字段） ----------------------
        # 4. 租船方风险
        charterers_risk = StakeholderRisk(
            name=request.charterers.strip(),
            risk_screening_status=get_sanction_level(request.charterers),
            risk_screening_time=current_time
        )

        # 5. 船舶管理人风险
        manager_risk = StakeholderRisk(
            name=request.Vessel_manager.strip(),
            risk_screening_status=get_sanction_level(request.Vessel_manager),
            risk_screening_time=current_time
        )

        # 6. 注册船东风险
        owner_risk = StakeholderRisk(
            name=request.Vessel_owner.strip(),
            risk_screening_status=get_sanction_level(request.Vessel_owner),
            risk_screening_time=current_time
        )

        # 7. 船舶最终受益人风险（租入场景专属）
        final_beneficiary_risk = StakeholderRisk(
            name=request.Vessel_final_beneficiary.strip(),
            risk_screening_status=get_sanction_level(request.Vessel_final_beneficiary),
            risk_screening_time=current_time
        )

        # 8. 船舶经营人风险
        operator_risk = StakeholderRisk(
            name=request.Vessel_operator.strip(),
            risk_screening_status=get_sanction_level(request.Vessel_operator),
            risk_screening_time=current_time
        )

        # 9. 租船经纪风险（数组）
        broker_risk = [
            StakeholderRisk(
                name=broker.strip(),
                risk_screening_status=get_sanction_level(broker),
                risk_screening_time=current_time
            ) for broker in request.Vessel_broker if broker.strip()  # 过滤空字符串
        ]

        # 10. 船舶二船东风险（数组）
        second_owner_risk = [
            StakeholderRisk(
                name=owner.strip(),
                risk_screening_status=get_sanction_level(owner),
                risk_screening_time=current_time
            ) for owner in request.Second_vessel_owner if owner.strip()
        ]

        # 11. 船舶保险人风险（数组）
        insurer_risk = [
            StakeholderRisk(
                name=insurer.strip(),
                risk_screening_status=get_sanction_level(insurer),
                risk_screening_time=current_time
            ) for insurer in request.Vessel_insurer if insurer.strip()
        ]

        # 12. 租约相对方实控人风险（数组）
        lease_controller_risk = [
            StakeholderRisk(
                name=controller.strip(),
                risk_screening_status=get_sanction_level(controller),
                risk_screening_time=current_time
            ) for controller in request.Lease_actual_controller if controller.strip()
        ]

        # ---------------------- 步骤2：调用外部API获取船舶风险原始数据 ----------------------
        # 2.1 劳氏数据（13/15/16/18/23/27字段）
        vessel_sanction_res = get_vessel_sanctions_info(vessel_imo, AUTH_TOKEN) if vessel_imo else {"success": False}
        company_sanction_res = get_vessel_company_sanction_info(
            vessel_imo=vessel_imo,
            start_date="2024-01-01",  # 固定查询近1年数据
            end_date=datetime.now().strftime("%Y-%m-%d"),
            api_key=AUTH_TOKEN
        ) if vessel_imo else {"success": False}
        risk_analysis_res = get_vessel_risk_analysis(
            vessel_imo=vessel_imo,
            start_date="2024-01-01",
            end_date=datetime.now().strftime("%Y-%m-%d"),
            api_key=AUTH_TOKEN
        ) if vessel_imo else {"success": False}

        # 2.2 Kpler数据（14/19/20/21/22/25/26/28字段）
        kpler_imo_list = [int(vessel_imo)] if (vessel_imo and vessel_imo.isdigit()) else []
        kpler_res = get_kpler_vessel_risk_info(kpler_imo_list, KPLER_TOKEN) if kpler_imo_list else {"success": False}

        # 2.3 船旗变更数据（24字段）
        flag_change_res = get_vessel_flag_change_info(vessel_imo, AUTH_TOKEN) if vessel_imo else {"success": False}

        # ---------------------- 步骤3：解析船舶风险字段（13-28） ----------------------
        # 13. 劳氏船舶相关方制裁（Vessel_stakeholder_is_sanction_Lloyd）
        vessel_stakeholder_lloyd = []
        if company_sanction_res.get("success"):
            sanctioned_owners = company_sanction_res["data"].get("SanctionedOwners", [])
            for owner in sanctioned_owners:
                # 相关方类型（有几个返回几个）
                ownership_types = owner.get("OwnershipTypes", ["未知类型"])
                company_name = owner.get("CompanyName", "未知公司").strip()
                # 风险状态（有制裁→高风险，无→无风险）
                risk_status = "高风险" if owner.get("OwnerIsCurrentlySanctioned", False) else "无风险"
                # 生成每个类型的条目
                for stake_type in ownership_types:
                    vessel_stakeholder_lloyd.append(VesselStakeholderSanction(
                        Vessel_stakeholder_type=stake_type,
                        name=company_name,
                        risk_screening_status=risk_status,
                        risk_screening_time=current_time
                    ))

        # 14. Kpler船舶相关方制裁（Vessel_stakeholder_is_sanction_kpler）
        vessel_stakeholder_kpler = []
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 核心判断：has_sanctioned_companies_list有数据即视为有制裁
            sanction_comp_source = kpler_data.get("has_sanctioned_companies_list", kpler_data.get("sanctionedCompanies", ""))
            if sanction_comp_source and str(sanction_comp_source).strip():
                # 相关方类型（取typeName）
                stake_type = kpler_data.get("typeName", "未知类型")
                # 风险状态（直接取kpler的risk_level）
                risk_status = kpler_data.get("risk_level", "无风险")
                # 解析所有name并拼接（兼容JSON/字符串格式）
                all_names = []
                try:
                    # 检查是否是有效的JSON格式
                    if isinstance(sanction_comp_source, str) and sanction_comp_source.strip().startswith(('{', '[')):
                        # 场景1：JSON格式（如[{"name":"公司A"},{"name":"公司B"}]）
                        sanction_json = json.loads(sanction_comp_source)
                        all_names = [item.get("name", "").strip() for item in sanction_json if item.get("name")]
                    else:
                        # 场景2：字符串格式（如"name:公司A|name:公司B"或"公司A,公司B"）
                        split_parts = str(sanction_comp_source).replace("|", ",").split(",")
                        all_names = [
                            p.split("name:")[-1].strip() if "name:" in p else p.strip()
                            for p in split_parts if p.strip()
                        ]
                except (json.JSONDecodeError, TypeError) as e:
                    # 解析失败时，按字符串处理
                    split_parts = str(sanction_comp_source).replace("|", ",").split(",")
                    all_names = [
                        p.split("name:")[-1].strip() if "name:" in p else p.strip()
                        for p in split_parts if p.strip()
                    ]
                # 去重并拼接名称
                unique_names = list(set(all_names))  # 去重
                joined_names = ", ".join(unique_names) if unique_names else "未知名称"
                # 生成条目（有数据即返回1条）
                vessel_stakeholder_kpler.append(VesselStakeholderSanction(
                    Vessel_stakeholder_type=stake_type,
                    name=joined_names,
                    risk_screening_status=risk_status,
                    risk_screening_time=current_time
                ))

        # 15. 船舶当前制裁（Vessel_is_sanction，劳氏）
        vessel_is_sanction = VesselRiskItem()
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data = vessel_sanction_res["data"][0]
            vessel_is_sanction = VesselRiskItem(
                risk_screening_status=vessel_data.get("sanctions_lev", "low-risk"),  # 按劳氏格式返回
                risk_screening_time=current_time
            )

        # 16. 船舶历史制裁（Vessel_history_is_sanction，劳氏）
        vessel_history_sanction = VesselRiskItem()
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data = vessel_sanction_res["data"][0]
            # 有历史制裁→high-risk，无→low-risk
            risk_status = "high-risk" if vessel_data.get("is_in_sanctions_his", "否") == "是" else "low-risk"
            vessel_history_sanction = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time
            )

        # 17. 船舶UANI清单（Vessel_in_uani）
        vessel_in_uani = VesselRiskItem(
            risk_screening_status="高风险" if check_vessel_in_uani(vessel_name) else "无风险",
            risk_screening_time=current_time
        )

        # 18. 船舶劳氏风险等级（Vessel_risk_level_Lloyd）
        vessel_risk_lloyd = VesselRiskItem()
        if vessel_sanction_res.get("success") and vessel_sanction_res["data"]:
            vessel_data = vessel_sanction_res["data"][0]
            vessel_risk_lloyd = VesselRiskItem(
                risk_screening_status=vessel_data.get("sanctions_lev", "low-risk"),  # 直接取劳氏等级
                risk_screening_time=current_time
            )

        # 19. 船舶Kpler风险等级（Vessel_risk_level_kpler）
        vessel_risk_kpler = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            # 映射为Kpler要求格式：Sanctioned/Risks detected/No risk
            lev_map = {
                "高": "Sanctioned",
                "中": "Risks detected",
                "低": "No risk",
                "无风险": "No risk"
            }
            risk_status = lev_map.get(kpler_data.get("risk_level"), "No risk")
            vessel_risk_kpler = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time
            )

        # 20. 船舶AIS信号缺失（Vessel_ais_gap）
        vessel_ais_gap = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_ais_gap = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_ais_gap_risk", "无风险"),
                risk_screening_time=current_time
            )

        # 21. 船舶人为伪造及操纵（Vessel_Manipulation）
        vessel_manipulation = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_manipulation = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_ais_spoofs_risk", "无风险"),
                risk_screening_time=current_time
            )

        # 22. 船舶挂靠高风险港口（Vessel_risky_port_call）
        vessel_risky_port = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_risky_port = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_port_calls_risk", "无风险"),
                risk_screening_time=current_time
            )

        # 23. 船舶Dark Port Call（Vessel_dark_port_call）
        vessel_dark_port = VesselRiskItem()
        if risk_analysis_res.get("success"):
            risk_data = risk_analysis_res["data"]
            # 有暗港数据→中风险，无→无风险
            risk_status = "中风险" if bool(risk_data.get("possible_dark_port", [])) else "无风险"
            vessel_dark_port = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time
            )

        # 24. 船舶近一年换船旗（Vessel_change_flag）
        vessel_change_flag = VesselRiskItem()
        if flag_change_res.get("success"):
            has_change = flag_change_res["data"].get("has_change", False)
            vessel_change_flag = VesselRiskItem(
                risk_screening_status="高风险" if has_change else "无风险",
                risk_screening_time=current_time
            )
        else:
            vessel_change_flag = VesselRiskItem(
                risk_screening_status="无风险",  # 接口异常时默认无风险
                risk_screening_time=current_time
            )

        # 25. 船舶运受制裁货物（Vessel_cargo_sanction）
        vessel_cargo_sanction = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_cargo_sanction = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_sanctioned_cargo_risk", "无风险"),
                risk_screening_time=current_time
            )

        # 26. 船舶运营受制裁贸易（Vessel_trade_sanction）
        vessel_trade_sanction = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_trade_sanction = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_sanctioned_trades_risk", "无风险"),
                risk_screening_time=current_time
            )

        # 27. 船舶Dark STS事件（Vessel_dark_sts_events）
        vessel_dark_sts = VesselRiskItem()
        if risk_analysis_res.get("success"):
            risk_data = risk_analysis_res["data"]
            has_dark_sts = bool(risk_data.get("dark_sts", []))
            if has_dark_sts:
                risk_status = "高风险"
            else:
                # 无暗STS数据时，取Kpler的has_dark_sts_risk
                kpler_data = kpler_res["data"].get(vessel_imo, {}) if kpler_res.get("success") else {}
                risk_status = kpler_data.get("has_dark_sts_risk", "无风险")
            vessel_dark_sts = VesselRiskItem(
                risk_screening_status=risk_status,
                risk_screening_time=current_time
            )

        # 28. 船舶STS转运不合规（Vessel_sts_transfer）
        vessel_sts_transfer = VesselRiskItem()
        if kpler_res.get("success"):
            kpler_data = kpler_res["data"].get(vessel_imo, {})
            vessel_sts_transfer = VesselRiskItem(
                risk_screening_status=kpler_data.get("has_sts_events_risk", "无风险"),
                risk_screening_time=current_time
            )

        # ---------------------- 步骤4：构造完整响应对象 ----------------------
        response = RiskScreenResponse(
            # 基础标识字段
            uuid=request.Uuid,
            process_id=request.Process_id,
            vessel_name=vessel_name,
            vessel_imo=vessel_imo,
            # 4-12：相关方风险
            charterers=charterers_risk,
            Vessel_manager=manager_risk,
            Vessel_owner=owner_risk,
            Vessel_final_beneficiary=final_beneficiary_risk,
            Vessel_operator=operator_risk,
            Vessel_broker=broker_risk,
            Second_vessel_owner=second_owner_risk,
            Vessel_insurer=insurer_risk,
            Lease_actual_controller=lease_controller_risk,
            # 13-28：船舶风险
            Vessel_stakeholder_is_sanction_Lloyd=vessel_stakeholder_lloyd,
            Vessel_stakeholder_is_sanction_kpler=vessel_stakeholder_kpler,
            Vessel_is_sanction=vessel_is_sanction,
            Vessel_history_is_sanction=vessel_history_sanction,
            Vessel_in_uani=vessel_in_uani,
            Vessel_risk_level_Lloyd=vessel_risk_lloyd,
            Vessel_risk_level_kpler=vessel_risk_kpler,
            Vessel_ais_gap=vessel_ais_gap,
            Vessel_Manipulation=vessel_manipulation,
            Vessel_risky_port_call=vessel_risky_port,
            Vessel_dark_port_call=vessel_dark_port,
            Vessel_change_flag=vessel_change_flag,
            Vessel_cargo_sanction=vessel_cargo_sanction,
            Vessel_trade_sanction=vessel_trade_sanction,
            Vessel_dark_sts_events=vessel_dark_sts,
            Vessel_sts_transfer=vessel_sts_transfer
        )

        # ---------------------- 步骤5：写入数据库日志 ----------------------
        insert_charter_in_risk_log(request, response)

        logger.info(f"船舶租入合规筛查完成（UUID: {request.Uuid}）")
        return response

    except Exception as e:
        logger.error(f"筛查接口异常（UUID: {request.Uuid}）: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"租入合规筛查失败: {str(e)}")


# ---------------------- 5. 服务启动配置（调试/部署用） ----------------------
def create_app():
    """创建FastAPI应用（含CORS跨域配置）"""
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(title="船舶租入合规状态筛查接口", version="1.0")

    # 跨域配置（允许前端调用）
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 生产环境需替换为具体前端域名
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 挂载路由器
    app.include_router(charter_in_router)

    # 健康检查接口（用于监控）
    @app.get("/health", tags=["系统"])
    async def health_check():
        return {
            "status": "healthy",
            "service": "船舶租入合规筛查接口",
            "timestamp": get_current_local_time(),
            "database": "kingbase (lng.vessle_charter_in_risk_log)"
        }

    # 根路径说明
    @app.get("/", tags=["系统"])
    async def root():
        return {
            "message": "船舶租入合规状态筛查接口",
            "version": "1.0",
            "api": {
                "method": "POST",
                "url": "/charter_in/vessel_charter_risk",
                "content_type": "application/json"
            }
        }

    return app


if __name__ == "__main__":
    """本地调试启动（端口8000）"""
    import uvicorn
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, log_level="info")