#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修改后的 external_api.py 示例
展示如何集成 functions_risk_check_framework.py 的道琼斯制裁风险检查功能
"""

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# 导入风险检查框架
from functions_risk_check_framework import RiskCheckOrchestrator, create_api_config

# 导入数据记录模块
from voyage_risk_log_insert import insert_voyage_risk_log

# 基础配置类，支持大小写不敏感
class CaseInsensitiveBaseModel(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
        # 支持大小写不敏感
        populate_by_name=True
    )

# KingBase数据库配置
KINGBASE_CONFIG = {
    'host': '10.11.142.145',
    'port': 54321,
    'database': 'lngdb',
    'user': 'system',
    'password': 'zV2,oB5%',
    'cursor_factory': RealDictCursor
}

external_router = APIRouter(prefix="/external", tags=["External APIs"])

# 创建风险检查编排器（全局实例）
api_config = create_api_config()
risk_orchestrator = RiskCheckOrchestrator(api_config)

def query_port_risk(port_name: str) -> str:
    """
    查询港口风险等级
    Args:
        port_name: 港口名称
    Returns:
        str: 风险等级，如果匹配countryname则为"高风险"，否则为"无风险"
    """
    try:
        config = KINGBASE_CONFIG
        connection = psycopg2.connect(**config)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = "SELECT countryname, countryname_china FROM lng.contry_port WHERE countryname = %s"
            cursor.execute(sql, (port_name,))
            result = cursor.fetchone()
            if result:
                return "高风险"
            else:
                return "无风险"
    except Exception as e:
        print(f"查询港口风险时出错: {e}")
        return "无风险"
    finally:
        if 'connection' in locals():
            connection.close()

def query_sanctions_risk_with_framework(entity_name: str) -> dict:
    """
    使用风险检查框架查询实体制裁风险等级和制裁列表
    
    Args:
        entity_name: 实体名称
        
    Returns:
        dict: 包含制裁风险等级和制裁列表的字典
        {
            'risk_level': str,  # 制裁风险等级
            'sanctions_list': str,  # 制裁列表
            'mid_sanctions_list': str,  # 中等制裁列表
            'no_sanctions_list': str,  # 无制裁列表
            'is_san': str,  # 是否受制裁
            'is_sco': str,  # 是否受SCO制裁
            'is_ool': str,  # 是否在其他官方名单中
            'is_one_year': str,  # 是否一年内受制裁
            'is_sanctioned_countries': str  # 是否来自制裁国家
        }
    """
    try:
        # 调用风险检查框架的道琼斯制裁风险检查
        result = risk_orchestrator.execute_dowjones_sanctions_risk_check(entity_name)
        
        # 检查返回结果类型
        if isinstance(result, dict):
            # 从风险状态原因中提取详细信息
            risk_status_reason = result.get('risk_status_reason', {})
            
            return {
                'risk_level': result.get('risk_screening_status', '无风险'),
                'sanctions_list': risk_status_reason.get('sanctions_list', []),
                'mid_sanctions_list': risk_status_reason.get('mid_sanctions_list', []),
                'no_sanctions_list': risk_status_reason.get('no_sanctions_list', []),
                'is_san': risk_status_reason.get('is_san', ''),
                'is_sco': risk_status_reason.get('is_sco', ''),
                'is_ool': risk_status_reason.get('is_ool', ''),
                'is_one_year': risk_status_reason.get('is_one_year', ''),
                'is_sanctioned_countries': risk_status_reason.get('is_sanctioned_countries', ''),
                'risk_description': result.get('risk_description', ''),
                'risk_screening_time': result.get('risk_screening_time', '')
            }
        else:
            # 如果是CheckResult对象，转换为字典格式
            result_dict = result.to_dict()
            return {
                'risk_level': result_dict.get('risk_value', '无风险'),
                'sanctions_list': result_dict.get('tab', []),
                'mid_sanctions_list': [],
                'no_sanctions_list': [],
                'is_san': '',
                'is_sco': '',
                'is_ool': '',
                'is_one_year': '',
                'is_sanctioned_countries': '',
                'risk_description': result_dict.get('risk_desc', ''),
                'risk_screening_time': ''
            }
                
    except Exception as e:
        print(f"查询制裁风险时出错: {e}")
        return {
            'risk_level': '无风险',
            'sanctions_list': [],
            'mid_sanctions_list': [],
            'no_sanctions_list': [],
            'is_san': '',
            'is_sco': '',
            'is_ool': '',
            'is_one_year': '',
            'is_sanctioned_countries': '',
            'risk_description': '',
            'risk_screening_time': ''
        }

def get_current_time() -> str:
    """获取当前时间，格式为 YYYY-MM-DD hh:mm:ss"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def parse_sanctions_json(sanctions_str: str) -> dict:
    """
    解析制裁字符串为JSON格式
    
    Args:
        sanctions_str: 制裁字符串
        
    Returns:
        dict: 解析后的JSON对象，如果解析失败返回空字典
    """
    if not sanctions_str or sanctions_str.strip() == '':
        return {}
    
    try:
        import json
        # 首先尝试直接解析
        return json.loads(sanctions_str)
    except (json.JSONDecodeError, TypeError):
        try:
            # 处理包含转义字符的JSON字符串
            # 先尝试解码转义字符
            decoded_str = sanctions_str.encode().decode('unicode_escape')
            return json.loads(decoded_str)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            try:
                # 尝试处理可能是列表格式的字符串
                if sanctions_str.startswith('[') and sanctions_str.endswith(']'):
                    return json.loads(sanctions_str)
                elif sanctions_str.startswith('{') and sanctions_str.endswith('}'):
                    return json.loads(sanctions_str)
                else:
                    # 如果不是标准JSON格式，返回包含原始字符串的对象
                    return {"raw_data": sanctions_str}
            except:
                return {"raw_data": sanctions_str}

def build_sanctions_info(sanctions_data: dict) -> List[dict]:
    """
    构建制裁信息列表
    
    Args:
        sanctions_data: 制裁数据字典
        
    Returns:
        List[dict]: 制裁信息列表
    """
    sanctions_info_list = []
    
    # 只有当制裁列表不为空时才添加到结果中
    if sanctions_data.get('sanctions_list'):
        sanctions_info_list.append({
            'sanctions_list': parse_sanctions_json(sanctions_data['sanctions_list']),
            'mid_sanctions_list': parse_sanctions_json(sanctions_data.get('mid_sanctions_list', '')),
            'no_sanctions_list': parse_sanctions_json(sanctions_data.get('no_sanctions_list', '')),
            'is_san': sanctions_data.get('is_san', ''),
            'is_sco': sanctions_data.get('is_sco', ''),
            'is_ool': sanctions_data.get('is_ool', ''),
            'is_one_year': sanctions_data.get('is_one_year', ''),
            'is_sanctioned_countries': sanctions_data.get('is_sanctioned_countries', '')
        })
    
    return sanctions_info_list

def calculate_voyage_risk(all_risk_statuses: list) -> str:
    """
    计算航次风险等级
    Args:
        all_risk_statuses: 所有实体的风险筛查状态列表
    Returns:
        str: 航次风险等级 - "拦截"、"关注"、"正常"
    """
    if not all_risk_statuses:
        return "正常"
    
    # 统计各风险等级的数量
    risk_counts = {
        "高风险": 0,
        "中风险": 0, 
        "低风险": 0,
        "无风险": 0,
        "": 0  # 空值
    }
    
    for status in all_risk_statuses:
        if status in risk_counts:
            risk_counts[status] += 1
        else:
            # 未知状态按无风险处理
            risk_counts["无风险"] += 1
    
    # 判断逻辑
    if risk_counts["高风险"] > 0:
        return "拦截"
    elif risk_counts["中风险"] > 0:
        return "关注"
    else:
        return "正常"

def calculate_sts_risk_level(risk_statuses: list) -> str:
    """
    计算STS风控状态：拦截/关注/正常
    Args:
        risk_statuses: 风险状态列表
    Returns:
        str: STS风控状态 - "拦截"、"关注"、"正常"
    """
    if not risk_statuses:
        return "正常"
    
    # 检查是否有高风险
    if any(status == "高风险" for status in risk_statuses):
        return "拦截"
    # 检查是否有中风险
    elif any(status == "中风险" for status in risk_statuses):
        return "关注"
    else:
        return "正常"

def calculate_risk_level(risk_statuses: list) -> str:
    """
    计算风险等级：高/中/无
    Args:
        risk_statuses: 风险状态列表
    Returns:
        str: 风险等级 - "高"、"中"、"无"
    """
    if not risk_statuses:
        return "无"
    
    # 检查是否有高风险
    if any(status == "高风险" for status in risk_statuses):
        return "高"
    # 检查是否有中风险
    elif any(status == "中风险" for status in risk_statuses):
        return "中"
    else:
        return "无"

# ============ 航次接口请求模型 ============
# 这里保持原有的请求模型不变
class StsVessel(CaseInsensitiveBaseModel):
    sts_vessel_imo: str = Field(..., description="STS船舶IMO号", alias="STS_VESSEL_IMO")
    Sts_vessel_name: str = Field(..., description="STS船舶名称", alias="STS_VESSEL_NAME")
    Sts_water_area: str = Field(..., description="STS水域", alias="STS_WATER_AREA")
    Sts_Is_high_risk_area: str = Field(..., description="是否高风险区域", examples=["true", "false"], alias="STS_IS_HIGH_RISK_AREA")
    Sts_plan_start_time: str = Field(..., description="STS计划开始时间", examples=["2025-08-21"], alias="STS_PLAN_START_TIME")
    Sts_execution_status: Optional[str] = Field("", description="STS执行状态", alias="STS_EXECUTION_STATUS")

# 其他请求模型保持不变...
# 这里省略其他模型定义，实际使用时需要包含所有原有的模型

# ============ 航次接口响应模型 ============
# 这里保持原有的响应模型不变
class SanctionsInfo(CaseInsensitiveBaseModel):
    """制裁信息模型"""
    sanctions_list: dict = Field(..., description="制裁列表（JSON格式）")
    mid_sanctions_list: dict = Field(..., description="中等制裁列表（JSON格式）")
    no_sanctions_list: dict = Field(..., description="无制裁列表（JSON格式）")

# 其他响应模型保持不变...
# 这里省略其他模型定义，实际使用时需要包含所有原有的模型

@external_router.post("/voyage_risk", summary="航次风险筛查（POST）")
async def external_voyage_risk(req) -> dict:
    """
    根据航次接口文档定义返回航次风险筛查结果
    使用风险检查框架进行制裁风险检查
    """
    print(f"=== API调用开始 ===")
    print(f"请求数据: {req.model_dump()}")
    
    current_time = get_current_time()
    print(f"当前时间: {current_time}")
    
    # 收集所有实体的风险状态用于计算航次风险
    all_risk_statuses = []
    
    # 查询船舶风险信息 - 使用新的风险检查框架
    print("开始查询船舶风险信息...")
    vessel_risk_data = query_sanctions_risk_with_framework(req.vessel_name)
    vessel_risk_status = vessel_risk_data['risk_level']
    all_risk_statuses.append(vessel_risk_status)
    print(f"船舶风险状态: {vessel_risk_status}")
    
    # 处理其他相关方风险信息 - 使用新的风险检查框架
    # 这里以实际收货人为例，展示如何使用新的函数
    
    # 查询实际收货人风险信息
    actual_consignee_responses = []
    try:
        for actual_consignee in req.actual_consignee:
            # 使用新的风险检查框架
            actual_consignee_risk_data = query_sanctions_risk_with_framework(actual_consignee.actual_consignee)
            actual_consignee_risk = actual_consignee_risk_data['risk_level']
            all_risk_statuses.append(actual_consignee_risk)
            
            # 构建响应对象
            actual_consignee_responses.append({
                "actual_consignee": actual_consignee.actual_consignee,
                "risk_screening_status": actual_consignee_risk,
                "risk_screening_time": current_time,
                "risk_status_change_content": "",
                "risk_status_change_time": "",
                "risk_items": [],
                "risk_status_reason": build_sanctions_info(actual_consignee_risk_data)
            })
            
            print(f"实际收货人 {actual_consignee.actual_consignee} 风险状态: {actual_consignee_risk}")
            
    except Exception as e:
        print(f"处理实际收货人时出错: {e}")
        actual_consignee_responses = []
    
    # 计算航次风险等级
    voyage_risk_level = calculate_voyage_risk(all_risk_statuses)
    
    print(f"=================")
    print(f"航次风险等级: {voyage_risk_level}")
    print(f"=================")
    
    # 构建响应对象
    response = {
        "scenario": req.scenario,
        "uuid": req.uuid,
        "voyage_number": req.voyage_number,
        "voyage_risk": voyage_risk_level,
        "voyage_status": req.voyage_status,
        "Business_segment": req.Business_segment,
        "trade_type": req.trade_type,
        "Business_model": req.Business_model,
        "voyage_start_time": req.voyage_start_time,
        "voyage_end_time": req.voyage_end_time or "",
        "vessel_imo": req.vessel_imo,
        "vessel_name": req.vessel_name,
        "is_sts": req.is_sts,
        "actual_consignee": actual_consignee_responses,
        # 其他字段保持不变...
        "operator_id": req.operator_id,
        "operator_name": req.operator_name,
        "operator_department": req.operator_department,
        "operator_time": req.operator_time
    }
    
    # 将响应数据记录到数据库
    try:
        # 记录数据
        insert_success = insert_voyage_risk_log(response)
        if insert_success:
            print(f"航次风险筛查数据已成功记录到数据库，航次号: {req.voyage_number}, uuid: {req.uuid}")
        else:
            print(f"航次风险筛查数据记录失败，航次号: {req.voyage_number}, uuid: {req.uuid}")
    except Exception as e:
        print(f"记录航次风险筛查数据时出错: {e}")
    
    return response

# 创建FastAPI应用
external_app = FastAPI(
    title="航次服务接口",
    version="1.0.0",
    description="""
    # 航次服务接口
    
    提供航次风险筛查和船舶基础信息查询服务。
    
    ## 功能特性
    - **航次风险筛查**: 根据航次信息进行全面的风险筛查分析
    - **船舶基础信息查询**: 查询船舶的基础信息
    - **数据库记录**: 自动记录航次风险筛查结果到数据库
    - **风险检查框架集成**: 使用 functions_risk_check_framework.py 进行制裁风险检查
    
    ## API端点
    - `/external/voyage_risk` - 航次风险筛查 (POST)
    - `/external/vessel_basic` - 船舶基础信息查询 (POST)
    - `/health` - 健康检查
    - `/info` - 服务信息
    
    ## 技术特性
    - 支持CORS跨域请求
    - Gzip压缩响应
    - 完整的错误处理
    - 详细的API文档
    - 集成风险检查框架
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加中间件
external_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

external_app.add_middleware(GZipMiddleware, minimum_size=1000)

# 将路由器添加到应用
external_app.include_router(external_router)

# 添加健康检查端点
@external_app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "service": "航次服务接口",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": [
            "/external/voyage_risk",
            "/external/vessel_basic"
        ],
        "risk_framework": "已集成 functions_risk_check_framework.py"
    }

# 添加服务信息端点
@external_app.get("/info")
async def get_service_info():
    """获取服务信息"""
    return {
        "service_name": "航次服务接口",
        "version": "1.0.0",
        "description": "航次风险筛查和船舶基础信息查询服务",
        "endpoints": {
            "voyage_risk": {
                "path": "/external/voyage_risk",
                "method": "POST",
                "description": "航次风险筛查"
            },
            "vessel_basic": {
                "path": "/external/vessel_basic", 
                "method": "POST",
                "description": "船舶基础信息查询"
            }
        },
        "features": [
            "航次风险筛查",
            "船舶基础信息查询",
            "数据库记录",
            "CORS支持",
            "Gzip压缩",
            "风险检查框架集成"
        ],
        "risk_framework": {
            "name": "functions_risk_check_framework.py",
            "description": "道琼斯制裁风险检查框架",
            "status": "已集成"
        }
    }

# 添加根路径
@external_app.get("/")
async def root():
    """根路径"""
    return {
        "message": "航次服务接口",
        "version": "1.0.0",
        "description": "航次风险筛查和船舶基础信息查询服务",
        "timestamp": datetime.now().isoformat(),
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "health_check": "/health"
        },
        "risk_framework": "已集成 functions_risk_check_framework.py"
    }
