from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import uvicorn
from contextlib import asynccontextmanager
import logging
from datetime import datetime
from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# 导入两个API模块
from maritime_api import maritime_app
from business_api import business_app
from cosco_api import cosco_router

# 导入新增的船舶相关路由
from vessel_purchase_risk import purchase_router
from vessel_second_hand_disposal_risk import second_hand_router as disposal_router
from vessle_charter_in_risk import charter_in_router
from vessel_second_hand_disassemble_risk import disassemble_router
# 导入仓储码头合规筛查路由
from warehousing_wharf_risk import warehousing_router

# 配置日志
import logging
import sys
import json

def safe_json_parse(json_str, default_value=None):
    """安全解析JSON字符串，处理可能的格式错误"""
    if not json_str or json_str in ("null", "None"):
        return default_value
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default_value

# 配置根日志记录器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # 确保输出到stdout
    ],
    force=True  # 强制重新配置日志
)

logger = logging.getLogger(__name__)

# 确保所有日志都立即刷新
class FlushingStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

# 添加一个自定义的日志处理器
flushing_handler = FlushingStreamHandler(sys.stdout)
flushing_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
flushing_handler.setFormatter(formatter)

# 为根日志记录器添加处理器
root_logger = logging.getLogger()
root_logger.addHandler(flushing_handler)

# 全局异常处理器
import sys
import traceback
from fastapi.responses import JSONResponse

def global_exception_handler(request, exc):
    """全局异常处理器，防止服务崩溃"""
    logger.error(f"未处理的异常: {exc}")
    logger.error(f"异常详情: {traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "服务器内部错误",
            "message": "服务遇到异常但仍在运行",
            "timestamp": datetime.now().isoformat()
        }
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    import os
    # 启动时执行 - 简化日志输出
    try:
        # 加载COSCO API（延迟初始化，避免重复加载）
        from cosco_api import cosco_router
        app.include_router(cosco_router, prefix="/cosco", tags=["COSCO模型算法API"])
    except Exception as e:
        logger.warning(f"⚠️ COSCO模型算法API加载失败: {e}")
    
    try:
        # 加载海事数据综合API
        from maritime_api import maritime_app
        app.mount("/maritime", maritime_app)
    except Exception as e:
        logger.warning(f"⚠️ 海事数据综合API加载失败: {e}")
    
    try:
        # 加载业务逻辑API
        from business_api import VesselOut, VesselRiskResponse, QueryResponse
    except Exception as e:
        logger.warning(f"⚠️ 业务逻辑API加载失败: {e}")
    
    try:
        # 加载航次服务API
        from external_api import external_router
        app.include_router(external_router, prefix="/external", tags=["航次服务API"])
    except Exception as e:
        logger.warning(f"⚠️ 航次服务API加载失败: {e}")
    
    try:
        # 加载STS风险筛查API
        from sts_bunkering_risk import sts_router
        app.include_router(sts_router, prefix="/sts", tags=["STS风险筛查API"])
    except Exception as e:
        logger.warning(f"⚠️ STS风险筛查API加载失败: {e}")
    
    try:
        # 加载航次合规状态审批API（统一为 /external 前缀，与主版本一致）
        from external_voyage_approval_api import external_approval_router
        app.include_router(external_approval_router, prefix="/external", tags=["航次合规状态审批API"])
    except Exception as e:
        logger.warning(f"⚠️ 航次合规状态审批API加载失败: {e}")
    
    # 加载船舶相关路由
    try:
        app.include_router(purchase_router, tags=["船舶买入合规状态筛查API"])
        logger.info("✅ 船舶买入合规状态筛查API加载成功")
    except Exception as e:
        logger.warning(f"⚠️ 船舶买入合规状态筛查API加载失败: {e}")
    
    try:
        app.include_router(disposal_router, tags=["二手船出售合规状态筛查API"])
        logger.info("✅ 二手船出售合规状态筛查API加载成功")
    except Exception as e:
        logger.warning(f"⚠️ 二手船出售合规状态筛查API加载失败: {e}")
    
    try:
        app.include_router(charter_in_router, tags=["船舶租入合规状态筛查API"])
        logger.info("✅ 船舶租入合规状态筛查API加载成功")
    except Exception as e:
        logger.warning(f"⚠️ 船舶租入合规状态筛查API加载失败: {e}")
    
    try:
        app.include_router(disassemble_router, tags=["二手船拆解合规状态筛查API"])
        logger.info("✅ 二手船拆解合规状态筛查API加载成功")
    except Exception as e:
        logger.warning(f"⚠️ 二手船拆解合规状态筛查API加载失败: {e}")
        
    try:
        app.include_router(warehousing_router, tags=["仓储码头合规状态筛查API"])
        logger.info("✅ 仓储码头合规状态筛查API加载成功")
    except Exception as e:
        logger.warning(f"⚠️ 仓储码头合规状态筛查API加载失败: {e}")
    
    # 只在主进程显示启动完成信息
    if os.getenv("UVICORN_WORKER_ID") is None:
        logger.info("📊 所有模块加载完成！")
    
    yield
    
    # 关闭时执行
    if os.getenv("UVICORN_WORKER_ID") is None:
        logger.info("🛑 服务正在关闭...")

# 创建主应用
main_app = FastAPI(
    title="船舶信息综合API服务",
    version="2.0.0",
    description="""
    # 船舶信息综合API服务
    
    这是一个整合了多个船舶相关API的综合服务，包括：
    
    ## 🚢 海事数据综合API (maritime_api)
    - **Lloyd's合规数据**: 获取船舶合规信息
    - **Lloyd's制裁数据**: 查询船舶制裁状态
    - **UANI数据**: 检查船舶是否在UANI清单中
    - **Kpler数据分析**: 执行船舶风险分析
    - **航次风险分析**: 分析船舶航次风险
    - **船舶状态综合评估**: 综合判断船舶风险等级
    
    ## 🏢 业务逻辑API (business_api)
    - **企业制裁风险查询**: 根据公司名称查询制裁风险
    - **船舶风险信息查询**: 根据IMO编号查询船舶风险
    - **船舶风险数据保存**: 保存船舶风险分析结果
    
    ## 🏭 COSCO模型算法API (cosco_api)
    - **公司名称匹配**: 实时匹配公司名称，支持精确匹配和模糊匹配
    - **智能索引**: 基于Whoosh的快速搜索索引
    - **多语言支持**: 支持中文、英文等多种语言的公司名称处理
    - **缓存机制**: 智能缓存和自动刷新机制

    ## ⚓ 船舶交易相关API
    - **船舶买入合规筛查**: 评估船舶买入的合规风险
    - **二手船出售合规筛查**: 评估二手船出售的合规风险
    - **船舶租入合规筛查**: 评估船舶租入的合规风险
    - **二手船拆解合规筛查**: 评估二手船拆解的合规风险
    
    ## 🔧 技术特性
    - 支持CORS跨域请求
    - Gzip压缩响应
    - 完整的错误处理
    - 详细的API文档
    - 健康检查接口
    
    ## 📚 API文档
    - 访问 `/docs` 查看Swagger UI文档
    - 访问 `/redoc` 查看ReDoc文档
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# 导入性能监控中间件
from performance_monitor import PerformanceMonitorMiddleware, get_performance_stats

# 添加中间件
main_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_app.add_middleware(GZipMiddleware, minimum_size=1000)

# 添加性能监控中间件
main_app.add_middleware(PerformanceMonitorMiddleware)

# 注册全局异常处理器
main_app.add_exception_handler(Exception, global_exception_handler)

# 挂载海事API子应用
main_app.mount("/maritime", maritime_app, name="海事数据综合API")

# 挂载COSCO模型算法API路由
main_app.include_router(cosco_router)

# 挂载航次服务API路由
from external_api import external_router
main_app.include_router(external_router)

# 挂载STS风险筛查API路由
from sts_bunkering_risk import sts_router
main_app.include_router(sts_router)

# 挂载航次合规状态审批API路由（统一使用 /external 前缀）
from external_voyage_approval_api import external_approval_router
main_app.include_router(external_approval_router, prefix="/external")

# 挂载船舶相关路由
main_app.include_router(purchase_router)
main_app.include_router(disposal_router)
main_app.include_router(charter_in_router)
main_app.include_router(disassemble_router)
from warehousing_wharf_risk import warehousing_router
main_app.include_router(warehousing_router)

# 导入业务API的路由和模型，直接在主应用中定义
from business_api import (
    VesselOut, VesselRiskResponse, QueryResponse,
    get_db, fetch_one, BASE_SQL, DB_CONFIG
)
import psycopg2
import json
from fastapi import Request
from sqlalchemy.orm import Session

# 主应用路由
@main_app.get("/")
async def root():
    """主应用根路径"""
    return {
        "message": "船舶信息综合API服务",
        "version": "2.0.0",
        "description": "整合了海事数据综合API和业务逻辑API的综合服务",
        "timestamp": datetime.now().isoformat(),
        "modules": {
            "maritime_api": {
                "name": "海事数据综合API",
                "base_path": "/maritime",
                "description": "整合Lloyd's、Kpler和UANI数据的海事风险分析服务",
                "endpoints": [
                    "/maritime/api/execute_full_analysis",
                    "/maritime/api/lloyds_compliance",
                    "/maritime/api/lloyds_sanctions",
                    "/maritime/api/uani_check",
                    "/maritime/api/kpler_analysis",
                    "/maritime/api/voyage_risks",
                    "/maritime/api/vessel_status",
                    "/maritime/api/save_results"
                ]
            },
            "external_api": {
                "name": "航次服务API",
                "base_path": "/external",
                "description": "航次风险筛查和船舶基础信息查询服务",
                "endpoints": [
                    "/external/voyage_risk (POST)",
                    "/external/vessel_basic (POST)"
                ]
            },
            "sts_api": {
                "name": "STS风险筛查API",
                "base_path": "/sts",
                "description": "STS（船对船）风险筛查服务，包含船舶相关方制裁、AIS信号分析等",
                "endpoints": [
                    "/sts/risk_screen (POST)"
                ]
            },
            "external_approval_api": {
                "name": "航次合规状态审批API",
                "base_path": "/external",
                "description": "航次合规状态审批信息返回服务，提供航次风险筛查结果",
                "endpoints": [
                    "/external/voyage_approval (POST)"
                ]
            },
            "cosco_api": {
                "name": "COSCO模型算法API",
                "base_path": "/cosco",
                "description": "公司名称实时匹配服务",
                "endpoints": [
                    "/cosco/match",
                    "/cosco/health",
                    "/cosco/info",
                    "/cosco/refresh-data"
                ]
            },
            "purchase_api": {
                "name": "船舶买入合规状态筛查API",
                "base_path": "/purchase",
                "description": "船舶买入合规状态筛查服务",
                "endpoints": [
                    "/purchase/vessel_purchase_risk (POST)"
                ]
            },
            "second_hand_disposal_api": {
                "name": "二手船出售合规状态筛查API",
                "base_path": "/second_hand",
                "description": "二手船出售合规状态筛查服务",
                "endpoints": [
                    "/second_hand/vessel_disposal_risk (POST)"
                ]
            },
            "charter_in_api": {
                "name": "船舶租入合规状态筛查API",
                "base_path": "/charter_in",
                "description": "船舶租入合规状态筛查服务",
                "endpoints": [
                    "/charter_in/vessel_charter_risk (POST)"
                ]
            },
            "second_hand_disassemble_api": {
                "name": "二手船拆解合规状态筛查API",
                "base_path": "/second_hand",
                "description": "二手船拆解合规状态筛查服务",
                "endpoints": [
                    "/second_hand/vessel_disassemble_risk (POST)"
                ]
            },
            "warehousing_api": {
                "name": "仓储码头合规状态筛查API",
                "base_path": "/warehousing",
                "description": "仓储码头合规状态筛查服务",
                "endpoints": [
                    "/warehousing/wharf_risk (POST)"
                ]
            },
            "main_app": {
                "name": "主应用API",
                "base_path": "/",
                "description": "船舶信息查询和风险数据管理服务",
                "endpoints": [
                    "/query_by_name (支持历史记录)",
                    "/search-entities/ (基础查询)",
                    "/search-vessel/",
                    "/api/save_vessel_data",
                    "/api/get_vessel_all_data",
                    "/api/sts_data",
                    "/api/vessel_list"
                ]
            }
        },
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "health_check": "/health"
        }
    }

@main_app.get("/performance")
async def get_performance_metrics():
    """获取性能监控指标"""
    return get_performance_stats()

@main_app.get("/health")
async def health_check():
    """主应用健康检查"""
    return {
        "status": "healthy",
        "service": "船舶信息综合API服务",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "modules": {
            "maritime_api": "active",
            "external_api": "active",
            "sts_api": "active",
            "external_approval_api": "active",
            "cosco_api": "active",
            "business_api": "active",
            "purchase_api": "active",
            "second_hand_disposal_api": "active",
            "charter_in_api": "active",
            "second_hand_disassemble_api": "active"
        }
    }

# 添加缺失的API端点，保持向后兼容性
@main_app.get("/api/get_vessel_all_data")
async def get_vessel_all_data_main(
    vessel_imo: str = Query(..., description="船舶IMO号"),
    start_date: str = Query(None, description="开始日期 (YYYY-MM-DD)，默认为1年前的今天"),
    end_date: str = Query(None, description="结束日期 (YYYY-MM-DD)，默认为当前日期")
):
    """获取船舶所有相关数据（一次性调用所有接口）- 主应用兼容端点"""
    try:
        # 导入maritime_api中的函数
        from maritime_api import get_vessel_all_data
        return await get_vessel_all_data(vessel_imo, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取船舶数据失败: {str(e)}")

@main_app.get("/api/sts_data")
async def get_sts_data_main(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取船舶STS（船对船转运）数据 - 主应用兼容端点"""
    try:
        # 导入maritime_api中的函数
        from maritime_api import get_sts_data
        return await get_sts_data(vessel_imo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取STS数据失败: {str(e)}")

@main_app.get("/api/vessel_list")
async def get_vessel_list_main(
    vessel_imo: str = Query(..., description="船舶IMO号")
):
    """获取船舶基础信息 - 主应用兼容端点"""
    try:
        # 导入maritime_api中的函数
        from maritime_api import get_vessel_basic_info
        return await get_vessel_basic_info(vessel_imo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取船舶基础信息失败: {str(e)}")

@main_app.get("/info")
async def get_service_info():
    """获取服务详细信息"""
    return {
        "service_name": "船舶信息综合API服务",
        "version": "2.0.0",
        "startup_time": datetime.now().isoformat(),
        "total_endpoints": 19,  # 原有15个 + 新增4个船舶相关API
        "features": [
            "Lloyd's API集成",
            "Kpler API集成", 
            "UANI数据爬取",
            "船舶风险分析",
            "企业制裁风险查询（支持历史记录）",
            "船舶风险数据管理",
            "航次风险分析",
            "综合风险评估",
            "船舶买入合规筛查",
            "二手船出售合规筛查",
            "船舶租入合规筛查",
            "二手船拆解合规筛查"
        ],
        "api_groups": [
            {
                "name": "海事数据综合API",
                "base_path": "/maritime",
                "endpoint_count": 8,
                "description": "海事风险分析相关接口"
            },
            {
                "name": "业务逻辑API", 
                "base_path": "/business",
                "endpoint_count": 7,
                "description": "业务查询和数据管理接口（包含兼容端点）"
            },
            {
                "name": "船舶交易相关API",
                "base_path": "/",
                "endpoint_count": 4,
                "description": "船舶买入、出售、租入和拆解的合规筛查接口"
            }
        ]
    }

# 业务API路由 - 直接在主应用中定义
@main_app.get("/query_by_name", response_model=QueryResponse)
async def query_by_name(
    entity_id: str = Query(..., min_length=1, description="企业id查询"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    search_time: Optional[str] = Query(None, description="查询时间，格式为YYYY-MM-DD HH:mm:ss"),
    user_id: Optional[str] = Query(None, description="用户ID"),
    user_name: Optional[str] = Query(None, description="用户名称"),
    depart_id: Optional[str] = Query(None, description="部门ID"),
    depart_name: Optional[str] = Query(None, description="部门名称")
):
    """根据企业ID查询制裁风险结果（支持分页）并记录查询历史"""
    conn = None
    try:
        conn = psycopg2.connect(** DB_CONFIG)
        with conn.cursor() as cursor:
            offset = (page - 1) * page_size

            sql = """
            SELECT 
                entity_id, entity_dt, activestatus, ENTITYNAME1, ENTITYNAME4,
                country_nm1, country_nm2, DATEVALUE1, sanctions_lev,
                sanctions_list, mid_sanctions_list, no_sanctions_list, unknown_risk_list, other_list
            FROM lng.sanctions_risk_result
            WHERE entity_id = %s
            LIMIT %s OFFSET %s
            """
            cursor.execute(sql, [entity_id, page_size, offset])
            results = cursor.fetchall()

            count_sql = "SELECT COUNT(*) AS total FROM lng.sanctions_risk_result WHERE entity_id = %s"
            cursor.execute(count_sql, [entity_id])
            total = cursor.fetchone()['total']

            if not results:
                return {
                    "total": 0,
                    "data": [],
                    "message": "暂无数据"
                }

            processed_results = []
            for item in results:
                processed_item = {
                    "entity_id": str(item["entity_id"]),  # 转换为字符串
                    "entity_dt": item.get("entity_dt"),
                    "activestatus": item.get("activestatus"),
                    "ENTITYNAME1": item.get("ENTITYNAME1"),
                    "ENTITYNAME4": item.get("ENTITYNAME4"),
                    "country_nm1": item.get("country_nm1"),
                    "country_nm2": item.get("country_nm2"),
                    "DATEVALUE1": item.get("DATEVALUE1"),
                    "sanctions_lev": item.get("sanctions_lev"),
                    "sanctions_list": safe_json_parse(item.get("sanctions_list"), []),
                    "mid_sanctions_list": safe_json_parse(item.get("mid_sanctions_list"), []),
                    "no_sanctions_list": safe_json_parse(item.get("no_sanctions_list"), []),
                    "unknown_risk_list": safe_json_parse(item.get("unknown_risk_list"), []),
                    "other_list": safe_json_parse(item.get("other_list"), [])
                }
                processed_results.append(processed_item)

            # 记录查询历史（如果提供了用户信息）
            if user_id and user_name:
                try:
                    # 获取第一个结果用于历史记录
                    first_result = results[0] if results else None
                    if first_result:
                        actual_search_time = search_time if search_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # 构建历史记录
                        his_record = {
                            "entity_id": str(first_result["entity_id"]),  # 转换为字符串
                            "risk_lev": first_result.get("sanctions_lev"),
                            "risk_type": "制裁风险查询",  # 固定风险类型
                            "ENTITYNAME1": first_result.get("ENTITYNAME1"),
                            "ENTITYNAME4": first_result.get("ENTITYNAME4"),
                            "serch_time": actual_search_time,
                            "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "user_id": user_id,
                            "user_name": user_name,
                            "depart_id": depart_id,
                            "depart_name": depart_name,
                            "is_delete": "0",
                            "entity_dt": first_result.get("entity_dt"),
                            "activestatus": first_result.get("activestatus"),
                            "sanctions_lev": first_result.get("sanctions_lev"),
                            "country_nm1": first_result.get("country_nm1"),
                            "country_nm2": first_result.get("country_nm2"),
                            "DATEVALUE1": first_result.get("DATEVALUE1"),
                            "sanctions_list": json.dumps(first_result.get("sanctions_list"), ensure_ascii=False) if first_result.get("sanctions_list") else None,
                            "mid_sanctions_list": json.dumps(first_result.get("mid_sanctions_list"), ensure_ascii=False) if first_result.get("mid_sanctions_list") else None,
                            "no_sanctions_list": json.dumps(first_result.get("no_sanctions_list"), ensure_ascii=False) if first_result.get("no_sanctions_list") else None,
                            "unknown_risk_list": json.dumps(first_result.get("unknown_risk_list"), ensure_ascii=False) if first_result.get("unknown_risk_list") else None,
                            "other_list": json.dumps(first_result.get("other_list"), ensure_ascii=False) if first_result.get("other_list") else None
                        }
                        
                        # 插入历史记录
                        columns = []
                        placeholders = []
                        values = []
                        for k, v in his_record.items():
                            if v is not None:
                                columns.append(k)
                                placeholders.append("%s")
                                values.append(v)
                        
                        insert_sql = f"""
                        INSERT INTO lng.sanctions_risk_result_his 
                        ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                        """
                        cursor.execute(insert_sql, values)
                        conn.commit()
                        logger.info(f"查询历史记录已保存: {entity_id}")
                        
                except Exception as e:
                    logger.error(f"保存查询历史记录失败: {str(e)}")
                    # 不影响主查询结果，只记录错误日志

            return {"total": total, "data": processed_results}

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"JSON parsing error: {str(e)}")
    finally:
        if conn:
            conn.close()

# @main_app.get("/search-entities/", response_model=List[VesselOut])
# def search_entities(
#     entity_id: Optional[str] = Query(None),
#     search_time: Optional[str] = Query(None, description="查询时间，格式为YYYY-MM-DD HH:mm:ss"),
#     user_id: Optional[str] = Query(None),
#     user_name: Optional[str] = Query(None),
#     depart_id: Optional[str] = Query(None),
#     depart_name: Optional[str] = Query(None)
# ):
#     """公司风险查询（已移除历史记录存储功能，该功能已迁移到query_by_name接口）"""
#     if not entity_id:
#         raise HTTPException(status_code=400, detail="entityname1参数不能为空")
    
#     sql = f"""
#     {BASE_SQL}
#     WHERE entityname1 LIKE CONCAT('%', :entityname1, '%')
#     LIMIT 1
#     """
    
#     try:
#         logger.info(f"执行查询: {sql} with params: { {'entityname1': entityname1} }")
#         vessel_data = VesselOut.model_validate(fetch_one(sql, {"entityname1": entityname1}))
        
#     except HTTPException as e:
#         if "未找到该实体" in str(e.detail):
#             conn = psycopg2.connect(**DB_CONFIG)
#             try:
#                 with conn.cursor() as cursor:
#                     cursor.execute(
#                         "SELECT entityname1 FROM dqs_entity_sanctions_test WHERE entityname1 LIKE %s LIMIT 5",
#                         [{entity_id}]
#                     )
#                     similar_names = [row["entityname1"] for row in cursor.fetchall()]
                    
#                     if similar_names:
#                         raise HTTPException(
#                             status_code=404,
#                             detail={
#                                 "message": f"未找到精确匹配 '{entityname1}' 的记录",
#                                 "suggestions": similar_names
#                             }
#                         )
#                     else:
#                         raise HTTPException(
#                             status_code=404,
#                             detail=f"数据库中没有名称包含 '{entityname1}' 的记录"
#                         )
#             finally:
#                 conn.close()
#         raise
    
#     # 查询补充数据
#     query_name_data = None
#     try:
#         conn = psycopg2.connect(** DB_CONFIG)
#         with conn.cursor() as cursor:
#             query_sql = """
#             SELECT 
#                 country_nm1, country_nm2, DATEVALUE1, sanctions_lev,
#                 sanctions_list, mid_sanctions_list, no_sanctions_list,
#                 unknown_risk_list, other_list
#             FROM lng.sanctions_risk_result
#             WHERE ENTITYNAME1 = %s
#             LIMIT 1
#             """
#             cursor.execute(query_sql, [entityname1])
#             query_name_data = cursor.fetchone()
#     except Exception as e:
#         logger.error(f"查询制裁风险结果失败: {str(e)}")
#     finally:
#         if conn:
#             conn.close()
    
#     # 注意：历史记录存储功能已迁移到query_by_name接口
#     # 如需记录查询历史，请使用query_by_name接口并传入用户信息参数
    
#     return [vessel_data]

# @main_app.get("/search-vessel/", response_model=List[VesselRiskResponse])
# async def search_vessel(
#         vessel_imo: str = Query(..., regex=r"^\d{7}$", example="9263215"),
#         db: Session = Depends(get_db)
# ):
#     """根据IMO编号查询船舶风险信息"""
#     try:
#         query = text("""
#             SELECT 
#                 t0.vessel_imo,
#                 t0.vessel_name,
#                 CASE 
#                     WHEN '高风险' IN (
#                         t.has_sanctioned_cargo_risk,
#                         t1.has_sanctioned_trades_risk,
#                         t2.has_sanctioned_flag_risk,
#                         t6.has_port_calls_risk
#                     ) THEN '高风险'
#                     WHEN '中风险' IN (
#                         t3.has_ais_gap_risk,
#                         t4.has_sts_events_risk,
#                         t5.has_dark_sts_risk,
#                         t7.has_ais_spoofs_risk
#                     ) THEN '中风险'
#                     ELSE '无风险' 
#                 END AS risk_level,
#                 '' AS flag_country,
#                 '' AS risk_type
#             FROM 
#                 (SELECT task_uuid, vessel_imo, vessel_name FROM ods_zyhy_rpa_vessel_risk_data) t0
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' THEN '高风险'
#                          WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '高风险'
#                          ELSE '无风险' END AS has_sanctioned_cargo_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Cargo') t 
#                  ON t0.task_uuid = t.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '高风险'
#                          ELSE '无风险' END AS has_sanctioned_trades_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Trade') t1
#                  ON t0.task_uuid = t1.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '高风险'
#                          ELSE '无风险' END AS has_sanctioned_flag_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Flag') t2
#                  ON t0.task_uuid = t2.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '中风险'
#                          ELSE '无风险' END AS has_ais_gap_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='AIS gaps') t3
#                  ON t0.task_uuid = t3.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '中风险'
#                          ELSE '无风险' END AS has_sts_events_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='High risk STS transfers') t4
#                  ON t0.task_uuid = t4.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '中风险'
#                          ELSE '无风险' END AS has_dark_sts_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Dark STS transfers') t5
#                  ON t0.task_uuid = t5.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '高风险'
#                          ELSE '无风险' END AS has_port_calls_risk
#                  FROM ods_byte_zyhy_rpa_vessel_risk_content WHERE data_type='Port call risks') t6
#                  ON t0.task_uuid = t6.task_uuid
#             LEFT JOIN 
#                 (SELECT task_uuid, data_type, content_data,
#                     CASE WHEN data_format='Table' AND content_data LIKE 'icon-unverified' THEN '中风险'
#                          ELSE '无风险' END AS has_ais_spoofs_risk
#                  FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='AIS spoofing') t7
#                  ON t0.task_uuid = t7.task_uuid
#             WHERE 
#                 t0.vessel_imo = :imo
#         """)

#         result = db.execute(query, {"imo": vessel_imo})

#         if not result.rowcount:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"未找到IMO编号为 {vessel_imo} 的船舶记录"
#             )

#         return [
#             VesselRiskResponse(
#                 vessel_imo=row.vessel_imo,
#                 vessel_name=row.vessel_name,
#                 risk_level=row.risk_level,
#                 flag_country=row.flag_country,
#                 risk_type=row.risk_type
#             )
#             for row in result
#         ]

#     except SQLAlchemyError as e:
#         logger.error(f"Database error: {str(e)}")
#         raise HTTPException(status_code=500, detail="数据库操作失败，请检查查询参数")
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.critical(f"Unexpected error: {str(e)}")
#         raise HTTPException(status_code=500, detail="服务器内部错误")

# @main_app.post("/api/save_vessel_data")
# async def save_vessel_data(request: Request):
#     """船舶风险数据保存"""
#     try:
#         raw_data = await request.json()
#         logger.info(f"接收原始数据: {json.dumps(raw_data, indent=2)}")

#         required_fields = ['taskUuid', 'taskStatus', 'vesselImo']
#         for field in required_fields:
#             if field not in raw_data:
#                 raise HTTPException(400, f"缺少必填字段: {field}")

#         main_data = {
#             "task_uuid": raw_data.get("taskUuid"),
#             "task_status": raw_data.get("taskStatus"),
#             "task_message": raw_data.get("taskMessage"),
#             "task_begin_time": raw_data.get("taskBeginTime", ""),
#             "task_end_time": raw_data.get("taskEndTime", ""),
#             "vessel_imo": str(raw_data.get("vesselImo", "")),
#             "vessel_name": raw_data.get("vesselName", ""),
#             "risk_type": raw_data.get("riskType", ""),
#             "content_json": json.dumps(raw_data.get("content", [])),
#             "raw_content": json.dumps(raw_data.get("rawContent")) if raw_data.get("rawContent") else None
#         }

#         content_items = []
#         for content in raw_data.get("content", []):
#             for data_item in content.get("data", []):
#                 content_items.append({
#                     "task_uuid": raw_data["taskUuid"],
#                     "content_type": content.get("type", "unknown"),
#                     "data_type": data_item.get("type", "unknown"),
#                     "data_format": data_item.get("dataType", data_item.get("data_type", "text")),
#                     "content_data": json.dumps(data_item.get("content", {}))
#                 })

#         conn = psycopg2.connect(
#             host="10.13.16.186",
#             user="coscohw",
#             password="WS8k*123",
#             database="hwda",
#             charset="utf8mb4"
#         )
        
#         try:
#             with conn.cursor() as cursor:
#                 columns = ", ".join(main_data.keys())
#                 placeholders = ", ".join(["%s"] * len(main_data))
#                 sql = f"INSERT INTO ods_zyhy_rpa_vessel_risk_data ({columns}) VALUES ({placeholders})"
#                 cursor.execute(sql, list(main_data.values()))
                
#                 for item in content_items:
#                     columns = ", ".join(item.keys())
#                     placeholders = ", ".join(["%s"] * len(item))
#                     sql = f"INSERT INTO ods_zyhy_rpa_vessel_risk_content ({columns}) VALUES ({placeholders})"
#                     cursor.execute(sql, list(item.values()))
                    
#             conn.commit()
#             logger.info("数据保存成功")
#         except Exception as e:
#             conn.rollback()
#             logger.error(f"数据库操作失败: {str(e)}")
#             raise
#         finally:
#             conn.close()

#         return {
#             "status": "success",
#             "message": "数据保存成功",
#             "task_uuid": raw_data["taskUuid"]
#         }

#     except json.JSONDecodeError:
#         raise HTTPException(400, "无效的JSON格式")
#     except psycopg2.Error as e:
#         logger.error(f"数据库错误: {str(e)}")
#         raise HTTPException(500, "数据库操作失败")
#     except Exception as e:
#         logger.error(f"未处理异常: {str(e)}", exc_info=True)
#         raise HTTPException(500, "服务器内部错误")

@main_app.get("/endpoints")
async def list_all_endpoints():
    """列出所有可用的API端点"""
    return {
        "total_endpoints": 16,
        "endpoints": {
            "maritime_api": {
                "base_path": "/maritime",
                "endpoints": [
                    {
                        "path": "/maritime/",
                        "method": "GET",
                        "description": "海事API根路径"
                    },
                    {
                        "path": "/maritime/health",
                        "method": "GET", 
                        "description": "海事API健康检查"
                    },
                    {
                        "path": "/maritime/api/execute_full_analysis",
                        "method": "POST",
                        "description": "执行完整分析流程",
                        "params": ["vessel_imo", "start_date", "end_date"]
                    },
                    {
                        "path": "/maritime/api/lloyds_compliance",
                        "method": "GET",
                        "description": "获取Lloyd's合规数据",
                        "params": ["vessel_imo", "start_date", "end_date"]
                    },
                    {
                        "path": "/maritime/api/lloyds_sanctions",
                        "method": "GET",
                        "description": "获取Lloyd's制裁数据",
                        "params": ["vessel_imo"]
                    },
                    {
                        "path": "/maritime/api/uani_check",
                        "method": "GET",
                        "description": "检查船舶是否在UANI清单中",
                        "params": ["vessel_imo"]
                    },
                    {
                        "path": "/maritime/api/kpler_analysis",
                        "method": "POST",
                        "description": "执行Kpler数据分析",
                        "params": ["vessel_imo", "start_date", "end_date"]
                    },
                    {
                        "path": "/maritime/api/voyage_risks",
                        "method": "GET",
                        "description": "获取航次风险分析",
                        "params": ["vessel_imo", "start_date", "end_date"]
                    },
                    {
                        "path": "/maritime/api/vessel_status",
                        "method": "GET",
                        "description": "获取船舶综合状态",
                        "params": ["vessel_imo"]
                    },
                    {
                        "path": "/maritime/api/save_results",
                        "method": "POST",
                        "description": "保存所有分析结果",
                        "params": ["output_dir"]
                    },
                    {
                        "path": "/maritime/api/available_endpoints",
                        "method": "GET",
                        "description": "获取可用的API端点"
                    }
                ]
            },
            "main_app": {
                "base_path": "/",
                "endpoints": [
                    {
                        "path": "/",
                        "method": "GET",
                        "description": "主应用根路径"
                    },
                    {
                        "path": "/health",
                        "method": "GET",
                        "description": "主应用健康检查"
                    },
                    {
                        "path": "/info",
                        "method": "GET",
                        "description": "获取服务详细信息"
                    },
                    {
                        "path": "/query_by_name",
                        "method": "GET",
                        "description": "根据企业ID查询制裁风险结果（支持分页和历史记录）",
                        "params": ["entity_id", "page", "page_size", "search_time", "user_id", "user_name", "depart_id", "depart_name"]
                    },
                    {
                        "path": "/search-entities/",
                        "method": "GET",
                        "description": "公司风险查询（基础查询功能，历史记录功能已迁移到query_by_name）",
                        "params": ["entityname1", "search_time", "user_id", "user_name", "depart_id", "depart_name"]
                    },
                    {
                        "path": "/search-vessel/",
                        "method": "GET",
                        "description": "根据IMO编号查询船舶风险信息",
                        "params": ["vessel_imo"]
                    },
                    {
                        "path": "/api/save_vessel_data",
                        "method": "POST",
                        "description": "船舶风险数据保存",
                        "body": "JSON数据"
                    }
                ]
            },
            "purchase_api": {
                "base_path": "/purchase",
                "endpoints": [
                    {
                        "path": "/purchase/vessel_purchase_risk",
                        "method": "POST",
                        "description": "船舶买入合规状态筛查",
                        "params": ["请求体参数"]
                    }
                ]
            },
            "second_hand_disposal_api": {
                "base_path": "/second_hand",
                "endpoints": [
                    {
                        "path": "/second_hand/vessel_disposal_risk",
                        "method": "POST",
                        "description": "二手船出售合规状态筛查",
                        "params": ["请求体参数"]
                    }
                ]
            },
            "charter_in_api": {
                "base_path": "/charter_in",
                "endpoints": [
                    {
                        "path": "/charter_in/vessel_charter_risk",
                        "method": "POST",
                        "description": "船舶租入合规状态筛查",
                        "params": ["请求体参数"]
                    }
                ]
            },
            "second_hand_disassemble_api": {
                "base_path": "/second_hand",
                "endpoints": [
                    {
                        "path": "/second_hand/vessel_disassemble_risk",
                        "method": "POST",
                        "description": "二手船拆解合规状态筛查",
                        "params": ["请求体参数"]
                    }
                ]
            }
        }
    }

if __name__ == "__main__":
    # 启动服务器
    # 简化启动日志
    
    uvicorn.run(
        "main_server:main_app",
        host="0.0.0.0",
        port=8000,
        #reload=False,  # 开发模式启用热重载
        log_level="info",
        # 添加并发配置
        workers=1,  # 单进程模式，多进程由start_server.py控制
        access_log=True,
        # 连接配置
        limit_concurrency=1000,  # 限制并发连接数
        limit_max_requests=100000,  # 设置一个很大的数值，而不是0
        timeout_keep_alive=1200,  # keep-alive超时时间（翻倍）
    )