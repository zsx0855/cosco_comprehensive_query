from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.responses import JSONResponse
from psycopg2 import Error as KingbaseError
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, List
import logging
import traceback
import psycopg2
from datetime import datetime
import json
from dotenv import load_dotenv

# 导入Kingbase配置
from kingbase_config import get_kingbase_url, get_db_pool_config, get_kingbase_config

# 配置
load_dotenv()
DB_URL = get_kingbase_url()
engine = create_engine(DB_URL, **get_db_pool_config())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 数据库配置
DB_CONFIG = get_kingbase_config()

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据模型
class VesselOut(BaseModel):
    entity_id: str  # 改为字符串类型
    entity_dt: Optional[str] = None
    activestatus: Optional[str] = None
    entityname1: Optional[str] = None
    entityname4: Optional[str] = None
    description1_value_cn: Optional[str] = None
    description2_value_cn: Optional[str] = None
    NMTOKEN_LEVEL: Optional[str] = None
    risk_lev: Optional[str] = None
    risk_type: Optional[str] = None
    is_san: Optional[str] = None
    is_sco: Optional[str] = None
    is_ool: Optional[str] = None
    is_one_year: Optional[str] = None
    is_sanctioned_countries: Optional[str] = None
    description3_value_cn: Optional[str] = None
    datevalue1: Optional[str] = None
    sanctions_lev: Optional[str] = None
    sanctions_nm: Optional[str] = None
    country_nm1: Optional[str] = None
    country_nm2: Optional[str] = None

class VesselRiskResponse(BaseModel):
    vessel_imo: int = Field(..., gt=0, example=9263215)
    vessel_name: str = Field(..., example="Ever Given")
    risk_level: str = Field(..., example="高风险")
    flag_country: Optional[str] = Field(None, example="Panama")
    risk_type: Optional[str] = Field(None, example="Sanctioned Cargo")

class QueryResponse(BaseModel):
    total: int
    data: List[dict]

# 辅助函数
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def fetch_one(sql: str, params: dict):
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql), params).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="未找到该实体")
            return row._mapping
    except SQLAlchemyError as e:
        logger.error(f"数据库查询错误: {str(e)}")
        raise HTTPException(status_code=500, detail="数据库操作失败")

# 公共SQL模板
BASE_SQL = """
    SELECT
        entity_id, entity_dt, activestatus, entityname1, entityname4,
        description1_value_cn, description2_value_cn, NMTOKEN_LEVEL,
        CASE 
            WHEN '高风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '高风险' 
            WHEN '中风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '中风险' 
            ELSE '无风险' 
        END AS risk_lev,
        NMTOKEN_LEVEL as risk_type,
        is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries,
        description3_value_cn, SANCTIONS_NM AS sanctions_nm,
        DATEVALUE1 AS datevalue1, country_nm1, country_nm2
    FROM dqs_entity_sanctions_test
"""

# 创建FastAPI应用
business_app = FastAPI(
    title="船舶信息综合API服务",
    version="1.0.0",
    description="整合了5个船舶相关API的服务"
)

@business_app.get("/")
async def root():
    return {"message": "船舶信息综合API服务", "version": "1.0.0"}

@business_app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@business_app.get("/query_by_name", response_model=QueryResponse)
async def query_by_name(
    ENTITYNAME1: str = Query(..., min_length=1, description="企业名称模糊查询"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数")
):
    """根据企业名称查询制裁风险结果（支持分页）"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            offset = (page - 1) * page_size

            sql = """
            SELECT 
                entity_id, entity_dt, activestatus, ENTITYNAME1, ENTITYNAME4,
                description1_value_cn, description2_value_cn, NMTOKEN_LEVEL,
                CASE 
                    WHEN '高风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '高风险' 
                    WHEN '中风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '中风险' 
                    ELSE '无风险' 
                END AS risk_lev,
                NMTOKEN_LEVEL as risk_type,
                is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries,
                description3_value_cn, SANCTIONS_NM AS sanctions_nm,
                DATEVALUE1 AS datevalue1, country_nm1, country_nm2
            FROM dqs_entity_sanctions_test
            WHERE ENTITYNAME1 LIKE %s
            LIMIT %s OFFSET %s
            """
            cursor.execute(sql, (f"%{ENTITYNAME1}%", page_size, offset))
            results = cursor.fetchall()

            count_sql = "SELECT COUNT(*) AS total FROM dqs_entity_sanctions_test WHERE ENTITYNAME1 LIKE %s"
            cursor.execute(count_sql, (f"%{ENTITYNAME1}%",))
            total = cursor.fetchone()['total']  # PostgreSQL返回字典格式

            if not results:
                raise HTTPException(status_code=404, detail="No records found")

            processed_results = []
            for item in results:
                processed_item = {
                    "entity_id": item["entity_id"],
                    "entity_dt": item["entity_dt"],
                    "activestatus": item["activestatus"],
                    "ENTITYNAME1": item["ENTITYNAME1"],
                    "ENTITYNAME4": item["ENTITYNAME4"],
                    "description1_value_cn": item["description1_value_cn"],
                    "description2_value_cn": item["description2_value_cn"],
                    "NMTOKEN_LEVEL": item["NMTOKEN_LEVEL"],
                    "risk_lev": item["risk_lev"],
                    "risk_type": item["risk_type"],
                    "is_san": item["is_san"],
                    "is_sco": item["is_sco"],
                    "is_ool": item["is_ool"],
                    "is_one_year": item["is_one_year"],
                    "is_sanctioned_countries": item["is_sanctioned_countries"],
                    "description3_value_cn": item["description3_value_cn"],
                    "sanctions_nm": item["sanctions_nm"],
                    "datevalue1": item["datevalue1"],
                    "country_nm1": item["country_nm1"],
                    "country_nm2": item["country_nm2"]
                }
                processed_results.append(processed_item)

            return {"total": total, "data": processed_results}

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"JSON parsing error: {str(e)}")
    finally:
        if conn:
            conn.close()

@business_app.get("/search-entities/", response_model=List[VesselOut])
def search_entities(
    entityname1: Optional[str] = Query(None),
    search_time: Optional[str] = Query(None, description="查询时间，格式为YYYY-MM-DD HH:mm:ss"),
    user_id: Optional[str] = Query(None),
    user_name: Optional[str] = Query(None),
    depart_id: Optional[str] = Query(None),
    depart_name: Optional[str] = Query(None)
):
    """公司风险查询"""
    if not entityname1:
        raise HTTPException(status_code=400, detail="entityname1参数不能为空")
    
    actual_search_time = search_time if search_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sql = f"""
    {BASE_SQL}
    WHERE entityname1 LIKE CONCAT('%', :entityname1, '%')
    LIMIT 1
    """
    
    try:
        logger.info(f"执行查询: {sql} with params: { {'entityname1': entityname1} }")
        vessel_data = VesselOut.model_validate(fetch_one(sql, {"entityname1": entityname1}))
        
    except HTTPException as e:
        if "未找到该实体" in str(e.detail):
            conn = psycopg2.connect(**DB_CONFIG)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT entityname1 FROM dqs_entity_sanctions_test WHERE entityname1 LIKE %s LIMIT 5",
                        (f"%{entityname1}%",)
                    )
                    similar_names = [row["entityname1"] for row in cursor.fetchall()]
                    
                    if similar_names:
                        raise HTTPException(
                            status_code=404,
                            detail={
                                "message": f"未找到精确匹配 '{entityname1}' 的记录",
                                "suggestions": similar_names
                            }
                        )
                    else:
                        raise HTTPException(
                            status_code=404,
                            detail=f"数据库中没有名称包含 '{entityname1}' 的记录"
                        )
            finally:
                conn.close()
        raise
    
    # 查询补充数据
    query_name_data = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            query_sql = """
            SELECT 
                country_nm1, country_nm2, DATEVALUE1, sanctions_lev,
                description1_value_cn, description2_value_cn, NMTOKEN_LEVEL,
                is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries,
                description3_value_cn, SANCTIONS_NM, entity_dt
            FROM dqs_entity_sanctions_test
            WHERE ENTITYNAME1 = %s
            LIMIT 1
            """
            cursor.execute(query_sql, (entityname1,))
            query_name_data = cursor.fetchone()
    except Exception as e:
        logger.error(f"查询制裁风险结果失败: {str(e)}")
    finally:
        if conn:
            conn.close()
    
    # 构建历史记录
    his_record = {
        "entity_id": str(vessel_data.entity_id),  # 确保转换为字符串
        "risk_lev": query_name_data["risk_lev"] if query_name_data else None,
        "risk_type": vessel_data.NMTOKEN_LEVEL,
        "ENTITYNAME1": vessel_data.entityname1,
        "ENTITYNAME4": vessel_data.entityname4,
        "serch_time": actual_search_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "user_name": user_name,
        "depart_id": depart_id,
        "depart_name": depart_name,
        "is_delete": "0",
        "entity_dt": vessel_data.entity_dt if hasattr(vessel_data, 'entity_dt') else None,
        "activestatus": vessel_data.activestatus if hasattr(vessel_data, 'activestatus') else None,
        "sanctions_lev": query_name_data["sanctions_lev"] if query_name_data else None
    }
    
    if query_name_data:
        his_record.update({
            "country_nm1": query_name_data["country_nm1"] if query_name_data else None,
            "country_nm2": query_name_data["country_nm2"] if query_name_data else None,
            "DATEVALUE1": query_name_data["DATEVALUE1"] if query_name_data else None,
            "sanctions_list": json.dumps(query_name_data["sanctions_lev"], ensure_ascii=False) if query_name_data and query_name_data["sanctions_lev"] else None,
            "mid_sanctions_list": json.dumps(query_name_data["description1_value_cn"], ensure_ascii=False) if query_name_data and query_name_data["description1_value_cn"] else None,
            "no_sanctions_list": json.dumps(query_name_data["description2_value_cn"], ensure_ascii=False) if query_name_data and query_name_data["description2_value_cn"] else None,
            "unknown_risk_list": json.dumps(query_name_data["NMTOKEN_LEVEL"], ensure_ascii=False) if query_name_data and query_name_data["NMTOKEN_LEVEL"] else None,
            "other_list": json.dumps(query_name_data["is_san"], ensure_ascii=False) if query_name_data and query_name_data["is_san"] else None,
            "entity_dt": query_name_data["entity_dt"] if query_name_data else None,
            "sanctions_lev": query_name_data["sanctions_lev"] if query_name_data else None
        })
    
    # 插入历史记录
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            columns = []
            placeholders = []
            values = []
            for k, v in his_record.items():
                if v is not None:
                    columns.append(k)
                    placeholders.append("%s")
                    values.append(v)
            
            insert_sql = f"""
            INSERT INTO dqs_entity_sanctions_test_his 
            ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
            cursor.execute(insert_sql, values)
            conn.commit()
    except Exception as e:
        logger.error(f"插入历史记录失败: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
    return [vessel_data]

@business_app.get("/search-vessel/", response_model=List[VesselRiskResponse])
async def search_vessel(
        vessel_imo: str = Query(..., regex=r"^\d{7}$", example="9263215"),
        db: Session = Depends(get_db)
):
    """根据IMO编号查询船舶风险信息"""
    try:
        query = text("""
            SELECT 
                t0.vessel_imo,
                t0.vessel_name,
                CASE 
                    WHEN '高风险' IN (
                        t.has_sanctioned_cargo_risk,
                        t1.has_sanctioned_trades_risk,
                        t2.has_sanctioned_flag_risk,
                        t6.has_port_calls_risk
                    ) THEN '高风险'
                    WHEN '中风险' IN (
                        # t3.has_ais_gap_risk,
                        t4.has_sts_events_risk,
                        t5.has_dark_sts_risk,
                        t7.has_ais_spoofs_risk
                    ) THEN '中风险'
                    ELSE '无风险' 
                END AS risk_level,
                '' AS flag_country,
                '' AS risk_type
            FROM 
                (SELECT task_uuid, vessel_imo, vessel_name FROM ods_zyhy_rpa_vessel_risk_data) t0
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '高风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '高风险'
                         ELSE '无风险' END AS has_sanctioned_cargo_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Cargo') t 
                 ON t0.task_uuid = t.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '高风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '高风险'
                         ELSE '无风险' END AS has_sanctioned_trades_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Trade') t1
                 ON t0.task_uuid = t1.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '高风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '高风险'
                         ELSE '无风险' END AS has_sanctioned_flag_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Flag') t2
                 ON t0.task_uuid = t2.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    # CASE WHEN data_format='Table' THEN '中风险'
                    #      WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '中风险'
                    #      ELSE '无风险' END AS has_ais_gap_risk
                    CASE WHEN 1=0 THEN '中风险' ELSE '无风险' END AS has_ais_gap_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='AIS gaps') t3
                 ON t0.task_uuid = t3.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '中风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '中风险'
                         ELSE '无风险' END AS has_sts_events_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='High risk STS transfers') t4
                 ON t0.task_uuid = t4.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '中风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '中风险'
                         ELSE '无风险' END AS has_dark_sts_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Dark STS transfers') t5
                 ON t0.task_uuid = t5.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '高风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '高风险'
                         ELSE '无风险' END AS has_port_calls_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='Port call risks') t6
                 ON t0.task_uuid = t6.task_uuid
            LEFT JOIN 
                (SELECT task_uuid, data_type, content_data,
                    CASE WHEN data_format='Table' THEN '中风险'
                         WHEN data_format='Text' AND content_data LIKE 'icon-unverified' THEN '中风险'
                         ELSE '无风险' END AS has_ais_spoofs_risk
                 FROM ods_zyhy_rpa_vessel_risk_content WHERE data_type='AIS spoofing') t7
                 ON t0.task_uuid = t7.task_uuid
            WHERE 
                t0.vessel_imo = :imo
        """)

        result = db.execute(query, {"imo": vessel_imo})

        if not result.rowcount:
            raise HTTPException(
                status_code=404,
                detail=f"未找到IMO编号为 {vessel_imo} 的船舶记录"
            )

        return [
            VesselRiskResponse(
                vessel_imo=row.vessel_imo,
                vessel_name=row.vessel_name,
                risk_level=row.risk_level,
                flag_country=row.flag_country,
                risk_type=row.risk_type
            )
            for row in result
        ]

    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail="数据库操作失败，请检查查询参数")
    except HTTPException:
        raise
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="服务器内部错误")

@business_app.post("/api/save_vessel_data")
async def save_vessel_data(request: Request):
    """船舶风险数据保存"""
    try:
        raw_data = await request.json()
        logger.info(f"接收原始数据: {json.dumps(raw_data, indent=2)}")

        required_fields = ['taskUuid', 'taskStatus', 'vesselImo']
        for field in required_fields:
            if field not in raw_data:
                raise HTTPException(400, f"缺少必填字段: {field}")

        main_data = {
            "task_uuid": raw_data.get("taskUuid"),
            "task_status": raw_data.get("taskStatus"),
            "task_message": raw_data.get("taskMessage"),
            "task_begin_time": raw_data.get("taskBeginTime", ""),
            "task_end_time": raw_data.get("taskEndTime", ""),
            "vessel_imo": str(raw_data.get("vesselImo", "")),
            "vessel_name": raw_data.get("vesselName", ""),
            "risk_type": raw_data.get("riskType", ""),
            "content_json": json.dumps(raw_data.get("content", [])),
            "raw_content": json.dumps(raw_data.get("rawContent")) if raw_data.get("rawContent") else None
        }

        content_items = []
        for content in raw_data.get("content", []):
            for data_item in content.get("data", []):
                content_items.append({
                    "task_uuid": raw_data["taskUuid"],
                    "content_type": content.get("type", "unknown"),
                    "data_type": data_item.get("type", "unknown"),
                    "data_format": data_item.get("dataType", data_item.get("data_type", "text")),
                    "content_data": json.dumps(data_item.get("content", {}))
                })

        conn = psycopg2.connect(**DB_CONFIG)
        
        try:
            with conn.cursor() as cursor:
                columns = ", ".join(main_data.keys())
                placeholders = ", ".join(["%s"] * len(main_data))
                sql = f"INSERT INTO ods_zyhy_rpa_vessel_risk_data ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, list(main_data.values()))
                
                for item in content_items:
                    columns = ", ".join(item.keys())
                    placeholders = ", ".join(["%s"] * len(item))
                    sql = f"INSERT INTO ods_zyhy_rpa_vessel_risk_content ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, list(item.values()))
                    
            conn.commit()
            logger.info("数据保存成功")
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库操作失败: {str(e)}")
            raise
        finally:
            conn.close()

        return {
            "status": "success",
            "message": "数据保存成功",
            "task_uuid": raw_data["taskUuid"]
        }

    except json.JSONDecodeError:
        raise HTTPException(400, "无效的JSON格式")
    except psycopg2.Error as e:
        logger.error(f"数据库错误: {str(e)}")
        raise HTTPException(500, "数据库操作失败")
    except Exception as e:
        logger.error(f"未处理异常: {str(e)}", exc_info=True)
        raise HTTPException(500, "服务器内部错误")

# 全局异常处理
@business_app.exception_handler(SQLAlchemyError)
async def sqlalchemy_exception_handler(request, exc):
    logger.error(f"SQLAlchemy错误: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "数据库错误", "detail": str(exc)},
    )

@business_app.exception_handler(Exception)
async def unicorn_exception_handler(request, exc):
    logger.error(f"Error: {str(exc)}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error", "detail": str(exc)},
    )
