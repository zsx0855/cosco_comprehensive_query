from fastapi import FastAPI, Request, HTTPException, Query, Path as FPath, Depends
from fastapi.responses import JSONResponse
from psycopg2 import Error as KingbaseError
from pydantic import BaseModel, field_validator, Field
from pydantic import computed_field  # For Pydantic v2
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
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import unquote  # 解码URL编码参数

def safe_json_parse(json_str, default_value=None):
    """安全解析JSON字符串，处理可能的格式错误"""
    if not json_str or json_str in ("null", "None"):
        return default_value
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default_value

# ------------- 配置 -------------
load_dotenv()

# 导入Kingbase配置
from kingbase_config import get_kingbase_url, get_db_pool_config

# 数据库配置
DB_URL = get_kingbase_url()
engine = create_engine(DB_URL, **get_db_pool_config())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 验证配置
VALID_REFERERS = ['Console', 'AdminPanel', 'MobileApp']  # 合法的请求来源
VALID_API_KEYS = {
    "admin": "a1b2c3d4-e5f6-7890-g1h2-i3j4k5l6m7n8",
    "client": "x9y8z7-w6v5-u4t3-s2r1-q0p9o8n7m6l5"
}  # 有效的API密钥
SUPPORTED_LANGUAGES = ['zh-cn', 'en-us']  # 支持的语言

# 导入Kingbase配置
from kingbase_config import get_kingbase_config

# 数据库配置（用于文档1的接口）
DB_CONFIG = get_kingbase_config()

# ------------- 日志配置 -------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


# ------------- 数据模型 -------------
class VesselOut(BaseModel):
    entity_id: str  # 改为字符串类型
    entity_dt: Optional[str] = None
    activestatus: Optional[str] = None
    entityname1: Optional[str] = None
    entityname4: Optional[str] = None
    description1_value_cn: Optional[str] = None
    description2_value_cn: Optional[str] = None
    NMTOKEN_LEVEL: Optional[str] = None
    risk_lev: Optional[str]
    risk_type: Optional[str] = Field(None, description="风险类型")
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

    @field_validator("*", mode="before")
    def stringify(cls, v):
        return None if v is None else str(v).strip()

    @computed_field
    @property
    def sanctions_list(self) -> List[str]:
        return self._parse_list_str(self.sanctions_nm)

    @staticmethod
    def _parse_list_str(raw: Optional[str]) -> List[str]:
        if not raw or not isinstance(raw, str):
            return []

        # 按分号分割，保留完整条目（包括管道符分隔的部分）
        return [
            entry.strip()  # 去除前后空格
            for entry in raw.split(';')
            if entry.strip()  # 过滤空条目
        ]

class SanctionsRiskResultHis(BaseModel):
    entity_id: str  # 改为字符串类型
    risk_lev: Optional[str] = None
    risk_type: Optional[str] = None
    entity_dt: Optional[str] = None
    activestatus: Optional[str] = None
    ENTITYNAME1: Optional[str] = None
    ENTITYNAME4: Optional[str] = None
    country_nm1: Optional[str] = None
    country_nm2: Optional[str] = None
    DATEVALUE1: Optional[str] = None
    sanctions_lev: Optional[str] = None
    sanctions_list: Optional[str] = None
    mid_sanctions_list: Optional[str] = None
    no_sanctions_list: Optional[str] = None
    unknown_risk_list: Optional[str] = None
    other_list: Optional[str] = None
    create_time: Optional[str] = None
    serch_time: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    depart_id: Optional[str] = None
    depart_name: Optional[str] = None
    is_delete: Optional[str] = None


class EntityData(BaseModel):
    entity_id: str  # 改为字符串类型
    entityname1: str
    entityname4: str
    risk_lev: str
    country_nm1: str
    country_nm2: str
    risk_type: str


class RiskAssessment(BaseModel):
    imo: Optional[str]
    imo_name: Optional[str] = None
    has_sanctioned_cargo_risk: Optional[str] = None
    has_sanctioned_trades_risk: Optional[str] = None
    has_sanctioned_flag_risk: Optional[str] = None
    has_port_calls_risk: Optional[str] = None
    has_sts_events_risk: Optional[str] = None
    # has_ais_gap_risk: Optional[str] = None
    has_ais_spoofs_risk: Optional[str] = None
    has_dark_sts_risk: Optional[str] = None
    has_sanctioned_companies_risk: Optional[str] = None
    risk_level: Optional[str] = None
    has_sanctioned_cargo_list: Optional[List[Dict[str, str]]] = None
    has_sanctioned_trades_list: Optional[List[Dict[str, str]]] = None
    has_sanctioned_flag_list: Optional[List[Dict[str, str]]] = None
    has_port_calls_list: Optional[List[Dict[str, str]]] = None
    has_sts_events_list: Optional[List[Dict[str, str]]] = None
    # has_ais_gap_list: Optional[List[Dict[str, str]]] = None
    has_ais_spoofs_list: Optional[List[Dict[str, str]]] = None
    has_dark_sts_list: Optional[List[Dict[str, str]]] = None
    has_sanctioned_companies_list: Optional[List[Dict[str, str]]] = None


class VesselRiskData(BaseModel):
    """
    船舶风险数据模型（明确声明接收 camelCase 字段）
    注意：字段名使用 PascalCase，但通过 alias 匹配 camelCase 请求
    """
    taskUuid: str = Field(..., alias="taskUuid", description="任务唯一ID")
    taskStatus: str = Field(..., alias="taskStatus", description="任务状态")
    taskMessage: str = Field(..., alias="taskMessage", description="任务消息")
    taskBeginTime: str = Field(..., alias="taskBeginTime", description="任务开始时间")
    taskEndTime: str = Field(..., alias="taskEndTime", description="任务结束时间")
    vesselImo: str = Field(..., alias="vesselImo", description="船舶IMO编号")
    vesselName: str = Field(..., alias="vesselName", description="船舶名称")
    riskType: str = Field(..., alias="riskType", description="风险类型")
    content: List[dict] = Field(..., description="风险内容数据")
    rawContent: Optional[dict] = Field(None, alias="rawContent", description="原始数据")

    class Config:
        populate_by_name = True  # 允许通过字段名或别名填充数据


class VesselRiskResponse(BaseModel):
    vessel_imo: int = Field(..., gt=0, example=9263215)
    vessel_name: str = Field(..., example="Ever Given")
    risk_level: str = Field(..., example="高风险")
    flag_country: Optional[str] = Field(None, example="Panama")
    risk_type: Optional[str] = Field(None, example="Sanctioned Cargo")


class VesselRiskContent(BaseModel):
    task_uuid: str
    content_type: str
    data_type: str
    data_format: str
    content_data: dict


# 新增制裁记录模型
class SanctionRecord(BaseModel):
    entity_id: str
    entity_dt: Optional[str]
    activestatus: Optional[str]
    ENTITYNAME1: Optional[str]
    ENTITYNAME4: Optional[str]
    country_nm1: Optional[str]
    country_nm2: Optional[str]
    DATEVALUE1: Optional[str]
    sanctions_lev: Optional[str]
    sanctions_list: List[dict]
    mid_sanctions_list: List[dict]
    no_sanctions_list: List[dict]
    unknown_risk_list: List[dict]
    other_list: List[dict]


class QueryResponse(BaseModel):
    total: int
    data: List[SanctionRecord]


# ------------- 辅助函数 -------------
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


def fetch_many(sql: str, params: dict):
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
            return [r._mapping for r in rows]
    except SQLAlchemyError as e:
        logger.error(f"数据库查询错误: {str(e)}")
        raise HTTPException(status_code=500, detail="数据库操作失败")

def convert_to_json_list(field_value: Optional[str]) -> List[Dict[str, str]]:
    """安全处理管道分隔的字符串"""
    if not field_value:
        return []
    
    try:
        items = field_value.split(' | ')
        json_list = []
        for item in items:
            pairs = [p.strip() for p in item.split(',') if p.strip()]
            item_dict = {}
            for pair in pairs:
                if ':' in pair:
                    key, value = pair.split(':', 1)
                    item_dict[key.strip()] = value.strip()
            if item_dict:
                json_list.append(item_dict)
        return json_list
    except Exception as e:
        logging.error(f"字段解析失败: {e}")
        return []
    

def validate_headers(headers: dict):
    """验证请求头"""
    # 验证X-API-KEY
    api_key = headers.get('X-API-KEY')
    if not api_key or api_key not in VALID_API_KEYS.values():
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-KEY header")

    # 验证X-Referer
    referer = headers.get('X-Referer')
    if not referer or referer not in VALID_REFERERS:
        raise HTTPException(status_code=400, detail="Invalid or missing X-Referer header")

    # 验证Accept-Language（可选）
    accept_language = headers.get('Accept-Language')
    if accept_language and accept_language not in SUPPORTED_LANGUAGES:
        raise HTTPException(status_code=400, detail="Unsupported language")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------- FastAPI 应用 -------------
app = FastAPI(
    title="船舶信息综合API服务",
    version="1.0.0",
    description="整合了5个船舶相关API的服务"
)

# 公共SQL模板
BASE_SQL = """
    SELECT
        entity_id,
        entity_dt,
        activestatus,
        entityname1,
        entityname4,
        description1_value_cn,
        description2_value_cn,
        NMTOKEN_LEVEL,
				CASE 
            WHEN '高风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '高风险' 
            WHEN '中风险' IN (is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries) THEN '中风险' 
            ELSE '无风险' 
        END AS risk_lev,
		NMTOKEN_LEVEL as risk_type,
        is_san,
        is_sco,
        is_ool,
        is_one_year,
        is_sanctioned_countries,
        description3_value_cn,
        SANCTIONS_NM AS sanctions_nm,
        DATEVALUE1 AS datevalue1,
        country_nm1,
        country_nm2
    FROM dqs_entity_sanctions_test
"""


# ------------- 新增接口: 根据公司名称查询制裁风险结果 -------------
@app.get("/query_by_name", response_model=QueryResponse)
async def query_by_name(
    ENTITYNAME1: str = Query(..., min_length=1, description="企业名称模糊查询"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数")
):
    """
    根据企业名称查询制裁风险结果（支持分页）
    - 保留 sanctions_list/mid_sanctions_list/no_sanctions_list 原始JSON结构
    - 自动处理JSON字段的序列化与反序列化
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # 计算分页偏移量
            offset = (page - 1) * page_size

            # 主查询（带分页）
            sql = """
            SELECT 
                entity_id, entity_dt, activestatus,
                ENTITYNAME1, ENTITYNAME4,
                country_nm1, country_nm2, DATEVALUE1,sanctions_lev,
                sanctions_list, mid_sanctions_list, no_sanctions_list,unknown_risk_list,other_list
            FROM sanctions_risk_result
            WHERE ENTITYNAME1 LIKE %s
            LIMIT %s OFFSET %s
            """
            cursor.execute(sql, [f"%{ENTITYNAME1}%", page_size, offset])
            results = cursor.fetchall()

            # 总数查询
            count_sql = "SELECT COUNT(*) AS total FROM sanctions_risk_result WHERE ENTITYNAME1 LIKE %s"
            cursor.execute(count_sql, [f"%{ENTITYNAME1}%"])
            total = cursor.fetchone()['total']

            if not results:
                raise HTTPException(status_code=404, detail="No records found")

            # 处理JSON字段
            processed_results = []
            for item in results:
                processed_item = {
                    "entity_id": item["entity_id"],
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

            return {
                "total": total,
                "data": processed_results
            }

    except KingbaseError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"JSON parsing error: {str(e)}"
        )
    finally:
        if conn:
            conn.close()



# ------------- 公司风险查询 -------------
@app.get("/search-entities/", response_model=List[VesselOut])
def search_entities(
    entityname1: Optional[str] = Query(None),
    search_time: Optional[str] = Query(None, description="查询时间，格式为YYYY-MM-DD HH:mm:ss"),
    user_id: Optional[str] = Query(None),
    user_name: Optional[str] = Query(None),
    depart_id: Optional[str] = Query(None),
    depart_name: Optional[str] = Query(None)
):
    # 1. 检查必要参数
    if not entityname1:
        raise HTTPException(status_code=400, detail="entityname1参数不能为空")
    
    # 2. 处理search_time参数
    actual_search_time = search_time if search_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 3. 修改查询SQL（使用更宽松的模糊匹配）
    sql = f"""
    {BASE_SQL}
    WHERE entityname1 LIKE CONCAT('%', :entityname1, '%')
    LIMIT 1
    """
    
    try:
        # 4. 执行查询
        logger.info(f"执行查询: {sql} with params: { {'entityname1': entityname1} }")
        vessel_data = VesselOut.model_validate(fetch_one(sql, {"entityname1": entityname1}))
        
    except HTTPException as e:
        if "未找到该实体" in str(e.detail):
            # 5. 如果找不到记录，检查数据库中是否存在相似名称
            conn = psycopg2.connect(**DB_CONFIG)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT entityname1 FROM dqs_entity_sanctions_test WHERE entityname1 LIKE %s LIMIT 5",
                        [f"%{entityname1}%"]
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
    
    # 4. 调用/query_by_name接口获取补充数据
    query_name_data = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            query_sql = """
            SELECT 
                country_nm1, country_nm2, DATEVALUE1,sanctions_lev,
                sanctions_list, mid_sanctions_list, no_sanctions_list,
                unknown_risk_list, other_list
            FROM sanctions_risk_result
            WHERE ENTITYNAME1 = %s
            LIMIT 1
            """
            cursor.execute(query_sql, [entityname1])
            query_name_data = cursor.fetchone()
    except Exception as e:
        logger.error(f"查询制裁风险结果失败: {str(e)}")
    finally:
        if conn:
            conn.close()
    
    # 5. 构建要插入的历史记录数据
    his_record = {
        "entity_id": str(vessel_data.entity_id),
        "risk_lev": query_name_data.get("risk_lev") if query_name_data else None,
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
        "entity_dt": vessel_data.entity_dt if hasattr(vessel_data, 'entity_dt') else None,  # 制裁时间
        "activestatus": vessel_data.activestatus if hasattr(vessel_data, 'activestatus') else None,  # 活跃状态
        "sanctions_lev": query_name_data.get("sanctions_lev") if query_name_data else None  # 制裁等级
    }
    
    # 如果查询到了制裁风险数据，则合并
    if query_name_data:
        his_record.update({
            "country_nm1": query_name_data.get("country_nm1"),
            "country_nm2": query_name_data.get("country_nm2"),
            "DATEVALUE1": query_name_data.get("DATEVALUE1"),
            "sanctions_list": json.dumps(query_name_data.get("sanctions_list"), ensure_ascii=False) if query_name_data.get("sanctions_list") else None,
            "mid_sanctions_list": json.dumps(query_name_data.get("mid_sanctions_list"), ensure_ascii=False) if query_name_data.get("mid_sanctions_list") else None,
            "no_sanctions_list": json.dumps(query_name_data.get("no_sanctions_list"), ensure_ascii=False) if query_name_data.get("no_sanctions_list") else None,
            "unknown_risk_list": json.dumps(query_name_data.get("unknown_risk_list"), ensure_ascii=False) if query_name_data.get("unknown_risk_list") else None,
            "other_list": json.dumps(query_name_data.get("other_list"), ensure_ascii=False) if query_name_data.get("other_list") else None,
            #"risk_type": query_name_data.get("risk_type"),  # 从制裁风险结果表获取
            "entity_dt": query_name_data.get("entity_dt"),
            "sanctions_lev": query_name_data.get("sanctions_lev")
        })
    
    # 6. 插入到历史记录表
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # 构建插入SQL
            columns = []
            placeholders = []
            values = []
            for k, v in his_record.items():
                if v is not None:
                    columns.append(k)
                    placeholders.append("%s")
                    values.append(v)
            
            insert_sql = f"""
            INSERT INTO sanctions_risk_result_his 
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


# ------------- 新增接口: 查询公司制裁风险历史记录 -------------
@app.get("/sanctions_risk_history", response_model=List[SanctionsRiskResultHis])
async def get_sanctions_risk_history(
    entity_id: Optional[str] = Query(None, description="实体ID"),  # 改为字符串类型
    ENTITYNAME1: Optional[str] = Query(None, description="企业名称模糊查询"),
    serch_time: Optional[str] = Query(None, description="查询时间范围(YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    db: Session = Depends(get_db)
):
    """
    查询制裁风险历史记录
    - 返回数据结构与/query_by_name接口保持一致
    - 支持通过entity_id、ENTITYNAME1或serch_time进行筛选
    - 返回分页结果
    """
    try:
        # 构建基础查询
        query = text("""
        SELECT 
            entity_id, 
            risk_lev, 
            risk_type, 
            entity_dt, 
            activestatus,
            ENTITYNAME1, 
            ENTITYNAME4, 
            country_nm1, 
            country_nm2, 
            DATEVALUE1,
            sanctions_lev, 
            sanctions_list, 
            mid_sanctions_list, 
            no_sanctions_list,
            unknown_risk_list, 
            other_list,
            create_time,
            serch_time
        FROM sanctions_risk_result_his
        WHERE is_delete = 0
        """)
        
        # 添加筛选条件
        conditions = []
        params = {}
        
        if entity_id is not None:
            conditions.append("entity_id = :entity_id")
            params["entity_id"] = str(entity_id)  # 确保转换为字符串
            
        if ENTITYNAME1:
            conditions.append("ENTITYNAME1 LIKE :entity_name")
            params["entity_name"] = f"%{ENTITYNAME1}%"
            
        if serch_time:
            conditions.append("DATE(serch_time) = :serch_time")
            params["serch_time"] = serch_time
        
        # 组合完整查询
        if conditions:
            query = text(str(query) + " AND " + " AND ".join(conditions))
        
        # 添加分页
        offset = (page - 1) * page_size
        query = text(str(query) + " ORDER BY create_time DESC LIMIT :limit OFFSET :offset")
        params.update({"limit": page_size, "offset": offset})
        
        # 执行查询
        result = db.execute(query, params)
        
        # 获取总数
        count_query = text("""
        SELECT COUNT(*) as total FROM sanctions_risk_result_his 
        WHERE is_delete = 0
        """ + (" AND " + " AND ".join(conditions) if conditions else ""))
        
        total_result = db.execute(count_query, params)
        total = total_result.scalar()
        
        # 处理结果
        records = []
        for row in result:
            record = dict(row._mapping)
            
            # 处理日期时间字段，转换为字符串（与query_by_name一致）
            date_fields = ['entity_dt', 'DATEVALUE1', 'create_time', 'serch_time']
            for field in date_fields:
                if record.get(field) and isinstance(record[field], datetime):
                    record[field] = record[field].strftime('%Y-%m-%d %H:%M:%S')
                elif record.get(field) is None:
                    record[field] = None
            
            # 处理JSON字段（与query_by_name一致的处理方式）
            json_fields = ['sanctions_list', 'mid_sanctions_list', 'no_sanctions_list', 'unknown_risk_list', 'other_list']
            for field in json_fields:
                if isinstance(record.get(field), str):
                    try:
                        record[field] = json.loads(record[field])
                    except json.JSONDecodeError:
                        record[field] = []
                elif record.get(field) is None:
                    record[field] = []
            
            # 确保返回字段结构与query_by_name一致
            formatted_record = {
                "entity_id": record["entity_id"],
                "entity_dt": record.get("entity_dt"),
                "activestatus": record.get("activestatus"),
                "ENTITYNAME1": record.get("ENTITYNAME1"),
                "ENTITYNAME4": record.get("ENTITYNAME4"),
                "country_nm1": record.get("country_nm1"),
                "country_nm2": record.get("country_nm2"),
                "DATEVALUE1": record.get("DATEVALUE1"),
                "sanctions_list": record["sanctions_list"],
                "mid_sanctions_list": record["mid_sanctions_list"],
                "no_sanctions_list": record["no_sanctions_list"],
                "unknown_risk_list": record["unknown_risk_list"],
                "other_list": record["other_list"],
                # 以下是额外字段（比query_by_name多的字段）
                "risk_lev": record.get("risk_lev"),
                "risk_type": record.get("risk_type"),
                "create_time": record.get("create_time"),
                "serch_time": record.get("serch_time"),
                "sanctions_lev": record.get("sanctions_lev")
            }
            
            records.append(formatted_record)
        
        # 添加分页信息到响应头
        response_headers = {
            "X-Total-Count": str(total),
            "X-Page": str(page),
            "X-Page-Size": str(page_size),
            "X-Total-Pages": str((total + page_size - 1) // page_size)
        }
        
        return JSONResponse(
            content=records,
            headers=response_headers
        )
        
    except SQLAlchemyError as e:
        logger.error(f"数据库查询错误: {str(e)}")
        raise HTTPException(status_code=500, detail="数据库操作失败")
    except Exception as e:
        logger.error(f"未知错误: {str(e)}")
        raise HTTPException(status_code=500, detail="服务器内部错误")
    


@app.get("/search-vessel/", response_model=List[VesselRiskResponse])
async def search_vessel(
        vessel_imo: str = Query(..., regex=r"^\d{7}$", example="9263215"),
        db: Session = Depends(get_db)
):
    """
    根据IMO编号查询船舶风险信息
    - **vessel_imo**: 国际海事组织船舶编号（7位数字）
    - 返回: 船舶风险详情列表
    """
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
        raise HTTPException(
            status_code=500,
            detail="数据库操作失败，请检查查询参数"
        )
    except HTTPException:
        raise  # 直接传递已处理的HTTP异常
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="服务器内部错误"
        )


# ------------- 接口5: 船舶风险数据保存 -------------
@app.post("/api/save_vessel_data")
async def save_vessel_data(request: Request):
    try:
        # 1. 获取并记录原始请求
        raw_data = await request.json()
        logger.info(f"接收原始数据: {json.dumps(raw_data, indent=2)}")

        # 2. 宽松验证（仅检查必填字段是否存在）
        required_fields = ['taskUuid', 'taskStatus', 'vesselImo']
        for field in required_fields:
            if field not in raw_data:
                raise HTTPException(400, f"缺少必填字段: {field}")

        # 3. 准备主表数据（自动适应各种格式）
        main_data = {
            "task_uuid": raw_data.get("taskUuid"),
            "task_status": raw_data.get("taskStatus"),
            "task_message": raw_data.get("taskMessage"),
            "task_begin_time": raw_data.get("taskBeginTime", ""),  # 接受任何字符串
            "task_end_time": raw_data.get("taskEndTime", ""),
            "vessel_imo": str(raw_data.get("vesselImo", "")),  # 强制转为字符串
            "vessel_name": raw_data.get("vesselName", ""),
            "risk_type": raw_data.get("riskType", ""),
            "content_json": json.dumps(raw_data.get("content", [])),
            "raw_content": json.dumps(raw_data.get("rawContent")) if raw_data.get("rawContent") else None
        }

        # 4. 准备详情数据
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

        # 5. 保存数据
        # 使用文档1的数据库连接方式
        conn = psycopg2.connect(
            host="10.13.16.186",
            user="coscohw",
            password="WS8k*123",
            database="hwda",
            charset="utf8mb4"
        )
        
        try:
            with conn.cursor() as cursor:
                # 插入主表数据
                columns = ", ".join(main_data.keys())
                placeholders = ", ".join(["%s"] * len(main_data))
                sql = f"INSERT INTO ods_zyhy_rpa_vessel_risk_data ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, list(main_data.values()))
                
                # 插入详情数据
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
    except KingbaseError as e:
        logger.error(f"数据库错误: {str(e)}")
        raise HTTPException(500, "数据库操作失败")
    except Exception as e:
        logger.error(f"未处理异常: {str(e)}", exc_info=True)
        raise HTTPException(500, "服务器内部错误")


# ------------- 全局异常处理 -------------
@app.exception_handler(SQLAlchemyError)
async def sqlalchemy_exception_handler(request, exc):
    logger.error(f"SQLAlchemy错误: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "数据库错误", "detail": str(exc)},
    )


@app.exception_handler(Exception)
async def unicorn_exception_handler(request, exc):
    logger.error(f"Error: {str(exc)}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error", "detail": str(exc)},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)