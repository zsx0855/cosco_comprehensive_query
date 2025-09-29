from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import unicodedata
# 导入数据记录模块
from voyage_risk_log_insert import insert_voyage_risk_log
# 导入风险检查框架
from functions_risk_check_framework import RiskCheckOrchestrator, create_api_config
from sts_bunkering_risk import run_sts_risk_by_imo


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

# ============ 审批对比与重算辅助 ============
def _normalize_risk_status(value: str) -> str:
    v = (value or '').strip()
    mapping = {
        '1': '高风险', '高': '高风险', '高风险': '高风险', 'intercept': '高风险',
        '2': '中风险', '中': '中风险', '中风险': '中风险', 'attention': '中风险',
        '0': '无风险', '无': '无风险', '无风险': '无风险', 'normal': '无风险'
    }
    return mapping.get(v.lower(), v)
def _load_all_approvals_by_uuid(uuid: str) -> list:
    """查询同一 uuid 的所有审批记录，按 approval_date 升序返回列表[dict]"""
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = (
                "SELECT relevant_parties_type, parties_name, risk_change_status, approval_date, change_reason "
                "FROM lng.approval_records_table WHERE uuid = %s ORDER BY approval_date ASC"
            )
            cursor.execute(sql, (uuid,))
            rows = cursor.fetchall() or []
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[voyage_risk] 查询审批记录失败: {e}")
        return []
    finally:
        if 'connection' in locals():
            connection.close()

def _parse_dt_loose(dt_str: str):
    try:
        from datetime import datetime as _dt
        if not dt_str:
            return _dt.min
        try:
            return _dt.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return _dt.fromisoformat(str(dt_str).replace("Z", "+00:00"))
    except Exception:
        from datetime import datetime as _dt
        return _dt.min

def _apply_approvals_to_lists(response_lists: dict, approvals: list) -> None:
    """
    将审批结果应用到各列表项上，就地更新：匹配 same name 且审批时间较新时，覆盖 risk_screening_status、risk_status_change_time、risk_status_change_content。
    response_lists: { field_name -> list[dict] }
    approvals: 来自表的记录列表
    """
    if not approvals:
        return
    # 将 approvals 归并到 {type -> [(name, status, time, reason), ...]}
    merged: dict = {}
    for ap in approvals:
        f = (ap.get('relevant_parties_type') or '').strip()
        n = (ap.get('parties_name') or '').strip()
        s = (ap.get('risk_change_status') or '').strip()
        t = ap.get('approval_date') or ''
        r = ap.get('change_reason') or ''
        if not f or not n or not t:
            continue
        merged.setdefault(f, []).append((n, s, t, r))

    def _norm_name(s):
        if not isinstance(s, str):
            s2 = str(s or '')
        else:
            s2 = s
        s2 = unicodedata.normalize('NFKC', s2)
        s2 = ' '.join(s2.strip().split())
        return s2.casefold()

    def _upd_list(field: str, items: list):
        if not items:
            return items
        # 找大小写无关的 key
        # 允许审批里的类型名与代码字段名大小写不同
        candidates = [field, field.upper(), field.lower(), field.title()]
        # 收集所有同类型审批
        ap_list = []
        for k in merged.keys():
            if k in candidates or k.lower() == field.lower():
                ap_list.extend(merged.get(k, []))
        if not ap_list:
            return items
        for it in items:
            try:
                name = _norm_name(it.get('name') or it.get('Name') or '')
                old_t = _parse_dt_loose(it.get('risk_status_change_time'))
                for (ap_name, ap_status, ap_time, ap_reason) in ap_list:
                    if name and name == _norm_name(ap_name or ''):
                        if old_t < _parse_dt_loose(ap_time):
                            if ap_status:
                                it['risk_screening_status'] = _normalize_risk_status(ap_status)
                            it['risk_status_change_time'] = ap_time
                            it['risk_status_change_content'] = ap_reason or it.get('risk_status_change_content', '')
            except Exception:
                continue
        return items

    # 针对所有已构建的列表字段逐一应用
    for field_name, items in response_lists.items():
        response_lists[field_name] = _upd_list(field_name, items)

def _collect_statuses_from_lists(response_lists: dict) -> list:
    statuses = []
    for items in response_lists.values():
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    v = it.get('risk_screening_status')
                    if v:
                        statuses.append(str(v))
    return statuses

# DowJones 风险检查：统一辅助函数，返回原始字典
def _query_dowjones_raw(entity_name: str) -> dict:
    try:
        # 直接调用 query_sanctions_risk 获取原始数据
        sanctions_data = query_sanctions_risk(entity_name)
        
        # 构建风险描述
        risk_description_parts = []
        if sanctions_data.get('is_san'):
            risk_description_parts.append(f"{sanctions_data['is_san']}")
        if sanctions_data.get('is_sco'):
            risk_description_parts.append(f"{sanctions_data['is_sco']}")
        if sanctions_data.get('is_ool'):
            risk_description_parts.append(f"{sanctions_data['is_ool']}")
        
        risk_description = f"在道琼斯的判定为：{', '.join(risk_description_parts)}" if risk_description_parts else ""
        
        # 解析制裁列表
        import json
        def parse_sanctions_list(json_str):
            try:
                if not json_str or json_str.strip() == "":
                    return []
                return json.loads(json_str)
            except:
                return []
        
        sanctions_list = parse_sanctions_list(sanctions_data.get('sanctions_list', ''))
        mid_sanctions_list = parse_sanctions_list(sanctions_data.get('mid_sanctions_list', ''))
        no_sanctions_list = parse_sanctions_list(sanctions_data.get('no_sanctions_list', ''))
        
        return {
            'name': entity_name,
            'risk_screening_status': sanctions_data.get('risk_level', '无风险'),
            'risk_screening_time': get_current_time(),
            'risk_status_change_content': '',
            'risk_status_change_time': '',
            'risk_type_number': '13',
            'risk_description': '',
            'risk_info': risk_description,
            'risk_status_reason': {
                'sanctions_list': sanctions_list,
                'mid_sanctions_list': mid_sanctions_list,
                'no_sanctions_list': no_sanctions_list,
                'is_san': sanctions_data.get('is_san', ''),
                'is_sco': sanctions_data.get('is_sco', ''),
                'is_ool': sanctions_data.get('is_ool', ''),
                'is_one_year': sanctions_data.get('is_one_year', ''),
                'is_sanctioned_countries': sanctions_data.get('is_sanctioned_countries', '')
            },
            'vessel_imo': {'0': entity_name}
        }
    except Exception as e:
        print(f"调用DowJones风控失败: {e}")
        return {
            'name': entity_name,
            'risk_screening_status': '无风险',
            'risk_screening_time': '',
            'risk_status_change_content': '',
            'risk_status_change_time': '',
            'risk_type_number': '13',
            'risk_description': '',
            'risk_info': '',
            'risk_status_reason': {},
            'vessel_imo': {'0': entity_name}
        }

# 港口国家风险：返回原始字典
def _query_port_country_raw(country_name: str) -> dict:
    try:
        result = risk_orchestrator.execute_port_origin_from_sanctioned_country_check(country_name)
        return result if isinstance(result, dict) else (result.to_dict() if hasattr(result, 'to_dict') else {
            'risk_screening_status': getattr(result, 'risk_value', '无风险'),
            'risk_description': getattr(result, 'risk_desc', ''),
            'risk_status_reason': {}
        })
    except Exception as e:
        print(f"调用PortCountry风控失败: {e}")
        return {'risk_screening_status': '无风险', 'risk_description': '', 'risk_status_reason': {}}

# 货物原产地国家风险：返回原始字典
def _query_cargo_country_raw(country_name: str) -> dict:
    try:
        result = risk_orchestrator.execute_cargo_origin_from_sanctioned_country_check(country_name)
        return result if isinstance(result, dict) else (result.to_dict() if hasattr(result, 'to_dict') else {
            'risk_screening_status': getattr(result, 'risk_value', '无风险'),
            'risk_description': getattr(result, 'risk_desc', ''),
            'risk_status_reason': {}
        })
    except Exception as e:
        print(f"调用CargoCountry风控失败: {e}")
        return {'risk_screening_status': '无风险', 'risk_description': '', 'risk_status_reason': {}}

# 创建风险检查编排器（全局实例）
api_config = create_api_config()
risk_orchestrator = RiskCheckOrchestrator(api_config)


def _get_previous_risk_data(uuid: str, entity_type: str, entity_name: str) -> dict:
    """
    获取之前的风险筛查数据
    Args:
        uuid: 航次UUID
        entity_type: 实体类型 (如 'Sts_vessel_owner', 'shipper' 等)
        entity_name: 实体名称
    Returns:
        dict: 之前的风险数据，如果没有则返回空字典
    """
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # 查询之前的风险筛查记录
            sql = """
                SELECT full_response 
                FROM lng.voyage_risk_log 
                WHERE uuid = %s 
                ORDER BY request_time DESC 
                LIMIT 2
            """
            cursor.execute(sql, (uuid,))
            rows = cursor.fetchall()
            
            if len(rows) < 2:
                # 没有之前的记录，这是首次筛查
                return {}
            
            # 取倒数第二条记录（之前的记录）
            previous_record = rows[1]
            if not previous_record or not previous_record.get('full_response'):
                return {}
            
            import json
            try:
                previous_data = json.loads(previous_record['full_response'])
                
                # 根据实体类型获取对应的数据
                if entity_type in previous_data:
                    entity_list = previous_data[entity_type]
                    if isinstance(entity_list, str):
                        entity_list = json.loads(entity_list)
                    
                    # 查找匹配的实体
                    for item in entity_list:
                        if isinstance(item, dict) and item.get('name') == entity_name:
                            return item
                
                return {}
            except (json.JSONDecodeError, TypeError):
                return {}
                
    except Exception as e:
        print(f"获取之前风险数据失败: {e}")
        return {}
    finally:
        if 'connection' in locals():
            connection.close()


def _determine_risk_change_time(current_status: str, previous_data: dict, current_time: str) -> str:
    """
    确定风险状态变更时间
    Args:
        current_status: 当前风险状态
        previous_data: 之前的风险数据
        current_time: 当前筛查时间
    Returns:
        str: 风险状态变更时间
    """
    if not previous_data:
        # 首次筛查，如果有风险状态就设置变更时间
        return current_time if current_status and current_status != '无风险' else ''
    
    previous_status = previous_data.get('risk_screening_status', '')
    
    # 如果状态发生了变化，设置变更时间
    if current_status != previous_status:
        return current_time
    
    # 状态没有变化，保持之前的变更时间
    return previous_data.get('risk_status_change_time', '')


def _process_entity_list(entity_list: list, entity_type: str, uuid: str, current_time: str, 
                        process_func, all_risk_statuses: list) -> list:
    """
    通用的实体列表处理函数
    Args:
        entity_list: 实体列表
        entity_type: 实体类型名称
        uuid: 航次UUID
        current_time: 当前时间
        process_func: 处理单个实体的函数
        all_risk_statuses: 收集风险状态的列表
    Returns:
        list: 处理后的响应列表
    """
    # 检查空入参
    if not entity_list:
        return []
    
    responses = []
    for entity in entity_list:
        # 调用具体的处理函数
        result = process_func(entity, entity_type, uuid, current_time)
        if result:
            # 收集风险状态
            risk_status = result.get('risk_screening_status', '无风险')
            all_risk_statuses.append(risk_status)
            responses.append(result)
    
    return responses 


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


def query_port_country_risk(country_name: str) -> str:
    """
    查询港口国家风险等级
    Args:
        country_name: 国家名称
    Returns:
        str: 风险等级
    """
    try:
        # 调用风险检查框架的港口国家风险检查
        result = risk_orchestrator.execute_port_origin_from_sanctioned_country_check(country_name)
        
        # 检查返回结果类型
        if isinstance(result, dict):
            risk_level = result.get('risk_screening_status', '无风险')
        else:
            # 如果是CheckResult对象
            risk_level = result.risk_value if hasattr(result, 'risk_value') else '无风险'
        
        # 转换为标准风险等级
        if risk_level in ['高风险', '高']:
            return '高风险'
        elif risk_level in ['中风险', '中']:
            return '中风险'
        else:
            return '无风险'
            
    except Exception as e:
        print(f"查询港口国家风险时出错: {e}")
        return '无风险'

def query_cargo_country_risk(country_name: str) -> str:
    """
    查询货物原产地国家风险等级
    Args:
        country_name: 国家名称
    Returns:
        str: 风险等级
    """
    try:
        # 调用风险检查框架的货物原产地国家风险检查
        result = risk_orchestrator.execute_cargo_origin_from_sanctioned_country_check(country_name)
        
        # 检查返回结果类型
        if isinstance(result, dict):
            risk_level = result.get('risk_screening_status', '无风险')
        else:
            # 如果是CheckResult对象
            risk_level = result.risk_value if hasattr(result, 'risk_value') else '无风险'
        
        # 转换为标准风险等级
        if risk_level in ['高风险', '高']:
            return '高风险'
        elif risk_level in ['中风险', '中']:
            return '中风险'
        else:
            return '无风险'
            
    except Exception as e:
        print(f"查询货物原产地国家风险时出错: {e}")
        return '无风险'

def query_sanctions_risk(entity_name: str) -> dict:
    """
    查询实体制裁风险等级和制裁列表
    
    Args:
        entity_name: 实体名称
        
    Returns:
        dict: 包含制裁风险等级和制裁列表的字典
        {
            'risk_level': str,  # 制裁风险等级
            'sanctions_list': str,  # 制裁列表
            'mid_sanctions_list': str,  # 中等制裁列表
            'no_sanctions_list': str  # 无制裁列表
        }
    """
    try:
        # 建立KingBase数据库连接
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        
        with connection.cursor() as cursor:
            # 执行查询，获取制裁列表字段和新增的风险字段
            sql = """SELECT entity_id, ENTITYNAME1, sanctions_lev, 
                            sanctions_list, mid_sanctions_list, no_sanctions_list,
                            is_san, is_sco, is_ool, is_one_year, is_sanctioned_countries
                     FROM lng.sanctions_risk_result WHERE ENTITYNAME1 = %s"""
            cursor.execute(sql, (entity_name,))
            result = cursor.fetchone()
            
            if result:
                return {
                    'risk_level': result.get('sanctions_lev', ''),
                    'sanctions_list': result.get('sanctions_list', ''),
                    'mid_sanctions_list': result.get('mid_sanctions_list', ''),
                    'no_sanctions_list': result.get('no_sanctions_list', ''),
                    'is_san': result.get('is_san', ''),
                    'is_sco': result.get('is_sco', ''),
                    'is_ool': result.get('is_ool', ''),
                    'is_one_year': result.get('is_one_year', ''),
                    'is_sanctioned_countries': result.get('is_sanctioned_countries', '')
                }
            else:
                return {
                    'risk_level': '无风险',
                    'sanctions_list': '',
                    'mid_sanctions_list': '',
                    'no_sanctions_list': '',
                    'is_san': '',
                    'is_sco': '',
                    'is_ool': '',
                    'is_one_year': '',
                    'is_sanctioned_countries': ''
                }
                
    except Exception as e:
        print(f"查询制裁风险时出错: {e}")
        return {
            'risk_level': '无风险',
            'sanctions_list': '',
            'mid_sanctions_list': '',
            'no_sanctions_list': '',
            'is_san': '',
            'is_sco': '',
            'is_ool': '',
            'is_one_year': '',
            'is_sanctioned_countries': ''
        }
    finally:
        if 'connection' in locals():
            connection.close()


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
class StsVessel(CaseInsensitiveBaseModel):
    
    sts_vessel_imo: str = Field(..., description="STS船舶IMO号", alias="STS_VESSEL_IMO")
    Sts_vessel_name: str = Field(..., description="STS船舶名称", alias="STS_VESSEL_NAME")
    Sts_water_area: str = Field(..., description="STS水域", alias="STS_WATER_AREA")
    Sts_Is_high_risk_area: str = Field(..., description="是否高风险区域", examples=["true", "false"], alias="STS_IS_HIGH_RISK_AREA")
    Sts_plan_start_time: str = Field(..., description="STS计划开始时间", examples=["2025-08-21"], alias="STS_PLAN_START_TIME")
    Sts_execution_status: Optional[str] = Field("", description="STS执行状态", alias="STS_EXECUTION_STATUS")


class StsVesselOwner(CaseInsensitiveBaseModel):
    Sts_vessel_owner: str = Field(..., description="STS船舶所有人", alias="STS_VESSEL_OWNER")


class StsVesselManager(CaseInsensitiveBaseModel):
    Sts_vessel_manager: str = Field(..., description="STS船舶管理人", alias="STS_VESSEL_MANAGER")


class StsVesselOperator(CaseInsensitiveBaseModel):
    Sts_vessel_operator: str = Field(..., description="STS船舶经营人", alias="STS_VESSEL_OPERATOR")


class TimeCharterer(CaseInsensitiveBaseModel):
    time_charterer: str = Field(..., description="期租承租人", alias="TIME_CHARTERER")


class VoyageCharterer(CaseInsensitiveBaseModel):
    voyage_charterer: str = Field(..., description="程租承租人", alias="VOYAGE_CHARTERER")


class LoadingPort(CaseInsensitiveBaseModel):
    loading_port: str = Field(..., description="装货港", alias="LOADING_PORT")
    loading_port_country: str = Field(..., description="装货港所在国家", alias="LOADING_PORT_COUNTRY")


class LoadingPortAgent(CaseInsensitiveBaseModel):
    loading_port_agent: str = Field(..., description="装货港代理", alias="LOADING_PORT_AGENT")


class LoadingTerminal(CaseInsensitiveBaseModel):
    loading_terminal: str = Field(..., description="装货码头", alias="LOADING_TERMINAL")


class LoadingTerminalOperator(CaseInsensitiveBaseModel):
    loading_terminal_operator: str = Field(..., description="装货码头经营人", alias="LOADING_TERMINAL_OPERATOR")


class LoadingTerminalOwner(CaseInsensitiveBaseModel):
    loading_terminal_owner: str = Field(..., description="装货码头所有人", alias="LOADING_TERMINAL_OWNER")


class Shipper(CaseInsensitiveBaseModel):
    shipper: str = Field(..., description="发货人", alias="SHIPPER")


class ShipperActualController(CaseInsensitiveBaseModel):
    shipper_actual_controller: str = Field(..., description="发货人实际控制人", alias="SHIPPER_ACTUAL_CONTROLLER")


class Consignee(CaseInsensitiveBaseModel):
    consignee: str = Field(..., description="收货人", alias="CONSIGNEE")


class ConsigneeController(CaseInsensitiveBaseModel):
    consignee_controller: str = Field(..., description="收货人控制人", alias="CONSIGNEE_CONTROLLER")


class ActualConsignee(CaseInsensitiveBaseModel):
    actual_consignee: str = Field(..., description="实际收货人", alias="ACTUAL_CONSIGNEE")


class ActualConsigneeController(CaseInsensitiveBaseModel):
    actual_consignee_controller: str = Field(..., description="实际收货人控制人", alias="ACTUAL_CONSIGNEE_CONTROLLER")


class CargoOrigin(CaseInsensitiveBaseModel):
    cargo_origin: str = Field(..., description="货物原产地", alias="CARGO_ORIGIN")


class DischargingPort(CaseInsensitiveBaseModel):
    discharging_port: str = Field(..., description="卸货港", alias="DISCHARGING_PORT")
    discharging_port_country: str = Field(..., description="卸货港所在国家", alias="DISCHARGING_PORT_COUNTRY")


class DischargingPortAgent(CaseInsensitiveBaseModel):
    discharging_port_agent: str = Field(..., description="卸货港代理", alias="DISCHARGING_PORT_AGENT")


class DischargingTerminal(CaseInsensitiveBaseModel):
    discharging_terminal: str = Field(..., description="卸货码头", alias="DISCHARGING_TERMINAL")


class DischargingTerminalOperator(CaseInsensitiveBaseModel):
    discharging_terminal_operator: str = Field(..., description="卸货码头经营人")


class DischargingTerminalOwner(CaseInsensitiveBaseModel):
    discharging_terminal_owner: str = Field(..., description="卸货码头所有人")


class BunkeringShip(CaseInsensitiveBaseModel):
    bunkering_ship: str = Field(..., description="加油船IMO号")


class BunkeringSupplier(CaseInsensitiveBaseModel):
    bunkering_supplier: str = Field(..., description="燃料供应商")


class BunkeringPort(CaseInsensitiveBaseModel):
    bunkering_port: str = Field(..., description="加油港")
    bunkering_port_country: str = Field(..., description="加油港所在国家")


class BunkeringPortAgent(CaseInsensitiveBaseModel):
    bunkering_port_agent: str = Field(..., description="加油港代理")


class VoyageRiskRequest(CaseInsensitiveBaseModel):
    scenario: str = Field(..., description="场景", examples=["及时查询"], alias="SCENARIO")
    uuid: str = Field(..., description="唯一标识", examples=["3b6157f4-e262-45cd-8a90-cfbd06640521"], alias="uuid")
    voyage_number: str = Field(..., description="航次号", examples=["12935780"], alias="voyage_number")
    voyage_status: str = Field(..., description="航次状态", examples=["执行中"], alias="VOYAGE_STATUS")
    Business_segment: str = Field(..., description="业务板块", examples=["油轮"], alias="BUSINESS_SEGMENT")
    trade_type: str = Field(..., description="贸易类型", examples=["外贸"], alias="TRADE_TYPE")
    Business_model: str = Field(..., description="经营模式", examples=["自营"], alias="BUSINESS_MODEL")
    voyage_start_time: str = Field(..., description="航次开始时间", examples=["2025-08-22T05:18:20+03:00"], alias="VOYAGE_START_TIME")
    voyage_end_time: Optional[str] = Field("", description="航次结束时间", alias="VOYAGE_END_TIME")
    vessel_imo: str = Field(..., description="船舶IMO号", pattern=r"^\d{7}$", examples=["9842190"], alias="VESSEL_IMO")
    vessel_name: str = Field(..., description="船舶名称", examples=["Akademik Gubkin"], alias="VESSEL_NAME")
    is_sts: str = Field(..., description="是否STS", examples=["true", "false"], alias="IS_STS")
    Sts_vessel: List[StsVessel] = Field(default_factory=list, description="STS船舶列表", alias="STS_VESSEL")
    Sts_vessel_owner: List[StsVesselOwner] = Field(default_factory=list, description="STS船舶所有人列表", alias="STS_VESSEL_OWNER")
    Sts_vessel_manager: List[StsVesselManager] = Field(default_factory=list, description="STS船舶管理人列表", alias="STS_VESSEL_MANAGER")
    Sts_vessel_operator: List[StsVesselOperator] = Field(default_factory=list, description="STS船舶经营人列表", alias="STS_VESSEL_OPERATOR")
    time_charterer: List[TimeCharterer] = Field(default_factory=list, description="期租承租人列表", alias="TIME_CHARTERER")
    voyage_charterer: List[VoyageCharterer] = Field(default_factory=list, description="程租承租人列表", alias="VOYAGE_CHARTERER")
    loading_port: List[LoadingPort] = Field(default_factory=list, description="装货港列表", alias="LOADING_PORT")
    loading_port_agent: List[LoadingPortAgent] = Field(default_factory=list, description="装货港代理列表", alias="LOADING_PORT_AGENT")
    loading_terminal: List[LoadingTerminal] = Field(default_factory=list, description="装货码头列表", alias="LOADING_TERMINAL")
    loading_terminal_operator: List[LoadingTerminalOperator] = Field(default_factory=list, description="装货码头经营人列表", alias="LOADING_TERMINAL_OPERATOR")
    loading_terminal_owner: List[LoadingTerminalOwner] = Field(default_factory=list, description="装货码头所有人列表", alias="LOADING_TERMINAL_OWNER")
    shipper: List[Shipper] = Field(default_factory=list, description="发货人列表", alias="SHIPPER")
    shipper_actual_controller: List[ShipperActualController] = Field(default_factory=list, description="发货人实际控制人列表", alias="SHIPPER_ACTUAL_CONTROLLER")
    consignee: List[Consignee] = Field(default_factory=list, description="收货人列表", alias="CONSIGNEE")
    consignee_controller: List[ConsigneeController] = Field(default_factory=list, description="收货人控制人列表", alias="CONSIGNEE_CONTROLLER")
    actual_consignee: List[ActualConsignee] = Field(default_factory=list, description="实际收货人列表", alias="ACTUAL_CONSIGNEE")
    actual_consignee_controller: List[ActualConsigneeController] = Field(default_factory=list, description="实际收货人控制人列表", alias="ACTUAL_CONSIGNEE_CONTROLLER")
    cargo_origin: List[CargoOrigin] = Field(default_factory=list, description="货物原产地列表", alias="CARGO_ORIGIN")
    discharging_port: List[DischargingPort] = Field(default_factory=list, description="卸货港列表", alias="DISCHARGING_PORT")
    discharging_port_agent: List[DischargingPortAgent] = Field(default_factory=list, description="卸货港代理列表", alias="DISCHARGING_PORT_AGENT")
    discharging_terminal: List[DischargingTerminal] = Field(default_factory=list, description="卸货码头列表", alias="DISCHARGING_TERMINAL")
    discharging_terminal_operator: List[DischargingTerminalOperator] = Field(default_factory=list, description="卸货码头经营人列表", alias="DISCHARGING_TERMINAL_OPERATOR")
    discharging_terminal_owner: List[DischargingTerminalOwner] = Field(default_factory=list, description="卸货码头所有人列表", alias="DISCHARGING_TERMINAL_OWNER")
    bunkering_ship: List[BunkeringShip] = Field(default_factory=list, description="加油船列表", alias="BUNKERING_SHIP")
    bunkering_supplier: List[BunkeringSupplier] = Field(default_factory=list, description="燃料供应商列表", alias="BUNKERING_SUPPLIER")
    bunkering_port: List[BunkeringPort] = Field(default_factory=list, description="加油港列表", alias="BUNKERING_PORT")
    bunkering_port_agent: List[BunkeringPortAgent] = Field(default_factory=list, description="加油港代理列表", alias="BUNKERING_PORT_AGENT")
    operator_id: str = Field(..., description="操作员ID", examples=["77852"], alias="OPERATOR_ID")
    operator_name: str = Field(..., description="操作员姓名", examples=["劳氏"], alias="OPERATOR_NAME")
    operator_department: str = Field(..., description="操作员部门", examples=["2025-08-30"], alias="OPERATOR_DEPARTMENT")
    operator_time: str = Field(..., description="操作时间", examples=["2025-08-30 11：43：09"], alias="OPERATOR_TIME")


# ============ 航次接口响应模型 ============
class SanctionsInfo(CaseInsensitiveBaseModel):
    """制裁信息模型"""
    sanctions_list: dict = Field(..., description="制裁列表（JSON格式）")
    mid_sanctions_list: dict = Field(..., description="中等制裁列表（JSON格式）")
    no_sanctions_list: dict = Field(..., description="无制裁列表（JSON格式）")


class EntityRiskInfo(CaseInsensitiveBaseModel):
    """通用实体风险信息模型"""
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class VesselInfo(CaseInsensitiveBaseModel):
    imo: str = Field(..., description="船舶IMO号")
    ship_name: str = Field(..., description="船舶名称")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")


class StsVesselOwnerResponse(CaseInsensitiveBaseModel):
    Sts_vessel_owner: str = Field(..., description="STS船舶所有人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class StsVesselManagerResponse(CaseInsensitiveBaseModel):
    Sts_vessel_manager: str = Field(..., description="STS船舶管理人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class StsVesselOperatorResponse(CaseInsensitiveBaseModel):
    Sts_vessel_operator: str = Field(..., description="STS船舶经营人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class StsVesselResponse(CaseInsensitiveBaseModel):
    sts_vessel_imo: str = Field(..., description="STS船舶IMO号")
    Sts_vessel_name: str = Field(..., description="STS船舶名称", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class StsWaterAreaResponse(CaseInsensitiveBaseModel):
    Sts_water_area: str = Field(..., description="STS水域", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class TimeChartererResponse(CaseInsensitiveBaseModel):
    time_charterer: str = Field(..., description="期租承租人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class VoyageChartererResponse(CaseInsensitiveBaseModel):
    voyage_charterer: str = Field(..., description="程租承租人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class LoadingPortResponse(CaseInsensitiveBaseModel):
    loading_port: str = Field(..., description="装货港", alias="name")
    loading_port_country: str = Field(..., description="装货港所在国家")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class LoadingPortAgentResponse(CaseInsensitiveBaseModel):
    loading_port_agent: str = Field(..., description="装货港代理", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class LoadingTerminalResponse(CaseInsensitiveBaseModel):
    loading_terminal: str = Field(..., description="装货码头", alias="name")
    loading_terminal_country: str = Field(..., description="装货码头所在国家")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class LoadingTerminalOperatorResponse(CaseInsensitiveBaseModel):
    loading_terminal_operator: str = Field(..., description="装货码头经营人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class LoadingTerminalOwnerResponse(CaseInsensitiveBaseModel):
    loading_terminal_owner: str = Field(..., description="装货码头所有人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ShipperResponse(CaseInsensitiveBaseModel):
    shipper: str = Field(..., description="发货人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ShipperControllerResponse(CaseInsensitiveBaseModel):
    shipper_actual_controller: str = Field(..., description="发货人实际控制人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ConsigneeResponse(CaseInsensitiveBaseModel):
    consignee: str = Field(..., description="收货人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ConsigneeControllerResponse(CaseInsensitiveBaseModel):
    consignee_controller: str = Field(..., description="收货人控制人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ActualConsigneeResponse(CaseInsensitiveBaseModel):
    actual_consignee: str = Field(..., description="实际收货人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class ActualConsigneeControllerResponse(CaseInsensitiveBaseModel):
    actual_consignee_controller: str = Field(..., description="实际收货人控制人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class CargoOriginResponse(CaseInsensitiveBaseModel):
    cargo_origin: str = Field(..., description="货物原产地", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class DischargingPortResponse(CaseInsensitiveBaseModel):
    discharging_port: str = Field(..., description="卸货港", alias="name")
    discharging_port_country: str = Field(..., description="卸货港所在国家")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class DischargingPortAgentResponse(CaseInsensitiveBaseModel):
    discharging_port_agent: str = Field(..., description="卸货港代理", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class DischargingTerminalResponse(CaseInsensitiveBaseModel):
    discharging_terminal: str = Field(..., description="卸货码头", alias="name")
    discharging_terminal_country: str = Field(..., description="卸货码头所在国家")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class DischargingTerminalOperatorResponse(CaseInsensitiveBaseModel):
    discharging_terminal_operator: str = Field(..., description="卸货码头经营人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class DischargingTerminalOwnerResponse(CaseInsensitiveBaseModel):
    discharging_terminal_owner: str = Field(..., description="卸货码头所有人", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class BunkeringShipResponse(CaseInsensitiveBaseModel):
    bunkering_ship: str = Field(..., description="加油船IMO号", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class BunkeringSupplierResponse(CaseInsensitiveBaseModel):
    bunkering_supplier: str = Field(..., description="燃料供应商", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class BunkeringPortResponse(CaseInsensitiveBaseModel):
    bunkering_port: str = Field(..., description="加油港", alias="name")
    bunkering_port_country: str = Field(..., description="加油港所在国家")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class BunkeringPortAgentResponse(CaseInsensitiveBaseModel):
    bunkering_port_agent: str = Field(..., description="加油港代理", alias="name")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_screening_time: str = Field(..., description="风险筛查时间")
    risk_status_change_content: Optional[str] = Field("", description="风险状态变化内容")
    risk_status_change_time: Optional[str] = Field("", description="风险状态变化时间")
    risk_items: List[dict] = Field(default_factory=list, description="风险项目列表")
    risk_status_reason: List[dict] = Field(default_factory=list, description="风险状态原因列表")


class VoyageRiskResponse(CaseInsensitiveBaseModel):
    scenario: str = Field(..., description="场景")
    uuid: str = Field(..., description="唯一标识")
    voyage_number: str = Field(..., description="航次号")
    voyage_risk: str = Field(..., description="航次风险状态")
    voyage_status: str = Field(..., description="航行状态")
    Business_segment: str = Field(..., description="业务板块")
    trade_type: str = Field(..., description="贸易类型")
    Business_model: str = Field(..., description="经营方式")
    voyage_start_time: str = Field(..., description="航次开始时间")
    voyage_end_time: str = Field(..., description="航次完成时间")
    vessel_imo: str = Field(..., description="船舶IMO号")
    vessel_name: str = Field(..., description="船舶名称")
    is_sts: str = Field(..., description="是否STS")
    sts_water_area: str = Field(..., description="STS水域")
    Sts_vessel: List[dict] = Field(default_factory=list, description="STS船舶列表")
    Sts_vessel_owner: List[dict] = Field(default_factory=list, description="STS船舶所有人列表")
    Sts_vessel_manager: List[dict] = Field(default_factory=list, description="STS船舶管理人列表")
    Sts_vessel_operator: List[dict] = Field(default_factory=list, description="STS船舶经营人列表")
    time_charterer: List[dict] = Field(default_factory=list, description="期租承租人列表")
    voyage_charterer: List[dict] = Field(default_factory=list, description="程租承租人列表")
    loading_port: List[dict] = Field(default_factory=list, description="装货港列表")
    loading_port_agent: List[dict] = Field(default_factory=list, description="装货港代理列表")
    loading_terminal: List[dict] = Field(default_factory=list, description="装货码头列表")
    loading_terminal_operator: List[dict] = Field(default_factory=list, description="装货码头经营人列表")
    loading_terminal_owner: List[dict] = Field(default_factory=list, description="装货码头所有人列表")
    shipper: List[dict] = Field(default_factory=list, description="发货人列表")
    shipper_actual_controller: List[dict] = Field(default_factory=list, description="发货人实际控制人列表")
    consignee: List[dict] = Field(default_factory=list, description="收货人列表")
    consignee_controller: List[dict] = Field(default_factory=list, description="收货人控制人列表")
    actual_consignee: List[dict] = Field(default_factory=list, description="实际收货人列表")
    actual_consignee_controller: List[dict] = Field(default_factory=list, description="实际收货人控制人列表")
    cargo_origin: List[dict] = Field(default_factory=list, description="货物原产地列表")
    discharging_port: List[dict] = Field(default_factory=list, description="卸货港列表")
    discharging_port_agent: List[dict] = Field(default_factory=list, description="卸货港代理列表")
    discharging_terminal: List[dict] = Field(default_factory=list, description="卸货码头列表")
    discharging_terminal_operator: List[dict] = Field(default_factory=list, description="卸货码头经营人列表")
    discharging_terminal_owner: List[dict] = Field(default_factory=list, description="卸货码头所有人列表")
    bunkering_ship: List[dict] = Field(default_factory=list, description="加油船列表")
    bunkering_supplier: List[dict] = Field(default_factory=list, description="燃料供应商列表")
    bunkering_port: List[dict] = Field(default_factory=list, description="加油港列表")
    bunkering_port_agent: List[dict] = Field(default_factory=list, description="加油港代理列表")
    # 新增的5个风险状态字段
    sts_risk_status: str = Field(..., description="STS风控状态：拦截/关注/正常")
    customer_risk: str = Field(..., description="客商风险：高/中/无")
    shipper_to_consignee: str = Field(..., description="收发货人：高/中/无")
    cargo_risk_status: str = Field(..., description="货物风险：高/中/无")
    port_risk_status: str = Field(..., description="港口码头风险：高/中/无")
    operator_id: str = Field(..., description="操作员ID")
    operator_name: str = Field(..., description="操作员姓名")
    operator_department: str = Field(..., description="操作员部门")
    operator_time: str = Field(..., description="操作时间")


@external_router.post("/voyage_risk", response_model=VoyageRiskResponse, summary="航次风险筛查（POST）")
async def external_voyage_risk(req: VoyageRiskRequest) -> VoyageRiskResponse:
    """根据航次接口文档定义返回航次风险筛查结果"""
    print(f"=== API调用开始 ===")
    print(f"请求数据: {req.model_dump()}")
    
    current_time = get_current_time()
    print(f"当前时间: {current_time}")
    
    # 收集所有实体的风险状态用于计算航次风险
    all_risk_statuses = []
    
    # 船舶风险信息 - 不需要调用风险检查函数
    print("船舶风险信息 - 跳过风险检查（将调用STS接口）")
    
    print("开始处理STS船舶...")
    
    # 处理STS船舶相关实体的风险信息 - 分别处理每个字段
    sts_vessel_owner_responses = []
    sts_vessel_manager_responses = []
    sts_vessel_operator_responses = []
    sts_vessel_responses = []
    
    try:
        # STS船舶所有人 - 检查空入参
        if not req.Sts_vessel_owner:
            sts_vessel_owner_responses = []
        else:
            for owner in req.Sts_vessel_owner:
                owner_risk_data = query_sanctions_risk(owner.Sts_vessel_owner)
                owner_risk = owner_risk_data['risk_level']
                all_risk_statuses.append(owner_risk)
                # 直接使用原始返回
                raw = _query_dowjones_raw(owner.Sts_vessel_owner)
                owner_risk = raw.get('risk_screening_status', owner_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'Sts_vessel_owner', owner.Sts_vessel_owner)
                change_time = _determine_risk_change_time(owner_risk, previous_data, current_time)
                
                entry = {'name': owner.Sts_vessel_owner}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                sts_vessel_owner_responses.append(entry)
        
        # STS船舶管理人 - 检查空入参
        if not req.Sts_vessel_manager:
            sts_vessel_manager_responses = []
        else:
            for manager in req.Sts_vessel_manager:
                manager_risk_data = query_sanctions_risk(manager.Sts_vessel_manager)
                manager_risk = manager_risk_data['risk_level']
                all_risk_statuses.append(manager_risk)
                raw = _query_dowjones_raw(manager.Sts_vessel_manager)
                manager_risk = raw.get('risk_screening_status', manager_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'Sts_vessel_manager', manager.Sts_vessel_manager)
                change_time = _determine_risk_change_time(manager_risk, previous_data, current_time)
                
                entry = {'name': manager.Sts_vessel_manager}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                sts_vessel_manager_responses.append(entry)
        
        # STS船舶经营人 - 检查空入参
        if not req.Sts_vessel_operator:
            sts_vessel_operator_responses = []
        else:
            for operator in req.Sts_vessel_operator:
                operator_risk_data = query_sanctions_risk(operator.Sts_vessel_operator)
                operator_risk = operator_risk_data['risk_level']
                all_risk_statuses.append(operator_risk)
                raw = _query_dowjones_raw(operator.Sts_vessel_operator)
                operator_risk = raw.get('risk_screening_status', operator_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'Sts_vessel_operator', operator.Sts_vessel_operator)
                change_time = _determine_risk_change_time(operator_risk, previous_data, current_time)
                
                entry = {'name': operator.Sts_vessel_operator}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                sts_vessel_operator_responses.append(entry)
        
        # STS船舶：对每个 IMO 调用 sts_bunkering_risk，并将 Project_risk_status 映射为本项 risk_screening_status
        import asyncio
        async def _call_one_sts(imo: str, name: str):
            try:
                d = await run_sts_risk_by_imo(imo)
                proj_status = d.get('Project_risk_status', '正常')
                mapped = {'拦截': '高风险', '关注': '中风险'}.get(proj_status, '无风险')
                all_risk_statuses.append(mapped)
                return {
                    'sts_vessel_imo': imo,
                    'name': name or d.get('Vessel_name', f'STS-{imo}'),
                    'risk_screening_status': mapped,
                    'risk_screening_time': d.get('Process_end_time') or d.get('Process_start_time') or current_time,
                    'risk_status_change_content': '',
                    'risk_status_change_time': '',
                    'risk_type_number': 0,
                    'risk_description': 'STS项目风控映射',
                    'risk_info': d,
                    'risk_status_reason': {}
                }
            except Exception as e:
                print(f"调用STS风控失败 IMO={imo}: {e}")
                return {
                    'sts_vessel_imo': imo,
                    'name': name or f'STS-{imo}',
                    'risk_screening_status': '无风险',
                    'risk_screening_time': current_time,
                    'risk_status_change_content': '',
                    'risk_status_change_time': '',
                    'risk_type_number': 0,
                    'risk_description': 'STS项目风控调用失败',
                    'risk_info': {},
                    'risk_status_reason': {}
                }

        if req.Sts_vessel:
            tasks_sts = [_call_one_sts(sts.sts_vessel_imo, sts.Sts_vessel_name) for sts in req.Sts_vessel]
            sts_vessel_responses = await asyncio.gather(*tasks_sts)
        print(f"STS船舶处理完成，共处理 {len(sts_vessel_responses)} 个")
    except Exception as e:
        print(f"处理STS船舶时出错: {e}")
        sts_vessel_owner_responses = []
        sts_vessel_manager_responses = []
        sts_vessel_operator_responses = []
        sts_vessel_responses = []
    
    # 查询期租承租人风险信息 - 检查空入参
    time_charterer_responses = []
    try:
        if not req.time_charterer:
            time_charterer_responses = []
        else:
            for charterer in req.time_charterer:
                charterer_risk_data = query_sanctions_risk(charterer.time_charterer)
                charterer_risk = charterer_risk_data['risk_level']
                all_risk_statuses.append(charterer_risk)
                raw = _query_dowjones_raw(charterer.time_charterer)
                charterer_risk = raw.get('risk_screening_status', charterer_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'time_charterer', charterer.time_charterer)
                change_time = _determine_risk_change_time(charterer_risk, previous_data, current_time)
                
                entry = {'name': charterer.time_charterer}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                time_charterer_responses.append(entry)
    except Exception as e:
        print(f"处理期租承租人时出错: {e}")
        time_charterer_responses = []
    
    # 查询程租承租人风险信息 - 检查空入参
    voyage_charterer_responses = []
    try:
        if not req.voyage_charterer:
            voyage_charterer_responses = []
        else:
            for charterer in req.voyage_charterer:
                charterer_risk_data = query_sanctions_risk(charterer.voyage_charterer)
                charterer_risk = charterer_risk_data['risk_level']
                all_risk_statuses.append(charterer_risk)
                raw = _query_dowjones_raw(charterer.voyage_charterer)
                charterer_risk = raw.get('risk_screening_status', charterer_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'voyage_charterer', charterer.voyage_charterer)
                change_time = _determine_risk_change_time(charterer_risk, previous_data, current_time)
                
                entry = {'name': charterer.voyage_charterer}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                voyage_charterer_responses.append(entry)
    except Exception as e:
        print(f"处理程租承租人时出错: {e}")
        voyage_charterer_responses = []
    
    # 查询装货港相关风险 - 检查空入参
    loading_port_responses = []
    try:
        if not req.loading_port:
            loading_port_responses = []
        else:
            for port in req.loading_port:
                raw = _query_port_country_raw(port.loading_port_country)
                all_risk_statuses.append(raw.get('risk_screening_status', '无风险'))
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'loading_port', port.loading_port)
                change_time = _determine_risk_change_time(raw.get('risk_screening_status', '无风险'), previous_data, current_time)
                
                entry = {
                    'name': port.loading_port,
                    'loading_port_country': port.loading_port_country
                }
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                loading_port_responses.append(entry)
        print(f"装货港处理完成，共处理 {len(loading_port_responses)} 个")
    except Exception as e:
        print(f"处理装货港时出错: {e}")
        loading_port_responses = []
    
    # 查询装货港代理相关风险 - 检查空入参
    loading_port_agent_responses = []
    try:
        if not req.loading_port_agent:
            loading_port_agent_responses = []
        else:
            for agent in req.loading_port_agent:
                agent_risk_data = query_sanctions_risk(agent.loading_port_agent)
                agent_risk = agent_risk_data['risk_level']
                all_risk_statuses.append(agent_risk)
                raw = _query_dowjones_raw(agent.loading_port_agent)
                agent_risk = raw.get('risk_screening_status', agent_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'loading_port_agent', agent.loading_port_agent)
                change_time = _determine_risk_change_time(agent_risk, previous_data, current_time)
                
                entry = {'name': agent.loading_port_agent}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                loading_port_agent_responses.append(entry)
        print(f"装货港代理处理完成，共处理 {len(loading_port_agent_responses)} 个")
    except Exception as e:
        print(f"处理装货港代理时出错: {e}")
        loading_port_agent_responses = []
    
    # 查询装货码头相关风险 - 分别处理每个字段
    loading_terminal_responses = []
    loading_terminal_operator_responses = []
    loading_terminal_owner_responses = []
    
    try:
        # 处理装货码头：不查国家，直接回传入参 - 检查空入参
        if not req.loading_terminal:
            loading_terminal_responses = []
        else:
            for terminal in req.loading_terminal:
                entry = {
                    'loading_terminal': terminal.loading_terminal,
                    'loading_terminal_country': getattr(terminal, 'loading_terminal_country', '')
                }
                loading_terminal_responses.append(entry)
        
        # 处理装货码头经营人 - 检查空入参
        if not req.loading_terminal_operator:
            loading_terminal_operator_responses = []
        else:
            for operator in req.loading_terminal_operator:
                operator_risk_data = query_sanctions_risk(operator.loading_terminal_operator)
                operator_risk = operator_risk_data['risk_level']
                all_risk_statuses.append(operator_risk)
                raw = _query_dowjones_raw(operator.loading_terminal_operator)
                operator_risk = raw.get('risk_screening_status', operator_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'loading_terminal_operator', operator.loading_terminal_operator)
                change_time = _determine_risk_change_time(operator_risk, previous_data, current_time)
                
                entry = {'name': operator.loading_terminal_operator}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                loading_terminal_operator_responses.append(entry)
        
        # 处理装货码头所有人 - 检查空入参
        if not req.loading_terminal_owner:
            loading_terminal_owner_responses = []
        else:
            for owner in req.loading_terminal_owner:
                owner_risk_data = query_sanctions_risk(owner.loading_terminal_owner)
                owner_risk = owner_risk_data['risk_level']
                all_risk_statuses.append(owner_risk)
                raw = _query_dowjones_raw(owner.loading_terminal_owner)
                owner_risk = raw.get('risk_screening_status', owner_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'loading_terminal_owner', owner.loading_terminal_owner)
                change_time = _determine_risk_change_time(owner_risk, previous_data, current_time)
                
                entry = {'name': owner.loading_terminal_owner}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                loading_terminal_owner_responses.append(entry)
        print(f"装货码头处理完成，共处理 {len(loading_terminal_responses)} 个")
    except Exception as e:
        print(f"处理装货码头时出错: {e}")
        loading_terminal_responses = []
        loading_terminal_operator_responses = []
        loading_terminal_owner_responses = []
    
    # 查询发货人相关风险 - 分别处理每个字段
    shipper_responses = []
    shipper_controller_responses = []
    
    try:
        # 处理发货人 - 检查空入参
        if not req.shipper:
            shipper_responses = []
        else:
            for shipper in req.shipper:
                # 检查实体名称是否为空
                if not shipper.shipper or shipper.shipper.strip() == "":
                    continue
                    
                shipper_risk_data = query_sanctions_risk(shipper.shipper)
                shipper_risk = shipper_risk_data['risk_level']
                all_risk_statuses.append(shipper_risk)
                raw = _query_dowjones_raw(shipper.shipper)
                shipper_risk = raw.get('risk_screening_status', shipper_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'shipper', shipper.shipper)
                change_time = _determine_risk_change_time(shipper_risk, previous_data, current_time)
                
                entry = {'name': shipper.shipper}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                shipper_responses.append(entry)
        
        # 处理发货人实际控制人 - 检查空入参
        if not req.shipper_actual_controller:
            shipper_controller_responses = []
        else:
            for controller in req.shipper_actual_controller:
                # 检查实体名称是否为空
                if not controller.shipper_actual_controller or controller.shipper_actual_controller.strip() == "":
                    continue
                    
                controller_risk_data = query_sanctions_risk(controller.shipper_actual_controller)
                controller_risk = controller_risk_data['risk_level']
                all_risk_statuses.append(controller_risk)
                raw = _query_dowjones_raw(controller.shipper_actual_controller)
                controller_risk = raw.get('risk_screening_status', controller_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'shipper_actual_controller', controller.shipper_actual_controller)
                change_time = _determine_risk_change_time(controller_risk, previous_data, current_time)
                
                entry = {'name': controller.shipper_actual_controller}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                shipper_controller_responses.append(entry)
        print(f"发货人处理完成，共处理 {len(shipper_responses)} 个")
    except Exception as e:
        print(f"处理发货人时出错: {e}")
        shipper_responses = []
        shipper_controller_responses = []
    
    # 查询收货人相关风险 - 分别处理每个字段
    consignee_responses = []
    consignee_controller_responses = []
    actual_consignee_responses = []
    actual_consignee_controller_responses = []
    
    try:
        # 处理收货人 - 检查空入参
        if not req.consignee:
            consignee_responses = []
        else:
            for consignee in req.consignee:
                consignee_risk_data = query_sanctions_risk(consignee.consignee)
                consignee_risk = consignee_risk_data['risk_level']
                all_risk_statuses.append(consignee_risk)
                raw = _query_dowjones_raw(consignee.consignee)
                consignee_risk = raw.get('risk_screening_status', consignee_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'consignee', consignee.consignee)
                change_time = _determine_risk_change_time(consignee_risk, previous_data, current_time)
                
                entry = {'name': consignee.consignee}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                consignee_responses.append(entry)
        
        # 处理收货人控制人 - 检查空入参
        if not req.consignee_controller:
            consignee_controller_responses = []
        else:
            for controller in req.consignee_controller:
                controller_risk_data = query_sanctions_risk(controller.consignee_controller)
                controller_risk = controller_risk_data['risk_level']
                all_risk_statuses.append(controller_risk)
                raw = _query_dowjones_raw(controller.consignee_controller)
                controller_risk = raw.get('risk_screening_status', controller_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'consignee_controller', controller.consignee_controller)
                change_time = _determine_risk_change_time(controller_risk, previous_data, current_time)
                
                entry = {'name': controller.consignee_controller}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                consignee_controller_responses.append(entry)
        
        # 处理实际收货人 - 检查空入参
        if not req.actual_consignee:
            actual_consignee_responses = []
        else:
            for actual_consignee in req.actual_consignee:
                actual_consignee_risk_data = query_sanctions_risk(actual_consignee.actual_consignee)
                actual_consignee_risk = actual_consignee_risk_data['risk_level']
                all_risk_statuses.append(actual_consignee_risk)
                raw = _query_dowjones_raw(actual_consignee.actual_consignee)
                actual_consignee_risk = raw.get('risk_screening_status', actual_consignee_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'actual_consignee', actual_consignee.actual_consignee)
                change_time = _determine_risk_change_time(actual_consignee_risk, previous_data, current_time)
                
                entry = {'name': actual_consignee.actual_consignee}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                actual_consignee_responses.append(entry)
        
        # 处理实际收货人控制人 - 检查空入参
        if not req.actual_consignee_controller:
            actual_consignee_controller_responses = []
        else:
            for controller in req.actual_consignee_controller:
                actual_controller_risk_data = query_sanctions_risk(controller.actual_consignee_controller)
                actual_controller_risk = actual_controller_risk_data['risk_level']
                all_risk_statuses.append(actual_controller_risk)
                raw = _query_dowjones_raw(controller.actual_consignee_controller)
                actual_controller_risk = raw.get('risk_screening_status', actual_controller_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'actual_consignee_controller', controller.actual_consignee_controller)
                change_time = _determine_risk_change_time(actual_controller_risk, previous_data, current_time)
                
                entry = {'name': controller.actual_consignee_controller}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                actual_consignee_controller_responses.append(entry)
        print(f"收货人处理完成，共处理 {len(consignee_responses)} 个")
    except Exception as e:
        print(f"处理收货人时出错: {e}")
        consignee_responses = []
        consignee_controller_responses = []
        actual_consignee_responses = []
        actual_consignee_controller_responses = []
    
    # 查询货物原产地风险 - 检查空入参
    cargo_origin_responses = []
    try:
        if not req.cargo_origin:
            cargo_origin_responses = []
        else:
            for origin in req.cargo_origin:
                raw = _query_cargo_country_raw(origin.cargo_origin)
                all_risk_statuses.append(raw.get('risk_screening_status', '无风险'))
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'cargo_origin', origin.cargo_origin)
                change_time = _determine_risk_change_time(raw.get('risk_screening_status', '无风险'), previous_data, current_time)
                
                entry = { 'name': origin.cargo_origin }
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                cargo_origin_responses.append(entry)
        print(f"货物原产地处理完成，共处理 {len(cargo_origin_responses)} 个")
    except Exception as e:
        print(f"处理货物原产地时出错: {e}")
        cargo_origin_responses = []
    
    # 查询卸货港相关风险
    discharging_port_responses = []
    discharging_port_agent_responses = []
    discharging_terminal_responses = []
    discharging_terminal_operator_responses = []
    discharging_terminal_owner_responses = []
    
    try:
        # 处理卸货港 - 检查空入参
        if not req.discharging_port:
            discharging_port_responses = []
        else:
            for port in req.discharging_port:
                raw = _query_port_country_raw(port.discharging_port_country)
                all_risk_statuses.append(raw.get('risk_screening_status', '无风险'))
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'discharging_port', port.discharging_port)
                change_time = _determine_risk_change_time(raw.get('risk_screening_status', '无风险'), previous_data, current_time)
                
                entry = { 'name': port.discharging_port, 'discharging_port_country': port.discharging_port_country }
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                discharging_port_responses.append(entry)
        
        # 处理卸货港代理 - 检查空入参
        if not req.discharging_port_agent:
            discharging_port_agent_responses = []
        else:
            for port_agent in req.discharging_port_agent:
                port_agent_risk_data = query_sanctions_risk(port_agent.discharging_port_agent)
                port_agent_risk = port_agent_risk_data['risk_level']
                all_risk_statuses.append(port_agent_risk)
                raw = _query_dowjones_raw(port_agent.discharging_port_agent)
                port_agent_risk = raw.get('risk_screening_status', port_agent_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'discharging_port_agent', port_agent.discharging_port_agent)
                change_time = _determine_risk_change_time(port_agent_risk, previous_data, current_time)
                
                entry = {'name': port_agent.discharging_port_agent}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                discharging_port_agent_responses.append(entry)
        
        # 处理卸货码头：不查国家，直接回传入参 - 检查空入参
        if not req.discharging_terminal:
            discharging_terminal_responses = []
        else:
            for terminal in req.discharging_terminal:
                entry = {
                    'discharging_terminal': terminal.discharging_terminal,
                    'discharging_terminal_country': getattr(terminal, 'discharging_terminal_country', '')
                }
                discharging_terminal_responses.append(entry)
        
        # 处理卸货码头经营人 - 检查空入参
        if not req.discharging_terminal_operator:
            discharging_terminal_operator_responses = []
        else:
            for operator in req.discharging_terminal_operator:
                operator_risk_data = query_sanctions_risk(operator.discharging_terminal_operator)
                operator_risk = operator_risk_data['risk_level']
                all_risk_statuses.append(operator_risk)
                raw = _query_dowjones_raw(operator.discharging_terminal_operator)
                operator_risk = raw.get('risk_screening_status', operator_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'discharging_terminal_operator', operator.discharging_terminal_operator)
                change_time = _determine_risk_change_time(operator_risk, previous_data, current_time)
                
                entry = {'name': operator.discharging_terminal_operator}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                discharging_terminal_operator_responses.append(entry)
        
        # 处理卸货码头所有人 - 检查空入参
        if not req.discharging_terminal_owner:
            discharging_terminal_owner_responses = []
        else:
            for owner in req.discharging_terminal_owner:
                owner_risk_data = query_sanctions_risk(owner.discharging_terminal_owner)
                owner_risk = owner_risk_data['risk_level']
                all_risk_statuses.append(owner_risk)
                raw = _query_dowjones_raw(owner.discharging_terminal_owner)
                owner_risk = raw.get('risk_screening_status', owner_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'discharging_terminal_owner', owner.discharging_terminal_owner)
                change_time = _determine_risk_change_time(owner_risk, previous_data, current_time)
                
                entry = {'name': owner.discharging_terminal_owner}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                discharging_terminal_owner_responses.append(entry)
        print(f"卸货港处理完成，共处理 {len(discharging_port_responses)} 个")
    except Exception as e:
        print(f"处理卸货港时出错: {e}")
        discharging_port_responses = []
        discharging_port_agent_responses = []
        discharging_terminal_responses = []
        discharging_terminal_operator_responses = []
        discharging_terminal_owner_responses = []
    
    # 查询加油相关风险 - 分别处理每个字段
    bunkering_ship_responses = []
    bunkering_supplier_responses = []
    bunkering_port_responses = []
    bunkering_port_agent_responses = []
    
    try:
        # 处理加油船（IMO号）- 调 sts_bunkering_risk 风控，并将 Project_risk_status 映射到本接口的 sts_risk_status
        import asyncio
        async def _call_one(imo: str):
            try:
                d = await run_sts_risk_by_imo(imo)
                proj_status = d.get('Project_risk_status', '正常')
                # 将项目风控状态计入总风险参考
                mapped = {'拦截': '高风险', '关注': '中风险'}.get(proj_status, '无风险')
                all_risk_statuses.append(mapped)
                # 作为 Sts_vessel 的一个元素返回
                return {
                    'sts_vessel_imo': imo,
                    'name': d.get('Vessel_name', f'STS-{imo}'),
                    'risk_screening_status': mapped,
                    'risk_screening_time': d.get('Process_end_time') or d.get('Process_start_time') or '',
                    'risk_status_change_content': '',
                    'risk_status_change_time': '',
                    'risk_type_number': 0,
                    'risk_description': 'STS项目风控映射',
                    'risk_info': d,
                    'risk_status_reason': {},
                    # 将 Project_risk_status 映射到外层的 sts_risk_status 由后续汇总使用
                }
            except Exception as e:
                print(f"调用STS风控失败 IMO={imo}: {e}")
                return {
                    'sts_vessel_imo': imo,
                    'name': f'STS-{imo}',
                    'risk_screening_status': '无风险',
                    'risk_screening_time': '',
                    'risk_status_change_content': '',
                    'risk_status_change_time': '',
                    'risk_type_number': 0,
                    'risk_description': 'STS项目风控调用失败',
                    'risk_info': {},
                    'risk_status_reason': {}
                }

        tasks = [_call_one(ship.bunkering_ship) for ship in req.bunkering_ship]
        if tasks:
            bunkering_ship_responses = await asyncio.gather(*tasks)
        
        # 处理燃料供应商 - 检查空入参
        if not req.bunkering_supplier:
            bunkering_supplier_responses = []
        else:
            for supplier in req.bunkering_supplier:
                supplier_risk_data = query_sanctions_risk(supplier.bunkering_supplier)
                supplier_risk = supplier_risk_data['risk_level']
                all_risk_statuses.append(supplier_risk)
                raw = _query_dowjones_raw(supplier.bunkering_supplier)
                supplier_risk = raw.get('risk_screening_status', supplier_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'bunkering_supplier', supplier.bunkering_supplier)
                change_time = _determine_risk_change_time(supplier_risk, previous_data, current_time)
                
                entry = {'name': supplier.bunkering_supplier}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                bunkering_supplier_responses.append(entry)
        
        # 处理加油港 - 检查空入参
        if not req.bunkering_port:
            bunkering_port_responses = []
        else:
            for port in req.bunkering_port:
                raw = _query_port_country_raw(port.bunkering_port_country)
                all_risk_statuses.append(raw.get('risk_screening_status', '无风险'))
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'bunkering_port', port.bunkering_port)
                change_time = _determine_risk_change_time(raw.get('risk_screening_status', '无风险'), previous_data, current_time)
                
                entry = { 'name': port.bunkering_port, 'bunkering_port_country': port.bunkering_port_country }
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                bunkering_port_responses.append(entry)
        
        # 处理加油港代理 - 检查空入参
        if not req.bunkering_port_agent:
            bunkering_port_agent_responses = []
        else:
            for port_agent in req.bunkering_port_agent:
                port_agent_risk_data = query_sanctions_risk(port_agent.bunkering_port_agent)
                port_agent_risk = port_agent_risk_data['risk_level']
                all_risk_statuses.append(port_agent_risk)
                raw = _query_dowjones_raw(port_agent.bunkering_port_agent)
                port_agent_risk = raw.get('risk_screening_status', port_agent_risk)
                
                # 获取之前的风险数据并确定变更时间
                previous_data = _get_previous_risk_data(req.uuid, 'bunkering_port_agent', port_agent.bunkering_port_agent)
                change_time = _determine_risk_change_time(port_agent_risk, previous_data, current_time)
                
                entry = {'name': port_agent.bunkering_port_agent}
                entry.update(raw)
                entry['risk_status_change_time'] = change_time
                bunkering_port_agent_responses.append(entry)
        print(f"加油相关处理完成，共处理 {len(bunkering_ship_responses)} 个")
    except Exception as e:
        print(f"处理加油相关时出错: {e}")
        bunkering_ship_responses = []
        bunkering_supplier_responses = []
        bunkering_port_responses = []
        bunkering_port_agent_responses = []
    
    # 获取STS水域风险信息（取第一个STS船舶的水域）
    sts_water_area = req.Sts_vessel[0].Sts_water_area if req.Sts_vessel else "Singapore"
    # 水域不需要处理风险，直接使用入参值
    
    # 计算航次风险等级
    voyage_risk_level = calculate_voyage_risk(all_risk_statuses)
    
    # 计算新的5个风险状态字段（从已有结果中获取，不重复查询数据库）
    # 1. STS风控状态
    sts_risk_statuses = []
    for sts_response in sts_vessel_owner_responses:
        sts_risk_statuses.append(sts_response.get('risk_screening_status', '无风险'))
    for sts_response in sts_vessel_manager_responses:
        sts_risk_statuses.append(sts_response.get('risk_screening_status', '无风险'))
    for sts_response in sts_vessel_operator_responses:
        sts_risk_statuses.append(sts_response.get('risk_screening_status', '无风险'))
    sts_risk_status = calculate_sts_risk_level(sts_risk_statuses)
    
    # 2. 客商风险
    customer_risk_statuses = []
    for charterer_response in time_charterer_responses:
        customer_risk_statuses.append(charterer_response.get('risk_screening_status', '无风险'))
    for charterer_response in voyage_charterer_responses:
        customer_risk_statuses.append(charterer_response.get('risk_screening_status', '无风险'))
    # 从装货港代理响应中获取
    for agent_response in loading_port_agent_responses:
        customer_risk_statuses.append(agent_response.get('risk_screening_status', '无风险'))
    # 从加油相关响应中获取
    for supplier_response in bunkering_supplier_responses:
        customer_risk_statuses.append(supplier_response.get('risk_screening_status', '无风险'))
    for agent_response in bunkering_port_agent_responses:
        customer_risk_statuses.append(agent_response.get('risk_screening_status', '无风险'))
    # 从卸货港代理响应中获取
    for agent_response in discharging_port_agent_responses:
        customer_risk_statuses.append(agent_response.get('risk_screening_status', '无风险'))
    customer_risk = calculate_risk_level(customer_risk_statuses)
    
    # 3. 收发货人
    shipper_consignee_risk_statuses = []
    for shipper_response in shipper_responses:
        shipper_consignee_risk_statuses.append(shipper_response.get('risk_screening_status', '无风险'))
    for controller_response in shipper_controller_responses:
        shipper_consignee_risk_statuses.append(controller_response.get('risk_screening_status', '无风险'))
    for consignee_response in consignee_responses:
        shipper_consignee_risk_statuses.append(consignee_response.get('risk_screening_status', '无风险'))
    for controller_response in consignee_controller_responses:
        shipper_consignee_risk_statuses.append(controller_response.get('risk_screening_status', '无风险'))
    for actual_response in actual_consignee_responses:
        shipper_consignee_risk_statuses.append(actual_response.get('risk_screening_status', '无风险'))
    for controller_response in actual_consignee_controller_responses:
        shipper_consignee_risk_statuses.append(controller_response.get('risk_screening_status', '无风险'))
    shipper_to_consignee = calculate_risk_level(shipper_consignee_risk_statuses)
    
    # 4. 货物风险
    cargo_risk_statuses = []
    for cargo_response in cargo_origin_responses:
        cargo_risk_statuses.append(cargo_response.get('risk_screening_status', '无风险'))
    cargo_risk_status = calculate_risk_level(cargo_risk_statuses)
    
    # 5. 港口码头风险
    port_risk_statuses = []
    for port_response in loading_port_responses:
        port_risk_statuses.append(port_response.get('risk_screening_status', '无风险'))
    for terminal_response in loading_terminal_responses:
        port_risk_statuses.append(terminal_response.get('risk_screening_status', '无风险'))
    for operator_response in loading_terminal_operator_responses:
        port_risk_statuses.append(operator_response.get('risk_screening_status', '无风险'))
    for owner_response in loading_terminal_owner_responses:
        port_risk_statuses.append(owner_response.get('risk_screening_status', '无风险'))
    for port_response in discharging_port_responses:
        port_risk_statuses.append(port_response.get('risk_screening_status', '无风险'))
    for terminal_response in discharging_terminal_responses:
        port_risk_statuses.append(terminal_response.get('risk_screening_status', '无风险'))
    for operator_response in discharging_terminal_operator_responses:
        port_risk_statuses.append(operator_response.get('risk_screening_status', '无风险'))
    for owner_response in discharging_terminal_owner_responses:
        port_risk_statuses.append(owner_response.get('risk_screening_status', '无风险'))
    for port_response in bunkering_port_responses:
        port_risk_statuses.append(port_response.get('risk_screening_status', '无风险'))
    port_risk_status = calculate_risk_level(port_risk_statuses)
    
    print(f"=================")
    print(f"航次风险等级: {voyage_risk_level}")
    print(f"STS风控状态: {sts_risk_status}")
    print(f"客商风险: {customer_risk}")
    print(f"收发货人: {shipper_to_consignee}")
    print(f"货物风险: {cargo_risk_status}")
    print(f"港口码头风险: {port_risk_status}")
    print(f"=================")
    
    # 构建响应对象
    print(f"=== 调试信息 ===")
    print(f"STS船舶数量: {len(sts_vessel_responses)}")
    print(f"装货港数量: {len(loading_port_responses)}")
    print(f"装货港代理数量: {len(loading_port_agent_responses)}")
    print(f"装货码头数量: {len(loading_terminal_responses)}")
    print(f"发货人数量: {len(shipper_responses)}")
    print(f"收货人数量: {len(consignee_responses)}")
    print(f"货物原产地数量: {len(cargo_origin_responses)}")
    print(f"卸货港数量: {len(discharging_port_responses)}")
    print(f"加油相关数量: {len(bunkering_ship_responses)}")
    print(f"=================")
    
    response = VoyageRiskResponse(
        scenario=req.scenario,
        uuid=req.uuid,
        voyage_number=req.voyage_number,
        voyage_risk=voyage_risk_level,  # 使用计算出的风险等级
        voyage_status=req.voyage_status,
        Business_segment=req.Business_segment,
        trade_type=req.trade_type,
        Business_model=req.Business_model,
        voyage_start_time=req.voyage_start_time,
        voyage_end_time=req.voyage_end_time or "",
        vessel_imo=req.vessel_imo,
        vessel_name=req.vessel_name,
        is_sts=req.is_sts,
        sts_water_area=",".join([sts.Sts_water_area for sts in req.Sts_vessel]) if req.Sts_vessel else "",
        Sts_vessel=sts_vessel_responses,
        Sts_vessel_owner=sts_vessel_owner_responses,
        Sts_vessel_manager=sts_vessel_manager_responses,
        Sts_vessel_operator=sts_vessel_operator_responses,
        time_charterer=time_charterer_responses,
        voyage_charterer=voyage_charterer_responses,
        loading_port=loading_port_responses,
        loading_port_agent=loading_port_agent_responses,
        loading_terminal=loading_terminal_responses,
        loading_terminal_operator=loading_terminal_operator_responses,
        loading_terminal_owner=loading_terminal_owner_responses,
        shipper=shipper_responses,
        shipper_actual_controller=shipper_controller_responses,
        consignee=consignee_responses,
        consignee_controller=consignee_controller_responses,
        actual_consignee=actual_consignee_responses,
        actual_consignee_controller=actual_consignee_controller_responses,
        cargo_origin=cargo_origin_responses,
        discharging_port=discharging_port_responses,
        discharging_port_agent=discharging_port_agent_responses,
        discharging_terminal=discharging_terminal_responses,
        discharging_terminal_operator=discharging_terminal_operator_responses,
        discharging_terminal_owner=discharging_terminal_owner_responses,
        bunkering_ship=bunkering_ship_responses,
        bunkering_supplier=bunkering_supplier_responses,
        bunkering_port=bunkering_port_responses,
        bunkering_port_agent=bunkering_port_agent_responses,
        # 新增的5个风险状态字段
        sts_risk_status=sts_risk_status,
        customer_risk=customer_risk,
        shipper_to_consignee=shipper_to_consignee,
        cargo_risk_status=cargo_risk_status,
        port_risk_status=port_risk_status,
        operator_id=req.operator_id,
        operator_name=req.operator_name,
        operator_department=req.operator_department,
        operator_time=req.operator_time
    )
    
    # 审批对比与重算：如存在同 uuid 的审批记录，则应用并重算
    try:
        approvals = _load_all_approvals_by_uuid(req.uuid)
        if approvals:
            response_lists = {
                'Sts_vessel': response.Sts_vessel,
                'Sts_vessel_owner': response.Sts_vessel_owner,
                'Sts_vessel_manager': response.Sts_vessel_manager,
                'Sts_vessel_operator': response.Sts_vessel_operator,
                'time_charterer': response.time_charterer,
                'voyage_charterer': response.voyage_charterer,
                'loading_port': response.loading_port,
                'loading_port_agent': response.loading_port_agent,
                'loading_terminal': response.loading_terminal,
                'loading_terminal_operator': response.loading_terminal_operator,
                'loading_terminal_owner': response.loading_terminal_owner,
                'shipper': response.shipper,
                'shipper_actual_controller': response.shipper_actual_controller,
                'consignee': response.consignee,
                'consignee_controller': response.consignee_controller,
                'actual_consignee': response.actual_consignee,
                'actual_consignee_controller': response.actual_consignee_controller,
                'cargo_origin': response.cargo_origin,
                'discharging_port': response.discharging_port,
                'discharging_port_agent': response.discharging_port_agent,
                'discharging_terminal': response.discharging_terminal,
                'discharging_terminal_operator': response.discharging_terminal_operator,
                'discharging_terminal_owner': response.discharging_terminal_owner,
                'bunkering_ship': response.bunkering_ship,
                'bunkering_supplier': response.bunkering_supplier,
                'bunkering_port': response.bunkering_port,
                'bunkering_port_agent': response.bunkering_port_agent
            }

            _apply_approvals_to_lists(response_lists, approvals)

            for k, v in response_lists.items():
                setattr(response, k, v)

            all_after = _collect_statuses_from_lists(response_lists)
            response.voyage_risk = calculate_voyage_risk(all_after)

            def _collect(names: list) -> list:
                arr = []
                for name in names:
                    items = response_lists.get(name, []) or []
                    arr.extend(items)
                return arr

            customer_items = _collect(['time_charterer', 'voyage_charterer', 'loading_port_agent', 'bunkering_supplier', 'bunkering_port_agent', 'discharging_port_agent'])
            response.customer_risk = calculate_risk_level([it.get('risk_screening_status', '') for it in customer_items if isinstance(it, dict)])

            s2c_items = _collect(['shipper', 'shipper_actual_controller', 'consignee', 'consignee_controller', 'actual_consignee', 'actual_consignee_controller'])
            response.shipper_to_consignee = calculate_risk_level([it.get('risk_screening_status', '') for it in s2c_items if isinstance(it, dict)])

            cargo_items = _collect(['cargo_origin'])
            response.cargo_risk_status = calculate_risk_level([it.get('risk_screening_status', '') for it in cargo_items if isinstance(it, dict)])

            port_items = _collect(['loading_port', 'loading_terminal', 'loading_terminal_operator', 'loading_terminal_owner', 'discharging_port', 'discharging_terminal', 'discharging_terminal_operator', 'discharging_terminal_owner', 'bunkering_port'])
            response.port_risk_status = calculate_risk_level([it.get('risk_screening_status', '') for it in port_items if isinstance(it, dict)])
    except Exception as e:
        print(f"[external_api] 审批对比重算失败: {e}")

    # 将响应数据记录到数据库
    try:
        # 将Pydantic模型转换为字典（使用别名导出统一键名）
        response_dict = response.model_dump(by_alias=True)
        # 记录数据
        insert_success = insert_voyage_risk_log(response_dict)
        if insert_success:
            print(f"航次风险筛查数据已成功记录到数据库，航次号: {req.voyage_number}, uuid: {req.uuid}")
        else:
            print(f"航次风险筛查数据记录失败，航次号: {req.voyage_number}, uuid: {req.uuid}")
    except Exception as e:
        print(f"记录航次风险筛查数据时出错: {e}")
    
    return response


class VesselBasicRequest(CaseInsensitiveBaseModel):
    vessel_imo: str = Field(..., description="船舶IMO号", pattern=r"^\d{7}$", examples=["9532824"]) 


class VesselBasicData(CaseInsensitiveBaseModel):
    VesselImo: str = Field(..., description="船舶IMO号")
    VesselMmsi: Optional[str] = Field(None, description="船舶MMSI号")
    VesselName: Optional[str] = Field(None, description="船舶名称")
    VesselType: Optional[str] = Field(None, description="船舶类型")
    Flag: Optional[str] = Field(None, description="船旗国")
    timestamp: str = Field(..., description="数据生成时间")


class VesselBasicResponse(CaseInsensitiveBaseModel):
    status: str = Field(..., description="状态：success/error")
    data: VesselBasicData
    timestamp: str = Field(..., description="响应时间戳")


@external_router.post("/vessel_basic", response_model=VesselBasicResponse, summary="外部船舶基础信息查询（POST）")
async def external_vessel_basic(req: VesselBasicRequest) -> VesselBasicResponse:
    """根据文档定义返回船舶基础信息结构（占位示例）。后续将接入真实加工逻辑。"""
    now_iso = datetime.utcnow().isoformat()
    sample = VesselBasicData(
        VesselImo=req.vessel_imo,
        VesselMmsi="244909000",
        VesselName="Timber Navigator",
        VesselType="general cargo with container capacity",
        Flag="Sweden",
        timestamp=now_iso
    )
    return VesselBasicResponse(status="success", data=sample, timestamp=now_iso)

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
        ]
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
            "Gzip压缩"
        ]
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
        }
    }

# 启动服务
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(external_app, host="0.0.0.0", port=8000)
