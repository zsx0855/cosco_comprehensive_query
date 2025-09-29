from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from kingbase_config import get_kingbase_config
import unicodedata
# 导入external_api中的响应模型
from external_api import VoyageRiskResponse, calculate_voyage_risk, calculate_sts_risk_level, calculate_risk_level
from sts_bunkering_risk import run_sts_risk_by_imo
from voyage_risk_log_insert import insert_voyage_risk_log


# KingBase数据库配置
KINGBASE_CONFIG = get_kingbase_config()

external_approval_router = APIRouter(tags=["External Approval APIs"]) 


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


def query_sanctions_risk(entity_name: str) -> str:
    """
    查询实体制裁风险等级
    
    Args:
        entity_name: 实体名称
        
    Returns:
        str: 制裁风险等级，如果未找到则返回空字符串
    """
    try:
        # 建立KingBase数据库连接
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        
        with connection.cursor() as cursor:
            # 执行查询
            sql = "SELECT entity_id, ENTITYNAME1, sanctions_lev FROM lng.sanctions_risk_result WHERE ENTITYNAME1 = %s"
            cursor.execute(sql, (entity_name,))
            result = cursor.fetchone()
            
            if result:
                return result.get('sanctions_lev', '')
            else:
                return ''
                
    except Exception as e:
        print(f"查询制裁风险时出错: {e}")
        return ''
    finally:
        if 'connection' in locals():
            connection.close()


def get_current_time() -> str:
    """获取当前时间，格式为 YYYY-MM-DD hh:mm:ss"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(dt_str: str) -> datetime:
    """宽松解析时间字符串，失败返回 datetime.min"""
    if not dt_str:
        return datetime.min
    try:
        # 尝试常见格式
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def _ensure_list_field(obj: dict, field: str) -> list:
    """确保 obj[field] 为 list，若为 JSON 字符串则解析，其他情况返回 []"""
    val = obj.get(field)
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _load_all_approvals_by_uuid(uuid: str) -> List[dict]:
    """查询同一 uuid 的所有审批记录，按 approval_date 升序返回"""
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT relevant_parties_type, parties_name, risk_change_status, approval_date, change_reason
            FROM lng.approval_records_table
            WHERE uuid = %s
            ORDER BY approval_date ASC
            """
            cursor.execute(sql, (uuid,))
            rows = cursor.fetchall() or []
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"查询审批记录失败: {e}")
        return []
    finally:
        if 'connection' in locals():
            connection.close()


def _find_key_case_insensitive(obj: dict, target_key: str) -> Optional[str]:
    """在字典中大小写不敏感地查找键，返回实际键名；找不到返回 None"""
    if not obj or not target_key:
        return None
    tk = target_key.casefold()
    for k in obj.keys():
        if k.casefold() == tk:
            return k
    return None


def _normalize_risk_status(value: str) -> str:
    v = (value or '').strip()
    mapping = {
        '1': '高风险', '高': '高风险', '高风险': '高风险', 'intercept': '高风险',
        '2': '中风险', '中': '中风险', '中风险': '中风险', 'attention': '中风险',
        '0': '无风险', '无': '无风险', '无风险': '无风险', 'normal': '无风险'
    }
    return mapping.get(v.lower(), v)


def _norm_name(s: str) -> str:
    s2 = s if isinstance(s, str) else str(s or '')
    s2 = unicodedata.normalize('NFKC', s2)
    s2 = ' '.join(s2.strip().split())
    return s2.casefold()


def _get_name_value(item: dict) -> str:
    """从元素中提取名称，兼容 name/Name，返回字符串"""
    if not isinstance(item, dict):
        return ""
    return (item.get('name') or item.get('Name') or "")


def _normalize_list_of_objs(value) -> list:
    """确保为列表[dict]，若为dict则包一层，其他返回空列表"""
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _project_name_status_snapshot(log_obj: dict) -> dict:
    """提取包含 name、risk_screening_status 的快照，用于对比。字段名大小写不敏感。"""
    fields = [
        'Sts_vessel','Sts_vessel_owner','Sts_vessel_manager','Sts_vessel_operator',
        'time_charterer','voyage_charterer','loading_port','loading_port_agent',
        'loading_terminal','loading_terminal_operator','loading_terminal_owner',
        'shipper','shipper_actual_controller','consignee','consignee_controller',
        'actual_consignee','actual_consignee_controller','cargo_origin',
        'discharging_port','discharging_port_agent','discharging_terminal',
        'discharging_terminal_operator','discharging_terminal_owner',
        'bunkering_ship','bunkering_supplier','bunkering_port','bunkering_port_agent'
    ]
    snapshot = {}
    for f in fields:
        actual = _find_key_case_insensitive(log_obj, f)
        if not actual:
            continue
        items = _normalize_list_of_objs(log_obj.get(actual))
        proj = []
        for it in items:
            name = _get_name_value(it).casefold()
            status = (it.get('risk_screening_status') or '').strip()
            if name:
                proj.append((name, status))
        # 排序，保证可比较
        proj.sort()
        snapshot[actual] = proj
    return snapshot


def _fetch_latest_change_log(uuid: str) -> dict:
    """按 uuid 查询 voyage_risk_log_change 最新一条 full_response，返回 dict，失败返回{}"""
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT full_response
            FROM lng.voyage_risk_log_change
            WHERE uuid = %s
            ORDER BY request_time DESC
            LIMIT 1
            """
            cursor.execute(sql, (uuid,))
            row = cursor.fetchone()
            if not row:
                return {}
            data = row.get('full_response')
            if isinstance(data, dict):
                return data
            try:
                return json.loads(data) if data else {}
            except Exception:
                return {}
    except Exception as e:
        print(f"查询 voyage_risk_log_change 最新记录失败: {e}")
        return {}
    finally:
        if 'connection' in locals():
            connection.close()


def _insert_voyage_risk_log_change(response_data: dict, request_time: datetime = None) -> bool:
    """将数据插入到 voyage_risk_log_change（结构与 voyage_risk_log 相同）"""
    try:
        if request_time is None:
            request_time = datetime.now()
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO lng.voyage_risk_log_change (
                request_time, response_time, scenario, voyage_id, voyage_risk,
                voyage_status, Business_segment, trade_type, Business_model, voyage_start_time, voyage_end_time,
                vessel_imo, vessel_name, is_sts, sts_water_area,
                sts_vessel, sts_vessel_owner,
                sts_vessel_manager, sts_vessel_operator, time_charterer,
                voyage_charterer, loading_port, loading_port_agent,
                loading_terminal, loading_terminal_operator, loading_terminal_owner,
                shipper, shipper_actual_controller, consignee, consignee_controller,
                actual_consignee, actual_consignee_controller, cargo_origin,
                discharging_port, discharging_port_agent, discharging_terminal,
                discharging_terminal_operator, discharging_terminal_owner,
                bunkering_ship, bunkering_supplier, bunkering_port, bunkering_port_agent,
                operator_id, operator_name, operator_department, operator_time,
                full_response, uuid
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            data = (
                request_time,
                datetime.now(),
                response_data.get('scenario', ''),
                response_data.get('voyage_number', ''),
                response_data.get('voyage_risk', ''),
                response_data.get('voyage_status', ''),
                response_data.get('Business_segment', ''),
                response_data.get('trade_type', ''),
                response_data.get('Business_model', ''),
                response_data.get('voyage_start_time', ''),
                response_data.get('voyage_end_time', ''),
                response_data.get('vessel_imo', ''),
                response_data.get('vessel_name', ''),
                response_data.get('is_sts', ''),
                response_data.get('sts_water_area', ''),
                json.dumps(response_data.get('Sts_vessel', []), ensure_ascii=False),
                json.dumps(response_data.get('Sts_vessel_owner', []), ensure_ascii=False),
                json.dumps(response_data.get('Sts_vessel_manager', []), ensure_ascii=False),
                json.dumps(response_data.get('Sts_vessel_operator', []), ensure_ascii=False),
                json.dumps(response_data.get('time_charterer', {}), ensure_ascii=False),
                json.dumps(response_data.get('voyage_charterer', {}), ensure_ascii=False),
                json.dumps(response_data.get('loading_port', []), ensure_ascii=False),
                json.dumps(response_data.get('loading_port_agent', []), ensure_ascii=False),
                json.dumps(response_data.get('loading_terminal', []), ensure_ascii=False),
                json.dumps(response_data.get('loading_terminal_operator', []), ensure_ascii=False),
                json.dumps(response_data.get('loading_terminal_owner', []), ensure_ascii=False),
                json.dumps(response_data.get('shipper', []), ensure_ascii=False),
                json.dumps(response_data.get('shipper_actual_controller', []), ensure_ascii=False),
                json.dumps(response_data.get('consignee', []), ensure_ascii=False),
                json.dumps(response_data.get('consignee_controller', []), ensure_ascii=False),
                json.dumps(response_data.get('actual_consignee', []), ensure_ascii=False),
                json.dumps(response_data.get('actual_consignee_controller', []), ensure_ascii=False),
                json.dumps(response_data.get('cargo_origin', []), ensure_ascii=False),
                json.dumps(response_data.get('discharging_port', []), ensure_ascii=False),
                json.dumps(response_data.get('discharging_port_agent', []), ensure_ascii=False),
                json.dumps(response_data.get('discharging_terminal', []), ensure_ascii=False),
                json.dumps(response_data.get('discharging_terminal_operator', []), ensure_ascii=False),
                json.dumps(response_data.get('discharging_terminal_owner', []), ensure_ascii=False),
                json.dumps(response_data.get('bunkering_ship', []), ensure_ascii=False),
                json.dumps(response_data.get('bunkering_supplier', []), ensure_ascii=False),
                json.dumps(response_data.get('bunkering_port', []), ensure_ascii=False),
                json.dumps(response_data.get('bunkering_port_agent', []), ensure_ascii=False),
                response_data.get('operator_id', ''),
                response_data.get('operator_name', ''),
                response_data.get('operator_department', ''),
                response_data.get('operator_time', ''),
                json.dumps(response_data, ensure_ascii=False),
                response_data.get('uuid', '')
            )
            cursor.execute(sql, data)
            connection.commit()
            print(f"成功插入 voyage_risk_log_change，航次ID: {response_data.get('voyage_number', '')}")
            return True
    except Exception as e:
        print(f"插入 voyage_risk_log_change 出错: {e}")
        if 'connection' in locals():
            connection.rollback()
        return False
    finally:
        if 'connection' in locals():
            connection.close()


def _apply_approvals_to_log(latest_log: dict, approvals: List[dict]) -> dict:
    """将审批记录按规则应用到最新 voyage_risk_log 对象并返回更新后的对象"""
    if not latest_log:
        return {}
    log_obj = dict(latest_log)  # 浅拷贝即可，子列表我们会重新赋回

    for ap in approvals:
        field = ap.get('relevant_parties_type') or ''
        parties_name = ap.get('parties_name') or ''
        change_status = ap.get('risk_change_status') or ''
        approval_date = ap.get('approval_date') or ''
        change_reason = ap.get('change_reason') or ''

        if not field or not parties_name or not approval_date:
            continue

        # 大小写不敏感查找日志实际字段名
        actual_key = _find_key_case_insensitive(log_obj, field) or field
        items = _ensure_list_field(log_obj, actual_key)
        if not items:
            continue

        updated = False
        for item in items:
            try:
                name = _get_name_value(item)
                if _norm_name(name) != _norm_name(parties_name or ''):
                    continue
                old_change_time = _parse_dt(item.get('risk_status_change_time'))
                ap_dt = _parse_dt(approval_date)
                # 仅当旧时间点早于审批时间才覆盖
                if old_change_time < ap_dt:
                    if change_status:
                        item['risk_screening_status'] = _normalize_risk_status(change_status)
                    item['risk_status_change_time'] = approval_date
                    item['risk_status_change_content'] = change_reason or item.get('risk_status_change_content', '')
                    updated = True
            except Exception:
                continue

        if updated:
            log_obj[actual_key] = items

    # 确保以下四个字段保留且为数组（大小写不敏感），不被意外置空
    for keep_field in ['Sts_vessel', 'Sts_vessel_owner', 'Sts_vessel_manager', 'Sts_vessel_operator']:
        k_actual = _find_key_case_insensitive(latest_log, keep_field)
        if k_actual:
            log_obj[k_actual] = _ensure_list_field(latest_log, k_actual)

    # 可选：更新顶层时间戳（若表结构支持）
    if 'request_time' in log_obj:
        log_obj['request_time'] = datetime.now().isoformat()
    if 'check_time' in log_obj:
        log_obj['check_time'] = datetime.now().isoformat()

    return log_obj

def insert_approval_record(req: 'VoyageApprovalRequest') -> tuple[bool, str]:
    """
    将审批记录插入到数据库中
    
    Args:
        req: 审批请求数据
        
    Returns:
        bool: 插入是否成功
    """
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor() as cursor:
            # 将审批人列表转换为JSON格式
            approvers_json = json.dumps([
                {
                    "approver_id": approver.Approver_id,
                    "approver_name": approver.Approver_name,
                    "approver_time": approver.Approver_time
                }
                for approver in req.approvers
            ], ensure_ascii=False)
            
            # 为每个相关方插入一条记录
            for party in req.parties:
                sql = """
                INSERT INTO lng.approval_records_table (
                    uuid, voyage_number, business_segment, trade_type, business_model,
                    vessel_imo, vessel_name, relevant_parties_type, parties_id, parties_name,
                    risk_screening_status, risk_change_status, change_reason,
                    approval_status, approval_date, applicant_id, applicant_name, 
                    applicant_time, approvers
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                values = (
                    req.uuid,
                    req.voyage_number,
                    req.Business_segment,
                    req.trade_type,
                    req.Business_model,
                    req.vessel_imo,
                    req.vessel_name,
                    party.Relevant_parties_type,
                    party.Parties_id,
                    party.Parties_name,
                    party.risk_screening_status,
                    party.risk_change_status,
                    party.change_reason,
                    req.approval_status,
                    req.approval_time,
                    req.Applicant_id,
                    req.Applicant_name,
                    req.Applicant_time,
                    approvers_json
                )
                try:
                    cursor.execute(sql, values)
                except Exception as exec_e:
                    # 打印详细调试信息（不含敏感信息）
                    print("[voyage_approval] SQL:", sql)
                    print("[voyage_approval] VALUES:", values)
                    raise
            
            connection.commit()
            return True, ""
            
    except Exception as e:
        import traceback
        print(f"插入审批记录时出错: {e}")
        print(traceback.format_exc())
        if 'connection' in locals():
            connection.rollback()
        return False, str(e)
    finally:
        if 'connection' in locals():
            connection.close()


def query_latest_voyage_risk_log(uuid: str) -> dict:
    """
    根据uuid查询voyage_risk_log表中最新的一条数据
    
    Args:
        uuid: 唯一标识
        
    Returns:
        dict: 查询到的数据，如果未找到则返回空字典
    """
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT * FROM lng.voyage_risk_log 
            WHERE uuid = %s 
            ORDER BY request_time DESC 
            LIMIT 1
            """
            cursor.execute(sql, (uuid,))
            result = cursor.fetchone()
            
            if result:
                return dict(result)
            else:
                return {}
                
    except Exception as e:
        print(f"查询航次风险日志时出错: {e}")
        return {}
    finally:
        if 'connection' in locals():
            connection.close()


# ============ 航次合规状态审批信息请求模型 ============
class RelevantParty(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    Relevant_parties_type: str = Field(..., description="相关方类型")
    Parties_id: str = Field(..., description="相关方ID")
    Parties_name: str = Field(..., description="相关方名称")
    risk_screening_status: str = Field(..., description="风险筛查状态")
    risk_change_status: str = Field(..., description="风险变更状态")
    change_reason: str = Field(..., description="变更原因")


class Approver(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    Approver_id: str = Field(..., description="审批人ID")
    Approver_name: str = Field(..., description="审批人姓名")
    Approver_time: str = Field(..., description="审批时间")


class VoyageApprovalRequest(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    uuid: str = Field(..., description="唯一标识")
    voyage_number: str = Field(..., description="航次号", examples=["12935780"])
    Business_segment: str = Field(..., description="业务板块", examples=["油轮"])
    trade_type: str = Field(..., description="贸易类型", examples=["外贸"])
    Business_model: str = Field(..., description="经营模式", examples=["自营"])
    vessel_imo: str = Field(..., description="船舶IMO号", pattern=r"^\d{7}$", examples=["9842190"])
    vessel_name: str = Field(..., description="船舶名称", examples=["Akademik Gubkin"])
    parties: List[RelevantParty] = Field(..., description="相关方列表")
    approval_status: str = Field(..., description="审批状态")
    approval_time: str = Field(..., description="审批时间")
    Applicant_id: str = Field(..., description="申请人ID", examples=["77852"])
    Applicant_name: str = Field(..., description="申请人姓名", examples=["劳氏"])
    Applicant_time: str = Field(..., description="申请时间", examples=["2025-08-30"])
    approvers: List[Approver] = Field(..., description="审批人列表", alias="Approver")


# ============ 航次合规状态审批信息响应模型 ============
# 使用external_api中的VoyageRiskResponse作为响应模型


@external_approval_router.post("/voyage_approval", response_model=VoyageRiskResponse, summary="航次合规状态审批信息存储（POST）")
async def external_voyage_approval(req: VoyageApprovalRequest) -> VoyageRiskResponse:
    """将航次合规状态审批信息存储到数据库中，并返回最新的航次风险筛查结果"""
    print(f"收到审批记录数据: {req}")
    
    try:
        # 1. 将审批数据插入到数据库
        success, err_msg = insert_approval_record(req)
        
        if not success:
            print(f"审批记录存储失败: {err_msg}")
            # 即使审批记录存储失败，也尝试查询航次风险日志
        
        # 2. 查询该 uuid 最新航次风险日志
        voyage_risk_data = query_latest_voyage_risk_log(req.uuid)

        if voyage_risk_data:
            print(f"找到航次风险日志数据，uuid: {req.uuid}")
            # 3. 查询该 uuid 的所有审批记录，并应用到日志对象
            approvals = _load_all_approvals_by_uuid(req.uuid)
            updated_log = _apply_approvals_to_log(voyage_risk_data, approvals)

            # 3.1 基于日志中的 Sts_vessel（大小写不敏感）逐个调用 STS 风控，并将结果直接放入 Sts_vessel 数组
            try:
                import asyncio

                def _get_sts_vessel_list(obj: dict) -> list:
                    key = _find_key_case_insensitive(obj, 'Sts_vessel')
                    if not key:
                        return []
                    return _ensure_list_field(obj, key)

                def _get_imo_from_item(item: dict) -> str:
                    # 兼容大小写/不同别名
                    for k in ['sts_vessel_imo', 'STS_VESSEL_IMO', 'imo', 'IMO']:
                        if isinstance(item, dict) and k in item and item[k]:
                            return str(item[k]).strip()
                    return ''

                sts_items = _get_sts_vessel_list(updated_log)
                async def _call_one(item: dict):
                    imo = _get_imo_from_item(item)
                    if not imo:
                        return item
                    try:
                        d = await run_sts_risk_by_imo(imo)
                        proj_status = d.get('Project_risk_status', '正常')
                        mapped = {'拦截': '高风险', '关注': '中风险'}.get(proj_status, '无风险')
                        # 直接返回一个融合后的元素
                        # 统一风险时间格式：YYYY-MM-DD HH:MM:SS
                        def _normalize_time(val: str) -> str:
                            try:
                                if not isinstance(val, str) or not val:
                                    return ""
                                # 简单规范：替换T为空格，去掉尾部Z
                                norm = val.replace('T', ' ').rstrip('Z')
                                # 再尝试严格解析ISO并格式化
                                from datetime import datetime
                                try:
                                    # 处理结尾Z的情况
                                    iso = val.replace('Z', '+00:00')
                                    dt = datetime.fromisoformat(iso)
                                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                                except Exception:
                                    # 如果严格解析失败，返回norm
                                    return norm
                            except Exception:
                                return ""

                        return {
                            **item,
                            'sts_vessel_imo': imo,
                            'name': d.get('Vessel_name', f'STS-{imo}'),
                            'risk_screening_status': mapped,
                            'risk_screening_time': _normalize_time(d.get('Process_end_time') or d.get('Process_start_time') or ''),
                            'risk_status_change_content': item.get('risk_status_change_content', ''),
                            'risk_status_change_time': item.get('risk_status_change_time', ''),
                            'risk_type_number': item.get('risk_type_number', 0),
                            'risk_description': 'STS项目风控映射',
                            'risk_info': d,
                            'risk_status_reason': item.get('risk_status_reason', {})
                        }
                    except Exception as e:
                        print(f"[voyage_approval] STS风控调用失败 IMO={imo}: {e}")
                        return item

                if sts_items:
                    tasks = [_call_one(it) for it in sts_items]
                    enriched = await asyncio.gather(*tasks)
                    # 回填 enriched 列表到原始实际键名
                    key_actual = _find_key_case_insensitive(updated_log, 'Sts_vessel') or 'Sts_vessel'
                    updated_log[key_actual] = enriched

                    # 计算并设置总体 sts_risk_status：拦截>关注>正常
                    proj_list = []
                    for it in enriched:
                        info = (it.get('risk_info') or {}) if isinstance(it, dict) else {}
                        proj = info.get('Project_risk_status')
                        if proj:
                            proj_list.append(proj)
                    overall = '正常'
                    if any(p == '拦截' for p in proj_list):
                        overall = '拦截'
                    elif any(p == '关注' for p in proj_list):
                        overall = '关注'
                    updated_log['sts_risk_status'] = overall
            except Exception as e:
                print(f"[voyage_approval] STS风控集成出错: {e}")

            # 4.1 基于 updated_log 重新聚合并计算 voyage_risk 与四个派生风险字段（高/中/无）
            try:
                def _ci(obj: dict, key: str):
                    # 不区分大小写获取键名
                    if not isinstance(obj, dict):
                        return None
                    for k in obj.keys():
                        if isinstance(k, str) and k.lower() == key.lower():
                            return k
                    return None

                def _get_list(obj: dict, key: str) -> list:
                    actual = _ci(obj, key)
                    val = obj.get(actual) if actual else None
                    return val if isinstance(val, list) else []

                def _collect_status(items: list) -> list:
                    statuses = []
                    for it in items:
                        if isinstance(it, dict):
                            ks = _ci(it, 'risk_screening_status')
                            if ks and it.get(ks):
                                statuses.append(str(it.get(ks)))
                    return statuses

                # 4.1.1 汇总 all_risk_statuses（用于 voyage_risk -> 拦截/关注/正常）
                all_risk_statuses = []
                for k, v in (updated_log.items() if isinstance(updated_log, dict) else []):
                    if isinstance(v, list):
                        all_risk_statuses.extend(_collect_status(v))
                updated_log['voyage_risk'] = calculate_voyage_risk(all_risk_statuses)

                # 4.1.2 计算四个派生风险（结果为 高/中/无）
                # 客商风险：期租承租人、程租承租人、装货港代理、燃料供应商、加油港代理、卸货港代理
                customer_sources = []
                customer_sources += _get_list(updated_log, 'time_charterer')
                customer_sources += _get_list(updated_log, 'voyage_charterer')
                customer_sources += _get_list(updated_log, 'loading_port_agent')
                customer_sources += _get_list(updated_log, 'bunkering_supplier')
                customer_sources += _get_list(updated_log, 'bunkering_port_agent')
                customer_sources += _get_list(updated_log, 'discharging_port_agent')
                updated_log['customer_risk'] = calculate_risk_level(_collect_status(customer_sources))

                # 收发货人：发货人、发货人实际控制人、收货人、收货人控制人、实际收货人、实际收货人控制人
                shipper_consignee_sources = []
                shipper_consignee_sources += _get_list(updated_log, 'shipper')
                shipper_consignee_sources += _get_list(updated_log, 'shipper_actual_controller')
                shipper_consignee_sources += _get_list(updated_log, 'consignee')
                shipper_consignee_sources += _get_list(updated_log, 'consignee_controller')
                shipper_consignee_sources += _get_list(updated_log, 'actual_consignee')
                shipper_consignee_sources += _get_list(updated_log, 'actual_consignee_controller')
                updated_log['shipper_to_consignee'] = calculate_risk_level(_collect_status(shipper_consignee_sources))

                # 货物风险：货物原产地
                cargo_sources = _get_list(updated_log, 'cargo_origin')
                updated_log['cargo_risk_status'] = calculate_risk_level(_collect_status(cargo_sources))

                # 港口码头风险：装货港、装货码头(经营人/所有人)、卸货港、卸货码头(经营人/所有人)、加油港
                port_sources = []
                port_sources += _get_list(updated_log, 'loading_port')
                port_sources += _get_list(updated_log, 'loading_terminal')
                port_sources += _get_list(updated_log, 'loading_terminal_operator')
                port_sources += _get_list(updated_log, 'loading_terminal_owner')
                port_sources += _get_list(updated_log, 'discharging_port')
                port_sources += _get_list(updated_log, 'discharging_terminal')
                port_sources += _get_list(updated_log, 'discharging_terminal_operator')
                port_sources += _get_list(updated_log, 'discharging_terminal_owner')
                port_sources += _get_list(updated_log, 'bunkering_port')
                updated_log['port_risk_status'] = calculate_risk_level(_collect_status(port_sources))
            except Exception as e:
                print(f"[voyage_approval] 风险字段重新计算出错: {e}")

            # 4. 对比 name + risk_screening_status 快照，与 voyage_risk_log_change 最新一条对比
            previous_change = _fetch_latest_change_log(req.uuid)
            prev_snapshot = _project_name_status_snapshot(previous_change) if previous_change else {}
            new_snapshot = _project_name_status_snapshot(updated_log)

            # 若无历史记录，则必须插入首条；否则仅在快照不同时插入
            should_insert_change = (not previous_change) or (prev_snapshot != new_snapshot)

            # 5. 若有变化，写入 voyage_risk_log_change；否则跳过写入
            if should_insert_change:
                try:
                    ok = _insert_voyage_risk_log_change(updated_log)
                    if not ok:
                        print("插入 voyage_risk_log_change 失败")
                except Exception as e:
                    print(f"插入 voyage_risk_log_change 异常: {e}")

            # 6. 将更新后的日志映射为 VoyageRiskResponse 返回
            log_for_resp = updated_log or voyage_risk_data
            response = VoyageRiskResponse(
                scenario=log_for_resp.get('scenario', ''),
                uuid=log_for_resp.get('uuid', req.uuid),
                voyage_number=log_for_resp.get('voyage_number', ''),
                voyage_risk=log_for_resp.get('voyage_risk', ''),
                voyage_status=log_for_resp.get('voyage_status', ''),
                Business_segment=log_for_resp.get('business_segment', ''),
                trade_type=log_for_resp.get('trade_type', ''),
                Business_model=log_for_resp.get('business_model', ''),
                voyage_start_time=log_for_resp.get('voyage_start_time', ''),
                voyage_end_time=log_for_resp.get('voyage_end_time', ''),
                vessel_imo=log_for_resp.get('vessel_imo', ''),
                vessel_name=log_for_resp.get('vessel_name', ''),
                is_sts=log_for_resp.get('is_sts', ''),
                sts_water_area=log_for_resp.get('sts_water_area', ''),
                Sts_vessel=log_for_resp.get('sts_vessel', []),
                Sts_vessel_owner=log_for_resp.get('sts_vessel_owner', []),
                Sts_vessel_manager=log_for_resp.get('sts_vessel_manager', []),
                Sts_vessel_operator=log_for_resp.get('sts_vessel_operator', []),
                time_charterer=log_for_resp.get('time_charterer', []),
                voyage_charterer=log_for_resp.get('voyage_charterer', []),
                loading_port=log_for_resp.get('loading_port', []),
                loading_port_agent=log_for_resp.get('loading_port_agent', []),
                loading_terminal=log_for_resp.get('loading_terminal', []),
                loading_terminal_operator=log_for_resp.get('loading_terminal_operator', []),
                loading_terminal_owner=log_for_resp.get('loading_terminal_owner', []),
                shipper=log_for_resp.get('shipper', []),
                shipper_actual_controller=log_for_resp.get('shipper_actual_controller', []),
                consignee=log_for_resp.get('consignee', []),
                consignee_controller=log_for_resp.get('consignee_controller', []),
                actual_consignee=log_for_resp.get('actual_consignee', []),
                actual_consignee_controller=log_for_resp.get('actual_consignee_controller', []),
                cargo_origin=log_for_resp.get('cargo_origin', []),
                discharging_port=log_for_resp.get('discharging_port', []),
                discharging_port_agent=log_for_resp.get('discharging_port_agent', []),
                discharging_terminal=log_for_resp.get('discharging_terminal', []),
                discharging_terminal_operator=log_for_resp.get('discharging_terminal_operator', []),
                discharging_terminal_owner=log_for_resp.get('discharging_terminal_owner', []),
                bunkering_ship=log_for_resp.get('bunkering_ship', []),
                bunkering_supplier=log_for_resp.get('bunkering_supplier', []),
                bunkering_port=log_for_resp.get('bunkering_port', []),
                bunkering_port_agent=log_for_resp.get('bunkering_port_agent', []),
                sts_risk_status=log_for_resp.get('sts_risk_status', ''),
                customer_risk=log_for_resp.get('customer_risk', ''),
                shipper_to_consignee=log_for_resp.get('shipper_to_consignee', ''),
                cargo_risk_status=log_for_resp.get('cargo_risk_status', ''),
                port_risk_status=log_for_resp.get('port_risk_status', ''),
                operator_id=log_for_resp.get('operator_id', ''),
                operator_name=log_for_resp.get('operator_name', ''),
                operator_department=log_for_resp.get('operator_department', ''),
                operator_time=log_for_resp.get('operator_time', '')
            )
            return response
        else:
            print(f"未找到航次风险日志数据，uuid: {req.uuid}")
            # 如果未找到数据，返回一个空的响应
            return VoyageRiskResponse(
                scenario="",
                uuid=req.uuid,
                voyage_number="",
                voyage_risk="",
                voyage_status="",
                Business_segment="",
                trade_type="",
                Business_model="",
                voyage_start_time="",
                voyage_end_time="",
                vessel_imo="",
                vessel_name="",
                is_sts="",
                sts_water_area="",
                Sts_vessel=[],
                Sts_vessel_owner=[],
                Sts_vessel_manager=[],
                Sts_vessel_operator=[],
                time_charterer=[],
                voyage_charterer=[],
                loading_port=[],
                loading_port_agent=[],
                loading_terminal=[],
                loading_terminal_operator=[],
                loading_terminal_owner=[],
                shipper=[],
                shipper_actual_controller=[],
                consignee=[],
                consignee_controller=[],
                actual_consignee=[],
                actual_consignee_controller=[],
                cargo_origin=[],
                discharging_port=[],
                discharging_port_agent=[],
                discharging_terminal=[],
                discharging_terminal_operator=[],
                discharging_terminal_owner=[],
                bunkering_ship=[],
                bunkering_supplier=[],
                bunkering_port=[],
                bunkering_port_agent=[],
                sts_risk_status="",
                customer_risk="",
                shipper_to_consignee="",
                cargo_risk_status="",
                port_risk_status="",
                operator_id="",
                operator_name="",
                operator_department="",
                operator_time=""
            )
            
    except Exception as e:
        print(f"处理审批记录时出错: {e}")
        # 返回一个空的响应
        return VoyageRiskResponse(
            scenario="",
            uuid=req.uuid,
            voyage_number="",
            voyage_risk="",
            voyage_status="",
            Business_segment="",
            trade_type="",
            Business_model="",
            voyage_start_time="",
            voyage_end_time="",
            vessel_imo="",
            vessel_name="",
            is_sts="",
            sts_water_area="",
            Sts_vessel=[],
            Sts_vessel_owner=[],
            Sts_vessel_manager=[],
            Sts_vessel_operator=[],
            time_charterer=[],
            voyage_charterer=[],
            loading_port=[],
            loading_port_agent=[],
            loading_terminal=[],
            loading_terminal_operator=[],
            loading_terminal_owner=[],
            shipper=[],
            shipper_actual_controller=[],
            consignee=[],
            consignee_controller=[],
            actual_consignee=[],
            actual_consignee_controller=[],
            cargo_origin=[],
            discharging_port=[],
            discharging_port_agent=[],
            discharging_terminal=[],
            discharging_terminal_operator=[],
            discharging_terminal_owner=[],
            bunkering_ship=[],
            bunkering_supplier=[],
            bunkering_port=[],
            bunkering_port_agent=[],
            sts_risk_status="",
            customer_risk="",
            shipper_to_consignee="",
            cargo_risk_status="",
            port_risk_status="",
            operator_id="",
            operator_name="",
            operator_department="",
            operator_time=""
        )

