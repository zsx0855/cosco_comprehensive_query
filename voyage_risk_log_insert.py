import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, Any

# KingBase数据库配置
KINGBASE_CONFIG = {
    'host': '10.11.142.145',
    'port': 54321,
    'database': 'lngdb',
    'user': 'system',
    'password': 'zV2,oB5%',
    'cursor_factory': RealDictCursor
}

def insert_voyage_risk_log(response_data: Dict[str, Any], request_time: datetime = None) -> bool:
    """
    将航次风险筛查接口返回数据插入到数据库
    
    Args:
        response_data: 接口返回的完整数据字典
        request_time: 请求时间，如果为None则使用当前时间
        
    Returns:
        bool: 插入是否成功
    """
    try:
        # 如果没有提供请求时间，使用当前时间
        if request_time is None:
            request_time = datetime.now()
        
        # 建立数据库连接
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        
        with connection.cursor() as cursor:
            # 准备插入SQL
            sql = """
            INSERT INTO lng.voyage_risk_log (
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
                full_response,uuid
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # 准备数据
            data = (
                request_time,                                    # request_time
                datetime.now(),                                  # response_time
                response_data.get('scenario', ''),              # scenario
                response_data.get('voyage_number', ''),             # voyage_id
                response_data.get('voyage_risk', ''),           # voyage_risk
                response_data.get('voyage_status', ''),         # voyage_status
                response_data.get('Business_segment', ''),      # Business_segment
                response_data.get('trade_type', ''),            # trade_type
                response_data.get('Business_model', ''),        # Business_model
                response_data.get('voyage_start_time', ''),     # voyage_start_time
                response_data.get('voyage_end_time', ''),       # voyage_end_time
                response_data.get('vessel_imo', ''),            # vessel_imo
                response_data.get('vessel_name', ''),           # vessel_name
                response_data.get('is_sts', ''),                # is_sts
                response_data.get('sts_water_area', ''),        # sts_water_area
                json.dumps(response_data.get('Sts_vessel', []), ensure_ascii=False),  # sts_vessel
                json.dumps(response_data.get('Sts_vessel_owner', []), ensure_ascii=False),  # sts_vessel_owner
                json.dumps(response_data.get('Sts_vessel_manager', []), ensure_ascii=False),  # sts_vessel_manager
                json.dumps(response_data.get('Sts_vessel_operator', []), ensure_ascii=False),  # sts_vessel_operator
                json.dumps(response_data.get('time_charterer', {}), ensure_ascii=False),  # time_charterer
                json.dumps(response_data.get('voyage_charterer', {}), ensure_ascii=False),  # voyage_charterer
                json.dumps(response_data.get('loading_port', []), ensure_ascii=False),  # loading_port
                json.dumps(response_data.get('loading_port_agent', []), ensure_ascii=False),  # loading_port_agent
                json.dumps(response_data.get('loading_terminal', []), ensure_ascii=False),  # loading_terminal
                json.dumps(response_data.get('loading_terminal_operator', []), ensure_ascii=False),  # loading_terminal_operator
                json.dumps(response_data.get('loading_terminal_owner', []), ensure_ascii=False),  # loading_terminal_owner
                json.dumps(response_data.get('shipper', []), ensure_ascii=False),  # shipper
                json.dumps(response_data.get('shipper_actual_controller', []), ensure_ascii=False),  # shipper_actual_controller
                json.dumps(response_data.get('consignee', []), ensure_ascii=False),  # consignee
                json.dumps(response_data.get('consignee_controller', []), ensure_ascii=False),  # consignee_controller
                json.dumps(response_data.get('actual_consignee', []), ensure_ascii=False),  # actual_consignee
                json.dumps(response_data.get('actual_consignee_controller', []), ensure_ascii=False),  # actual_consignee_controller
                json.dumps(response_data.get('cargo_origin', []), ensure_ascii=False),  # cargo_origin
                json.dumps(response_data.get('discharging_port', []), ensure_ascii=False),  # discharging_port
                json.dumps(response_data.get('discharging_port_agent', []), ensure_ascii=False),  # discharging_port_agent
                json.dumps(response_data.get('discharging_terminal', []), ensure_ascii=False),  # discharging_terminal
                json.dumps(response_data.get('discharging_terminal_operator', []), ensure_ascii=False),  # discharging_terminal_operator
                json.dumps(response_data.get('discharging_terminal_owner', []), ensure_ascii=False),  # discharging_terminal_owner
                json.dumps(response_data.get('bunkering_ship', []), ensure_ascii=False),  # bunkering_ship
                json.dumps(response_data.get('bunkering_supplier', []), ensure_ascii=False),  # bunkering_supplier
                json.dumps(response_data.get('bunkering_port', []), ensure_ascii=False),  # bunkering_port
                json.dumps(response_data.get('bunkering_port_agent', []), ensure_ascii=False),  # bunkering_port_agent
                response_data.get('operator_id', ''),            # operator_id
                response_data.get('operator_name', ''),          # operator_name
                response_data.get('operator_department', ''),    # operator_department
                response_data.get('operator_time', ''),          # operator_time
                json.dumps(response_data, ensure_ascii=False),    # full_response
                response_data.get('uuid', '')                 # uuid
            )
            
            # 执行插入
            cursor.execute(sql, data)
            connection.commit()
            
            print(f"成功插入航次风险筛查数据，航次ID: {response_data.get('voyage_number', '')}")
            return True
            
    except Exception as e:
        print(f"插入航次风险筛查数据时出错: {e}")
        if 'connection' in locals():
            connection.rollback()
        return False
    finally:
        if 'connection' in locals():
            connection.close()


def query_voyage_risk_log(voyage_id: str = None, start_time: datetime = None, end_time: datetime = None) -> list:
    """
    查询航次风险筛查日志数据
    
    Args:
        voyage_id: 航次ID，如果提供则按航次ID查询
        start_time: 开始时间
        end_time: 结束时间
        
    Returns:
        list: 查询结果列表
    """
    try:
        connection = psycopg2.connect(**KINGBASE_CONFIG)
        
        with connection.cursor() as cursor:
            # 构建查询条件
            conditions = []
            params = []
            
            if voyage_id:
                conditions.append("voyage_id = %s")
                params.append(voyage_id)
            
            if start_time:
                conditions.append("request_time >= %s")
                params.append(start_time)
            
            if end_time:
                conditions.append("request_time <= %s")
                params.append(end_time)
            
            # 构建SQL
            sql = "SELECT * FROM lng.voyage_risk_log"
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " ORDER BY request_time DESC"
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            return results
            
    except Exception as e:
        print(f"查询航次风险筛查日志时出错: {e}")
        return []
    finally:
        if 'connection' in locals():
            connection.close()


# 使用示例
if __name__ == "__main__":
    # 示例数据
    sample_response = {
        "scenario": "及时查询",
        "uuid": "3b6157f4-e262-45cd-8a90-cfbd06640521",
        "voyage_number": "12935780",
        "is_sts": "true",
        "vessel": {
            "imo": "9842190",
            "ship_name": "Akademik Gubkin",
            "risk_screening_status": "无风险",
            "risk_screening_time": "2025-08-30 11:43:09"
        },
        "Sts_vessel": [
            {
                "sts_vessel_imo": "1234567",
                "Sts_vessel_name": "STS Ship 1",
                "risk_screening_status": "无风险",
                "risk_screening_time": "2025-08-30 11:43:09"
            }
        ],
        "Sts_water_area": {
            "Sts_water_area": "Singapore",
            "risk_screening_status": "无风险",
            "risk_screening_time": "2025-08-30 11:43:09"
        },
        "operator_id": "77852",
        "operator_name": "劳氏",
        "operator_department": "操作部",
        "operator_time": "2025-08-30 11:43:09"
    }
    
    # 插入数据
    success = insert_voyage_risk_log(sample_response)
    print(f"插入结果: {'成功' if success else '失败'}")
    
    # 查询数据
    results = query_voyage_risk_log(voyage_id="12935780")
    print(f"查询到 {len(results)} 条记录")
