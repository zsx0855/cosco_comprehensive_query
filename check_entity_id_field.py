#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库查询返回的entity_id字段
"""

import psycopg2
from psycopg2.extras import DictCursor

def check_entity_id_field():
    """检查数据库查询返回的entity_id字段"""
    try:
        conn = psycopg2.connect(
            host='10.11.142.145',
            port=54321,
            user='system',
            password='zV2,oB5%',
            dbname='lngdb',
            cursor_factory=DictCursor
        )
        
        with conn.cursor() as cursor:
            # 使用与san_to_kingbase_test.py相同的查询
            sql = """
            SELECT 
                t1.entity_id, 
                t1.entity_dt, 
                t1.activestatus, 
                t1.ENTITYNAME1, 
                t1.ENTITYNAME4, 
                t1.country_nm1, 
                t1.country_nm2, 
                t1.DATEVALUE1, 
                t2.description2_value_cn, 
                t2.description3_value_cn, 
                t2.start_time, 
                t2.end_time,
                t2.SANCTIONS_NM, 
                t2.is_san, 
                t2.is_sco, 
                t2.is_ool, 
                t2.is_one_year, 
                t2.is_sanctioned_countries
            FROM (
                SELECT DISTINCT 
                    entity_id, 
                    entity_dt, 
                    activestatus, 
                    ENTITYNAME1, 
                    ENTITYNAME4, 
                    country_nm1, 
                    country_nm2, 
                    DATEVALUE1
                FROM lng.risk_dqs_test where entity_id in (1044520,13098553,13382278)
            ) t1
            INNER JOIN lng.risk_dqs_test t2 ON t1.entity_id = t2.entity_id
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            print(f"🔍 查询返回 {len(results)} 条记录")
            print("\n🔍 每条记录的entity_id:")
            for i, row in enumerate(results):
                print(f"  记录 {i+1}: entity_id = {row['entity_id']} (类型: {type(row['entity_id'])})")
                if i >= 5:  # 只显示前5条
                    print(f"  ... 还有 {len(results) - 5} 条记录")
                    break
            
            # 检查是否有entity_id为None的记录
            none_count = sum(1 for row in results if row['entity_id'] is None)
            print(f"\n🔍 entity_id为None的记录数: {none_count}")
            
            # 按entity_id分组统计
            entity_groups = {}
            for row in results:
                entity_id = row['entity_id']
                if entity_id not in entity_groups:
                    entity_groups[entity_id] = 0
                entity_groups[entity_id] += 1
            
            print(f"\n🔍 按entity_id分组统计:")
            for entity_id, count in entity_groups.items():
                print(f"  entity_id {entity_id}: {count} 条记录")
        
        conn.close()
        
    except Exception as e:
        print(f"数据库查询失败: {e}")

if __name__ == '__main__':
    check_entity_id_field()
