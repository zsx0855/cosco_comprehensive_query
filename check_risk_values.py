#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库中的风险值数据
"""

import psycopg2
from psycopg2.extras import DictCursor

def check_risk_values():
    """检查数据库中的风险值数据"""
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
            # 检查每个实体ID的风险值
            for entity_id in [1044520, 13098553, 13382278]:
                print(f"\n🔍 检查 entity_id {entity_id} 的风险值:")
                
                cursor.execute("""
                    SELECT 
                        entity_id,
                        is_san, 
                        is_sco, 
                        is_ool, 
                        is_one_year, 
                        is_sanctioned_countries,
                        country_nm1,
                        DATEVALUE1
                    FROM lng.risk_dqs_test 
                    WHERE entity_id = %s
                """, [entity_id])
                
                results = cursor.fetchall()
                print(f"  记录数: {len(results)}")
                
                for i, row in enumerate(results):
                    print(f"  记录 {i+1}:")
                    print(f"    is_san: {row['is_san']}")
                    print(f"    is_sco: {row['is_sco']}")
                    print(f"    is_ool: {row['is_ool']}")
                    print(f"    is_one_year: {row['is_one_year']}")
                    print(f"    is_sanctioned_countries: {row['is_sanctioned_countries']}")
                    print(f"    country_nm1: {row['country_nm1']}")
                    print(f"    DATEVALUE1: {row['DATEVALUE1']}")
                    print()
        
        conn.close()
        
    except Exception as e:
        print(f"数据库查询失败: {e}")

if __name__ == '__main__':
    check_risk_values()
