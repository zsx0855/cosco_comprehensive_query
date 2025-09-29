#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STS风险筛查服务测试脚本
"""

import requests
import json
from datetime import datetime

def test_sts_service():
    """测试STS风险筛查服务"""
    
    # 服务地址
    base_url = "http://localhost:8000"
    
    # 测试数据
    test_data = {
        "uuid": "test-uuid-001",
        "sts_execution_status": "计划中",
        "business_segment": "LNG",
        "business_model": "FOB",
        "operate_water_area": "中国沿海",
        "expected_execution_date": "2025/01/15",
        "is_port_sts": "是",
        "vessel_name": "测试船舶",
        "vessel_imo": "1234567",
        "charterers": "测试租家",
        "vessel_owner": ["测试船东"],
        "vessel_manager": ["测试管理人"],
        "vessel_operator": ["测试经营人"]
    }
    
    print("🚀 开始测试STS风险筛查服务...")
    print(f"📍 服务地址: {base_url}")
    print(f"📅 测试时间: {datetime.now()}")
    print("=" * 60)
    
    try:
        # 测试健康检查
        print("1️⃣ 测试健康检查...")
        health_response = requests.get(f"{base_url}/health", timeout=10)
        if health_response.status_code == 200:
            print("   ✅ 健康检查通过")
            print(f"   📊 响应: {health_response.json()}")
        else:
            print(f"   ❌ 健康检查失败: {health_response.status_code}")
            return False
        
        # 测试STS风险筛查接口
        print("\n2️⃣ 测试STS风险筛查接口...")
        sts_response = requests.post(
            f"{base_url}/sts/risk_screen",
            json=test_data,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if sts_response.status_code == 200:
            print("   ✅ STS风险筛查接口调用成功")
            response_data = sts_response.json()
            print(f"   📊 UUID: {response_data.get('uuid')}")
            print(f"   🚢 船舶名称: {response_data.get('vessel_name')}")
            print(f"   📋 租家风险: {response_data.get('charterers', {}).get('risk_screening_status')}")
            print(f"   🏢 船东风险: {response_data.get('vessel_owner', {}).get('risk_screening_status')}")
            
            # 检查新增字段
            print("   🔍 检查新增风险字段:")
            new_fields = [
                'vessel_stakeholder_is_sanction_lloyd',
                'vessel_stakeholder_is_sanction_kpler',
                'vessel_is_sanction',
                'vessel_history_is_sanction',
                'vessel_in_uani',
                'vessel_risk_level_lloyd',
                'vessel_risk_level_kpler',
                'vessel_ais_gap',
                'vessel_manipulation',
                'vessel_high_risk_port',
                'vessel_has_dark_port_call',
                'vessel_cargo_sanction',
                'vessel_trade_sanction',
                'cargo_origin_from_sanctioned_country',
                'vessel_dark_sts_events',
                'vessel_sts_transfer'
            ]
            
            for field in new_fields:
                value = response_data.get(field)
                if value:
                    print(f"      ✅ {field}: 有数据")
                else:
                    print(f"      ⚠️  {field}: 无数据")
            
        else:
            print(f"   ❌ STS风险筛查接口调用失败: {sts_response.status_code}")
            print(f"   📄 错误信息: {sts_response.text}")
            return False
        
        print("\n🎉 所有测试通过！")
        return True
        
    except requests.exceptions.ConnectionError:
        print("❌ 连接失败：请确保服务正在运行")
        print("💡 启动命令: python start_server.py")
        return False
    except requests.exceptions.Timeout:
        print("❌ 请求超时：服务响应时间过长")
        return False
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_sts_service()
    if success:
        print("\n✅ STS服务测试完成，服务运行正常！")
    else:
        print("\n❌ STS服务测试失败，请检查服务状态！")
