#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
船舶租入风险筛查接口测试脚本
"""

import requests
import json
from datetime import datetime


def test_vessel_charter_risk_service():
    """测试船舶租入风险筛查服务"""

    # 服务地址 (根据vessle_charter_risk.py中的配置，默认端口为8000)
    base_url = "http://localhost:8000"

    # 测试数据 (符合CharterInRequest模型要求)
    test_data = {
        "Uuid": "test-uuid-000",
        "Process_id": "proc-000",
        "Process_operator_id": "op-000",
        "Process_operator": "测试操作员",
        "Process_start_time": "2025/01/01 08:00:00",
        "Process_end_time": "2025/01/02 18:00:00",
        "Process_status": "进行中",
        "Vessel_name": "测试船舶A",
        "Vessel_imo": "9876543",
        "charterers": "测试租家",
        "Vessel_manager": "测试船舶管理人",
        "Vessel_owner": "测试船东",
        "Vessel_final_beneficiary": "测试最终受益人",
        "Vessel_operator": "测试船舶经营人",
        "Vessel_broker": ["测试经纪人1", "测试经纪人2"],
        "Second_vessel_owner": ["第二船东1", "第二船东2"],
        "Vessel_insurer": ["保险公司A", "保险公司B"],
        "Lease_actual_controller": ["实际控制人1", "实际控制人2"]
    }

    print("🚀 开始测试船舶租入风险筛查服务...")
    print(f"📍 服务地址: {base_url}")
    print(f"📅 测试时间: {datetime.now()}")
    print("=" * 60)

    try:
        # 测试船舶租入风险筛查接口（响应超时设置为60秒）
        print("1️⃣ 测试船舶租入风险筛查接口...")
        response = requests.post(
            f"{base_url}/charter_in/vessel_charter_risk",
            json=test_data,
            headers={"Content-Type": "application/json"},
            timeout=60  # 超时时间增加至1分钟
        )

        if response.status_code == 200:
            print("   ✅ 接口调用成功")
            response_data = response.json()
            print(f"   📊 UUID: {response_data.get('Uuid')}")
            print(f"   🚢 船舶名称: {response_data.get('Vessel_name')}")
            print(f"   🚢 IMO编号: {response_data.get('Vessel_imo')}")
            print(f"   📋 租家风险: {response_data.get('charterers', {}).get('risk_screening_status')}")
            print(f"   🏢 船东风险: {response_data.get('Vessel_owner', {}).get('risk_screening_status')}")

            # 检查响应中的风险字段
            print("   🔍 检查风险字段:")
            risk_fields = [
                'Vessel_stakeholder_is_sanction_Lloyd',
                'Vessel_stakeholder_is_sanction_kpler',
                'Vessel_is_sanction',
                'Vessel_history_is_sanction',
                'Vessel_in_uani',
                'Vessel_risk_level_Lloyd',
                'Vessel_risk_level_kpler',
                'Vessel_ais_gap',
                'Vessel_Manipulation',
                'Vessel_risky_port_call',
                'Vessel_dark_port_call',
                'Vessel_change_flag',
                'Vessel_cargo_sanction',
                'Vessel_trade_sanction',
                'Vessel_dark_sts_events',
                'Vessel_sts_transfer'
            ]

            for field in risk_fields:
                value = response_data.get(field)
                if value:
                    print(f"      ✅ {field}: 有数据")
                else:
                    print(f"      ⚠️  {field}: 无数据")

        else:
            print(f"   ❌ 接口调用失败: {response.status_code}")
            print(f"   📄 错误信息: {response.text}")
            return False

        print("\n🎉 所有测试通过！")
        return True

    except requests.exceptions.ConnectionError:
        print("❌ 连接失败：请确保服务正在运行")
        print("💡 启动命令: python vessle_charter_in_risk.py")
        return False
    except requests.exceptions.Timeout:
        print("❌ 请求超时：服务响应时间超过1分钟")  # 超时提示更新
        return False
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_vessel_charter_risk_service()
    if success:
        print("\n✅ 船舶租入风险筛查服务测试完成，服务运行正常！")
    else:
        print("\n❌ 船舶租入风险筛查服务测试失败，请检查服务状态！")