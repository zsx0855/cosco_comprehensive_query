#!/usr/bin/env python3
"""
劳氏API 403错误诊断脚本
"""

import requests
import json
from datetime import datetime

# 导入配置
from kingbase_config import get_lloyds_token

# API配置
LLOYDS_API_KEY = get_lloyds_token()

BASE_URL = "https://api.lloydslistintelligence.com/v1"
HEADERS = {
    "accept": "application/json",
    "Authorization": LLOYDS_API_KEY
}

def test_api_endpoint(endpoint, vessel_imo="9842190"):
    """测试API端点"""
    url = f"{BASE_URL}/{endpoint}"
    params = {"vesselImo": vessel_imo}
    
    print(f"\n🔍 测试端点: {endpoint}")
    print(f"📡 URL: {url}")
    print(f"📋 参数: {params}")
    
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        
        print(f"📊 状态码: {response.status_code}")
        print(f"📋 响应头: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ 请求成功")
            try:
                data = response.json()
                print(f"📄 响应数据: {json.dumps(data, indent=2)[:500]}...")
            except:
                print(f"📄 响应内容: {response.text[:500]}...")
        elif response.status_code == 403:
            print("❌ 403 Forbidden - 权限不足")
            print(f"📄 错误响应: {response.text}")
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - 认证失败")
            print(f"📄 错误响应: {response.text}")
        else:
            print(f"❌ 请求失败: {response.status_code}")
            print(f"📄 错误响应: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求异常: {e}")

def main():
    print("🚢 劳氏API 403错误诊断")
    print("=" * 50)
    
    # 测试不同的端点
    endpoints_to_test = [
        "vesselsanctions_v2",  # 问题端点
        "vesselstspairings_v2",  # 已知工作的端点
        "vesselcompliancescreening_v3",  # 合规检查
        "vesselriskscore",  # 风险评分
    ]
    
    for endpoint in endpoints_to_test:
        test_api_endpoint(endpoint)
    
    print("\n" + "=" * 50)
    print("🔍 诊断建议:")
    print("1. 检查API订阅是否包含vesselsanctions_v2权限")
    print("2. 确认账户状态是否正常")
    print("3. 联系劳氏技术支持获取帮助")
    print("4. 尝试使用其他已知工作的端点")

if __name__ == "__main__":
    main()
