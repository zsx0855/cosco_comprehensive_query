import pandas as pd
import requests
from datetime import datetime

# 配置常量
BASE_URL = "https://api.lloydslistintelligence.com/v1/"
headers = {
    "Authorization": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTc3Mzk2MDQsImlhdCI6MTc1NzczOTYwNCwiZXhwIjoxNzYwMzMxNjA0LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NzczOTYwNCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRRkRpWE1qYk1idGtKdGoyTTdsWjdKa3kxV09iRmtMZjJuMm9Ed0dBcllObVlzeWpEXzN6NTVYWlpXTzJDdHM5cUh5clB3elhXUHdMTTN4OGlVd1F6RXBGZWhwNCIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsc2FuY3Rpb25zIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlyY2FwaSJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTMwI1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxzYW5jdGlvbnMjdmVzc2Vsc2FuY3Rpb25zIzIwMjYtMDgtMzAjVHJ1ZSIsInZlc3NlbHZveWFnZWV2ZW50cyN2ZXNzZWx2b3lhZ2VldmVudHMjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNi0wOC0zMCNUcnVlIiwiUmlzayBcdTAwMjYgQ29tcGxpYW5jZSBBUEkjbGxpcmNhcGkjMjAyNi0wOC0zMCNUcnVlIl0sInVzZXJuYW1lIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsInVzZXJJZCI6IjAwNU56MDAwMDBDaTlHbklBSiIsImNvbnRhY3RBY2NvdW50SWQiOiIwMDFOejAwMDAwS2FCSkRJQTMiLCJ1c2VyVHlwZSI6IkNzcExpdGVQb3J0YWwiLCJlbWFpbCI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJnaXZlbl9uYW1lIjoiWGlueXVhbiIsImZhbWlseV9uYW1lIjoiQ2hhbmciLCJzaGlwVG8iOiIiLCJqdGkiOiIyODBFRjE2RUIxRjI3QTAyNjM3MzU5Qjc5RDM2QTM5NyJ9.FSAlQrg2343Zo4Bc04CvE__gBx6Iwj8Hw5i8WFqJq_imZjL2sOK3sncwJjknSYulp60-Nn1w3-Jm_rjoe9UO4YYycngwoZWLSNVcx7NaxmKULeJPBPcdQSELKWsTgF8FiD9HWxK-AlTps1UNXteAj734rYAgRWOooMi18U21mNt-Q25ewjENfrEKmbqO7q-UjFr_mk0B7BnQK2y9C9Wr57KPV7GEMjktJubNwDkzd9TwxS-dZgxGAi9mZ0wTx9Q_L4IiopHltlS-AdudUbLFCy7RPdwmeNlFH0iBdRAJSJ1VVekcDqtfXKUXoMQfEc-Juy_8nNcWzTiHup5t-KIkpA",
    "accept": "application/json"    
}
RISK_TYPES = [
    "VesselAisGap",
    "VesselAisManipulation",
    "VesselMovement",
    "VesselShipToShip",
    "VesselOwnership",
    "VesselFlag",
    "VesselLoitering"
]

def get_vessel_risks(vessel_imo):
    """获取船舶所有风险数据（包含全部7种风险类型）"""
    endpoint = f"vesseladvancedcompliancerisk_v3?vesselImo={vessel_imo}"
    url = BASE_URL + endpoint
    
    try:
        response = requests.get(url, headers=headers, timeout=1200)  # 翻倍
        response.raise_for_status()
        data = response.json()
        
        if data['IsSuccess'] and data['Data']['Items']:
            return data['Data']['Items'][0]  # 返回完整风险数据
        else:
            print(f"未找到IMO为 {vessel_imo} 的风险数据")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"API请求失败: {e}")
        return None

def extract_risk_type(full_data, risk_type):
    """
    从完整数据中提取指定风险类型的数据
    :param full_data: API返回的完整数据
    :param risk_type: 要提取的风险类型（必须为RISK_TYPES中的值）
    :return: 包含该风险类型所有记录的DataFrame
    """
    if not full_data:
        return pd.DataFrame()
    
    # 基础船舶信息
    base_info = {
        'VesselImo': full_data.get('VesselImo'),
        'VesselName': full_data.get('VesselName'),
        'RiskType': risk_type
    }
    
    # 查找匹配的风险数据
    matched_risks = []
    for risk in full_data.get('ComplianceRisks', []):
        if risk.get('ComplianceRiskType', {}).get('Description') == risk_type:
            # 处理风险详情
            details = risk.get('Details', [])
            if not details:  # 无详情时保留基础信息
                matched_risks.append({**base_info, **risk})
            else:
                for detail in details:
                    # 合并基础信息、风险属性和详情
                    matched_risks.append({
                        **base_info,
                        **risk,
                        **detail,
                        # 特殊处理嵌套字段
                        'PlaceInfo': detail.get('Place', {}),
                        'RiskIndicators': [ind['Description'] for ind in detail.get('RiskIndicators', [])]
                    })
    
    # 转换为DataFrame并处理嵌套字段
    df = pd.json_normalize(matched_risks, sep='_')
    
    # 添加处理时间戳
    if not df.empty:
        df['ProcessingDate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df

# 使用示例
if __name__ == "__main__":
    vessel_imo = "9326067"  # 示例IMO
    
    # 1. 获取完整风险数据（包含7种类型）
    print("正在获取船舶风险数据...")
    full_risk_data = get_vessel_risks(vessel_imo)
    
    if full_risk_data:
        # 2. 按需提取特定风险类型（示例：提取VesselMovement）
        target_risk = "VesselAisManipulation"
        print(f"\n正在提取 {target_risk} 数据...")
        risk_df = extract_risk_type(full_risk_data, target_risk)
        
        if not risk_df.empty:
            # 3. 展示提取结果
            print(f"\n提取到 {len(risk_df)} 条 {target_risk} 记录")
            print(risk_df.head())
            
            # 4. 保存到CSV（可根据需要修改格式）
            timestamp = datetime.now().strftime("%Y%m%d")
            risk_df.to_csv(f"{target_risk}_{vessel_imo}_{timestamp}.csv", index=False)
            print(f"\n数据已保存到 {target_risk}_{vessel_imo}_{timestamp}.csv")
        else:
            print(f"未找到 {target_risk} 类型的风险数据")