import requests
import json
from typing import List, Dict, Any
import pandas as pd

def extract_sts_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    从STS数据中提取所需字段，主体船舶字段放在Activity字段前面
    
    参数:
        data: 包含STS数据的字典
        
    返回:
        包含提取数据的字典列表
    """
    extracted_data = []
    
    if not data.get("IsSuccess", False) or "Data" not in data:
        return extracted_data
    
    for item in data["Data"]["Items"]:
        # 初始化条目字典
        entry = {}
        
        # 处理船舶配对信息
        vessel_pairings = item.get("VesselPairings", [])
        if vessel_pairings:
            # 第一个船舶的字段放在最前面
            main_vessel = vessel_pairings[0]
            entry.update({
                # 主体船舶信息（放在前面）
                "VesselImo": main_vessel.get("Imo"),
                "VesselName": main_vessel.get("VesselName"),
                "VesselRiskRating": main_vessel.get("RiskRating"),
                "VesselFlag": main_vessel.get("Flag"),
                "VesselDwtTonnage": main_vessel.get("DwtTonnage"),
                "VesselType": main_vessel.get("VesselType"),
                "StsType": main_vessel.get("StsType"),
                "VesselDraftStart": main_vessel.get("DraftStart"),
                "VesselDraftEnd": main_vessel.get("DraftEnd"),
                "VesselSogStart": main_vessel.get("SogStart"),
                "VesselSogEnd": main_vessel.get("SogEnd"),
                
                # 活动信息（放在后面）
                "ActivityStartDate": item.get("ActivityStartDate"),
                "ActivityEndDate": item.get("ActivityEndDate"),
                "ActivityAreaName": item.get("ActivityAreaName"),
                "ComplianceRiskScore": item.get("ActivityRiskRating", {}).get("ComplianceRiskScore"),
                "ComplianceRiskReason": item.get("ActivityRiskRating", {}).get("ComplianceRiskReason"),
                "NearestPlaceName": item.get("NearestPlace", {}).get("name"),
                "NearestPlaceCountry": item.get("NearestPlace", {}).get("countryName"),
                
                # 船舶配对信息（只保留非主体船舶）
                "VesselPairings": []
            })
            
            # 其余船舶保留在VesselPairings中
            if len(vessel_pairings) > 1:
                for vessel in vessel_pairings[1:]:
                    vessel_data = {
                        "Imo": vessel.get("Imo"),
                        "VesselName": vessel.get("VesselName"),
                        "RiskRating": vessel.get("RiskRating"),
                        "Flag": vessel.get("Flag"),
                        "DwtTonnage": vessel.get("DwtTonnage"),
                        "VesselType": vessel.get("VesselType"),
                        "StsType": vessel.get("StsType"),
                        "DraftStart": vessel.get("DraftStart"),
                        "DraftEnd": vessel.get("DraftEnd"),
                        "SogStart": vessel.get("SogStart"),
                        "SogEnd": vessel.get("SogEnd")
                    }
                    entry["VesselPairings"].append(vessel_data)
        else:
            # 如果没有船舶配对信息，只保留活动信息
            entry.update({
                "ActivityStartDate": item.get("ActivityStartDate"),
                "ActivityEndDate": item.get("ActivityEndDate"),
                "ActivityAreaName": item.get("ActivityAreaName"),
                "ComplianceRiskScore": item.get("ActivityRiskRating", {}).get("ComplianceRiskScore"),
                "ComplianceRiskReason": item.get("ActivityRiskRating", {}).get("ComplianceRiskReason"),
                "NearestPlaceName": item.get("NearestPlace", {}).get("name"),
                "NearestPlaceCountry": item.get("NearestPlace", {}).get("countryName"),
                "VesselPairings": []
            })
        
        extracted_data.append(entry)
    
    return extracted_data

def get_sts_data(api_url: str, api_token: str, vessel_imo: str) -> List[Dict[str, Any]]:
    """
    从API获取STS数据并提取所需字段
    
    参数:
        api_url: API基础URL
        api_token: 授权令牌
        vessel_imo: 船舶IMO号
        
    返回:
        包含提取数据的字典列表
    """
    headers = {
        "accept": "application/json",
        "Authorization": api_token
    }
    
    params = {
        "vesselImo": vessel_imo
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        return extract_sts_data(data)
    except requests.exceptions.RequestException as e:
        print(f"请求API时出错: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"解析JSON响应时出错: {e}")
        return []

# 示例用法
if __name__ == "__main__":
    # API配置
    API_URL = "https://api.lloydslistintelligence.com/v1/vesselstspairings_v2"
    API_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTc3Mzk2MDQsImlhdCI6MTc1NzczOTYwNCwiZXhwIjoxNzYwMzMxNjA0LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NzczOTYwNCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRRkRpWE1qYk1idGtKdGoyTTdsWjdKa3kxV09iRmtMZjJuMm9Ed0dBcllObVlzeWpEXzN6NTVYWlpXTzJDdHM5cUh5clB3elhXUHdMTTN4OGlVd1F6RXBGZWhwNCIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsc2FuY3Rpb25zIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlyY2FwaSJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTMwI1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxzYW5jdGlvbnMjdmVzc2Vsc2FuY3Rpb25zIzIwMjYtMDgtMzAjVHJ1ZSIsInZlc3NlbHZveWFnZWV2ZW50cyN2ZXNzZWx2b3lhZ2VldmVudHMjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNi0wOC0zMCNUcnVlIiwiUmlzayBcdTAwMjYgQ29tcGxpYW5jZSBBUEkjbGxpcmNhcGkjMjAyNi0wOC0zMCNUcnVlIl0sInVzZXJuYW1lIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsInVzZXJJZCI6IjAwNU56MDAwMDBDaTlHbklBSiIsImNvbnRhY3RBY2NvdW50SWQiOiIwMDFOejAwMDAwS2FCSkRJQTMiLCJ1c2VyVHlwZSI6IkNzcExpdGVQb3J0YWwiLCJlbWFpbCI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJnaXZlbl9uYW1lIjoiWGlueXVhbiIsImZhbWlseV9uYW1lIjoiQ2hhbmciLCJzaGlwVG8iOiIiLCJqdGkiOiIyODBFRjE2RUIxRjI3QTAyNjM3MzU5Qjc5RDM2QTM5NyJ9.FSAlQrg2343Zo4Bc04CvE__gBx6Iwj8Hw5i8WFqJq_imZjL2sOK3sncwJjknSYulp60-Nn1w3-Jm_rjoe9UO4YYycngwoZWLSNVcx7NaxmKULeJPBPcdQSELKWsTgF8FiD9HWxK-AlTps1UNXteAj734rYAgRWOooMi18U21mNt-Q25ewjENfrEKmbqO7q-UjFr_mk0B7BnQK2y9C9Wr57KPV7GEMjktJubNwDkzd9TwxS-dZgxGAi9mZ0wTx9Q_L4IiopHltlS-AdudUbLFCy7RPdwmeNlFH0iBdRAJSJ1VVekcDqtfXKUXoMQfEc-Juy_8nNcWzTiHup5t-KIkpA"
    VESSEL_IMO = "9567233"  # 示例船舶IMO
    
    # 获取并处理数据
    sts_data = get_sts_data(API_URL, API_TOKEN, VESSEL_IMO)
    # 打印结果
    print(json.dumps(sts_data, indent=2, ensure_ascii=False))
    df = pd.DataFrame(sts_data)

    df.head()