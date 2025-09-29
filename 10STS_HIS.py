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
    API_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTQ5Nzk2MTgsImlhdCI6MTc1NDk3OTYxOCwiZXhwIjoxNzU3NTcxNjE4LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NDk3OTYxOCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRTnNzX1F3T3IzT3E1blouZXBxR0tOcUNaWmRyNENyT2xlVVVSNklvTWRLUDBHcGZDV2swRGdrRnlQSmJtaUVjTGtsMFVPV1FTX2l3VmhvWEd3WksxamFWTDI3USIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJUcmlhbCI6WyJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTI5I1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTI5I1RydWUiLCJjb21wYW55c2FuY3Rpb25zI2NvbXBhbnlzYW5jdGlvbnMjMjAyNS0wOS0xMSNUcnVlIiwiY29tcGFueXJlcG9ydCNjb21wYW55cmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSNhaXNwb3NpdGlvbmdhcGhpc3RvcnkjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2VsY29tcGxpYW5jZXJpc2sjdmVzc2VsY29tcGxpYW5jZXJpc2sjMjAyNS0wOS0xMSNUcnVlIiwic2FuY3Rpb25zZWFyY2gjc2FuY3Rpb25zZWFyY2gjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc2FuY3Rpb25zI3Zlc3NlbHNhbmN0aW9ucyMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxyZXBvcnQjdmVzc2VscmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrcmVwb3J0YXVkaXQjdmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnRhdWRpdCMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCN2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCMyMDI1LTA5LTExI1RydWUiLCJjb21wYW55ZmxlZXRkZXRhaWxzI2NvbXBhbnlmbGVldGRldGFpbHMjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNS0wOS0xMSNUcnVlIiwiQWR2YW5jZWQgUlx1MDAyNkMgQVBJI2xsaWFyY2FwaSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWx2b3lhZ2VldmVudHMjdmVzc2Vsdm95YWdlZXZlbnRzIzIwMjUtMDktMTEjVHJ1ZSIsIlJpc2sgXHUwMDI2IENvbXBsaWFuY2UgQVBJI2xsaXJjYXBpIzIwMjUtMDktMTEjVHJ1ZSIsIlNlYXNlYXJjaGVyI1NlYXNlYXJjaGVyIzIwMjUtMDktMTEjVHJ1ZSJdLCJ1c2VybmFtZSI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJ1c2VySWQiOiIwMDVOejAwMDAwQ2k5R25JQUoiLCJjb250YWN0QWNjb3VudElkIjoiMDAxTnowMDAwMEthQkpESUEzIiwidXNlclR5cGUiOiJDc3BMaXRlUG9ydGFsIiwiZW1haWwiOiJjaGFuZy54aW55dWFuQGNvc2Nvc2hpcHBpbmcuY29tIiwiZ2l2ZW5fbmFtZSI6Ilhpbnl1YW4iLCJmYW1pbHlfbmFtZSI6IkNoYW5nIiwic2hpcFRvIjoiIiwianRpIjoiQ0UwRUExMzkyMTNBNjk0QzFEMDFENDg1NTEyMzdGRUMifQ.ISS2PlKd3ecndjgIk4Zmeuh01DpnWAXGCPlOfcK_K2RyDHj8Irp52u9IIEDm2Urazs_qcqGjQl2o097hFjZX-4i_H58lC3dFUtkZIpJAQ-t4cLLNt1wvzo20m7nIwjffhsoPyPAhmdDxdpmpP42MABD09XeAfcCHCGnh2L2gKomuqHpBivByAJ7tHs5x0oHAiroqXi2TVJTPQkH-mvveqHIiZFtS_SBDdteGeX6LNfQ1rPGfEIG-eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9"
    VESSEL_IMO = "9567233"  # 示例船舶IMO
    
    # 获取并处理数据
    sts_data = get_sts_data(API_URL, API_TOKEN, VESSEL_IMO)
    # 打印结果
    print(json.dumps(sts_data, indent=2, ensure_ascii=False))
    df = pd.DataFrame(sts_data)

    df.head()