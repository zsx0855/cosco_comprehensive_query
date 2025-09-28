import pandas as pd
import requests
from datetime import datetime
import json # Added for json.dumps

# 配置常量
BASE_URL = "https://api.lloydslistintelligence.com/v1/"
headers = {
    "Authorization": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTQ5Nzk2MTgsImlhdCI6MTc1NDk3OTYxOCwiZXhwIjoxNzU3NTcxNjE4LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NDk3OTYxOCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRTnNzX1F3T3IzT3E1blouZXBxR0tOcUNaWmRyNENyT2xlVVVSNklvTWRLUDBHcGZDV2swRGdrRnlQSmJtaUVjTGtsMFVPV1FTX2l3VmhvWEd3WksxamFWTDI3USIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJUcmlhbCI6WyJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTI5I1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTI5I1RydWUiLCJjb21wYW55c2FuY3Rpb25zI2NvbXBhbnlzYW5jdGlvbnMjMjAyNS0wOS0xMSNUcnVlIiwiY29tcGFueXJlcG9ydCNjb21wYW55cmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSNhaXNwb3NpdGlvbmdhcGhpc3RvcnkjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2VsY29tcGxpYW5jZXJpc2sjdmVzc2VsY29tcGxpYW5jZXJpc2sjMjAyNS0wOS0xMSNUcnVlIiwic2FuY3Rpb25zZWFyY2gjc2FuY3Rpb25zZWFyY2gjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc2FuY3Rpb25zI3Zlc3NlbHNhbmN0aW9ucyMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxyZXBvcnQjdmVzc2VscmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrcmVwb3J0YXVkaXQjdmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnRhdWRpdCMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCN2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCMyMDI1LTA5LTExI1RydWUiLCJjb21wYW55ZmxlZXRkZXRhaWxzI2NvbXBhbnlmbGVldGRldGFpbHMjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNS0wOS0xMSNUcnVlIiwiQWR2YW5jZWQgUlx1MDAyNkMgQVBJI2xsaWFyY2FwaSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWx2b3lhZ2VldmVudHMjdmVzc2Vsdm95YWdlZXZlbnRzIzIwMjUtMDktMTEjVHJ1ZSIsIlJpc2sgXHUwMDI2IENvbXBsaWFuY2UgQVBJI2xsaXJjYXBpIzIwMjUtMDktMTEjVHJ1ZSIsIlNlYXNlYXJjaGVyI1NlYXNlYXJjaGVyIzIwMjUtMDktMTEjVHJ1ZSJdLCJ1c2VybmFtZSI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJ1c2VySWQiOiIwMDVOejAwMDAwQ2k5R25JQUoiLCJjb250YWN0QWNjb3VudElkIjoiMDAxTnowMDAwMEthQkpESUEzIiwidXNlclR5cGUiOiJDc3BMaXRlUG9ydGFsIiwiZW1haWwiOiJjaGFuZy54aW55dWFuQGNvc2Nvc2hpcHBpbmcuY29tIiwiZ2l2ZW5fbmFtZSI6Ilhpbnl1YW4iLCJmYW1pbHlfbmFtZSI6IkNoYW5nIiwic2hpcFRvIjoiIiwianRpIjoiQ0UwRUExMzkyMTNBNjk0QzFEMDFENDg1NTEyMzdGRUMifQ.ISS2PlKd3ecndjgIk4Zmeuh01DpnWAXGCPlOfcK_K2RyDHj8Irp52u9IIEDm2Urazs_qcqGjQl2o097hFjZX-4i_H58lC3dFUtkZIpJAQ-t4cLLNt1wvzo20m7nIwjffhsoPyPAhmdDxdpmpP42MABD09XeAfcCHCGnh2L2gKomuqHpBivByAJ7tHs5x0oHAiroqXi2TVJTPQkH-mvveqHIiZFtS_SBDdteGeX6LNfQ1rPGfEIG-6BLpV0MXfXo9GPFOIDRnIhXLE9MgG5u_RsRmg5lVQG5sCIH5DdVUd2LkXOTBDepOjtZ0QdzsKgV5GRIbShLRC9CM6PT5MXgyzA",
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
        response = requests.get(url, headers=headers, timeout=600)
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


def get_vessel_ais_manipulation_info(vessel_imo: str, api_key: str, risk_type: str = "VesselAisManipulation") -> dict:
    """
    外部调用函数：获取船舶AIS人为篡改信息
    参数:
        vessel_imo: 船舶IMO编号（字符串）
        api_key: Lloyd's List API授权令牌
        risk_type: 风险类型（默认为"VesselAisManipulation"，可选其他7种类型）
    返回:
        包含船舶AIS人为篡改信息的字典，格式如下：
        {
            "success": True/False,
            "data": {
                "VesselImo": "IMO号",
                "VesselName": "船舶名称",
                "RiskType": "风险类型",
                "ComplianceRisks": [  # 合规风险列表
                    {
                        "ComplianceRiskType": {
                            "Description": "风险类型描述"
                        },
                        "Details": [  # 风险详情列表
                            {
                                "PlaceInfo": {},  # 地点信息
                                "RiskIndicators": [],  # 风险指标
                                "StartDateTime": "开始时间",
                                "EndDateTime": "结束时间",
                                "Duration": "持续时间",
                                "Location": "位置信息",
                                "RiskScore": "风险评分"
                            }
                        ]
                    }
                ],
                "ProcessingDate": "处理时间"
            },
            "error": "错误信息（如果有）"
        }
    """
    try:
        # 获取船舶所有风险数据
        full_risk_data = get_vessel_risks(vessel_imo)
        
        if not full_risk_data:
            return {
                "success": False,
                "data": {},
                "error": "未获取到有效数据"
            }
        
        # 基础船舶信息
        base_info = {
            'VesselImo': full_risk_data.get('VesselImo'),
            'VesselName': full_risk_data.get('VesselName'),
            'RiskType': risk_type,
            'ProcessingDate': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 查找匹配的风险数据
        matched_risks = []
        for risk in full_risk_data.get('ComplianceRisks', []):
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
        
        # 如果没有找到指定类型的风险数据
        if not matched_risks:
            return {
                "success": True,
                "data": {
                    **base_info,
                    "ComplianceRisks": [],
                    "message": f"未找到 {risk_type} 类型的风险数据"
                },
                "error": None
            }
        
        return {
            "success": True,
            "data": {
                **base_info,
                "ComplianceRisks": matched_risks
            },
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": {},
            "error": f"处理异常: {str(e)}"
        }


def get_vessel_all_risk_types(vessel_imo: str, api_key: str) -> dict:
    """
    外部调用函数：获取船舶所有风险类型信息
    参数:
        vessel_imo: 船舶IMO编号（字符串）
        api_key: Lloyd's List API授权令牌
    返回:
        包含船舶所有风险类型信息的字典
    """
    try:
        # 获取船舶所有风险数据
        full_risk_data = get_vessel_risks(vessel_imo)
        
        if not full_risk_data:
            return {
                "success": False,
                "data": {},
                "error": "未获取到有效数据"
            }
        
        # 基础船舶信息
        base_info = {
            'VesselImo': full_risk_data.get('VesselImo'),
            'VesselName': full_risk_data.get('VesselName'),
            'ProcessingDate': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 按风险类型分组数据
        risk_types_data = {}
        for risk_type in RISK_TYPES:
            matched_risks = []
            for risk in full_risk_data.get('ComplianceRisks', []):
                if risk.get('ComplianceRiskType', {}).get('Description') == risk_type:
                    details = risk.get('Details', [])
                    if not details:
                        matched_risks.append({**base_info, **risk})
                    else:
                        for detail in details:
                            matched_risks.append({
                                **base_info,
                                **risk,
                                **detail,
                                'PlaceInfo': detail.get('Place', {}),
                                'RiskIndicators': [ind['Description'] for ind in detail.get('RiskIndicators', [])]
                            })
            risk_types_data[risk_type] = matched_risks
        
        return {
            "success": True,
            "data": {
                **base_info,
                "RiskTypesData": risk_types_data
            },
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": {},
            "error": f"处理异常: {str(e)}"
        }


# 示例调用
if __name__ == "__main__":
    # 测试用的参数
    test_imo = "9326067"
    test_api_key = "your_lloyds_api_key_here"
    
    # 使用外部函数 - 获取AIS人为篡改信息
    result = get_vessel_ais_manipulation_info(test_imo, test_api_key)
    print("AIS人为篡改信息:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 使用外部函数 - 获取所有风险类型信息
    all_risks_result = get_vessel_all_risk_types(test_imo, test_api_key)
    print("\n所有风险类型信息:")
    print(json.dumps(all_risks_result, indent=2, ensure_ascii=False))