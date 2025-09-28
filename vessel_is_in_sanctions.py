import pandas as pd
import requests
import json
from typing import List, Dict


def fetch_vessel_sanctions(imo_number: str, auth_token: str) -> pd.DataFrame:
    """
    获取船舶制裁数据并提取指定字段到DataFrame
    参数:
        imo_number: 船舶IMO编号
        auth_token: API授权令牌
    返回:
        包含所有指定字段的DataFrame
    """
    url = f"https://api.lloydslistintelligence.com/v1/vesselsanctions_v2?vesselImo={imo_number}"
    headers = {"accept": "application/json", "Authorization": auth_token}

    try:
        # 发送API请求
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        if not data.get("IsSuccess"):
            raise ValueError(f"API请求失败: {data.get('Errors', '未知错误')}")

        # 提取items中的vesselSanctions数据
        items = data["Data"]["items"]
        vessel_data = [item["vesselSanctions"] for item in items]

        # 定义需要提取的字段（字符串类型）
        str_fields = [
            "vesselId", "vesselImo", "vesselMmsi", "vesselName",
            "sanctionId", "source", "type", "program", "name",
            "firstPublished", "lastPublished", "startDate",
            "startQualifier", "endDate", "endQualifier"
        ]

        # 构建结构化数据
        structured_data = []
        for vessel in vessel_data:
            record = {field: str(vessel.get(field, "")) for field in str_fields}
            
            # 处理sanctionVesselDetails列表
            details = vessel.get("sanctionVesselDetails", [])
            if details:
                # 取第一个详情（根据需求可调整）
                detail = details[0]
                record.update({
                    "vesselFlag": detail.get("vesselFlag", ""),
                    "formerVesselFlag": detail.get("formerVesselFlag", ""),
                    "vesselCallsign": detail.get("vesselCallsign", ""),
                    "vesselType": detail.get("vesselType", ""),
                    "vesselOwner": detail.get("vesselOwner", "")
                })
            else:
                record.update({
                    "vesselFlag": "", "formerVesselFlag": "",
                    "vesselCallsign": "", "vesselType": "", "vesselOwner": ""
                })
            
            structured_data.append(record)

        # 创建DataFrame
        df = pd.DataFrame(structured_data)
        
        # 保存到CSV（可选）
        df.to_csv(f"vessel_sanctions_{imo_number}.csv", index=False)
        return df

    except requests.exceptions.RequestException as e:
        print(f"请求异常: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        print(f"数据处理异常: {str(e)}")
        return pd.DataFrame()


def transform_sanctions_data(df: pd.DataFrame) -> list:
    """
    转换制裁数据为按IMO分组的嵌套结构
    参数:
        df: 包含船舶制裁数据的DataFrame
    返回:
        按IMO分组的嵌套结构列表
    """
    if df.empty or 'vesselImo' not in df.columns:
        return []

    # 按IMO分组并判断风险状态
    def get_risk_status(group):
        # 处理空值
        end_dates = group['endDate'].apply(
            lambda x: pd.isna(x) or str(x).strip() in ('', 'None')
        )
        
        # 判断当前是否在制裁中（任意endDate为空）
        is_in_sanctions = "是" if end_dates.any() else "否"
        
        # 判断是否有历史制裁（任意endDate有值）
        is_in_sanctions_his = "是" if (~end_dates).any() else "否"
        
        # 判断最高风险等级
        risk_level = '高风险' if end_dates.any() else '中风险'
        
        return risk_level, is_in_sanctions, is_in_sanctions_his

    # 构建嵌套结构
    result = []
    for imo, group in df.groupby('vesselImo'):
        sanctions_list = group[[
            'sanctionId', 'source', 'startDate', 'endDate'
        ]].to_dict('records')
        
        # 统一处理空值
        for item in sanctions_list:
            if pd.isna(item['endDate']) or str(item['endDate']).strip() in ('', 'None'):
                item['endDate'] = ""
        
        # 获取风险状态
        risk_level, is_in_sanctions, is_in_sanctions_his = get_risk_status(group)
        
        result.append({
            "vesselImo": imo,
            "vesselName": group['vesselName'].iloc[0],  # 取第一个非空值
            "sanctions_lev": risk_level,
            "is_in_sanctions": is_in_sanctions,
            "is_in_sanctions_his": is_in_sanctions_his,
            "sanctions_list": sanctions_list
        })

    return result


def get_vessel_sanctions_info(vessel_imo: str, auth_token: str) -> dict:
    """
    外部调用函数：获取船舶制裁信息
    参数:
        vessel_imo: 船舶IMO编号
        auth_token: API授权令牌
    返回:
        包含船舶制裁信息的字典，格式如下：
        {
            "success": True/False,
            "data": [
                {
                    "vesselImo": "IMO号",
                    "vesselName": "船舶名称",
                    "sanctions_lev": "风险等级",
                    "is_in_sanctions": "是否在制裁中",
                    "is_in_sanctions_his": "是否有历史制裁",
                    "sanctions_list": [
                        {
                            "sanctionId": "制裁ID",
                            "source": "来源",
                            "startDate": "开始日期",
                            "endDate": "结束日期"
                        }
                    ]
                }
            ],
            "error": "错误信息（如果有）"
        }
    """
    try:
        # 获取船舶制裁数据
        df = fetch_vessel_sanctions(vessel_imo, auth_token)
        
        if df.empty:
            return {
                "success": False,
                "data": [],
                "error": "未获取到有效数据"
            }
        
        # 转换数据格式
        result = transform_sanctions_data(df)
        
        return {
            "success": True,
            "data": result,
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": [],
            "error": f"处理异常: {str(e)}"
        }


# 示例调用
if __name__ == "__main__":
    imo = "9569671"
    token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTQ5Nzk2MTgsImlhdCI6MTc1NDk3OTYxOCwiZXhwIjoxNzU3NTcxNjE4LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NDk3OTYxOCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRTnNzX1F3T3IzT3E1blouZXBxR0tOcUNaWmRyNENyT2xlVVVSNklvTWRLUDBHcGZDV2swRGdrRnlQSmJtaUVjTGtsMFVPV1FTX2l3VmhvWEd3WksxamFWTDI3USIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJUcmlhbCI6WyJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTI5I1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTI5I1RydWUiLCJjb21wYW55c2FuY3Rpb25zI2NvbXBhbnlzYW5jdGlvbnMjMjAyNS0wOS0xMSNUcnVlIiwiY29tcGFueXJlcG9ydCNjb21wYW55cmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSNhaXNwb3NpdGlvbmdhcGhpc3RvcnkjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2VsY29tcGxpYW5jZXJpc2sjdmVzc2VsY29tcGxpYW5jZXJpc2sjMjAyNS0wOS0xMSNUcnVlIiwic2FuY3Rpb25zZWFyY2gjc2FuY3Rpb25zZWFyY2gjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc2FuY3Rpb25zI3Zlc3NlbHNhbmN0aW9ucyMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxyZXBvcnQjdmVzc2VscmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrcmVwb3J0YXVkaXQjdmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnRhdWRpdCMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCN2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCMyMDI1LTA5LTExI1RydWUiLCJjb21wYW55ZmxlZXRkZXRhaWxzI2NvbXBhbnlmbGVldGRldGFpbHMjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNS0wOS0xMSNUcnVlIiwiQWR2YW5jZWQgUlx1MDAyNkMgQVBJI2xsaWFyY2FwaSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWx2b3lhZ2VldmVudHMjdmVzc2Vsdm95YWdlZXZlbnRzIzIwMjUtMDktMTEjVHJ1ZSIsIlJpc2sgXHUwMDI2IENvbXBsaWFuY2UgQVBJI2xsaXJjYXBpIzIwMjUtMDktMTEjVHJ1ZSIsIlNlYXNlYXJjaGVyI1NlYXNlYXJjaGVyIzIwMjUtMDktMTEjVHJ1ZSJdLCJ1c2VybmFtZSI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJ1c2VySWQiOiIwMDVOejAwMDAwQ2k5R25JQUoiLCJjb250YWN0QWNjb3VudElkIjoiMDAxTnowMDAwMEthQkpESUEzIiwidXNlclR5cGUiOiJDc3BMaXRlUG9ydGFsIiwiZW1haWwiOiJjaGFuZy54aW55dWFuQGNvc2Nvc2hpcHBpbmcuY29tIiwiZ2l2ZW5fbmFtZSI6Ilhpbnl1YW4iLCJmYW1pbHlfbmFtZSI6IkNoYW5nIiwic2hpcFRvIjoiIiwianRpIjoiQ0UwRUExMzkyMTNBNjk0QzFEMDFENDg1NTEyMzdGRUMifQ.ISS2PlKd3ecndjgIk4Zmeuh01DpnWAXGCPlOfcK_K2RyDHj8Irp52u9IIEDm2Urazs_qcqGjQl2o097hFjZX-4i_H58lC3dFUtkZIpJAQ-t4cLLNt1wvzo20m7nIwjffhsoPyPAhmdDxdpmpP42MABD09XeAfcCHCGnh2L2gKomuqHpBivByAJ7tHs5x0oHAiroqXi2TVJTPQkH-mvveqHIiZFtS_SBDdteGeX6LNfQ1rPGfEIG-6BLpV0MXfXo9GPFOIDRnIhXLE9MgG5u_RsRmg5lVQG5sCIH5DdVUd2LkXOTBDepOjtZ0QdzsKgV5GRIbShLRC9CM6PT5MXgyzA"
    
    # 使用新的外部函数
    result = get_vessel_sanctions_info(imo, token)
    print(json.dumps(result, indent=2, ensure_ascii=False))