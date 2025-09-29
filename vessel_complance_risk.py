import requests
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import json

class VesselSanctionDataProcessor:
    """船舶制裁数据处理器（精确提取版）"""
    
    def __init__(self, api_key: str):
        self.base_url = "https://api.lloydslistintelligence.com/v1"
        self.headers = {"accept": "application/json", "Authorization": api_key}
    
    def fetch_data(self, endpoint: str, vessel_imo: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """通用数据获取方法"""
        url = f"{self.base_url}/{endpoint}"
        params = {
            "vesselImo": vessel_imo,
            "voyageDateRange": f"{start_date},{end_date}"
        }
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API请求失败({endpoint}): {e}")
            return {}

    def process_and_export(self, vessel_imo: str, start_date: str, end_date: str, output_file: str):
        """处理并导出数据（精确提取 SanctionedOwners 字段）"""
        # 获取原始数据
        compliance_data = self.fetch_data("vesselcompliancescreening_v3", vessel_imo, start_date, end_date)
        risk_data = self.fetch_data("vesselriskscore", vessel_imo, start_date, end_date)
        
        if not compliance_data or not risk_data:
            print("未获取到有效数据")
            return

        # 提取合规数据（保持原始字段）
        compliance_item = compliance_data.get("Data", {}).get("Items", [{}])[0]
        compliance_result = {
            "VesselImo": compliance_item.get("VesselImo"),
            "OwnerIsInSanctionedCountry": compliance_item.get("SanctionRisks", {}).get("OwnerIsInSanctionedCountry"),
            "OwnerIsCurrentlySanctioned": compliance_item.get("SanctionRisks", {}).get("OwnerIsCurrentlySanctioned"),
            "OwnerHasHistoricalSanctions": compliance_item.get("SanctionRisks", {}).get("OwnerHasHistoricalSanctions"),
            "ComplianceDataVersion": compliance_data.get("Data", {}).get("Version", "1.0")
        }

        # 提取风险数据（保持原始字段）
        risk_item = risk_data.get("Data", {}).get("Items", [{}])[0]
        vessel_info = {
            "VesselImo": risk_item.get("VesselImo"),
            "Mmsi": risk_item.get("Mmsi"),
            "VesselName": risk_item.get("VesselName"),
            "VesselType": risk_item.get("VesselType"),
            "RiskScores": risk_item.get("RiskScores", {}),
            "VesselOwnershipContainsLinksToSanctionedEntities": risk_item.get("VesselOwnershipContainsLinksToSanctionedEntities", False),
            "VesselOwnershipContainsLinksToSanctionedEntities": risk_item.get("VesselOwnershipContainsLinksToSanctionedEntities", False),
            "VesselOwnershipContainsLinksToSanctionedEntities": risk_item.get("VesselOwnershipContainsLinksToSanctionedEntities", False)
        }

        # 精确提取 SanctionedOwners 的字段（只提取您需要的部分）
        sanctioned_owners = []
        for owner in risk_item.get("SanctionedOwners", []):
            # 只提取以下字段（按需修改）：
            owner_data = {
                "CompanyName": owner.get("CompanyName"),
                "CompanyImo": owner.get("CompanyImo"),
                "OwnershipTypes": owner.get("OwnershipTypes", []),
                "OwnershipStartDate": owner.get("OwnershipStartDate", []),
                "HeadOffice": [{"Country": s.get("Country")} for s in owner.get("Sanctions", [])],#只取HeadOffice的Country
                "Sanctions": [{
                                "SanctionSource": s.get("SanctionSource"),
                                "SanctionStartDate": s.get("SanctionStartDate"),
                                "SanctionEndDate": s.get("SanctionEndDate")
                            } for s in owner.get("Sanctions", [])],    # 取Sanctions的数据
                "HeadOfficeBasedInSanctionedCountry": owner.get("HeadOfficeBasedInSanctionedCountry", False),
                "HasSanctionedVesselsInFleet": owner.get("HasSanctionedVesselsInFleet", False),
                "HeadOfficeBasedInSanctionedCountry": owner.get("HeadOfficeBasedInSanctionedCountry", False),
                "SanctionedVesselsFleet": [{
                                "VesselName": s.get("VesselName"),
                                "VesselImo": s.get("VesselImo")
                            } for s in owner.get("SanctionedVesselsFleet", [])],    # 取SanctionedVesselsFleet的三个月会员
                "RelatedSanctionedCompanies": [{
                                "CompanyImo": s.get("CompanyImo"),
                                "CompanyName": s.get("CompanyName")
                            } for s in owner.get("RelatedSanctionedCompanies", [])]    # 取SanctionedVesselsFleet的数据
            }
            sanctioned_owners.append(owner_data)

        # 合并数据
        final_data = {
            **vessel_info,
            **compliance_result,
            "SanctionedOwners": sanctioned_owners,  # 包含精确提取的字段
            "ProcessingTime": datetime.now().isoformat(),
            "VoyageDateRange": f"{start_date} to {end_date}"
        }

        # 转换为DataFrame
        df = pd.DataFrame([final_data])
        
        # 保存到Excel（嵌套结构会以JSON字符串形式存储）
        df.to_excel(output_file, index=False, sheet_name="VesselData")
        
        print(f"数据已保存到 {output_file}（仅包含指定字段）")


def get_vessel_company_sanction_info(vessel_imo: str, start_date: str, end_date: str, api_key: str) -> dict:
    """
    外部调用函数：获取船舶公司制裁信息
    参数:
        vessel_imo: 船舶IMO编号（字符串）
        start_date: 开始日期（格式：YYYY-MM-DD）
        end_date: 结束日期（格式：YYYY-MM-DD）
        api_key: Lloyd's List API授权令牌
    返回:
        包含船舶公司制裁信息的字典，格式如下：
        {
            "success": True/False,
            "data": {
                "VesselImo": "IMO号",
                "Mmsi": "MMSI",
                "VesselName": "船舶名称",
                "VesselType": "船舶类型",
                "RiskScores": {},  # 风险评分
                "VesselOwnershipContainsLinksToSanctionedEntities": True/False,
                "OwnerIsInSanctionedCountry": True/False,
                "OwnerIsCurrentlySanctioned": True/False,
                "OwnerHasHistoricalSanctions": True/False,
                "SanctionedOwners": [  # 受制裁船东列表
                    {
                        "CompanyName": "公司名称",
                        "CompanyImo": "公司IMO",
                        "OwnershipTypes": [],  # 所有权类型
                        "OwnershipStartDate": [],  # 所有权开始日期
                        "HeadOffice": [{"Country": "国家"}],  # 总部信息
                        "Sanctions": [  # 制裁信息
                            {
                                "SanctionSource": "制裁来源",
                                "SanctionStartDate": "制裁开始日期",
                                "SanctionEndDate": "制裁结束日期"
                            }
                        ],
                        "HeadOfficeBasedInSanctionedCountry": True/False,
                        "HasSanctionedVesselsInFleet": True/False,
                        "SanctionedVesselsFleet": [  # 受制裁船舶舰队
                            {
                                "VesselName": "船舶名称",
                                "VesselImo": "船舶IMO"
                            }
                        ],
                        "RelatedSanctionedCompanies": [  # 相关受制裁公司
                            {
                                "CompanyImo": "公司IMO",
                                "CompanyName": "公司名称"
                            }
                        ]
                    }
                ],
                "ProcessingTime": "处理时间",
                "VoyageDateRange": "航次日期范围"
            },
            "error": "错误信息（如果有）"
        }
    """
    try:
        # 创建VesselSanctionDataProcessor实例
        processor = VesselSanctionDataProcessor(api_key)
        
        # 获取原始数据
        compliance_data = processor.fetch_data("vesselcompliancescreening_v3", vessel_imo, start_date, end_date)
        risk_data = processor.fetch_data("vesselriskscore", vessel_imo, start_date, end_date)
        
        if not compliance_data or not risk_data:
            return {
                "success": False,
                "data": {},
                "error": "未获取到有效数据"
            }

        # 提取合规数据
        compliance_item = compliance_data.get("Data", {}).get("Items", [{}])[0]
        compliance_result = {
            "OwnerIsInSanctionedCountry": compliance_item.get("SanctionRisks", {}).get("OwnerIsInSanctionedCountry"),
            "OwnerIsCurrentlySanctioned": compliance_item.get("SanctionRisks", {}).get("OwnerIsCurrentlySanctioned"),
            "OwnerHasHistoricalSanctions": compliance_item.get("SanctionRisks", {}).get("OwnerHasHistoricalSanctions"),
            "ComplianceDataVersion": compliance_data.get("Data", {}).get("Version", "1.0")
        }

        # 提取风险数据
        risk_item = risk_data.get("Data", {}).get("Items", [{}])[0]
        vessel_info = {
            "VesselImo": risk_item.get("VesselImo"),
            "Mmsi": risk_item.get("Mmsi"),
            "VesselName": risk_item.get("VesselName"),
            "VesselType": risk_item.get("VesselType"),
            "RiskScores": risk_item.get("RiskScores", {}),
            "VesselOwnershipContainsLinksToSanctionedEntities": risk_item.get("VesselOwnershipContainsLinksToSanctionedEntities", False)
        }

        # 精确提取 SanctionedOwners 的字段
        sanctioned_owners = []
        for owner in risk_item.get("SanctionedOwners", []):
            owner_data = {
                "CompanyName": owner.get("CompanyName"),
                "CompanyImo": owner.get("CompanyImo"),
                "OwnershipTypes": owner.get("OwnershipTypes", []),
                "OwnershipStartDate": owner.get("OwnershipStartDate", []),
                "HeadOffice": [{"Country": s.get("Country")} for s in owner.get("Sanctions", [])],
                "Sanctions": [{
                    "SanctionSource": s.get("SanctionSource"),
                    "SanctionStartDate": s.get("SanctionStartDate"),
                    "SanctionEndDate": s.get("SanctionEndDate")
                } for s in owner.get("Sanctions", [])],
                "HeadOfficeBasedInSanctionedCountry": owner.get("HeadOfficeBasedInSanctionedCountry", False),
                "HasSanctionedVesselsInFleet": owner.get("HasSanctionedVesselsInFleet", False),
                "SanctionedVesselsFleet": [{
                    "VesselName": s.get("VesselName"),
                    "VesselImo": s.get("VesselImo")
                } for s in owner.get("SanctionedVesselsFleet", [])],
                "RelatedSanctionedCompanies": [{
                    "CompanyImo": s.get("CompanyImo"),
                    "CompanyName": s.get("CompanyName")
                } for s in owner.get("RelatedSanctionedCompanies", [])]
            }
            sanctioned_owners.append(owner_data)

        # 合并数据
        final_data = {
            **vessel_info,
            **compliance_result,
            "SanctionedOwners": sanctioned_owners,
            "ProcessingTime": datetime.now().isoformat(),
            "VoyageDateRange": f"{start_date} to {end_date}"
        }
        
        return {
            "success": True,
            "data": final_data,
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
    test_imo = "9577082"
    test_start_date = "2024-08-21"
    test_end_date = "2025-08-21"
    test_api_key = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTQ5Nzk2MTgsImlhdCI6MTc1NDk3OTYxOCwiZXhwIjoxNzU3NTcxNjE4LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NDk3OTYxOCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRTnNzX1F3T3IzT3E1blouZXBxR0tOcUNaWmRyNENyT2xlVVVSNklvTWRLUDBHcGZDV2swRGdrRnlQSmJtaUVjTGtsMFVPV1FTX2l3VmhvWEd3WksxamFWTDI3USIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJUcmlhbCI6WyJjb21wYW55c2FuY3Rpb25zIiwiY29tcGFueXJlcG9ydCIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrIiwic2FuY3Rpb25zZWFyY2giLCJ2ZXNzZWxzYW5jdGlvbnMiLCJ2ZXNzZWxyZXBvcnQiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydGF1ZGl0IiwidmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnQiLCJjb21wYW55ZmxlZXRkZXRhaWxzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlhcmNhcGkiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwibGxpcmNhcGkiLCJTZWFzZWFyY2hlciJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTI5I1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTI5I1RydWUiLCJjb21wYW55c2FuY3Rpb25zI2NvbXBhbnlzYW5jdGlvbnMjMjAyNS0wOS0xMSNUcnVlIiwiY29tcGFueXJlcG9ydCNjb21wYW55cmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsImFpc3Bvc2l0aW9uZ2FwaGlzdG9yeSNhaXNwb3NpdGlvbmdhcGhpc3RvcnkjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2VsY29tcGxpYW5jZXJpc2sjdmVzc2VsY29tcGxpYW5jZXJpc2sjMjAyNS0wOS0xMSNUcnVlIiwic2FuY3Rpb25zZWFyY2gjc2FuY3Rpb25zZWFyY2gjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc2FuY3Rpb25zI3Zlc3NlbHNhbmN0aW9ucyMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxyZXBvcnQjdmVzc2VscmVwb3J0IzIwMjUtMDktMTEjVHJ1ZSIsInZlc3NlbGNvbXBsaWFuY2VyaXNrcmVwb3J0YXVkaXQjdmVzc2VsY29tcGxpYW5jZXJpc2tyZXBvcnRhdWRpdCMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCN2ZXNzZWxjb21wbGlhbmNlcmlza3JlcG9ydCMyMDI1LTA5LTExI1RydWUiLCJjb21wYW55ZmxlZXRkZXRhaWxzI2NvbXBhbnlmbGVldGRldGFpbHMjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNS0wOS0xMSNUcnVlIiwiQWR2YW5jZWQgUlx1MDAyNkMgQVBJI2xsaWFyY2FwaSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNS0wOS0xMSNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI1LTA5LTExI1RydWUiLCJ2ZXNzZWx2b3lhZ2VldmVudHMjdmVzc2Vsdm95YWdlZXZlbnRzIzIwMjUtMDktMTEjVHJ1ZSIsIlJpc2sgXHUwMDI2IENvbXBsaWFuY2UgQVBJI2xsaXJjYXBpIzIwMjUtMDktMTEjVHJ1ZSIsIlNlYXNlYXJjaGVyI1NlYXNlYXJjaGVyIzIwMjUtMDktMTEjVHJ1ZSJdLCJ1c2VybmFtZSI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJ1c2VySWQiOiIwMDVOejAwMDAwQ2k5R25JQUoiLCJjb250YWN0QWNjb3VudElkIjoiMDAxTnowMDAwMEthQkpESUEzIiwidXNlclR5cGUiOiJDc3BMaXRlUG9ydGFsIiwiZW1haWwiOiJjaGFuZy54aW55dWFuQGNvc2Nvc2hpcHBpbmcuY29tIiwiZ2l2ZW5fbmFtZSI6Ilhpbnl1YW4iLCJmYW1pbHlfbmFtZSI6IkNoYW5nIiwic2hpcFRvIjoiIiwianRpIjoiQ0UwRUExMzkyMTNBNjk0QzFEMDFENDg1NTEyMzdGRUMifQ.ISS2PlKd3ecndjgIk4Zmeuh01DpnWAXGCPlOfcK_K2RyDHj8Irp52u9IIEDm2Urazs_qcqGjQl2o097hFjZX-4i_H58lC3dFUtkZIpJAQ-t4cLLNt1wvzo20m7nIwjffhsoPyPAhmdDxdpmpP42MABD09XeAfcCHCGnh2L2gKomuqHpBivByAJ7tHs5x0oHAiroqXi2TVJTPQkH-mvveqHIiZFtS_SBDdteGeX6LNfQ1rPGfEIG-eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9"
    
    # 使用外部函数
    result = get_vessel_company_sanction_info(test_imo, test_start_date, test_end_date, test_api_key)
    print(json.dumps(result, indent=2, ensure_ascii=False))