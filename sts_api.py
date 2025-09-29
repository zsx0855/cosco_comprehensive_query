#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STS船舶对接历史API服务

基于FastAPI框架，提供船舶STS对接历史查询服务
数据来源：Lloyd's List Intelligence API

主要功能:
- 查询船舶STS对接历史
- 获取船舶风险评级信息
- 分析活动区域和合规风险
- 提供船舶配对详细信息
"""

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import requests
import json
import pandas as pd
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
sts_app = FastAPI(
    title="STS船舶对接历史API服务",
    version="1.0.0",
    description="""
    # STS船舶对接历史API服务
    
    基于Lloyd's List Intelligence API，提供船舶STS（Ship-to-Ship）对接历史查询服务。
    
    ## 🚢 主要功能
    - **船舶STS对接历史查询**: 根据IMO编号查询船舶的STS对接记录
    - **船舶风险评级**: 获取船舶的风险评级和合规状态
    - **活动区域分析**: 分析船舶活动的地理区域和风险
    - **船舶配对信息**: 获取STS对接中涉及的船舶详细信息
    
    ## 📊 数据字段说明
    - **主体船舶信息**: IMO、船名、风险评级、船旗、载重吨位、船舶类型等
    - **活动信息**: 活动开始/结束时间、活动区域、合规风险评分等
    - **船舶配对**: 参与STS对接的其他船舶信息
    
    ## 🔍 使用示例
    ```
    GET /sts/{vessel_imo}
    GET /sts/9567233
    ```
    
    ## 🌐 数据来源
    - Lloyd's List Intelligence API
    - 实时船舶数据
    - 合规风险评估
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
sts_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 导入配置
from kingbase_config import get_lloyds_token

# API配置
API_URL = "https://api.lloydslistintelligence.com/v1/vesselstspairings_v2"
API_TOKEN = get_lloyds_token()

# 数据模型
class VesselPairing(BaseModel):
    """船舶配对信息模型"""
    Imo: Optional[str] = Field(None, description="船舶IMO编号")
    VesselName: Optional[str] = Field(None, description="船舶名称")
    RiskRating: Optional[str] = Field(None, description="风险评级")
    Flag: Optional[str] = Field(None, description="船旗")
    DwtTonnage: Optional[float] = Field(None, description="载重吨位")
    VesselType: Optional[str] = Field(None, description="船舶类型")
    StsType: Optional[str] = Field(None, description="STS类型")
    DraftStart: Optional[float] = Field(None, description="开始吃水")
    DraftEnd: Optional[float] = Field(None, description="结束吃水")
    SogStart: Optional[float] = Field(None, description="开始速度")
    SogEnd: Optional[float] = Field(None, description="结束速度")

class STSDataItem(BaseModel):
    """STS数据项模型"""
    # 主体船舶信息
    VesselImo: Optional[str] = Field(None, description="主体船舶IMO编号")
    VesselName: Optional[str] = Field(None, description="主体船舶名称")
    VesselRiskRating: Optional[str] = Field(None, description="主体船舶风险评级")
    VesselFlag: Optional[str] = Field(None, description="主体船舶船旗")
    VesselDwtTonnage: Optional[float] = Field(None, description="主体船舶载重吨位")
    VesselType: Optional[str] = Field(None, description="主体船舶类型")
    StsType: Optional[str] = Field(None, description="主体船舶STS类型")
    VesselDraftStart: Optional[float] = Field(None, description="主体船舶开始吃水")
    VesselDraftEnd: Optional[float] = Field(None, description="主体船舶结束吃水")
    VesselSogStart: Optional[float] = Field(None, description="主体船舶开始速度")
    VesselSogEnd: Optional[float] = Field(None, description="主体船舶结束速度")
    
    # 活动信息
    ActivityStartDate: Optional[str] = Field(None, description="活动开始日期")
    ActivityEndDate: Optional[str] = Field(None, description="活动结束日期")
    ActivityAreaName: Optional[str] = Field(None, description="活动区域名称")
    ComplianceRiskScore: Optional[float] = Field(None, description="合规风险评分")
    ComplianceRiskReason: Optional[str] = Field(None, description="合规风险原因")
    NearestPlaceName: Optional[str] = Field(None, description="最近地点名称")
    NearestPlaceCountry: Optional[str] = Field(None, description="最近地点国家")
    
    # 船舶配对信息
    VesselPairings: List[VesselPairing] = Field(default_factory=list, description="船舶配对列表")

class STSResponse(BaseModel):
    """STS查询响应模型"""
    success: bool = Field(description="查询是否成功")
    message: str = Field(description="响应消息")
    data: List[STSDataItem] = Field(description="STS数据列表")
    total_count: int = Field(description="数据总数")
    query_time: str = Field(description="查询时间")
    vessel_imo: str = Field(description="查询的船舶IMO")

class ErrorResponse(BaseModel):
    """错误响应模型"""
    success: bool = Field(False, description="查询失败")
    message: str = Field(description="错误消息")
    error_code: Optional[str] = Field(None, description="错误代码")
    timestamp: str = Field(description="错误时间")

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
        logger.error(f"请求API时出错: {e}")
        raise HTTPException(status_code=500, detail=f"API请求失败: {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"解析JSON响应时出错: {e}")
        raise HTTPException(status_code=500, detail=f"JSON解析失败: {str(e)}")

# API路由
@sts_app.get("/", response_model=Dict[str, str])
async def root():
    """根路径，返回服务信息"""
    return {
        "service": "STS船舶对接历史API服务",
        "version": "1.0.0",
        "description": "基于Lloyd's List Intelligence API的船舶STS对接历史查询服务",
        "endpoints": {
            "health": "/health",
            "info": "/info",
            "sts": "/sts/{vessel_imo}",
            "docs": "/docs"
        }
    }

@sts_app.get("/health", response_model=Dict[str, str])
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "service": "STS船舶对接历史API服务",
        "timestamp": datetime.now().isoformat()
    }

@sts_app.get("/info", response_model=Dict[str, Any])
async def service_info():
    """服务信息接口"""
    return {
        "service_name": "STS船舶对接历史API服务",
        "version": "1.0.0",
        "description": "提供船舶STS对接历史查询服务",
        "data_source": "Lloyd's List Intelligence API",
        "features": [
            "船舶STS对接历史查询",
            "船舶风险评级信息",
            "活动区域和合规风险分析",
            "船舶配对详细信息"
        ],
        "api_endpoints": {
            "sts_query": "/sts/{vessel_imo}",
            "health_check": "/health",
            "service_info": "/info"
        },
        "supported_formats": ["JSON"],
        "rate_limit": "根据Lloyd's API限制",
        "last_updated": datetime.now().isoformat()
    }

@sts_app.get("/sts/{vessel_imo}", response_model=STSResponse)
async def get_sts_history(
    vessel_imo: str = Path(..., description="船舶IMO编号", example="9567233"),
    format: Optional[str] = Query(None, description="响应格式", example="json")
):
    """
    查询船舶STS对接历史
    
    参数:
        vessel_imo: 船舶IMO编号
        format: 响应格式（可选，目前只支持JSON）
    
    返回:
        STS对接历史数据，包括船舶信息、活动信息和配对信息
    """
    try:
        logger.info(f"查询船舶IMO {vessel_imo} 的STS历史")
        
        # 验证IMO格式
        if not vessel_imo.isdigit() or len(vessel_imo) != 7:
            raise HTTPException(
                status_code=400, 
                detail="无效的IMO编号格式，IMO应该是7位数字"
            )
        
        # 获取STS数据
        sts_data = get_sts_data(API_URL, API_TOKEN, vessel_imo)
        
        # 构建响应
        response = STSResponse(
            success=True,
            message=f"成功获取船舶IMO {vessel_imo} 的STS历史数据",
            data=sts_data,
            total_count=len(sts_data),
            query_time=datetime.now().isoformat(),
            vessel_imo=vessel_imo
        )
        
        logger.info(f"成功查询船舶IMO {vessel_imo}，返回 {len(sts_data)} 条记录")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询船舶IMO {vessel_imo} 时发生未知错误: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"服务器内部错误: {str(e)}"
        )

@sts_app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """HTTP异常处理器"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            success=False,
            message=exc.detail,
            error_code=str(exc.status_code),
            timestamp=datetime.now().isoformat()
        ).dict()
    )

@sts_app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """通用异常处理器"""
    logger.error(f"未处理的异常: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False,
            message="服务器内部错误",
            error_code="INTERNAL_ERROR",
            timestamp=datetime.now().isoformat()
        ).dict()
    )

# 如果直接运行此文件，启动开发服务器
if __name__ == "__main__":
    import uvicorn
    print("🚀 启动STS船舶对接历史API服务...")
    print("📍 服务地址: http://localhost:8000")
    print("📖 API文档: http://localhost:8000/docs")
    print("🔍 健康检查: http://localhost:8000/health")
    
    uvicorn.run(
        "sts_api:sts_app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

