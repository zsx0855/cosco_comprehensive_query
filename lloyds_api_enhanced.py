"""
增强版劳氏API调用模块
包含更好的错误处理、重试机制和超时管理
"""

import requests
import time
import logging
from typing import Dict, Any, Optional
from api_config import LLOYDS_API_CONFIG, get_lloyds_session

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LloydsAPIEnhanced:
    """增强版劳氏API客户端"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = LLOYDS_API_CONFIG['base_url']
        self.headers = {
            "accept": "application/json",
            "Authorization": api_key
        }
        self.session = get_lloyds_session()
    
    def _make_request_with_retry(self, url: str, method: str = "GET", 
                                params: Optional[Dict] = None, 
                                data: Optional[Dict] = None) -> Optional[requests.Response]:
        """
        带重试机制的请求方法
        
        Args:
            url: 请求URL
            method: HTTP方法
            params: 查询参数
            data: 请求体数据
            
        Returns:
            Response对象或None（如果所有重试都失败）
        """
        max_retries = LLOYDS_API_CONFIG['max_retries']
        retry_delay = LLOYDS_API_CONFIG['retry_delay']
        
        for attempt in range(max_retries):
            try:
                logger.info(f"发送请求到 {url} (尝试 {attempt + 1}/{max_retries})")
                
                if method.upper() == "GET":
                    response = self.session.get(
                        url, 
                        headers=self.headers, 
                        params=params,
                        timeout=LLOYDS_API_CONFIG['timeout']
                    )
                elif method.upper() == "POST":
                    response = self.session.post(
                        url, 
                        headers=self.headers, 
                        params=params,
                        json=data,
                        timeout=LLOYDS_API_CONFIG['timeout']
                    )
                else:
                    raise ValueError(f"不支持的HTTP方法: {method}")
                
                # 检查HTTP状态码
                response.raise_for_status()
                
                logger.info(f"请求成功，状态码: {response.status_code}")
                return response
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt) if LLOYDS_API_CONFIG['exponential_backoff'] else retry_delay
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("所有重试都失败了")
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"连接错误 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt) if LLOYDS_API_CONFIG['exponential_backoff'] else retry_delay
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("所有重试都失败了")
                    return None
                    
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP错误: {str(e)}")
                return None
                
            except Exception as e:
                logger.error(f"未知错误: {str(e)}")
                return None
        
        return None
    
    def get_vessel_sanctions(self, vessel_imo: str) -> Optional[Dict[str, Any]]:
        """
        获取船舶制裁数据
        
        Args:
            vessel_imo: 船舶IMO号
            
        Returns:
            制裁数据字典或None
        """
        url = f"{self.base_url}/vesselsanctions_v2"
        params = {"vesselImo": vessel_imo}
        
        response = self._make_request_with_retry(url, params=params)
        
        if response is None:
            return None
        
        try:
            data = response.json()
            
            if not data.get("IsSuccess"):
                logger.error(f"API请求失败: {data.get('Errors', '未知错误')}")
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"解析响应数据失败: {str(e)}")
            return None
    
    def get_vessel_compliance(self, vessel_imo: str) -> Optional[Dict[str, Any]]:
        """
        获取船舶合规数据
        
        Args:
            vessel_imo: 船舶IMO号
            
        Returns:
            合规数据字典或None
        """
        url = f"{self.base_url}/vesselcompliance"
        params = {"vesselImo": vessel_imo}
        
        response = self._make_request_with_retry(url, params=params)
        
        if response is None:
            return None
        
        try:
            data = response.json()
            
            if not data.get("IsSuccess"):
                logger.error(f"API请求失败: {data.get('Errors', '未知错误')}")
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"解析响应数据失败: {str(e)}")
            return None
    
    def get_vessel_risks(self, vessel_imo: str) -> Optional[Dict[str, Any]]:
        """
        获取船舶风险数据
        
        Args:
            vessel_imo: 船舶IMO号
            
        Returns:
            风险数据字典或None
        """
        url = f"{self.base_url}/vesselrisks"
        params = {"vesselImo": vessel_imo}
        
        response = self._make_request_with_retry(url, params=params)
        
        if response is None:
            return None
        
        try:
            data = response.json()
            
            if not data.get("IsSuccess"):
                logger.error(f"API请求失败: {data.get('Errors', '未知错误')}")
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"解析响应数据失败: {str(e)}")
            return None
    
    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()

# 使用示例
if __name__ == "__main__":
    # 测试API调用
    api_key = "your_api_key_here"
    client = LloydsAPIEnhanced(api_key)
    
    try:
        # 测试获取制裁数据
        test_imo = "1234567"
        sanctions_data = client.get_vessel_sanctions(test_imo)
        
        if sanctions_data:
            print("成功获取制裁数据")
            print(f"数据项数量: {len(sanctions_data.get('Data', {}).get('items', []))}")
        else:
            print("获取制裁数据失败")
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
    finally:
        client.close()
