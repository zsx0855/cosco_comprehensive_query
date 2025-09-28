#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能监控中间件

用于监控API接口的并发处理性能和响应时间
"""

import time
import asyncio
import logging
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import psutil
import threading
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

class PerformanceMonitorMiddleware(BaseHTTPMiddleware):
    """性能监控中间件"""
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.request_count = 0
        self.active_requests = 0
        self.response_times = defaultdict(list)
        self.error_count = 0
        self.start_time = time.time()
        self._lock = threading.Lock()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """处理请求并记录性能指标"""
        start_time = time.time()
        
        with self._lock:
            self.request_count += 1
            self.active_requests += 1
        
        try:
            # 记录请求开始
            logger.info(f"🚀 请求开始 - {request.method} {request.url.path} - 活跃请求: {self.active_requests}")
            
            # 处理请求
            response = await call_next(request)
            
            # 计算响应时间
            response_time = time.time() - start_time
            
            # 记录性能指标
            with self._lock:
                self.response_times[request.url.path].append(response_time)
                # 只保留最近100个请求的时间
                if len(self.response_times[request.url.path]) > 100:
                    self.response_times[request.url.path] = self.response_times[request.url.path][-100:]
            
            # 记录请求完成
            logger.info(f"✅ 请求完成 - {request.method} {request.url.path} - 响应时间: {response_time:.3f}s")
            
            return response
            
        except Exception as e:
            # 记录错误
            with self._lock:
                self.error_count += 1
            
            logger.error(f"❌ 请求错误 - {request.method} {request.url.path} - 错误: {str(e)}")
            raise
        finally:
            with self._lock:
                self.active_requests -= 1
    
    def get_stats(self):
        """获取当前统计信息"""
        with self._lock:
            return {
                "uptime": time.time() - self.start_time,
                "total_requests": self.request_count,
                "active_requests": self.active_requests,
                "error_count": self.error_count,
                "avg_response_times": {
                    path: sum(times) / len(times) if times else 0
                    for path, times in self.response_times.items()
                },
                "system": {
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "memory_available": psutil.virtual_memory().available / (1024**3)
                }
            }

# 全局监控实例
performance_monitor = None

def get_performance_stats():
    """获取性能统计信息的API端点"""
    if performance_monitor:
        return performance_monitor.get_stats()
    return {"error": "性能监控未启用"}
