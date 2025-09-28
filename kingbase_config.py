#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kingbase数据库配置文件
统一管理所有数据库连接配置和API Token配置
"""

import psycopg2
from psycopg2.extras import RealDictCursor

# Kingbase数据库配置
KINGBASE_CONFIG = {
    'host': '10.11.142.145',
    'port': 54321,
    'database': 'lngdb',
    'user': 'system',
    'password': 'zV2,oB5%',
    'cursor_factory': RealDictCursor
}

# SQLAlchemy数据库URL
KINGBASE_URL = "postgresql://system:zV2,oB5%@10.11.142.145:54321/lngdb"

# 数据库连接池配置
DB_POOL_CONFIG = {
    'pool_pre_ping': True,
    'pool_size': 5,
    'max_overflow': 10,
    'echo': False  # 开发时显示SQL日志
}

def get_kingbase_config():
    """获取Kingbase配置"""
    return KINGBASE_CONFIG.copy()

def get_kingbase_url():
    """获取Kingbase SQLAlchemy URL"""
    return KINGBASE_URL

# ==================== API Token 配置 ====================
# 劳氏(Lloyd's) API Token
LLOYDS_API_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsIng1dCI6ImEzck1VZ01Gdjl0UGNsTGE2eUYzekFrZnF1RSIsInR5cCI6ImF0K2p3dCJ9.eyJpc3MiOiJodHRwOi8vbGxveWRzbGlzdGludGVsbGlnZW5jZS5jb20iLCJuYmYiOjE3NTc3Mzk2MDQsImlhdCI6MTc1NzczOTYwNCwiZXhwIjoxNzYwMzMxNjA0LCJzY29wZSI6WyJsbGl3ZWJhcGkiXSwiYW1yIjpbImN1c3RvbWVyQXBpX2dyYW50Il0sImNsaWVudF9pZCI6IkN1c3RvbWVyQXBpIiwic3ViIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsImF1dGhfdGltZSI6MTc1NzczOTYwNCwiaWRwIjoic2FsZXNmb3JjZSIsImFjY2Vzc1Rva2VuIjoiMDBEOGQwMDAwMDlvaTM4IUFRRUFRRkRpWE1qYk1idGtKdGoyTTdsWjdKa3kxV09iRmtMZjJuMm9Ed0dBcllObVlzeWpEXzN6NTVYWlpXTzJDdHM5cUh5clB3elhXUHdMTTN4OGlVd1F6RXBGZWhwNCIsInNlcnZpY2VJZCI6ImEyV056MDAwMDAyQ3FwaE1BQyIsImVudGl0bGVtZW50VHlwZSI6IkZ1bGwiLCJhY2NvdW50TmFtZSI6IkNvc2NvIFNoaXBwaW5nIEVuZXJneSBUcmFuc3BvcnRhdGlvbiIsInJvbGUiOlsiRmluYW5jZSIsIkxPTFMiLCJMTEkiLCJjYXJnb3Jpc2siLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nIiwidmVzc2Vscmlza3Njb3JlIiwidmVzc2Vsc2FuY3Rpb25zIiwidmVzc2Vsdm95YWdlZXZlbnRzIiwidmVzc2Vsc3RzcGFpcmluZ3MiLCJsbGlyY2FwaSJdLCJzdWJzY3JpcHRpb25JbmZvIjpbIlNlYXNlYXJjaGVyIENyZWRpdCBSaXNrI0ZpbmFuY2UjMjAyNi0wMS0zMCNUcnVlIiwiTGxveWRcdTAwMjdzIExpc3QjTE9MUyMyMDI2LTA4LTI5I1RydWUiLCJTZWFzZWFyY2hlciBBZHZhbmNlZCBSaXNrIFx1MDAyNiBDb21wbGlhbmNlI0xMSSMyMDI2LTA4LTMwI1RydWUiLCJDYXJnbyBSaXNrI2NhcmdvcmlzayMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxjb21wbGlhbmNlc2NyZWVuaW5nI3Zlc3NlbGNvbXBsaWFuY2VzY3JlZW5pbmcjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vscmlza3Njb3JlI3Zlc3NlbHJpc2tzY29yZSMyMDI2LTA4LTMwI1RydWUiLCJ2ZXNzZWxzYW5jdGlvbnMjdmVzc2Vsc2FuY3Rpb25zIzIwMjYtMDgtMzAjVHJ1ZSIsInZlc3NlbHZveWFnZWV2ZW50cyN2ZXNzZWx2b3lhZ2VldmVudHMjMjAyNi0wOC0zMCNUcnVlIiwidmVzc2Vsc3RzcGFpcmluZ3MjdmVzc2Vsc3RzcGFpcmluZ3MjMjAyNi0wOC0zMCNUcnVlIiwiUmlzayBcdTAwMjYgQ29tcGxpYW5jZSBBUEkjbGxpcmNhcGkjMjAyNi0wOC0zMCNUcnVlIl0sInVzZXJuYW1lIjoiY2hhbmcueGlueXVhbkBjb3Njb3NoaXBwaW5nLmNvbSIsInVzZXJJZCI6IjAwNU56MDAwMDBDaTlHbklBSiIsImNvbnRhY3RBY2NvdW50SWQiOiIwMDFOejAwMDAwS2FCSkRJQTMiLCJ1c2VyVHlwZSI6IkNzcExpdGVQb3J0YWwiLCJlbWFpbCI6ImNoYW5nLnhpbnl1YW5AY29zY29zaGlwcGluZy5jb20iLCJnaXZlbl9uYW1lIjoiWGlueXVhbiIsImZhbWlseV9uYW1lIjoiQ2hhbmciLCJzaGlwVG8iOiIiLCJqdGkiOiIyODBFRjE2RUIxRjI3QTAyNjM3MzU5Qjc5RDM2QTM5NyJ9.FSAlQrg2343Zo4Bc04CvE__gBx6Iwj8Hw5i8WFqJq_imZjL2sOK3sncwJjknSYulp60-Nn1w3-Jm_rjoe9UO4YYycngwoZWLSNVcx7NaxmKULeJPBPcdQSELKWsTgF8FiD9HWxK-AlTps1UNXteAj734rYAgRWOooMi18U21mNt-Q25ewjENfrEKmbqO7q-UjFr_mk0B7BnQK2y9C9Wr57KPV7GEMjktJubNwDkzd9TwxS-dZgxGAi9mZ0wTx9Q_L4IiopHltlS-AdudUbLFCy7RPdwmeNlFH0iBdRAJSJ1VVekcDqtfXKUXoMQfEc-Juy_8nNcWzTiHup5t-KIkpA"

# Kpler API Token
KPLER_API_TOKEN = "Basic ejdXOEkzSGFKOEJWdno0ZzRIdEZJZzJZUzR1VmJQOVA6YWZEZ2d0NG9mZFJDX0Yyd1lQUlNhbXhMZFdjMVlJdnlsX1ctYW1QRnV3QmI2SFNaOWtwSFZ4NlpaYmVyaHJnbQ=="

# API配置
API_CONFIG = {
    'lloyds': {
        'base_url': 'https://api.lloydslistintelligence.com/v1',
        'token': LLOYDS_API_TOKEN,
        'timeout': 120,
        'max_retries': 3,
        'retry_delay': 2
    },
    'kpler': {
        'base_url': 'https://api.kpler.com/v2',
        'token': KPLER_API_TOKEN,
        'timeout': 60,
        'max_retries': 2,
        'retry_delay': 1
    }
}

# ==================== 获取函数 ====================
def get_kingbase_config():
    """获取Kingbase配置"""
    return KINGBASE_CONFIG.copy()

def get_kingbase_url():
    """获取Kingbase SQLAlchemy URL"""
    return KINGBASE_URL

def get_db_pool_config():
    """获取数据库连接池配置"""
    return DB_POOL_CONFIG.copy()

def get_lloyds_token():
    """获取劳氏API Token"""
    return LLOYDS_API_TOKEN

def get_kpler_token():
    """获取Kpler API Token"""
    return KPLER_API_TOKEN

def get_api_config(api_name: str):
    """获取指定API的完整配置
    
    Args:
        api_name: API名称 ('lloyds' 或 'kpler')
    
    Returns:
        dict: API配置字典
    """
    return API_CONFIG.get(api_name, {}).copy()

def get_all_tokens():
    """获取所有API Token
    
    Returns:
        dict: 包含所有token的字典
    """
    return {
        'lloyds': LLOYDS_API_TOKEN,
        'kpler': KPLER_API_TOKEN
    }
