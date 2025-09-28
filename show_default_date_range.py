#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
显示默认的start_date和end_date时间
"""

from functions_risk_check_framework import get_default_date_range
from datetime import datetime, timedelta

def show_default_date_range():
    """显示默认日期范围"""
    
    print("=== 默认日期范围信息 ===\n")
    
    # 获取默认日期范围
    start_date, end_date = get_default_date_range()
    
    print(f"默认 start_date: {start_date}")
    print(f"默认 end_date: {end_date}")
    print()
    
    # 计算日期范围
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    days_diff = (end_dt - start_dt).days
    
    print(f"日期范围: {days_diff} 天")
    print(f"开始日期: {start_dt.strftime('%Y年%m月%d日')}")
    print(f"结束日期: {end_dt.strftime('%Y年%m月%d日')}")
    print()
    
    # 显示当前时间
    now = datetime.now()
    print(f"当前时间: {now.strftime('%Y年%m月%d日 %H:%M:%S')}")
    print()
    
    # 显示计算逻辑
    print("=== 计算逻辑 ===")
    print("end_date = 当前日期")
    print("start_date = 当前日期 - 365天")
    print("即：查询近一年的数据")
    print()
    
    # 显示具体示例
    print("=== 具体示例 ===")
    print("假设今天是 2024年12月25日：")
    print("- end_date = 2024-12-25")
    print("- start_date = 2023-12-25")
    print("- 查询范围：2023年12月25日 到 2024年12月25日")
    print()
    
    # 显示实际值
    print("=== 实际值（基于当前时间）===")
    print(f"- end_date = {end_date}")
    print(f"- start_date = {start_date}")
    print(f"- 查询范围：{start_date} 到 {end_date}")

if __name__ == "__main__":
    show_default_date_range()
