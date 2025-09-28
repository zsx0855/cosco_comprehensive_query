#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
内存使用监控脚本

用于监控多进程模式下的内存使用情况
"""

import psutil
import time
import os
import sys
from datetime import datetime

def get_memory_info():
    """获取系统内存信息"""
    memory = psutil.virtual_memory()
    return {
        "total": memory.total / (1024**3),  # GB
        "available": memory.available / (1024**3),  # GB
        "used": memory.used / (1024**3),  # GB
        "percent": memory.percent
    }

def get_process_memory(process_name="python"):
    """获取指定进程的内存使用"""
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                memory_mb = proc.info['memory_info'].rss / (1024**2)
                processes.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'memory_mb': memory_mb,
                    'memory_gb': memory_mb / 1024
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return processes

def monitor_memory_usage(duration=60, interval=5):
    """监控内存使用情况"""
    print("🔍 内存使用监控")
    print("="*60)
    print(f"监控时长: {duration}秒")
    print(f"监控间隔: {interval}秒")
    print("="*60)
    
    start_time = time.time()
    max_memory = 0
    max_processes = 0
    
    while time.time() - start_time < duration:
        # 获取系统内存
        mem_info = get_memory_info()
        
        # 获取Python进程内存
        python_processes = get_process_memory("python")
        uvicorn_processes = get_process_memory("uvicorn")
        all_processes = python_processes + uvicorn_processes
        
        # 计算总内存
        total_process_memory = sum(p['memory_mb'] for p in all_processes)
        
        # 更新最大值
        if mem_info['used'] > max_memory:
            max_memory = mem_info['used']
        if len(all_processes) > max_processes:
            max_processes = len(all_processes)
        
        # 打印当前状态
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{timestamp}] 内存使用情况:")
        print(f"  系统总内存: {mem_info['total']:.1f}GB")
        print(f"  系统已用: {mem_info['used']:.1f}GB ({mem_info['percent']:.1f}%)")
        print(f"  系统可用: {mem_info['available']:.1f}GB")
        print(f"  Python进程数: {len(all_processes)}")
        print(f"  进程总内存: {total_process_memory:.1f}MB ({total_process_memory/1024:.2f}GB)")
        
        if all_processes:
            print("  各进程详情:")
            for proc in all_processes:
                print(f"    PID {proc['pid']}: {proc['memory_mb']:.1f}MB ({proc['memory_gb']:.2f}GB)")
        
        # 等待下次监控
        time.sleep(interval)
    
    # 打印总结
    print("\n" + "="*60)
    print("📊 监控总结")
    print("="*60)
    print(f"最大系统内存使用: {max_memory:.1f}GB")
    print(f"最大进程数: {max_processes}")
    print(f"平均进程内存: {total_process_memory/max(1, len(all_processes)):.1f}MB")

def estimate_multi_process_memory(workers):
    """估算多进程模式的内存使用"""
    print(f"\n💡 {workers}进程模式内存估算:")
    
    # 单进程基础内存（不含COSCO数据）
    base_memory = 700  # MB
    
    # COSCO数据内存
    cosco_memory = 900  # MB
    
    # 每个进程的内存
    per_process = base_memory + cosco_memory  # 1600MB
    
    # 总内存
    total_memory = per_process * workers
    
    print(f"  单进程内存: {per_process}MB ({per_process/1024:.1f}GB)")
    print(f"  总内存使用: {total_memory}MB ({total_memory/1024:.1f}GB)")
    
    # 检查是否超出系统内存
    mem_info = get_memory_info()
    available_gb = mem_info['available']
    
    if total_memory/1024 > available_gb:
        print(f"  ⚠️  警告: 预估内存 ({total_memory/1024:.1f}GB) 超过可用内存 ({available_gb:.1f}GB)")
        print(f"  💡 建议: 减少进程数或增加系统内存")
    else:
        print(f"  ✅ 内存使用合理，可用内存充足")

if __name__ == "__main__":
    print("💾 内存使用监控工具")
    print("="*60)
    
    # 估算不同进程数的内存使用
    for workers in [1, 2, 3]:
        estimate_multi_process_memory(workers)
    
    print("\n" + "="*60)
    
    # 询问是否开始监控
    try:
        choice = input("是否开始实时监控内存使用？(y/n): ").lower()
        if choice == 'y':
            duration = int(input("监控时长(秒，默认60): ") or "60")
            interval = int(input("监控间隔(秒，默认5): ") or "5")
            monitor_memory_usage(duration, interval)
        else:
            print("监控已取消")
    except KeyboardInterrupt:
        print("\n监控已停止")
    except ValueError:
        print("输入无效，使用默认值")
        monitor_memory_usage()
