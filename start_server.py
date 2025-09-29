#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
船舶信息综合API服务启动脚本 (开发环境)

使用方法:
    python start_server.py
    python start_server.py --workers 4 --threads 4
    python start_server.py --host 0.0.0.0 --port 8000 --workers 6

功能:
    启动整合了海事数据综合API和业务逻辑API的统一服务
    支持多进程+多线程并发处理，提高性能
"""

import argparse
import sys
import os
import logging
import multiprocessing
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Depends, Request
import psutil

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def setup_logging():
    """配置开发环境日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            # logging.FileHandler(f'server_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8')  # 注释掉文件日志
        ]
    )
    return logging.getLogger(__name__)


def get_optimal_workers():
    """获取最优的工作进程数"""
    cpu_count = multiprocessing.cpu_count()
    memory = psutil.virtual_memory()

    # 根据内存情况动态调整
    if memory.total >= 8 * 1024 ** 3:  # 8GB以上内存
        # 使用2-3个进程，平衡内存和性能
        return min(3, cpu_count // 2)
    elif memory.total >= 4 * 1024 ** 3:  # 4GB以上内存
        # 使用2个进程
        return 2
    else:
        # 内存不足时使用单进程
        return 1


def get_system_info():
    """获取系统信息"""
    cpu_count = multiprocessing.cpu_count()
    memory = psutil.virtual_memory()
    return {
        "cpu_count": cpu_count,
        "memory_total": f"{memory.total / (1024 ** 3):.1f}GB",
        "memory_available": f"{memory.available / (1024 ** 3):.1f}GB",
        "memory_percent": memory.percent
    }


def check_dependencies():
    """检查依赖"""
    required_modules = [
        ('fastapi', 'fastapi'),
        ('uvicorn', 'uvicorn'),
        ('pandas', 'pandas'),
        ('requests', 'requests'),
        ('psycopg2', 'psycopg2'),
        ('sqlalchemy', 'sqlalchemy'),
        ('beautifulsoup4', 'bs4'),  # beautifulsoup4安装后的模块名是bs4
        ('python-dotenv', 'dotenv')
    ]

    missing_modules = []
    for package_name, module_name in required_modules:
        try:
            __import__(module_name)
        except ImportError:
            missing_modules.append(package_name)

    if missing_modules:
        print(f"[ERROR] 缺少以下依赖模块: {', '.join(missing_modules)}")
        print("请运行以下命令安装:")
        print(f"pip install {' '.join(missing_modules)}")
        return False

    return True


def check_files():
    """检查必要文件"""
    required_files = [
        'main_server.py',
        'maritime_api.py',
        'business_api.py',
        'external_api.py',
        'sts_bunkering_risk.py',
        'voyage_risk_log_insert.py',
        'vessle_charter_in_risk.py'  # 新增：船舶租入风险筛查脚本
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(f"[ERROR] 缺少以下文件: {', '.join(missing_files)}")
        return False

    return True


def start_server(host, port, workers=4, no_reload=False, max_restarts=5):
    """启动服务器（带自动重启机制）"""
    restart_count = 0

    while restart_count < max_restarts:
        try:
            import uvicorn
            import subprocess
            import sys
            import os
            import time

            print("[启动] 正在启动船舶信息综合API服务...")
            print(f"[地址] 主服务地址: http://{host}:{port}")
            print(f"[地址] 航次服务地址: http://{host}:{port}")
            print("[文档] API文档:")
            print(f"   - 主服务 Swagger UI: http://{host}:{port}/docs")
            print(f"   - 主服务 ReDoc: http://{host}:{port}/redoc")
            print(f"   - 航次服务 Swagger UI: http://{host}:{port}/docs")
            print(f"   - 航次服务 ReDoc: http://{host}:{port}/redoc")
            print("[信息] 服务信息:")
            print(f"   - 主服务状态: http://{host}:{port}/health")
            print(f"   - 主服务详细信息: http://{host}:{port}/info")
            print(f"   - 航次服务状态: http://{host}:{port}/health")
            print(f"   - 航次服务详细信息: http://{host}:{port}/info")
            print("[API] 海事API端点 (挂载在 /maritime 路径下):")
            print(f"   - 船舶列表: http://{host}:{port}/maritime/api/vessel_list?vessel_imo=IMO号")
            print(f"   - STS数据: http://{host}:{port}/maritime/api/sts_data?vessel_imo=IMO号")
            print(f"   - 船舶所有数据: http://{host}:{port}/maritime/api/get_vessel_all_data?vessel_imo=IMO号")
            print("[API] 航次服务接口端点:")
            print(f"   - 航次风险筛查: http://{host}:{port}/external/voyage_risk (POST)")
            print(f"   - 船舶基础信息: http://{host}:{port}/external/vessel_basic (POST)")
            print(f"   - 航次合规状态审批: http://{host}:{port}/external/voyage_approval (POST)")
            print("[API] STS风险筛查服务接口端点:")
            print(f"   - STS风险筛查: http://{host}:{port}/sts/risk_screen (POST)")
            # 新增：船舶租入风险筛查服务接口说明
            print("[API] 船舶租入风险筛查服务接口端点:")
            print(f"   - 船舶租入风险筛查: http://{host}:{port}/ship/charter-in/risk-screening (POST)")
            print("[说明] 使用说明:")
            print("   - 按 Ctrl+C 停止所有服务")
            print("   - 修改代码后服务会自动重启 (reload模式)")
            print("   - 注意：maritime_api的端点都在 /maritime 路径下")
            print("   - 注意：航次服务接口的端点都在 /external 路径下")
            print("   - 注意：STS风险筛查服务的端点都在 /sts 路径下")
            print("   - 注意：船舶租入风险筛查服务的端点在 /ship/charter-in 路径下")  # 新增：租入服务路径说明
            print("=" * 60)

            # 启动主服务（包含航次服务）
            config = {
                "app": "main_server:main_app",
                "host": host,
                "port": port,
                "log_level": "info",
                "access_log": True,
                "workers": workers,
                "limit_concurrency": 1000,
                "limit_max_requests": 100000,  # 设置一个很大的数值，而不是0
                "timeout_keep_alive": 1200,  # 增加到240秒，防止长时间请求超时（翻倍）
                "timeout_graceful_shutdown": 600,  # 优雅关闭超时（翻倍）
                "loop": "asyncio",
                # 确保日志正确输出
                "log_config": {
                    "version": 1,
                    "disable_existing_loggers": False,
                    "formatters": {
                        "default": {
                            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        },
                        "access": {
                            "format": "%(asctime)s - %(levelname)s - %(message)s",
                        },
                    },
                    "handlers": {
                        "default": {
                            "formatter": "default",
                            "class": "logging.StreamHandler",
                            "stream": "ext://sys.stdout",
                        },
                        "access": {
                            "formatter": "access",
                            "class": "logging.StreamHandler",
                            "stream": "ext://sys.stdout",
                        },
                    },
                    "loggers": {
                        "uvicorn": {
                            "handlers": ["default"],
                            "level": "INFO",
                            "propagate": False,
                        },
                        "uvicorn.access": {
                            "handlers": ["access"],
                            "level": "INFO",
                            "propagate": False,
                        },
                    },
                },
            }
            
            # Windows兼容性设置
            import platform
            if platform.system() == "Windows":
                # Windows系统特殊配置
                config["backlog"] = 2048
                # 在Windows上，如果workers > 1，可能需要调整一些参数
                if workers > 1:
                    print("[警告] Windows系统多进程模式可能不稳定，建议使用单进程模式")
                    print("[建议] 使用命令: python start_server.py --workers 1")

            if not no_reload and workers == 1:
                config["reload"] = True
                print("[重载] 主服务热重载已启用 (仅在单进程模式下)")

            try:
                uvicorn.run(**config)
            except KeyboardInterrupt:
                print("[停止] 收到中断信号，正在优雅关闭服务...")
                break
            except Exception as e:
                print(f"[错误] 服务运行异常: {e}")
                restart_count += 1
                if restart_count < max_restarts:
                    print(f"[重启] 5秒后进行第{restart_count}次重启...")
                    time.sleep(5)
                    continue
                else:
                    print(f"[错误] 已达到最大重启次数({max_restarts})，停止重启")
                    return False
            finally:
                # 服务已停止
                print("[停止] 服务已停止")

            return True

        except ImportError as e:
            print(f"[ERROR] 导入模块失败: {e}")
            print("请确保已安装所有依赖")
            return False
        except Exception as e:
            print(f"[ERROR] 启动服务失败: {e}")
            restart_count += 1
            if restart_count < max_restarts:
                print(f"[重启] 5秒后进行第{restart_count}次重启...")
                time.sleep(5)
                continue
            else:
                print(f"[错误] 已达到最大重启次数({max_restarts})，停止重启")
                return False

    return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="船舶信息综合API服务启动脚本 (开发环境)")
    parser.add_argument("--host", default="0.0.0.0", help="服务器主机地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口 (默认: 8000)")
    parser.add_argument("--workers", type=int, help="工作进程数 (默认: 自动检测)")
    parser.add_argument("--threads", type=int, default=4, help="每个进程的线程数 (默认: 4)")
    parser.add_argument("--no-reload", action="store_true", help="禁用热重载")
    parser.add_argument("--check-only", action="store_true", help="仅检查依赖和文件，不启动服务")

    args = parser.parse_args()

    # 设置日志
    logger = setup_logging()

    # 获取最优工作进程数
    if args.workers is None:
        args.workers = 3  # 固定3个worker进程

    print("[INFO] 船舶信息综合API服务启动检查 (开发环境)")
    print("=" * 60)

    # 显示系统信息
    system_info = get_system_info()
    print(f"[系统] 系统信息:")
    print(f"   - CPU核心数: {system_info['cpu_count']}")
    print(f"   - 总内存: {system_info['memory_total']}")
    print(f"   - 可用内存: {system_info['memory_available']}")
    print(f"   - 内存使用率: {system_info['memory_percent']:.1f}%")

    print(f"[并发] 并发配置:")
    print(f"   - 工作进程数: {args.workers}")
    print(f"   - 每进程线程数: {args.threads}")
    print(f"   - 总并发能力: {args.workers * args.threads}")

    # 检查依赖
    print("[检查] 检查Python依赖...")
    if not check_dependencies():
        sys.exit(1)
    print("[OK] 依赖检查通过")

    # 检查文件
    print("[检查] 检查必要文件...")
    if not check_files():
        sys.exit(1)
    print("[OK] 文件检查通过")

    if args.check_only:
        print("[OK] 所有检查通过，服务可以正常启动")
        return

    # 启动服务
    print("[启动] 开始启动开发环境服务...")
    success = start_server(args.host, args.port, args.workers, args.no_reload)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()