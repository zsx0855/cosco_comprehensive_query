#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kingbase数据库增强模块
用于为检查项添加risk_desc_info字段
支持数据库连接方式，自动查询风险描述信息
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional
import csv
import os

# Kingbase数据库配置（暂时禁用）
DB_USER = "system"
DB_PASSWORD = "zV2,oB5%"
DB_HOST = "10.11.142.145"
DB_PORT = 54321
DB_NAME = "lngdb"

class KingbaseEnhancer:
    """Kingbase数据库增强器"""
    
    def __init__(self):
        print("🚀 初始化KingbaseEnhancer...")
        self.connection = None
        self.connection_attempted = False  # 标记是否已经尝试过连接
        self.connection_success = False    # 标记连接是否成功
        self.data_cache = {}              # 数据缓存，避免重复查询
        self.csv_data = {}                # CSV数据缓存
        
        # 启动时就尝试连接数据库
        print("🔌 尝试连接数据库...")
        self._init_connection()
        
        # 如果数据库连接失败，尝试加载CSV文件
        if not self.connection_success:
            print("📄 数据库连接失败，尝试加载CSV文件...")
            self._load_csv_data()
        
        print(f"✅ KingbaseEnhancer初始化完成 - 数据库连接: {'成功' if self.connection_success else '失败'}, CSV数据: {len(self.csv_data)}条")
    
    def _init_connection(self):
        """初始化数据库连接"""
        try:
            self.connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                cursor_factory=RealDictCursor
            )
            self.connection_success = True
            print("✅ Kingbase数据库连接成功")
        except Exception as e:
            print(f"❌ Kingbase数据库连接失败: {str(e)}")
            self.connection_success = False
        finally:
            self.connection_attempted = True
    
    def _load_csv_data(self):
        """从CSV文件加载风险描述信息"""
        csv_file_path = "sanctions_des_info.csv"
        
        if not os.path.exists(csv_file_path):
            print(f"❌ CSV文件不存在: {csv_file_path}")
            return
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    risk_type = row.get('risk_type', '').strip()
                    risk_level = row.get('risk_level', '').strip()
                    risk_desc_info = row.get('risk_desc_info', '').strip()
                    info = row.get('info', '').strip()
                    
                    if risk_type and risk_level:
                        # 使用 (risk_type, risk_level) 作为键
                        key = (risk_type, risk_level)
                        self.csv_data[key] = {
                            'risk_desc_info': risk_desc_info,
                            'info': info
                        }
            
            print(f"✅ 成功从CSV文件加载 {len(self.csv_data)} 条风险描述信息")
            
        except Exception as e:
            print(f"❌ 读取CSV文件失败: {str(e)}")
    
    def connect(self) -> bool:
        """连接到Kingbase数据库"""
        # 如果从未尝试连接，先尝试连接
        if not self.connection_attempted:
            self._init_connection()
        
        # 如果连接成功，检查连接是否还活着
        if self.connection_success and self.connection:
            try:
                # 执行一个简单的查询来检查连接状态
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                return True
            except Exception as e:
                print(f"⚠️ 数据库连接已断开，尝试重连: {str(e)}")
                # 连接已断开，尝试重连
                self.connection_success = False
                self.connection = None
                self._init_connection()
                return self.connection_success
        
        return self.connection_success
    
    def disconnect(self):
        """断开数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def get_risk_desc_info(self, risk_type: str, risk_level: str) -> Dict[str, str]:
        """根据risk_type和risk_level获取risk_desc_info和info字段"""
        # 首先尝试从数据库获取
        if self.connect():
            try:
                with self.connection.cursor() as cursor:
                    # 查询风险描述信息和info字段（都来自同一张表）
                    cursor.execute("""
                        SELECT risk_desc_info, info 
                        FROM lng.sanctions_des_info 
                        WHERE risk_type = %s AND risk_level = %s
                        LIMIT 1
                    """, (risk_type, risk_level))
                    
                    result = cursor.fetchone()
                    if result:
                        return {
                            "risk_desc_info": result['risk_desc_info'] or "",
                            "info": result['info'] or ""
                        }
                        
            except Exception as e:
                print(f"❌ 查询风险描述失败: {str(e)}")
                # 如果查询失败，标记连接为失败状态，下次会尝试重连
                if "connection already closed" in str(e).lower() or "connection" in str(e).lower():
                    print("🔄 检测到连接问题，将在下次查询时重连")
                    self.connection_success = False
                    self.connection = None
                # 数据库查询失败，继续尝试CSV数据
        
        # 如果数据库连接失败或查询失败，尝试从CSV数据获取
        csv_key = (risk_type, risk_level)
        if csv_key in self.csv_data:
            csv_result = self.csv_data[csv_key]
            print(f"📄 从CSV文件获取风险描述: {risk_type} - {risk_level}")
            return {
                "risk_desc_info": csv_result['risk_desc_info'],
                "info": csv_result['info']
            }
        
        # 如果都没有找到，返回默认描述
        print(f"⚠️  未找到风险描述信息: {risk_type} - {risk_level}")
        return {
            "risk_desc_info": f"风险类型: {risk_type}, 风险等级: {risk_level}",
            "info": f"风险类型: {risk_type}, 风险等级: {risk_level}"
        }
    
    def enhance_check_items(self, check_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """为检查项列表添加risk_desc_info和info字段"""
        enhanced_items = []
        
        for item in check_items:
            enhanced_item = item.copy()
            
            # 获取risk_desc_info和info
            risk_type = item.get("name", "")
            sanctions_lev = item.get("sanctions_lev", "")
            
            if risk_type and sanctions_lev:
                result = self.get_risk_desc_info(risk_type, sanctions_lev)
                enhanced_item["risk_desc_info"] = result["risk_desc_info"]
                enhanced_item["info"] = result["info"]
            else:
                enhanced_item["risk_desc_info"] = ""
                enhanced_item["info"] = ""
            
            enhanced_items.append(enhanced_item)
        
        return enhanced_items
    
    def enhance_risk_groups(self, risk_groups: Any) -> Any:
        """为风险分组添加risk_desc_info字段（支持新的按title分组结构）"""
        if isinstance(risk_groups, list):
            # 新的数组格式
            enhanced_groups = []
            for group_item in risk_groups:
                enhanced_group_item = group_item.copy()
                
                # 为每个检查项添加risk_desc_info和info字段
                for key, value in group_item.items():
                    if key == "risk_items" and isinstance(value, list):
                        # 处理risk_items数组中的每个检查项
                        enhanced_risk_items = []
                        for item in value:
                            if isinstance(item, dict):
                                enhanced_item = item.copy()
                                risk_type = enhanced_item.get("name", "")
                                sanctions_lev = enhanced_item.get("sanctions_lev", "")
                                
                                if risk_type and sanctions_lev:
                                    result = self.get_risk_desc_info(risk_type, sanctions_lev)
                                    enhanced_item["risk_desc_info"] = result["risk_desc_info"]
                                    enhanced_item["info"] = result["info"]
                                else:
                                    enhanced_item["risk_desc_info"] = ""
                                    enhanced_item["info"] = ""
                                
                                enhanced_risk_items.append(enhanced_item)
                            else:
                                enhanced_risk_items.append(item)
                        
                        enhanced_group_item[key] = enhanced_risk_items
                    # 注意：不再处理其他字段，避免在group_item级别添加risk_desc_info和info
                
                enhanced_groups.append(enhanced_group_item)
            
            return enhanced_groups
        elif isinstance(risk_groups, dict):
            # 字典格式 - 处理risk_groups字典（包含high_risk, mid_risk, no_risk等键）
            enhanced_groups = {}
            
            for risk_level, risk_list in risk_groups.items():
                if isinstance(risk_list, list):
                    # 检查列表中的项目是否直接是风险项（有name和sanctions_lev字段）
                    # 还是包含risk_items字段的组对象
                    if risk_list and isinstance(risk_list[0], dict):
                        if "name" in risk_list[0] and "sanctions_lev" in risk_list[0]:
                            # 直接的风险项列表，需要为每个项目添加risk_desc_info和info
                            enhanced_risk_items = []
                            for item in risk_list:
                                if isinstance(item, dict):
                                    enhanced_item = item.copy()
                                    risk_type = enhanced_item.get("name", "")
                                    sanctions_lev = enhanced_item.get("sanctions_lev", "")
                                    
                                    if risk_type and sanctions_lev:
                                        result = self.get_risk_desc_info(risk_type, sanctions_lev)
                                        enhanced_item["risk_desc_info"] = result["risk_desc_info"]
                                        enhanced_item["info"] = result["info"]
                                    else:
                                        enhanced_item["risk_desc_info"] = ""
                                        enhanced_item["info"] = ""
                                    
                                    enhanced_risk_items.append(enhanced_item)
                                else:
                                    enhanced_risk_items.append(item)
                            
                            enhanced_groups[risk_level] = enhanced_risk_items
                        else:
                            # 这是新的数组格式（high_risk, mid_risk, no_risk等键对应的值）
                            enhanced_groups[risk_level] = self.enhance_risk_groups(risk_list)
                    else:
                        # 其他格式，直接返回
                        enhanced_groups[risk_level] = risk_list
                else:
                    # 其他格式，直接返回
                    enhanced_groups[risk_level] = risk_list
            
            return enhanced_groups
        else:
            # 其他类型，直接返回
            return risk_groups

# 全局增强器实例
_enhancer = None

def get_enhancer() -> KingbaseEnhancer:
    """获取全局增强器实例（暂时禁用）"""
    global _enhancer
    if _enhancer is None:
        _enhancer = KingbaseEnhancer()
    return _enhancer

def enhance_vessel_data_with_risk_desc(vessel_data: Dict[str, Any]) -> Dict[str, Any]:
    """为船舶数据添加risk_desc_info和info字段"""
    print("🔍 正在为船舶数据添加risk_desc_info和info字段...")
    
    # 获取增强器实例
    enhancer = get_enhancer()
    
    # 为所有检查项添加risk_desc_info和info字段
    if "risk_groups" in vessel_data:
        enhanced_risk_groups = enhancer.enhance_risk_groups(vessel_data["risk_groups"])
        vessel_data["risk_groups"] = enhanced_risk_groups
        print("✅ 风险分组增强完成")
    
    # 为其他检查项添加risk_desc_info和info字段
    if "check_items" in vessel_data:
        enhanced_check_items = enhancer.enhance_check_items(vessel_data["check_items"])
        vessel_data["check_items"] = enhanced_check_items
        print("✅ 检查项增强完成")
    
    # 为按title重新组织的检查项添加risk_desc_info和info字段
    if "check_items_by_title" in vessel_data:
        enhanced_check_items_by_title = enhancer.enhance_risk_groups(vessel_data["check_items_by_title"])
        vessel_data["check_items_by_title"] = enhanced_check_items_by_title
        print("✅ 按title重新组织的检查项增强完成")
    
    # 为按第三方判断分组的风险等级添加risk_desc_info和info字段
    if "risk_groups_by_title" in vessel_data:
        enhanced_risk_groups_by_title = {}
        for risk_level, title_groups in vessel_data["risk_groups_by_title"].items():
            enhanced_risk_groups_by_title[risk_level] = enhancer.enhance_risk_groups(title_groups)
        
        vessel_data["risk_groups_by_title"] = enhanced_risk_groups_by_title
        print("✅ 按第三方判断分组的风险等级增强完成")
    
    # 为lloyds_compliance字段添加risk_desc_info和info字段
    if "lloyds_compliance" in vessel_data and vessel_data["lloyds_compliance"]:
        lloyds_data = vessel_data["lloyds_compliance"]
        
        # 为lloyds_compliance本身添加risk_desc_info和info字段
        if isinstance(lloyds_data, dict):
            # 获取lloyds_compliance的风险等级
            sanctions_lev = lloyds_data.get("sanctions_lev", "")
            if sanctions_lev:
                result = enhancer.get_risk_desc_info("lloyds_compliance", sanctions_lev)
                lloyds_data["risk_desc_info"] = result["risk_desc_info"]
                lloyds_data["info"] = result["info"]
                print(f"✅ lloyds_compliance增强完成，风险等级: {sanctions_lev}")
            
            # 为lloyds_compliance中的risk_items添加risk_desc_info和info字段
            if "risk_items" in lloyds_data and isinstance(lloyds_data["risk_items"], list):
                enhanced_risk_items = []
                for item in lloyds_data["risk_items"]:
                    if isinstance(item, dict):
                        enhanced_item = item.copy()
                        risk_type = enhanced_item.get("name", "")
                        item_sanctions_lev = enhanced_item.get("sanctions_lev", "")
                        
                        if risk_type and item_sanctions_lev:
                            result = enhancer.get_risk_desc_info(risk_type, item_sanctions_lev)
                            enhanced_item["risk_desc_info"] = result["risk_desc_info"]
                            enhanced_item["info"] = result["info"]
                        else:
                            enhanced_item["risk_desc_info"] = ""
                            enhanced_item["info"] = ""
                        
                        enhanced_risk_items.append(enhanced_item)
                    else:
                        enhanced_risk_items.append(item)
                
                lloyds_data["risk_items"] = enhanced_risk_items
                print("✅ lloyds_compliance中的risk_items增强完成")
    
    print("🎉 船舶数据增强完成！")
    return vessel_data

def test_enhancement():
    """测试增强功能"""
    print("🧪 测试Kingbase增强功能")
    
    # 创建测试数据
    test_data = {
        "risk_groups": {
            "高风险": [
                {"name": "制裁风险", "sanctions_lev": "高", "description": "存在制裁风险"}
            ],
            "中风险": [
                {"name": "合规风险", "sanctions_lev": "中", "description": "存在合规风险"}
            ]
        }
    }
    
    print("📝 原始数据:", test_data)
    
    # 测试增强功能
    enhanced_data = enhance_vessel_data_with_risk_desc(test_data)
    
    print("🔍 增强后数据:", enhanced_data)
    
    # 验证新增字段
    for risk_level, items in enhanced_data["risk_groups"].items():
        for item in items:
            print(f"✅ {risk_level} - {item['name']}:")
            print(f"   risk_desc_info: {item.get('risk_desc_info', 'N/A')}")
            print(f"   info: {item.get('info', 'N/A')}")
    
    print("✅ 测试完成！")

if __name__ == "__main__":
    test_enhancement()
