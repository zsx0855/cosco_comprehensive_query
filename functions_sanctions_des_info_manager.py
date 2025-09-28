"""
sanctions_des_info表管理器
用于管理风险描述信息表
"""

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SanctionsDesInfoManager:
    """sanctions_des_info表管理器"""
    
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
        self.info_cache: Dict[str, Dict[str, Any]] = {}
        self._load_info_data()
    
    def _load_info_data(self):
        """加载sanctions_des_info数据"""
        # 直接加载CSV文件，不再从数据库加载
        self._load_default_info_data()
    
    def _load_default_info_data(self):
        """加载默认的sanctions_des_info数据"""
        try:
            import os
            project_root = os.path.dirname(os.path.abspath(__file__))
            csv_path = os.path.join(project_root, "sanctions_des_info.csv")
            
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding="utf-8")
                
                # 构建缓存
                for _, row in df.iterrows():
                    key = f"{row['risk_desc']}_{row['risk_level']}"
                    self.info_cache[key] = {
                        "info": row['info'],
                        "risk_desc_info": row['risk_desc_info']
                    }
                
                logger.info(f"从CSV加载了 {len(self.info_cache)} 条sanctions_des_info记录")
            else:
                logger.error(f"未找到CSV文件: {csv_path}，加载空缓存")
                self.info_cache = {}
                
        except Exception as e:
            logger.error(f"从CSV加载sanctions_des_info失败: {e}，加载空缓存")
            self.info_cache = {}
    
    def get_info(self, risk_desc: str, risk_level: str) -> Dict[str, str]:
        """获取风险描述信息"""
        key = f"{risk_desc}_{risk_level}"
        
        if key in self.info_cache:
            return self.info_cache[key]
        
        # 如果没有找到精确匹配，尝试模糊匹配
        for cache_key, info in self.info_cache.items():
            if risk_desc in cache_key and risk_level in cache_key:
                return info
        
        # 如果都没有找到，返回默认值
        logger.warning(f"未找到风险描述信息: {risk_desc}_{risk_level}")
        return {
            "info": f"风险判定为: {risk_level}",
            "risk_desc_info": f"风险描述: {risk_desc}"
        }
    
    def get_info_by_risk_type(self, risk_type: str, risk_level: str) -> Dict[str, str]:
        """根据风险类型获取信息"""
        # 风险类型到风险描述的映射
        risk_type_mapping = {
            "uani_check": "船舶涉UANI清单风险情况",
            "suspicious_ais_gap": "AIS信号缺失风险情况(劳氏)",
            "has_ais_gap_risk": "AIS信号缺失风险情况（开普勒）",
            "lloyds_compliance": "船舶相关方涉制裁风险情况",
            "lloyds_sanctions": "船舶涉制裁名单风险情况",
            "ais_manipulation": "AIS信号伪造及篡改风险情况",
            "high_risk_port": "挂靠高风险港口风险情况",
            "possible_dark_port": "暗港访问风险情况",
            "dark_sts": "隐蔽STS事件风险情况",
            "sanctioned_sts": "STS转运不合规风险情况",
            "loitering_behavior": "可疑徘徊风险情况",
            "has_sanctioned_cargo_risk": "船舶运输受制裁货物情况",
            "has_sanctioned_trades_risk": "船舶涉及受制裁贸易风险情况",
            "has_dark_sts_risk": "隐蔽STS事件风险情况",
            "has_ais_spoofs_risk": "AIS信号伪造及篡改风险情况",
            "has_port_calls_risk": "挂靠高风险港口风险情况（开普勒）",
            "has_sts_events_risk": "STS转运不合规风险情况",
            "lloydsRiskLevel": "船舶制裁合规结果(劳氏)",
            "kplerRiskLevel": "船舶制裁合规结果(开普勒)",
            "cargo_country": "货物原产地是否来源于高风险国家",
            "port_country": "港口是否来源于高风险国家"
        }
        
        risk_desc = risk_type_mapping.get(risk_type, risk_type)
        return self.get_info(risk_desc, risk_level)
    
    def add_info(self, risk_desc: str, risk_level: str, info: str, risk_desc_info: str):
        """添加风险描述信息"""
        key = f"{risk_desc}_{risk_level}"
        self.info_cache[key] = {
            "info": info,
            "risk_desc_info": risk_desc_info
        }
        logger.info(f"添加风险描述信息: {key}")
    
    def update_info(self, risk_desc: str, risk_level: str, info: str, risk_desc_info: str):
        """更新风险描述信息"""
        key = f"{risk_desc}_{risk_level}"
        if key in self.info_cache:
            self.info_cache[key] = {
                "info": info,
                "risk_desc_info": risk_desc_info
            }
            logger.info(f"更新风险描述信息: {key}")
        else:
            logger.warning(f"未找到要更新的风险描述信息: {key}")
    
    def delete_info(self, risk_desc: str, risk_level: str):
        """删除风险描述信息"""
        key = f"{risk_desc}_{risk_level}"
        if key in self.info_cache:
            del self.info_cache[key]
            logger.info(f"删除风险描述信息: {key}")
        else:
            logger.warning(f"未找到要删除的风险描述信息: {key}")
    
    def get_all_info(self) -> Dict[str, Dict[str, str]]:
        """获取所有风险描述信息"""
        return self.info_cache.copy()
    
    def export_info_to_json(self, file_path: str) -> bool:
        """导出风险描述信息到JSON文件"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.info_cache, f, ensure_ascii=False, indent=2)
            
            logger.info(f"风险描述信息已导出到 {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"导出风险描述信息失败: {e}")
            return False
    
    def import_info_from_json(self, file_path: str) -> bool:
        """从JSON文件导入风险描述信息"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.info_cache = json.load(f)
            
            logger.info(f"风险描述信息已从 {file_path} 导入")
            return True
            
        except Exception as e:
            logger.error(f"导入风险描述信息失败: {e}")
            return False

# ==================== 使用示例 ====================

def main():
    """主函数示例"""
    # 创建sanctions_des_info管理器
    info_manager = SanctionsDesInfoManager()
    
    # 获取风险描述信息
    info = info_manager.get_info("船舶涉UANI清单风险情况", "高风险")
    print("=== 风险描述信息 ===")
    print(json.dumps(info, indent=2, ensure_ascii=False))
    
    # 根据风险类型获取信息
    info_by_type = info_manager.get_info_by_risk_type("uani_check", "高风险")
    print("\n=== 根据风险类型获取信息 ===")
    print(json.dumps(info_by_type, indent=2, ensure_ascii=False))
    
    # 获取所有信息
    all_info = info_manager.get_all_info()
    print(f"\n=== 所有风险描述信息 ({len(all_info)} 条) ===")
    for key, info in all_info.items():
        print(f"- {key}: {info['info']}")
    
    # 导出信息
    info_manager.export_info_to_json("sanctions_des_info.json")

if __name__ == "__main__":
    main()
