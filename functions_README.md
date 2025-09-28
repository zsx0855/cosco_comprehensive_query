# 固定格式风险检查框架

## 概述

这是一个基于固定格式输出的风险检查框架，每个检查项都返回统一的JSON格式，便于API接口调用和前端展示。

## 核心特性

### 1. 固定格式输出
每个检查项都返回统一的JSON格式：
```json
{
    "risk_type": "检查项的英文名",
    "risk_desc": "检查项的中文名", 
    "risk_value": "检查项的风险判定等级",
    "info": "从sanctions_des_info表匹配的info字段信息",
    "risk_desc_info": "从sanctions_des_info表匹配的risk_desc_info字段信息",
    "tab": "该检查项的详情数据",
    "vessel_imo": "船舶IMO信息（可选）"
}
```

### 2. 风险等级标准化
- **高风险**: 存在严重风险
- **中风险**: 存在中等风险  
- **低风险**: 存在轻微风险
- **无风险**: 无风险
- **无数据**: 无法获取数据

### 3. 风险描述信息管理
通过`sanctions_des_info`表管理风险描述信息，支持：
- 根据检查项和风险等级匹配描述信息
- 支持数据库和默认数据两种模式
- 可扩展的风险描述信息

## 文件结构

```
functions_risk_check_framework.py      # 核心风险检查框架
functions_sanctions_des_info_manager.py # 风险描述信息管理器
functions_demo_fixed_format.py         # 使用示例和演示
```

## 支持的检查项

### Lloyd's 检查项
1. **lloyds_compliance** - 船舶相关方涉制裁风险情况
2. **lloyds_sanctions** - 船舶涉制裁名单风险情况
3. **ais_manipulation** - AIS信号伪造及篡改风险情况
4. **high_risk_port** - 挂靠高风险港口风险情况
5. **possible_dark_port** - 暗港访问风险情况
6. **suspicious_ais_gap** - AIS信号缺失风险情况(劳氏)
7. **dark_sts** - 隐蔽STS事件风险情况
8. **sanctioned_sts** - STS转运不合规风险情况
9. **loitering_behavior** - 可疑徘徊风险情况

### Kpler 检查项
1. **has_sanctioned_cargo_risk** - 船舶运输受制裁货物情况
2. **has_sanctioned_trades_risk** - 船舶涉及受制裁贸易风险情况
3. **has_ais_gap_risk** - AIS信号缺失风险情况（开普勒）
4. **ais_spoofs_risk** - AIS信号伪造及篡改风险情况（开普勒）
5. **dark_sts_kpler_risk** - 隐蔽STS事件风险情况（开普勒）
6. **sanctioned_companies_risk** - 受制裁公司检查（开普勒）

### UANI 检查项
1. **uani_check** - 船舶涉UANI清单风险情况

## 使用示例

### 基本使用
```python
from functions_risk_check_framework import RiskCheckOrchestrator, create_api_config
from functions_sanctions_des_info_manager import SanctionsDesInfoManager

# 创建API配置
api_config = create_api_config()

# 创建风险描述信息管理器
info_manager = SanctionsDesInfoManager()

# 创建编排器
orchestrator = RiskCheckOrchestrator(api_config, info_manager)

# 执行UANI检查
vessel_imo = "9842190"
result = orchestrator.execute_uani_check(vessel_imo)

# 输出结果
print(result.to_dict())
```

### 执行多个检查项
```python
# 执行所有检查项
all_results = []

# UANI检查
uani_result = orchestrator.execute_uani_check(vessel_imo)
all_results.append(uani_result)

# 劳氏合规检查
compliance_result = orchestrator.execute_lloyds_compliance_check(vessel_imo, start_date, end_date)
all_results.append(compliance_result)

# 输出所有结果
for result in all_results:
    print(f"{result.risk_type}: {result.risk_value}")
    print(f"描述: {result.risk_desc}")
    print(f"信息: {result.info}")
    print(f"详情数量: {len(result.tab)}")
```

## 输出格式示例

### UANI检查结果
```json
{
    "risk_type": "uani_check",
    "risk_desc": "船舶涉UANI清单风险情况",
    "risk_value": "高风险",
    "info": "船舶在UANI清单中判定为: 高风险",
    "risk_desc_info": "船舶在UANI清单中，下方表格详细记录了目标船舶被添加至UANI清单的日期、船旗国等关键信息",
    "tab": [
        {
            "imo": "9226009",
            "vessel_name": "YONG CHANG SHUN HANG",
            "date_added": "May 31, 2024",
            "current_flag": "MARSHALL ISLANDS",
            "former_flags": ""
        }
    ]
}
```

### AIS信号缺失检查结果（劳氏）
```json
{
    "risk_type": "suspicious_ais_gap",
    "risk_desc": "AIS信号缺失风险情况(劳氏)",
    "risk_value": "无风险",
    "info": "在劳氏中的风险判定为：No risk",
    "risk_desc_info": "船舶历史一年内无AIS信号缺失情况",
    "tab": [],
    "vessel_imo": {
        "0": "9569633"
    }
}
```

### AIS信号缺失检查结果（开普勒）
```json
{
    "risk_type": "has_ais_gap_risk",
    "risk_desc": "AIS信号缺失风险情况（开普勒）",
    "risk_value": "中风险",
    "info": "在开普勒中的风险判定为：Risks detected",
    "risk_desc_info": "船舶历史一年内有AIS信号缺失情况，下方表格详细记录了开普勒中目标船舶的AIS信号缺失记录，包括起始地区、结束地区、持续时长、吃水变化等关键信息",
    "tab": [
        {
            "startDate": "1744204357",
            "draughtChange": "9.0",
            "durationMin": "59956",
            "zone": {
                "start": {
                    "start_id": "557",
                    "start_name": "Malaysia"
                },
                "end": {
                    "end_id": "557",
                    "end_name": "Malaysia"
                }
            },
            "position": {
                "start": {
                    "start_lon": "100.09161376953125",
                    "start_lat": "3.829521894454956"
                },
                "end": {
                    "end_lon": "100.02703857421875",
                    "end_lat": "3.7411398887634277"
                }
            }
        }
    ]
}
```

## 风险描述信息管理

### 默认数据
框架内置了默认的风险描述信息，包括：
- UANI检查
- AIS信号缺失检查（劳氏和开普勒）
- 船舶相关方涉制裁风险情况
- 船舶涉制裁名单风险情况
- AIS信号伪造及篡改风险情况
- 挂靠高风险港口风险情况
- 暗港访问风险情况
- 隐蔽STS事件风险情况
- STS转运不合规风险情况
- 可疑徘徊风险情况
- 船舶运输受制裁货物情况
- 船舶涉及受制裁贸易风险情况

### 自定义数据
```python
# 添加自定义风险描述信息
info_manager.add_info("自定义检查项", "高风险", "自定义info", "自定义risk_desc_info")

# 更新风险描述信息
info_manager.update_info("自定义检查项", "高风险", "更新后的info", "更新后的risk_desc_info")

# 删除风险描述信息
info_manager.delete_info("自定义检查项", "高风险")
```

## 扩展检查项

### 1. 创建新的检查项类
```python
class CustomCheckItem(BaseCheckItem):
    """自定义检查项"""
    
    def get_required_params(self) -> List[str]:
        return ["vessel_imo", "start_date", "end_date"]
    
    def check(self, vessel_imo: str, **kwargs) -> CheckResult:
        if not self.validate_params(vessel_imo=vessel_imo, **kwargs):
            return self.create_check_result(RiskLevel.NO_DATA, [], {"0": vessel_imo})
        
        try:
            # 执行自定义检查逻辑
            data = self._fetch_custom_data(vessel_imo, kwargs["start_date"], kwargs["end_date"])
            risk_level = self._calculate_custom_risk(data)
            tab_data = self._build_custom_tab_data(data, vessel_imo)
            
            return self.create_check_result(risk_level, tab_data, {"0": vessel_imo})
            
        except Exception as e:
            self.logger.error(f"自定义检查失败: {e}")
            return self.create_check_result(RiskLevel.NO_DATA, [], {"0": vessel_imo})
```

### 2. 注册检查项
```python
# 在RiskCheckOrchestrator中注册
def register_custom_check_item(self, check_item: BaseCheckItem):
    """注册自定义检查项"""
    self.check_items[check_item.config.id] = check_item
    self.logger.info(f"注册自定义检查项: {check_item.config.id}")
```

## 配置管理

### API配置
```python
def create_api_config() -> Dict[str, Any]:
    """创建API配置"""
    return {
        "lloyds_base_url": "https://api.lloydslistintelligence.com",
        "lloyds_headers": {
            "Authorization": "Bearer YOUR_TOKEN",
            "Content-Type": "application/json"
        },
        "kpler_base_url": "https://api.kpler.com",
        "kpler_headers": {
            "Authorization": "Bearer YOUR_TOKEN",
            "Content-Type": "application/json"
        },
        "uani_base_url": "https://www.treasury.gov",
        "uani_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    }
```

### 检查项配置
```python
def create_check_item_configs() -> List[CheckItemConfig]:
    """创建检查项配置"""
    return [
        CheckItemConfig(
            id="uani_check",
            business_module="船舶风险检查",
            compliance_check_module="UANI检查",
            compliance_check_type="清单检查",
            entity_cn="船舶",
            entity_en="vessel",
            entity_type="vessel",
            risk_desc="船舶涉UANI清单风险情况",
            risk_type="uani_check",
            used_flag="Y",
            time_flag="N",
            time_period="",
            area_flag="N",
            area="",
            risk_flag="Y",
            risk_flag_type="高风险,中风险,低风险,无风险,无数据"
        ),
        # 更多配置...
    ]
```

## 运行演示

```bash
# 运行演示程序
python functions_demo_fixed_format.py
```

演示程序将展示：
1. 固定格式输出演示
2. 所有检查项演示
3. 风险信息管理演示

## 注意事项

1. **API配置**: 确保API配置正确，包括URL和认证信息
2. **数据库连接**: 如需使用数据库，请配置正确的数据库连接
3. **错误处理**: 框架包含完整的错误处理机制
4. **日志记录**: 所有操作都有详细的日志记录
5. **扩展性**: 支持轻松添加新的检查项

## 总结

这个固定格式风险检查框架提供了：
- ✅ 统一的输出格式
- ✅ 标准化的风险等级
- ✅ 完整的风险描述信息管理
- ✅ 支持多种数据源（Lloyd's、Kpler、UANI）
- ✅ 易于扩展的架构
- ✅ 完整的错误处理和日志记录
- ✅ 详细的使用示例和文档

通过这个框架，你可以轻松地：
1. 执行各种风险检查
2. 获取统一的输出格式
3. 管理风险描述信息
4. 扩展新的检查项
5. 构建API接口
