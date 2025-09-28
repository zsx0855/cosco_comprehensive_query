# Kpler制裁检查功能实现总结

## 📋 **实现概述**

成功为 `execute_vessel_is_sanction_check` 复合检查项添加了Kpler制裁检查功能，现在该复合检查项同时检查劳氏制裁和Kpler制裁两个数据源。

## 🔧 **实现内容**

### 1. **新增KplerSanctionsCheckItem类**
- **位置**: `functions_risk_check_framework.py` 第3992-4097行
- **功能**: 从Kpler API的 `compliance.sanctionRisks.sanctionedVessels` 数组中提取制裁数据
- **API端点**: `{kpler_base_url}/compliance/compliance-screening`
- **参数**: 只需要 `vessel_imo`

### 2. **新增execute_kpler_sanctions_check方法**
- **位置**: `functions_risk_check_framework.py` 第3040-3054行
- **功能**: 执行Kpler制裁检查的编排器方法
- **参数**: `vessel_imo: str`

### 3. **修改VesselIsSanctionCheckItem复合检查项**
- **位置**: `functions_risk_check_framework.py` 第4324-4374行
- **修改内容**: 
  - 添加了对 `execute_kpler_sanctions_check` 的调用
  - 实现了劳氏制裁和Kpler制裁的风险等级聚合
  - 合并了两个数据源的tab数据，并添加了"数据来源"字段区分

### 4. **注册新检查项**
- **检查项注册中心**: 添加了 `kpler_sanctions` -> `KplerSanctionsCheckItem` 映射
- **默认配置**: 添加了 `kpler_sanctions` 的完整配置信息

## 📊 **数据结构**

### Kpler制裁数据提取字段
```python
{
    "VesselImo": vessel.get("vesselImo"),
    "VesselName": vessel.get("vesselName"),
    "VesselMmsi": vessel.get("vesselMmsi"),
    "SanctionId": vessel.get("sanctionId"),
    "Source": vessel.get("source"),
    "Type": vessel.get("type"),
    "Program": vessel.get("program"),
    "Name": vessel.get("name"),
    "FirstPublished": vessel.get("firstPublished"),
    "LastPublished": vessel.get("lastPublished"),
    "StartDate": vessel.get("startDate"),
    "EndDate": vessel.get("endDate"),
    "SanctionVesselDetails": vessel.get("sanctionVesselDetails", []),
    "Aliases": vessel.get("aliases", []),
    "RiskLevel": vessel.get("riskLevel"),
    "Description": vessel.get("description")
}
```

## 🎯 **风险等级判定逻辑**

### Kpler制裁风险判定
```python
if has_current_sanctions:      # endDate为空
    return RiskLevel.HIGH      # 高风险
elif has_historical_sanctions: # endDate不为空
    return RiskLevel.MEDIUM    # 中风险
else:
    return RiskLevel.NO_RISK   # 无风险
```

### 复合检查风险聚合
```python
# 取两个数据源的最高风险等级
if "高风险" in [lloyds_risk, kpler_risk]:
    return RiskLevel.HIGH
elif "中风险" in [lloyds_risk, kpler_risk]:
    return RiskLevel.MEDIUM
else:
    return RiskLevel.NO_RISK
```

## 🚀 **使用方式**

### 单独调用Kpler制裁检查
```python
from functions_risk_check_framework import RiskCheckOrchestrator, create_api_config

orchestrator = RiskCheckOrchestrator(create_api_config())
result = orchestrator.execute_kpler_sanctions_check('9842190')
```

### 调用复合制裁检查（推荐）
```python
# 现在会同时检查劳氏制裁和Kpler制裁
result = orchestrator.execute_vessel_is_sanction_check('9842190')
```

## ✅ **测试结果**

- ✅ `kpler_sanctions` 检查项注册成功
- ✅ `execute_kpler_sanctions_check` 方法调用正常
- ✅ `execute_vessel_is_sanction_check` 复合检查项正常工作
- ✅ 风险等级聚合正确
- ✅ tab数据合并成功，包含数据来源标识

## 📈 **功能特点**

1. **数据源整合**: 同时检查劳氏和Kpler两个制裁数据源
2. **风险聚合**: 智能聚合两个数据源的风险等级
3. **数据区分**: tab数据中明确标识数据来源（"劳氏"或"Kpler"）
4. **向后兼容**: 保持原有API接口不变
5. **错误处理**: 完善的异常处理机制

## 🎉 **总结**

成功实现了Kpler制裁检查功能，并将其集成到现有的 `execute_vessel_is_sanction_check` 复合检查项中。现在该检查项能够从两个不同的数据源获取制裁信息，提供更全面的船舶制裁风险评估。
