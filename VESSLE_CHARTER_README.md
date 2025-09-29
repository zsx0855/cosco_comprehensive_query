# 船舶租入风险筛查服务

## 概述

船舶租入风险筛查服务是一个专门用于评估船舶租入操作中相关方及船舶自身风险的API服务。该服务通过查询内部制裁风险数据库，提供全面的租入风险分析结果。

## 功能特性

### 基础风险筛查
- **租家风险筛查**: 分析租船公司的制裁风险等级
- **船东风险筛查**: 评估船舶所有者的风险状态
- **管理人风险筛查**: 检查船舶管理公司的风险等级
- **经营人风险筛查**: 分析船舶经营人的制裁风险
- **最终受益人风险筛查**: 评估船舶最终受益方的风险状态
- **经纪人风险筛查**: 检查船舶经纪人的风险等级（支持多主体）
- **第二船东风险筛查**: 分析次要船舶所有者的风险状态（支持多主体）
- **保险公司风险筛查**: 评估船舶保险公司的风险等级（支持多主体）
- **租赁实际控制人风险筛查**: 检查租赁实际控制方的风险状态（支持多主体）

### 高级风险分析
1. **劳氏相关方制裁** (Vessel_stakeholder_is_sanction_Lloyd)
2. **Kpler相关方制裁** (Vessel_stakeholder_is_sanction_kpler)
3. **船舶当前制裁** (Vessel_is_sanction)
4. **船舶历史制裁** (Vessel_history_is_sanction)
5. **UANI清单检查** (Vessel_in_uani)
6. **劳氏风险等级** (Vessel_risk_level_Lloyd)
7. **Kpler风险等级** (Vessel_risk_level_kpler)
8. **AIS信号缺失** (Vessel_ais_gap)
9. **AIS人为篡改** (Vessel_Manipulation)
10. **高风险港口停靠** (Vessel_risky_port_call)
11. **暗港停靠** (Vessel_dark_port_call)
12. **船旗变更** (Vessel_change_flag)
13. **受制裁货物** (Vessel_cargo_sanction)
14. **受制裁贸易** (Vessel_trade_sanction)
15. **Dark STS事件** (Vessel_dark_sts_events)
16. **STS转运不合规** (Vessel_sts_transfer)

## 启动方式

### 方式一：独立启动python vessle_charter_risk.py服务将在 `http://localhost:8000` 启动

### 方式二：集成启动（推荐）python start_server.py船舶租入风险服务将作为主服务的一部分在 `/charter_in/vessel_charter_risk` 路径下可用

## API接口

### 主要接口
- **POST** `/charter_in/vessel_charter_risk` - 船舶租入风险筛查主接口

### 请求示例{
    "Uuid": "test-uuid-001",
    "Process_id": "proc-001",
    "Vessel_name": "测试船舶A",
    "Vessel_imo": "9876543",
    "charterers": "测试租家",
    "Vessel_manager": "测试船舶管理人",
    "Vessel_owner": "测试船东",
    "Vessel_final_beneficiary": "测试最终受益人",
    "Vessel_operator": "测试船舶经营人",
    "Vessel_broker": ["测试经纪人1", "测试经纪人2"],
    "Second_vessel_owner": ["第二船东1", "第二船东2"],
    "Vessel_insurer": ["保险公司A", "保险公司B"],
    "Lease_actual_controller": ["实际控制人1", "实际控制人2"]
}
### 响应示例{
    "Uuid": "test-uuid-001",
    "Vessel_name": "测试船舶A",
    "Vessel_imo": "9876543",
    "charterers": {
        "name": "测试租家",
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "Vessel_owner": {
        "name": "测试船东",
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "Vessel_is_sanction": {
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "Vessel_in_uani": {
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    }
}
## 测试

运行测试脚本验证服务：python test_vessel_charter.py
## 数据源

- **内部制裁风险数据库**: 存储实体制裁等级信息（lng.sanctions_risk_result表）
- **Kingbase数据库**: 用于存储风险筛查日志（lng.test_vessel_charter表）和配置信息

## 注意事项

1. 确保Kingbase数据库连接配置正确（通过kingbase_config.py获取）
2. 输入参数中Uuid、Vessel_imo、charterers为必填字段
3. 时间格式必须为 `YYYY/MM/DD HH:MM:SS`（适用于流程时间字段）
4. 相关方名称需准确填写，影响风险筛查结果准确性

## 错误处理

服务包含完整的错误处理机制：
- 数据库连接异常（OperationalError）
- 数据完整性错误（IntegrityError）
- 接口参数验证错误
- 超时处理（响应超时设置为60秒）

所有错误都会记录到日志中，便于问题排查。
