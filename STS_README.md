# STS风险筛查服务

## 概述

STS（Ship-to-Ship）风险筛查服务是一个专门用于船舶对船舶转运操作风险分析的API服务。该服务整合了多个数据源，提供全面的风险筛查功能。

## 功能特性

### 基础风险筛查
- **租家风险筛查**: 分析租船公司的制裁风险
- **船东风险筛查**: 检查船舶所有者的风险状态
- **管理人风险筛查**: 评估船舶管理公司的风险
- **经营人风险筛查**: 分析船舶经营人的风险等级

### 高级风险分析（13-28号字段）
1. **劳氏相关方制裁** (vessel_stakeholder_is_sanction_lloyd)
2. **Kpler相关方制裁** (vessel_stakeholder_is_sanction_kpler)
3. **劳氏船舶当前制裁** (vessel_is_sanction)
4. **劳氏船舶历史制裁** (vessel_history_is_sanction)
5. **UANI清单检查** (vessel_in_uani)
6. **劳氏风险等级** (vessel_risk_level_lloyd)
7. **Kpler风险等级** (vessel_risk_level_kpler)
8. **AIS信号缺失** (vessel_ais_gap)
9. **AIS人为篡改** (vessel_manipulation)
10. **高风险港口停靠** (vessel_high_risk_port)
11. **暗港停靠** (vessel_has_dark_port_call)
12. **受制裁货物** (vessel_cargo_sanction)
13. **受制裁贸易** (vessel_trade_sanction)
14. **制裁国货物原产地** (cargo_origin_from_sanctioned_country)
15. **Dark STS事件** (vessel_dark_sts_events)
16. **STS转运不合规** (vessel_sts_transfer)

## 启动方式

### 方式一：独立启动
```bash
python sts_bunkering_risk.py
```
服务将在 `http://localhost:8000` 启动

### 方式二：集成启动（推荐）
```bash
python start_server.py
```
STS服务将作为主服务的一部分在 `/sts` 路径下可用

## API接口

### 主要接口
- **POST** `/sts/risk_screen` - STS风险筛查主接口

### 请求示例
```json
{
    "uuid": "unique-identifier",
    "sts_execution_status": "计划中",
    "business_segment": "LNG",
    "business_model": "FOB",
    "operate_water_area": "中国沿海",
    "expected_execution_date": "2025/01/15",
    "is_port_sts": "是",
    "vessel_name": "船舶名称",
    "vessel_imo": "IMO号",
    "charterers": "租家名称",
    "vessel_owner": ["船东名称"],
    "vessel_manager": ["管理人名称"],
    "vessel_operator": ["经营人名称"]
}
```

### 响应示例
```json
{
    "uuid": "unique-identifier",
    "vessel_name": "船舶名称",
    "charterers": {
        "name": "租家名称",
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "vessel_owner": {
        "name": "船东名称",
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "vessel_is_sanction": {
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    },
    "vessel_in_uani": {
        "risk_screening_status": "无风险",
        "risk_screening_time": "2025-01-15T10:30:00Z"
    }
}
```

## 测试

运行测试脚本验证服务：
```bash
python test_sts_service.py
```

## 数据源

- **Lloyd's List Intelligence**: 船舶制裁和合规数据
- **Kpler**: 船舶行为分析和风险评分
- **UANI**: 伊朗相关船舶清单
- **内部数据库**: 制裁国家和港口信息

## 注意事项

1. 确保所有依赖的外部API服务可用
2. 数据库连接配置正确
3. API令牌有效且未过期
4. 时间格式必须为 `YYYY/MM/DD HH:MM:SS`

## 错误处理

服务包含完整的错误处理机制：
- 数据库连接异常
- API调用失败
- 数据格式错误
- 超时处理

所有错误都会记录到日志中，便于问题排查。
