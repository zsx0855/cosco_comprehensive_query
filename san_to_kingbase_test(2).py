import json
import psycopg2
from datetime import datetime
from collections import defaultdict
from psycopg2.extras import DictCursor


def get_db_connection():
    """建立 Kingbase 数据库连接"""
    try:
        conn = psycopg2.connect(
            host='10.11.142.145',
            port=54321,
            user='system',
            password='zV2,oB5%',  # 请替换为实际的密码
            dbname='lngdb',
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"Kingbase 数据库连接失败: {e}")
        return None


def fetch_all_risk_data(conn):
    """查询所有风险数据（已添加description3_value_cn和end_time字段）"""
    try:
        with conn.cursor() as cursor:
            sql = """
select 
entity_id, 
entity_dt, 
activestatus, 
ENTITYNAME1, 
ENTITYNAME4, 
country_nm1, 
country_nm2, 
DATEVALUE1,
description2_value_cn, 
description3_value_cn, 
start_time, 
end_time,
SANCTIONS_NM, 
is_san, 
is_sco, 
is_ool, 
is_one_year, 
is_sanctioned_countries
from lng.risk_dqs_test 

                  """
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        print(f"查询数据失败: {e}")
        return []


def fetch_sanctions_des_info(conn):
    """查询制裁描述信息表"""
    try:
        with conn.cursor() as cursor:
            sql = """
                  SELECT risk_type, 
                         risk_desc, 
                         risk_level as risk_value, 
                         risk_desc_info, 
                         info
                  FROM lng.sanctions_des_info 
                  """
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        print(f"查询制裁描述信息失败: {e}")
        return []


def fetch_associated_parties(conn, entity_ids):
    """查询关联方信息（修复类型不一致问题）"""
    try:
        # 确保entity_ids是字符串列表
        str_entity_ids = [str(eid) for eid in entity_ids]

        # 去重并排序
        unique_entity_ids = sorted(set(str_entity_ids))
        total_count = len(unique_entity_ids)

        print(f"需要处理 {total_count} 个唯一实体ID")

        # 分批处理，每批1000个ID
        batch_size = 1000
        associated_parties = defaultdict(list)

        for i in range(0, total_count, batch_size):
            batch_ids = unique_entity_ids[i:i + batch_size]
            print(
                f"正在处理第 {i // batch_size + 1}/{(total_count - 1) // batch_size + 1} 批，本批 {len(batch_ids)} 个ID")

            # 跳过空批次
            if not batch_ids:
                continue

            with conn.cursor() as cursor:
                # 使用当前批次的ID数量构建占位符
                placeholders = ','.join(['%s'] * len(batch_ids))
                sql = f"""
                        SELECT t.associate_id   AS associate_id, 
                               t.SORP_ID        AS SORP_ID, 
                               IF(t.SOURCE_TYPE = 'PERSON',t3.nmtoken_level,t2.nmtoken_level) AS nmtoken_level,
                               IF(t.SOURCE_TYPE = 'PERSON',t3.ENTITYNAME1,t2.ENTITYNAME1)   AS ENTITYNAME11, 
                               t.SOURCE_TYPE    AS SOURCE_TYPE, 
                               t1.RELATION_NM   AS RELATION_NM
                        FROM lng.ods_zyhy_rm_associate_df_3 t
                        LEFT JOIN lng.ods_zyhy_rm_relation_df_3 t1 ON t.ASSOCIATE_CD = t1.RELATION_CD
                        LEFT JOIN lng.dqs_entity_sanctions_test t2 ON t.SORP_ID = t2.entity_id
                        LEFT JOIN lng.dqs_person_sanctions_test t3 ON t.SORP_ID = t3.entity_id
                        WHERE t.associate_id IN ({placeholders})
                          AND t.SORP_ID IS NOT NULL
                """
                # 直接传递batch_ids列表
                cursor.execute(sql, batch_ids)

                # 处理本批结果
                batch_count = 0
                for row in cursor.fetchall():
                    # 确保键是字符串类型
                    key = str(row['associate_id'])
                    associated_parties[key].append({
                        'SORP_ID': row['SORP_ID'],
                        'nmtoken_level': row['nmtoken_level'],
                        'ENTITYNAME11': row['ENTITYNAME11'],
                        'SOURCE_TYPE': row['SOURCE_TYPE'],
                        'RELATION_NM': row['RELATION_NM']
                    })
                    batch_count += 1

                print(f"本批获取到 {batch_count} 条关联方记录")

        total_parties = sum(len(v) for v in associated_parties.values())
        print(f"总共获取 {len(associated_parties)} 个实体的 {total_parties} 条关联方记录")

        return associated_parties

    except Exception as e:
        print(f"查询关联方信息失败: {e}")
        import traceback
        traceback.print_exc()
        return {}

def process_risk_data(records, sanctions_des_info, associated_parties):
    """处理风险数据并生成所需JSON结构（完整修复版）"""
    # 构建风险描述信息映射
    des_info_map = {}
    for item in sanctions_des_info:
        key = (item['risk_type'], item['risk_value'])
        des_info_map[key] = {
            'risk_desc': item['risk_desc'],
            'risk_desc_info': item['risk_desc_info'],
            'info': item['info']
        }

    result = []
    entities = defaultdict(lambda: {
        'entity_id': None,
        'entity_dt': None,
        'activestatus': None,
        'ENTITYNAME1': None,
        'ENTITYNAME4': None,
        'country_nm1': None,
        'country_nm2': None,
        'DATEVALUE1': None,
        'sanctions_lev': '无风险',  # 默认值
        'sanctions_list': [],
        'mid_sanctions_list': [],
        'no_sanctions_list': [],
        'unknown_risk_list': [],
        'other_list': [],
        'is_san': None,  # 新增字段
        'is_sco': None,  # 新增字段
        'is_ool': None,  # 新增字段
        'is_one_year': None,  # 新增字段
        'is_sanctioned_countries': None  # 新增字段
    })

    # 用于存储sco相关的description3_value_cn（按风险等级分组）
    sco_data = defaultdict(lambda: {
        'high_risk': set(),
        'mid_risk': set(),
        'no_risk': set()
    })

    # 第一遍遍历：收集所有风险类型的状态
    risk_type_status = defaultdict(lambda: {
        'has_high': False,
        'has_mid': False,
        'has_unknown': False
    })

    for record in records:
        entity_id = record['entity_id']
        risk_types = [
            ('is_san', record['is_san']),
            ('is_sco', record['is_sco']),
            ('is_ool', record['is_ool']),
            ('is_one_year', record['is_one_year']),
            ('is_sanctioned_countries', record['is_sanctioned_countries'])
        ]

        for risk_type, risk_value in risk_types:
            if not risk_value:
                continue

            if risk_value == '高风险':
                risk_type_status[(entity_id, risk_type)]['has_high'] = True
            elif risk_value == '中风险':
                risk_type_status[(entity_id, risk_type)]['has_mid'] = True
            elif risk_value == '无法判断':
                risk_type_status[(entity_id, risk_type)]['has_unknown'] = True

    # 第二遍遍历：收集sco相关数据用于去重
    for record in records:
        if record['is_sco'] in ['高风险', '中风险', '无风险']:
            entity_id = record['entity_id']
            risk_value = record['is_sco']
            desc_value = str(record.get('description3_value_cn', '')).strip()

            if risk_value == '高风险':
                sco_data[entity_id]['high_risk'].add(desc_value)
            elif risk_value == '中风险':
                sco_data[entity_id]['mid_risk'].add(desc_value)
            elif risk_value == '无风险':
                sco_data[entity_id]['no_risk'].add(desc_value)

    # 第三遍遍历：处理所有数据
    for record in records:
        entity_id = record['entity_id']
        entity = entities[entity_id]
        entity.update({
            'entity_id': entity_id,
            'entity_dt': record['entity_dt'],
            'activestatus': record['activestatus'],
            'ENTITYNAME1': record['ENTITYNAME1'],
            'ENTITYNAME4': record['ENTITYNAME4'],
            'country_nm1': record['country_nm1'],
            'country_nm2': record['country_nm2'],
            'DATEVALUE1': record['DATEVALUE1'],
            'other_list': associated_parties.get(str(entity_id), [])
        })

        risk_types = [
            ('is_san', record['is_san']),
            ('is_sco', record['is_sco']),
            ('is_ool', record['is_ool']),
            ('is_one_year', record['is_one_year']),
            ('is_sanctioned_countries', record['is_sanctioned_countries'])
        ]

        for risk_type, risk_value in risk_types:
            if not risk_value:
                continue

            des_info = des_info_map.get((risk_type, risk_value), {})

            risk_item = {
                'risk_type': risk_type,
                'risk_value': risk_value,
                'risk_desc': des_info.get('risk_desc', ''),
                'risk_desc_info': des_info.get('risk_desc_info', ''),
                'info': des_info.get('info', ''),
                'tab': []
            }

            # 处理无法判断的数据
            if risk_value == '无法判断':
                if risk_type == 'is_sco':
                    risk_item['tab'] = [{'description3_value_cn': desc}
                                        for desc in sorted(sco_data[entity_id]['high_risk'] |
                                                           sco_data[entity_id]['mid_risk'] |
                                                           sco_data[entity_id]['no_risk']) if desc]
                else:
                    # 对于is_san和is_ool，过滤掉SANCTIONS_NM为null、空字符串或'null'的记录
                    if risk_type in ['is_san', 'is_ool'] and (not record['SANCTIONS_NM'] or record['SANCTIONS_NM'] == 'null' or record['SANCTIONS_NM'].strip() == ''):
                        continue  # 跳过这条记录
                    
                    risk_item['tab'] = [{
                        'start_time': record['start_time'],
                        'end_time': record.get('end_time'),
                        'SANCTIONS_NM': record['SANCTIONS_NM'],
                        'description2_value_cn': record['description2_value_cn']
                    }]

                existing = next((item for item in entity['unknown_risk_list']
                                 if item['risk_type'] == risk_type), None)
                if existing:
                    if risk_type == 'is_sco':
                        existing_tab_values = {item['description3_value_cn'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['description3_value_cn'] not in existing_tab_values:
                                existing['tab'].append(item)
                                existing_tab_values.add(item['description3_value_cn'])
                    else:
                        # 对于无法判断的情况，is_one_year和其他风险类型都使用extend
                        existing['tab'].extend(risk_item['tab'])
                else:
                    entity['unknown_risk_list'].append(risk_item)
                continue

            # 处理高风险和中风险
            if risk_value in ['高风险', '中风险']:
                if risk_type == 'is_sco':
                    if risk_value == '高风险':
                        risk_item['tab'] = [{'description3_value_cn': desc}
                                            for desc in sorted(sco_data[entity_id]['high_risk']) if desc]
                    elif risk_value == '中风险':
                        risk_item['tab'] = [{'description3_value_cn': desc}
                                            for desc in sorted(sco_data[entity_id]['mid_risk']) if desc]
                elif risk_type == 'is_sanctioned_countries':
                    risk_item['tab'] = [{'country_nm1': record['country_nm1']}]
                elif risk_type == 'is_one_year':
                    risk_item['tab'] = [{'DATEVALUE1': record['DATEVALUE1']}]
                else:
                    # 对于is_san和is_ool，过滤掉SANCTIONS_NM为null、空字符串或'null'的记录
                    if risk_type in ['is_san', 'is_ool'] and (not record['SANCTIONS_NM'] or record['SANCTIONS_NM'] == 'null' or record['SANCTIONS_NM'].strip() == ''):
                        continue  # 跳过这条记录
                    
                    risk_item['tab'] = [{
                        'start_time': record['start_time'],
                        'end_time': record.get('end_time'),
                        'SANCTIONS_NM': record['SANCTIONS_NM'],
                        'description2_value_cn': record['description2_value_cn']
                    }]

            # 无风险的处理（tab设为空数组）
            else:
                risk_item['tab'] = []

            # 根据风险类型和等级分配到不同列表
            if risk_value == '高风险':
                existing = next((item for item in entity['sanctions_list']
                                 if item['risk_type'] == risk_type), None)
                if existing:
                    if risk_type == 'is_sco':
                        existing_tab_values = {item['description3_value_cn'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['description3_value_cn'] not in existing_tab_values:
                                existing['tab'].append(item)
                                existing_tab_values.add(item['description3_value_cn'])
                    elif risk_type == 'is_sanctioned_countries':
                        # 对于is_sanctioned_countries，确保country_nm1不重复
                        existing_countries = {item['country_nm1'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['country_nm1'] not in existing_countries:
                                existing['tab'].append(item)
                                existing_countries.add(item['country_nm1'])
                    elif risk_type == 'is_one_year':
                        # 对于is_one_year，确保DATEVALUE1不重复
                        existing_dates = {item['DATEVALUE1'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['DATEVALUE1'] not in existing_dates:
                                existing['tab'].append(item)
                                existing_dates.add(item['DATEVALUE1'])
                    else:
                        existing['tab'].extend(risk_item['tab'])
                else:
                    entity['sanctions_list'].append(risk_item)
            elif risk_value == '中风险':
                existing = next((item for item in entity['mid_sanctions_list']
                                 if item['risk_type'] == risk_type), None)
                if existing:
                    if risk_type == 'is_sco':
                        existing_tab_values = {item['description3_value_cn'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['description3_value_cn'] not in existing_tab_values:
                                existing['tab'].append(item)
                                existing_tab_values.add(item['description3_value_cn'])
                    elif risk_type == 'is_sanctioned_countries':
                        # 对于is_sanctioned_countries，确保country_nm1不重复
                        existing_countries = {item['country_nm1'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['country_nm1'] not in existing_countries:
                                existing['tab'].append(item)
                                existing_countries.add(item['country_nm1'])
                    elif risk_type == 'is_one_year':
                        # 对于is_one_year，确保DATEVALUE1不重复
                        existing_dates = {item['DATEVALUE1'] for item in existing['tab']}
                        for item in risk_item['tab']:
                            if item['DATEVALUE1'] not in existing_dates:
                                existing['tab'].append(item)
                                existing_dates.add(item['DATEVALUE1'])
                    else:
                        existing['tab'].extend(risk_item['tab'])
                else:
                    entity['mid_sanctions_list'].append(risk_item)
            else:  # 无风险
                if not risk_type_status[(entity_id, risk_type)]['has_high'] and \
                        not risk_type_status[(entity_id, risk_type)]['has_mid'] and \
                        not risk_type_status[(entity_id, risk_type)]['has_unknown']:
                    existing = next((item for item in entity['no_sanctions_list']
                                     if item['risk_type'] == risk_type), None)
                    if not existing:
                        entity['no_sanctions_list'].append(risk_item)

        # 最终风险等级判定（严格优先级）
        if entity['sanctions_list']:  # 只要存在高风险记录
            entity['sanctions_lev'] = '高风险'
        elif entity['mid_sanctions_list']:  # 存在中风险记录
            entity['sanctions_lev'] = '中风险'
        elif entity['unknown_risk_list']:  # 存在无法判断记录
            entity['sanctions_lev'] = '无风险'
        else:  # 默认无风险
            entity['sanctions_lev'] = '无风险'

        # 新增字段逻辑判断
        # is_san: 如果有高风险或中风险就赋值"SAN"，仅有无风险就给空值
        has_san_high_or_mid = any(item['risk_type'] == 'is_san' for item in entity['sanctions_list'] + entity['mid_sanctions_list'])
        entity['is_san'] = 'SAN' if has_san_high_or_mid else None

        # is_sco: 如果有高风险或中风险就赋值"SCO"，仅有无风险就给空值
        has_sco_high_or_mid = any(item['risk_type'] == 'is_sco' for item in entity['sanctions_list'] + entity['mid_sanctions_list'])
        entity['is_sco'] = 'SCO' if has_sco_high_or_mid else None

        # is_ool: 如果有高风险或中风险就赋值"OOL"，仅有无风险就给空值
        has_ool_high_or_mid = any(item['risk_type'] == 'is_ool' for item in entity['sanctions_list'] + entity['mid_sanctions_list'])
        entity['is_ool'] = 'OOL' if has_ool_high_or_mid else None

        # is_one_year: 如果有高风险或中风险就赋值"is_one_year"，仅有无风险或无法判断就给空值
        has_one_year_high_or_mid = any(item['risk_type'] == 'is_one_year' for item in entity['sanctions_list'] + entity['mid_sanctions_list'])
        entity['is_one_year'] = 'is_one_year' if has_one_year_high_or_mid else None

        # is_sanctioned_countries: 如果有高风险或中风险就赋值"is_sanctioned_countries"，仅有无风险或无法判断就给空值
        has_countries_high_or_mid = any(item['risk_type'] == 'is_sanctioned_countries' for item in entity['sanctions_list'] + entity['mid_sanctions_list'])
        entity['is_sanctioned_countries'] = 'is_sanctioned_countries' if has_countries_high_or_mid else None

    # 转换为列表并处理空tab的情况
    for entity_id, data in entities.items():
        # 确保无风险记录的tab为空数组
        for item in data['no_sanctions_list']:
            item['tab'] = []

        result.append(data)

    return result


def main():
    """主函数，执行完整的数据处理流程"""
    # 1. 建立数据库连接
    conn = get_db_connection()
    if not conn:
        print("无法建立数据库连接，程序终止")
        return

    try:
        # 2. 获取主风险数据
        print("开始获取主风险数据...")
        records = fetch_all_risk_data(conn)
        if not records:
            print("警告：没有获取到任何主风险数据")
            return

        print(f"成功获取 {len(records)} 条主风险记录")

        # 3. 获取制裁描述信息
        print("开始获取制裁描述信息...")
        sanctions_des_info = fetch_sanctions_des_info(conn)
        if not sanctions_des_info:
            print("错误：没有获取到制裁描述信息")
            return

        print(f"成功获取 {len(sanctions_des_info)} 条制裁描述信息")

        # 4. 获取关联方信息
        print("开始获取关联方信息...")
        entity_ids = [int(record['entity_id']) for record in records]
        associated_parties = fetch_associated_parties(conn, entity_ids)

        print(f"成功获取 {len(associated_parties)} 个实体的关联方信息")
        total_parties = sum(len(v) for v in associated_parties.values())
        print(f"总计 {total_parties} 条关联方记录")

        # 在获取关联方数据后立即检查
        print("\n调试关联方数据示例：")
        sample_entity_id = next(iter(associated_parties.keys()), None)
        print(f"首个关联方实体ID: {sample_entity_id} (类型: {type(sample_entity_id)})")
        print(f"该实体的关联方数量: {len(associated_parties.get(sample_entity_id, []))}")

        # 在处理数据前检查主数据ID
        print("\n主数据实体ID示例：")
        sample_record = records[0] if records else None
        print(
            f"首个主数据实体ID: {sample_record['entity_id'] if sample_record else None} (类型: {type(sample_record['entity_id']) if sample_record else None})")

        # 5. 处理数据
        print("开始处理风险数据...")
        processed_data = process_risk_data(
            records=records,
            sanctions_des_info=sanctions_des_info,
            associated_parties=associated_parties
        )

        print(f"成功处理 {len(processed_data)} 条实体数据")

        # 6. 生成带时间戳的输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'sanctions_risk_data_{timestamp}.json'

        # 7. 保存为JSON文件
        print(f"开始将结果保存到 {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
        print("JSON文件保存完成")

        # 8. 准备数据库插入数据
        print("准备数据库插入数据...")
        data_to_insert = []
        for item in processed_data:
            data_to_insert.append((
                str(item['entity_id']),
                item.get('entity_dt'),
                item.get('activestatus'),
                item.get('ENTITYNAME1'),
                item.get('ENTITYNAME4'),
                item.get('country_nm1'),
                item.get('country_nm2'),
                item.get('DATEVALUE1'),
                item.get('sanctions_lev'),
                json.dumps(item.get('sanctions_list', []), ensure_ascii=False),
                json.dumps(item.get('mid_sanctions_list', []), ensure_ascii=False),
                json.dumps(item.get('no_sanctions_list', []), ensure_ascii=False),
                json.dumps(item.get('unknown_risk_list', []), ensure_ascii=False),
                json.dumps(item.get('other_list', []), ensure_ascii=False),
                item.get('is_san'),
                item.get('is_sco'),
                item.get('is_ool'),
                item.get('is_one_year'),
                item.get('is_sanctioned_countries')
            ))

        # 9. 检查数据库表是否存在所需字段
        print("检查数据库表结构...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'sanctions_risk_result'")
            columns = [col[0] for col in cursor.fetchall()]  # 修改这里，Kingbase返回的是元组不是字典

            required_columns = {
                'entity_id', 'entity_dt', 'activestatus', 'ENTITYNAME1', 'ENTITYNAME4',
                'country_nm1', 'country_nm2', 'DATEVALUE1', 'sanctions_lev',
                'sanctions_list', 'mid_sanctions_list', 'no_sanctions_list',
                'unknown_risk_list', 'other_list', 'is_san', 'is_sco', 'is_ool',
                'is_one_year', 'is_sanctioned_countries'
            }

            missing_columns = required_columns - set(columns)
            if missing_columns:
                print(f"错误：数据库表缺少以下字段: {missing_columns}")
                print("请先执行以下SQL语句添加缺失字段:")
                for col in missing_columns:
                    print(f"ALTER TABLE sanctions_risk_result ADD COLUMN {col} TEXT;")
                return

        # 10. 将数据存入Kingbase
        print("开始将数据插入数据库...")
        with conn.cursor() as cursor:
            # 先清空表（如果需要）
            cursor.execute("TRUNCATE TABLE lng.sanctions_risk_result")

            sql = """
                  INSERT INTO lng.sanctions_risk_result (entity_id, entity_dt, activestatus, ENTITYNAME1, ENTITYNAME4, 
                                                     country_nm1, country_nm2, DATEVALUE1, sanctions_lev, 
                                                     sanctions_list, mid_sanctions_list, no_sanctions_list, 
                                                     unknown_risk_list, other_list, is_san, is_sco, is_ool,
                                                     is_one_year, is_sanctioned_countries) 
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  """

            # 分批插入以避免内存问题
            batch_size = 1000
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                cursor.executemany(sql, batch)
                conn.commit()
                print(f"已插入 {i + len(batch)}/{len(data_to_insert)} 条记录")

        print(f"成功将 {len(data_to_insert)} 条记录插入数据库")

    except psycopg2.Error as e:
        print(f"数据库操作出错: {e}")
        if conn:
            conn.rollback()
    except (TypeError, ValueError) as e:  # 捕获序列化或编码错误
        print(f"JSON编码出错: {e}")
    except Exception as e:
        print(f"处理数据时出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")
        print("程序执行完毕")


if __name__ == '__main__':
    main()