import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
from datetime import datetime
import threading

class KingbaseConnector:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        self.start_time = None
        self.current_sql_index = 0
        self.sql_names = ["清理并插入 lng.risk_dqs_test 表", "清理并插入 lng.dqs_entity_sanctions_test 表", "清理并插入 lng.dqs_person_sanctions_test 表"]

    def connect(self):
        """连接到 Kingbase 数据库"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()
            print("成功连接到 Kingbase 数据库")
            return True
        except Exception as e:
            print(f"连接数据库失败: {e}")
            return False

    def execute_sql(self, sql_statement, params=None):
        """执行 SQL 语句"""
        try:
            if params:
                self.cursor.execute(sql_statement, params)
            else:
                self.cursor.execute(sql_statement)

            # 如果是查询语句，返回结果
            if sql_statement.strip().upper().startswith('SELECT'):
                result = self.cursor.fetchall()
                return result
            else:
                # 对于非查询语句，返回影响的行数
                return self.cursor.rowcount
        except Exception as e:
            print(f"执行 SQL 失败: {e}")
            print(f"SQL: {sql_statement}")
            return None

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("数据库连接已关闭")


def main():
    # 数据库连接参数
    db_config = {
        'host': '10.11.142.145',
        'port': '54321',
        'dbname': 'lngdb',
        'user': 'system',
        'password': 'zV2,oB5%'
    }

    # 两段 SQL 语句
    sql_statements = [
        # 第一段 SQL
        """
        truncate table lng.risk_dqs_test;
insert into lng.risk_dqs_test
SELECT entity_id,-- 实体id
       entity_dt,-- 制裁时间
       activestatus,-- 活跃状态
       -- start_date_active,-- 制裁开始时间
       t1.ENTITYNAME1,-- 实体名称
       t2.ENTITYNAME4,-- 实体曾用名
       -- t3.description1_value_cn,-- 制裁对象类型
       t3.description2_value_cn,-- 制裁行为类型
       t3.NMTOKEN_LEVEL,-- 制裁行为类型标签
       CASE
           WHEN NMTOKEN_LEVEL = 'SAN' THEN
               '高风险'
           ELSE '无风险'
           END AS is_san,
       CASE
           WHEN NMTOKEN_LEVEL = 'SCO-高风险' THEN '高风险'
           WHEN NMTOKEN_LEVEL = 'SCO-中风险' THEN '中风险'
           ELSE '无风险'
           END AS is_sco,
       CASE
           WHEN NMTOKEN_LEVEL = 'OOL' THEN
               '中风险'
           ELSE '无风险'
           END AS is_ool,
       CASE
           WHEN DATEVALUE1 IS NULL OR TRIM(DATEVALUE1) = '' THEN '无法判断'
           WHEN LENGTH(DATEVALUE1) = 11
               AND SUBSTR(DATEVALUE1, 5, 1) = '-'
               AND SUBSTR(DATEVALUE1, 9, 1) = '-' THEN
               CASE
                   WHEN EXTRACT(DAY FROM (CAST(CURRENT_DATE AS TIMESTAMP) - TO_DATE(DATEVALUE1, 'YYYY-Mon-DD'))) <= 365 THEN '中风险'
                   ELSE '无风险'
                   END
           ELSE '无法判断'
           END AS is_one_year,
       CASE
           WHEN T4.country_nm1 IN (SELECT Countryname FROM lng.contry_port) THEN '中风险'
           when T4.country_nm1 = '' or T4.country_nm1 is null then '无法判断'
           ELSE '无风险'
           END AS is_sanctioned_countries,
       t3.description3_value_cn,-- 制裁实体类型
       t3.start_time,
       t3.end_time,
       t3.SANCTIONS_NM,-- 被制裁区间及规则
       DATEVALUE1,--
       t4.country_nm1,-- 注册国
       t5.country_nm2 -- 所属国
FROM-- 制裁实体（公司、船舶）基础信息
    (SELECT id,
                  entity_id,
                  entity_dt, action, activestatus
           FROM
               (select id,
                  entity_id,
                  entity_dt,
                  action, 
                  activestatus,
                  row_number() over(partition by entity_id order by to_date(entity_dt, 'DD-Mon-YYYY') desc, create_time desc) rn
                  from lng.ods_zyhy_rm_en_entity_df_3) t
           WHERE rn = 1 
) t
        LEFT JOIN (SELECT main_id, CONCAT(DATE_YEAR, '-', DATE_MON, '-', DATE_DAY) AS DATEVALUE1
                   FROM lng.ods_zyhy_rm_enpr_datedetails_df_3
                   WHERE datetype = 'Date of Registration') t0 ON t.id = t0.main_id
        LEFT JOIN -- 公司名称
-- 一个MAIN_ID对应多个NAMEDETAILS_ID
-- NAMEDETAILS_ID,MAIN_ID,  batch_id -- batchi_id  取最新的
        (SELECT NAMEDETAILS_ID,
                MAIN_ID,
                NAMETYPE,
                CONVERT(FROM_BASE64(SUFFIX) USING utf8mb4)             AS SUFFIX,
                CONVERT(FROM_BASE64(ENTITYNAME) USING utf8mb4)         AS ENTITYNAME1,
                CONVERT(FROM_BASE64(ORIGINALSCRIPTNAME) USING utf8mb4) AS ORIGINALSCRIPTNAME
         FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
         WHERE DATATYPE = 'ENTITY'
           AND NAMETYPE = 'Primary Name') t1 ON t.id = t1.MAIN_ID

        LEFT JOIN -- 公司曾用名
        (SELECT MAIN_ID,
                GROUP_CONCAT(DISTINCT CONVERT ( FROM_BASE64 ( ENTITYNAME ) USING utf8mb4 )
                    ORDER BY CONVERT ( FROM_BASE64 ( ENTITYNAME ) USING utf8mb4 ) SEPARATOR ';' ) AS ENTITYNAME4
         FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
         WHERE DATATYPE = 'ENTITY'
           AND NAMETYPE = 'Formerly Known As'
         GROUP BY MAIN_ID) t2 ON t.id = t2.MAIN_ID
        LEFT JOIN (
SELECT DISTINCT
q.MAIN_ID,
q2.NMTOKEN AS description2_value_cn,
q3.NMTOKEN AS description3_value_cn,
CASE

WHEN q2.NMTOKEN = 'Sanctions Lists' THEN
'SAN' 
WHEN q2.NMTOKEN = 'Other Official Lists' THEN
'OOL' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Majority Owned', 'OFAC Related - Majority Owned', 'OFAC - Regional Sanctions Related - Majority Owned', 'EU - Regional Sanctions Related - Majority Owned', 'UK Related - Control', 'OFAC Related - Control', 'OFAC - Regional Sanctions Related - Control', 'EU - Regional Sanctions Related - Control', 'UK Related - Ownership Unknown', 'OFAC Related - Ownership Unknown', 'OFAC - Regional Sanctions Related - Ownership Unknown', 'EU - Regional Sanctions Related - Ownership Unknown' ) THEN
'SCO-高风险' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Minority Owned', 'OFAC Related - Minority Owned', 'OFAC - Regional Sanctions Related - Minority Owned', 'EU - Regional Sanctions Related - Minority Owned' ) THEN
'SCO-中风险' 
ELSE'无风险' 
END NMTOKEN_LEVEL,
start_time,
end_time,
CONCAT ( SANCTIONS_NM, '(', STATUS, ')' ) AS SANCTIONS_NM 
FROM
(
SELECT DESCRIPTIONS_ID, MAIN_ID, DESCRIPTION1_ID, DESCRIPTION2_ID, DESCRIPTION3_ID, DATATYPE FROM lng.ods_zyhy_rm_enpr_descriptions_df_3 
where DATATYPE='ENTITY'
) q
LEFT JOIN ( SELECT DESCRIPTION1_ID, RECORDTYPE, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description1_df_3 ) q1 ON q.DESCRIPTION1_ID = q1.DESCRIPTION1_ID
LEFT JOIN ( SELECT DESCRIPTION2_ID, DESCRIPTION1_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description2_df_3 ) q2 ON q.DESCRIPTION2_ID = q2.DESCRIPTION2_ID
LEFT JOIN ( SELECT DESCRIPTION3_ID, DESCRIPTION2_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description3_df_4 ) q3 ON q.DESCRIPTION3_ID = q3.DESCRIPTION3_ID
LEFT JOIN (
SELECT
MAIN_ID,
CONCAT (CASE WHEN SINCEYEAR = 'null' THEN '' ELSE SINCEYEAR END,
CASE WHEN SINCEMONTH = 'null' THEN '' ELSE CONCAT ( '-', SINCEMONTH ) END,
CASE WHEN SINCEDAY = 'null' THEN '' ELSE CONCAT ( '-', SINCEDAY ) END ) AS START_time,
CONCAT ( CASE WHEN TOYEAR = 'null' THEN '' ELSE TOYEAR END,
CASE WHEN TOMONTH = 'null' THEN '' ELSE CONCAT ( '-', TOMONTH ) END,
CASE WHEN TODAY = 'null' THEN '' ELSE CONCAT ( '-', TODAY ) END ) AS end_time,
VALUE	
FROM
lng.ods_zyhy_rm_enpr_sanctisection_df_3 ) q4
on q.MAIN_ID=q4.MAIN_ID
LEFT JOIN ( 
SELECT 
SANCTIONS_CD, 
CONVERT ( FROM_BASE64 ( SANCTIONS_NM ) USING utf8mb4 ) AS SANCTIONS_NM, 
STATUS, 
DESCRIPTION2ID 
FROM lng.ods_zyhy_rm_sanctions_df_2 ) q5 
ON q2.DESCRIPTION2_ID=q5.DESCRIPTION2ID
and q4.VALUE= q5.SANCTIONS_CD

WHERE end_time = '' 
) t3 ON t.id = t3.MAIN_ID
        LEFT JOIN -- 注册国
        (SELECT COMPANYDETAILS_ID,
                MAIN_ID,
                DATATYPE,
                COUNTRYTYPE,
                CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm1
         FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
         WHERE DATATYPE = 'ENTITY'
           AND COUNTRYTYPE = 'Country of Registration') t4 ON t.id = t4.MAIN_ID
        LEFT JOIN -- 所属国
        (SELECT COMPANYDETAILS_ID,
                MAIN_ID,
                DATATYPE,
                COUNTRYTYPE,
                CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm2
         FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
         WHERE DATATYPE = 'ENTITY'
           AND COUNTRYTYPE = 'Sanctioned Region') t5 ON t.id = t5.MAIN_ID
        LEFT JOIN -- 公司识别号码         idvalue1
        (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue1
         FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
         WHERE IDTYPE = 'Company Identification No.'
         GROUP BY MAIN_ID) t6 ON t.id = t6.MAIN_ID
        LEFT JOIN -- OFAC制裁项目代码	   idvalue6
        (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue6
         FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
         WHERE IDTYPE = 'OFAC Program ID'
         GROUP BY MAIN_ID) t7 ON t.id = t7.MAIN_ID
        LEFT JOIN -- OFAC唯一识别码		  idvalue7
        (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue7
         FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
         WHERE IDTYPE = 'OFAC Unique ID'
         GROUP BY MAIN_ID) t8 ON t.id = t8.MAIN_ID
        LEFT JOIN -- 国际海事组织（IMO）船舶编号		idvalue25
        (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue25
         FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
         WHERE IDTYPE = 'International Maritime Organization (IMO) Ship No.'
         GROUP BY MAIN_ID) t9 ON t.id = t9.MAIN_ID
        """,

        # 第二段 SQL
        """
truncate table lng.dqs_entity_sanctions_test;
insert into lng.dqs_entity_sanctions_test
select entity_id,
       group_concat(DISTINCT entity_dt ORDER BY entity_dt ASC SEPARATOR ';' )                                                                                                                           AS entity_dt,
       group_concat(DISTINCT activestatus ORDER BY activestatus ASC SEPARATOR ';' )                                                                                                                     AS activestatus,
       group_concat(DISTINCT ENTITYNAME1 ORDER BY ENTITYNAME1 ASC SEPARATOR ';' )                                                                                                                       AS ENTITYNAME1,
       group_concat(DISTINCT ENTITYNAME4 ORDER BY ENTITYNAME4 ASC SEPARATOR ';' )                                                                                                                       AS ENTITYNAME4,
           ''                                                                                                 AS description1_value_cn,
       group_concat(DISTINCT description2_value_cn ORDER BY description2_value_cn ASC SEPARATOR ';' )                                                                                                   AS description2_value_cn,
       group_concat(DISTINCT 
case when NMTOKEN_LEVEL like '%SCO%' then 'SCO' 
when NMTOKEN_LEVEL like '%SAN%' then 'SAN' 
when NMTOKEN_LEVEL like '%OOL%' then 'OOL' 
 END ORDER BY 
 case when NMTOKEN_LEVEL like '%SCO%' then 'SCO' 
 when NMTOKEN_LEVEL like '%SAN%' then 'SAN' 
when NMTOKEN_LEVEL like '%OOL%' then 'OOL' 
 END ASC SEPARATOR ';'  ) AS NMTOKEN_LEVEL,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SAN' THEN 1 ELSE 0 END) = 1 THEN '高风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_san,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SCO-高风险' THEN 1 ELSE 0 END) = 1 THEN '高风险'
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SCO-中风险' THEN 1 ELSE 0 END) = 1 THEN '中风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_sco,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'OOL' THEN 1 ELSE 0 END) = 1 THEN '中风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_ool,
       MAX(
               CASE
                   WHEN DATEVALUE1 IS NULL OR TRIM(DATEVALUE1) = '' THEN '无法判断'
                   WHEN LENGTH(DATEVALUE1) = 11
                       AND SUBSTR(DATEVALUE1, 5, 1) = '-'
                       AND SUBSTR(DATEVALUE1, 9, 1) = '-' THEN
                       CASE
                           WHEN EXTRACT(DAY FROM (CAST(CURRENT_DATE AS TIMESTAMP) - TO_DATE(DATEVALUE1, 'YYYY-Mon-DD'))) <= 365 THEN '中风险'
                           ELSE '无风险'
                           END
                   ELSE '无法判断'
                   END
       )                                                                                                                                                                                                AS is_one_year,
       MAX(CASE
               WHEN country_nm1 IN (SELECT Countryname FROM lng.contry_port) THEN '中风险'
               when country_nm1 = '' or country_nm1 is null then '无法判断'
               ELSE '无风险'
           END)                                                                                                                                                                                         AS is_sanctioned_countries,
       group_concat(DISTINCT description3_value_cn ORDER BY description3_value_cn ASC SEPARATOR ';' )                                                                                                   AS description3_value_cn,
       group_concat(DISTINCT concat(`NMTOKEN_LEVEL`,'|',`START_time`,concat( '~', `end_time` ),'|',`SANCTIONS_NM`)
ORDER BY
concat(`NMTOKEN_LEVEL`,'|',`START_time`,concat( '~', `end_time` ),'|',`SANCTIONS_NM`) ASC SEPARATOR ';')                                                                                                                                                                                   AS SANCTIONS_NM,
       group_concat(DISTINCT DATEVALUE1 ORDER BY DATEVALUE1 ASC SEPARATOR ';' )                                                                                                                         AS DATEVALUE1,
       group_concat(DISTINCT country_nm1 ORDER BY country_nm1 ASC SEPARATOR ';' )                                                                                                                       AS country_nm1,
       group_concat(DISTINCT country_nm2 ORDER BY country_nm2 ASC SEPARATOR ';' )                                                                                                                       AS country_nm2,
       group_concat(DISTINCT idvalue1 ORDER BY idvalue1 ASC SEPARATOR ';' )                                                                                                                             AS idvalue1,
       group_concat(DISTINCT idvalue6 ORDER BY idvalue6 ASC SEPARATOR ';' )                                                                                                                             AS idvalue6,
       group_concat(DISTINCT idvalue7 ORDER BY idvalue7 ASC SEPARATOR ';' )                                                                                                                             AS idvalue7,
       group_concat(DISTINCT idvalue25 ORDER BY idvalue25 ASC SEPARATOR ';' )                                                                                                                           AS idvalue25

from (SELECT entity_id,-- 实体id
             entity_dt,-- 制裁时间  
             activestatus,-- 活跃状态
--  start_date_active,-- 制裁开始时间
             t1.ENTITYNAME1,-- 实体名称
             t2.ENTITYNAME4,-- 实体曾用名
-- t3.description1_value_cn,-- 制裁对象类型
             t3.description2_value_cn,-- 制裁行为类型
             t3.NMTOKEN_LEVEL,-- 制裁行为类型标签
             t3.description3_value_cn,-- 制裁实体类型
             t3.start_time,
             t3.end_time,
             t3.SANCTIONS_NM,-- 被制裁区间及规则
             DATEVALUE1,--
             t4.country_nm1,-- 注册国
             t5.country_nm2, -- 所属国
             idvalue1,
             idvalue6,
             idvalue7,
             idvalue25
      FROM-- 制裁实体（公司、船舶）基础信息
          (SELECT id,
                  entity_id,
                  entity_dt, action, activestatus
           FROM
               (select id,
                  entity_id,
                  entity_dt,
                  action, 
                  activestatus,
                  row_number() over(partition by entity_id order by to_date(entity_dt, 'DD-Mon-YYYY') desc, create_time desc) rn
                  from lng.ods_zyhy_rm_en_entity_df_3) t
           WHERE rn = 1) t
              LEFT JOIN (SELECT main_id, CONCAT(DATE_YEAR, '-', DATE_MON, '-', DATE_DAY) AS DATEVALUE1
                         FROM lng.ods_zyhy_rm_enpr_datedetails_df_3
                         WHERE datetype = 'Date of Registration') t0 ON t.id = t0.main_id
              LEFT JOIN -- 公司名称
-- 一个MAIN_ID对应多个NAMEDETAILS_ID
-- NAMEDETAILS_ID,MAIN_ID,  batch_id -- batchi_id  取最新的
              (SELECT NAMEDETAILS_ID,
                      MAIN_ID,
                      NAMETYPE,
                      CONVERT(FROM_BASE64(SUFFIX) USING utf8mb4)             AS SUFFIX,
                      CONVERT(FROM_BASE64(ENTITYNAME) USING utf8mb4)         AS ENTITYNAME1,
                      CONVERT(FROM_BASE64(ORIGINALSCRIPTNAME) USING utf8mb4) AS ORIGINALSCRIPTNAME
               FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
               WHERE DATATYPE = 'ENTITY'
                 AND NAMETYPE = 'Primary Name') t1 ON t.id = t1.MAIN_ID
              LEFT JOIN -- 公司曾用名
              (SELECT MAIN_ID,
                      GROUP_CONCAT(DISTINCT CONVERT ( FROM_BASE64 ( ENTITYNAME ) USING utf8mb4 )
                          ORDER BY CONVERT ( FROM_BASE64 ( ENTITYNAME ) USING utf8mb4 ) SEPARATOR ';' ) AS ENTITYNAME4
               FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
               WHERE DATATYPE = 'ENTITY'
                 AND NAMETYPE = 'Formerly Known As'
               GROUP BY MAIN_ID) t2 ON t.id = t2.MAIN_ID
              LEFT JOIN (
SELECT DISTINCT
q.MAIN_ID,
q2.NMTOKEN AS description2_value_cn,
q3.NMTOKEN AS description3_value_cn,
CASE

WHEN q2.NMTOKEN = 'Sanctions Lists' THEN
'SAN' 
WHEN q2.NMTOKEN = 'Other Official Lists' THEN
'OOL' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Majority Owned', 'OFAC Related - Majority Owned', 'OFAC - Regional Sanctions Related - Majority Owned', 'EU - Regional Sanctions Related - Majority Owned', 'UK Related - Control', 'OFAC Related - Control', 'OFAC - Regional Sanctions Related - Control', 'EU - Regional Sanctions Related - Control', 'UK Related - Ownership Unknown', 'OFAC Related - Ownership Unknown', 'OFAC - Regional Sanctions Related - Ownership Unknown', 'EU - Regional Sanctions Related - Ownership Unknown' ) THEN
'SCO-高风险' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Minority Owned', 'OFAC Related - Minority Owned', 'OFAC - Regional Sanctions Related - Minority Owned', 'EU - Regional Sanctions Related - Minority Owned' ) THEN
'SCO-中风险' 
ELSE'无风险' 
END NMTOKEN_LEVEL,
start_time,
end_time,
CONCAT ( SANCTIONS_NM, '(', STATUS, ')' ) AS SANCTIONS_NM 
FROM
(
SELECT DESCRIPTIONS_ID, MAIN_ID, DESCRIPTION1_ID, DESCRIPTION2_ID, DESCRIPTION3_ID, DATATYPE FROM lng.ods_zyhy_rm_enpr_descriptions_df_3 
where DATATYPE='ENTITY'
) q
LEFT JOIN ( SELECT DESCRIPTION1_ID, RECORDTYPE, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description1_df_3 ) q1 ON q.DESCRIPTION1_ID = q1.DESCRIPTION1_ID
LEFT JOIN ( SELECT DESCRIPTION2_ID, DESCRIPTION1_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description2_df_3 ) q2 ON q.DESCRIPTION2_ID = q2.DESCRIPTION2_ID
LEFT JOIN ( SELECT DESCRIPTION3_ID, DESCRIPTION2_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description3_df_4 ) q3 ON q.DESCRIPTION3_ID = q3.DESCRIPTION3_ID
LEFT JOIN (
SELECT
MAIN_ID,
CONCAT (CASE WHEN SINCEYEAR = 'null' THEN '' ELSE SINCEYEAR END,
CASE WHEN SINCEMONTH = 'null' THEN '' ELSE CONCAT ( '-', SINCEMONTH ) END,
CASE WHEN SINCEDAY = 'null' THEN '' ELSE CONCAT ( '-', SINCEDAY ) END ) AS START_time,
CONCAT ( CASE WHEN TOYEAR = 'null' THEN '' ELSE TOYEAR END,
CASE WHEN TOMONTH = 'null' THEN '' ELSE CONCAT ( '-', TOMONTH ) END,
CASE WHEN TODAY = 'null' THEN '' ELSE CONCAT ( '-', TODAY ) END ) AS end_time,
VALUE	
FROM
lng.ods_zyhy_rm_enpr_sanctisection_df_3 ) q4
on q.MAIN_ID=q4.MAIN_ID
LEFT JOIN ( 
SELECT 
SANCTIONS_CD, 
CONVERT ( FROM_BASE64 ( SANCTIONS_NM ) USING utf8mb4 ) AS SANCTIONS_NM, 
STATUS, 
DESCRIPTION2ID 
FROM lng.ods_zyhy_rm_sanctions_df_2 ) q5 
ON q2.DESCRIPTION2_ID=q5.DESCRIPTION2ID
and q4.VALUE= q5.SANCTIONS_CD

WHERE end_time = '' ) t3 ON t.id = t3.MAIN_ID
              LEFT JOIN -- 注册国
              (SELECT COMPANYDETAILS_ID,
                      MAIN_ID,
                      DATATYPE,
                      COUNTRYTYPE,
                      CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm1
               FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
               WHERE DATATYPE = 'ENTITY'
                 AND COUNTRYTYPE = 'Country of Registration') t4 ON t.id = t4.MAIN_ID
              LEFT JOIN -- 所属国
              (SELECT COMPANYDETAILS_ID,
                      MAIN_ID,
                      DATATYPE,
                      COUNTRYTYPE,
                      CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm2
               FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
               WHERE DATATYPE = 'ENTITY'
                 AND COUNTRYTYPE = 'Sanctioned Region') t5 ON t.id = t5.MAIN_ID
              LEFT JOIN -- 公司识别号码         idvalue1
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue1
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'Company Identification No.'
               GROUP BY MAIN_ID) t6 ON t.id = t6.MAIN_ID
              LEFT JOIN -- OFAC制裁项目代码	   idvalue6
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue6
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'OFAC Program ID'
               GROUP BY MAIN_ID) t7 ON t.id = t7.MAIN_ID
              LEFT JOIN -- OFAC唯一识别码		  idvalue7
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue7
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'OFAC Unique ID'
               GROUP BY MAIN_ID) t8 ON t.id = t8.MAIN_ID
              LEFT JOIN -- 国际海事组织（IMO）船舶编号		idvalue25
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue25
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'International Maritime Organization (IMO) Ship No.'
               GROUP BY MAIN_ID) t9 ON t.id = t9.MAIN_ID) a
group by entity_id
        """,

        # 第三段 SQL
        """
        truncate table lng.dqs_person_sanctions_test;
        insert into lng.dqs_person_sanctions_test
select entity_id,
       group_concat(DISTINCT entity_dt ORDER BY entity_dt ASC SEPARATOR ';' )                                                                                                                           AS entity_dt,
       group_concat(DISTINCT activestatus ORDER BY activestatus ASC SEPARATOR ';' )                                                                                                                     AS activestatus,
       group_concat(DISTINCT ENTITYNAME1 ORDER BY ENTITYNAME1 ASC SEPARATOR ';' )                                                                                                                       AS ENTITYNAME1,
       group_concat(DISTINCT ENTITYNAME4 ORDER BY ENTITYNAME4 ASC SEPARATOR ';' )                                                                                                                       AS ENTITYNAME4,
           ''                                                                                                 AS description1_value_cn,
       group_concat(DISTINCT description2_value_cn ORDER BY description2_value_cn ASC SEPARATOR ';' )                                                                                                   AS description2_value_cn,
       group_concat(
       DISTINCT 
case when NMTOKEN_LEVEL like '%SCO%' then 'SCO' 
when NMTOKEN_LEVEL like '%SAN%' then 'SAN' 
when NMTOKEN_LEVEL like '%OOL%' then 'OOL' 
 END ORDER BY 
 case when NMTOKEN_LEVEL like '%SCO%' then 'SCO' 
 when NMTOKEN_LEVEL like '%SAN%' then 'SAN' 
when NMTOKEN_LEVEL like '%OOL%' then 'OOL' 
 END ASC SEPARATOR ';'  ) AS NMTOKEN_LEVEL,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SAN' THEN 1 ELSE 0 END) = 1 THEN '高风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_san,
       --group_concat(DISTINCT NMTOKEN_LEVEL ORDER BY NMTOKEN_LEVEL ASC SEPARATOR ';' )                                                                                                                   AS NMTOKEN_LEVELs,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SCO-高风险' THEN 1 ELSE 0 END) = 1 THEN '高风险'
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'SCO-中风险' THEN 1 ELSE 0 END) = 1 THEN '中风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_sco,
       CASE
           WHEN MAX(CASE WHEN TRIM(NMTOKEN_LEVEL) = 'OOL' THEN 1 ELSE 0 END) = 1 THEN '中风险'
           ELSE '无风险'
           END                                                                                                                                                                                          AS is_ool,
       MAX(
               CASE
                   WHEN DATEVALUE1 IS NULL OR TRIM(DATEVALUE1) = '' THEN '无法判断'
                   WHEN LENGTH(DATEVALUE1) = 11
                       AND SUBSTR(DATEVALUE1, 5, 1) = '-'
                       AND SUBSTR(DATEVALUE1, 9, 1) = '-' THEN
                       CASE
                           WHEN EXTRACT(DAY FROM (CAST(CURRENT_DATE AS TIMESTAMP) - TO_DATE(DATEVALUE1, 'YYYY-Mon-DD'))) <= 365 THEN '中风险'
                           ELSE '无风险'
                           END
                   ELSE '无法判断'
                   END
       )                                                                                                                                                                                                AS is_one_year,
       MAX(CASE
               WHEN country_nm1 IN (SELECT Countryname FROM lng.contry_port) THEN '中风险'
               when country_nm1 = '' or country_nm1 is null then '无法判断'
               ELSE '无风险'
           END)                                                                                                                                                                                         AS is_sanctioned_countries,
       group_concat(DISTINCT description3_value_cn ORDER BY description3_value_cn ASC SEPARATOR ';' )                                                                                                   AS description3_value_cn,
       group_concat(DISTINCT concat(`NMTOKEN_LEVEL`,'|',`START_time`,concat( '~', `end_time` ),'|',`SANCTIONS_NM`)
ORDER BY
concat(`NMTOKEN_LEVEL`,'|',`START_time`,concat( '~', `end_time` ),'|',`SANCTIONS_NM`) ASC SEPARATOR ';')                                                                                                                                                                                   AS SANCTIONS_NM,
       group_concat(DISTINCT DATEVALUE1 ORDER BY DATEVALUE1 ASC SEPARATOR ';' )                                                                                                                         AS DATEVALUE1,
       group_concat(DISTINCT country_nm1 ORDER BY country_nm1 ASC SEPARATOR ';' )                                                                                                                       AS country_nm1,
       group_concat(DISTINCT country_nm2 ORDER BY country_nm2 ASC SEPARATOR ';' )                                                                                                                       AS country_nm2,
       group_concat(DISTINCT idvalue1 ORDER BY idvalue1 ASC SEPARATOR ';' )                                                                                                                             AS idvalue1,
       group_concat(DISTINCT idvalue6 ORDER BY idvalue6 ASC SEPARATOR ';' )                                                                                                                             AS idvalue6,
       group_concat(DISTINCT idvalue7 ORDER BY idvalue7 ASC SEPARATOR ';' )                                                                                                                             AS idvalue7,
       group_concat(DISTINCT idvalue25 ORDER BY idvalue25 ASC SEPARATOR ';' )                                                                                                                           AS idvalue25
from (SELECT entity_id,-- 实体id
             entity_dt,-- 制裁时间
             activestatus,-- 活跃状态
--  start_date_active,-- 制裁开始时间
             t1.ENTITYNAME1,-- 实体名称
             t2.ENTITYNAME4,-- 实体曾用名
-- t3.description1_value_cn,-- 制裁对象类型
             t3.description2_value_cn,-- 制裁行为类型
             t3.NMTOKEN_LEVEL,-- 制裁行为类型标签
             t3.description3_value_cn,-- 制裁实体类型
             t3.start_time,
             t3.end_time,
             t3.SANCTIONS_NM,-- 被制裁区间及规则
             DATEVALUE1,--
             t4.country_nm1,-- 注册国
             t5.country_nm2, -- 所属国
             idvalue1,
             idvalue6,
             idvalue7,
             idvalue25
      FROM-- 制裁实体（公司、船舶）基础信息
          (SELECT id,
                  person_id as entity_id,
                  entity_dt, action, activestatus
           FROM
               (select id,
                  person_id,
                  entity_dt,
                  action,
                  activestatus,
                  row_number() over(partition by person_id order by entity_dt desc,create_time desc) rn
                  from lng.ODS_ZYHY_RM_PR_PERSON_DF_3) t
           where rn = 1) t
              LEFT JOIN (SELECT main_id, CONCAT(DATE_YEAR, '-', DATE_MON, '-', DATE_DAY) AS DATEVALUE1
                         FROM lng.ods_zyhy_rm_enpr_datedetails_df_3
                         WHERE datetype = 'Date of Registration') t0 ON t.id = t0.main_id
              LEFT JOIN -- 公司名称
-- 一个MAIN_ID对应多个NAMEDETAILS_ID
-- NAMEDETAILS_ID,MAIN_ID,  batch_id -- batchi_id  取最新的
              (SELECT NAMEDETAILS_ID,
                      MAIN_ID,
                      NAMETYPE,
                      CONCAT(firstname,' ',middlename,' ',surname) AS ENTITYNAME1,
                      ORIGINALSCRIPTNAME
               FROM
	               (SELECT NAMEDETAILS_ID,
	                      MAIN_ID,
	                      NAMETYPE,
	                      CONVERT(FROM_BASE64(SUFFIX) USING utf8mb4)             AS SUFFIX,
	                      NVL(CONVERT(FROM_BASE64(firstname) USING utf8mb4),'') AS firstname,
	                      NVL(CONVERT(FROM_BASE64(middlename) USING utf8mb4),'') AS middlename,
	                      NVL(CONVERT(FROM_BASE64(surname) USING utf8mb4),'') AS surname,
	                      CONVERT(FROM_BASE64(ORIGINALSCRIPTNAME) USING utf8mb4) AS ORIGINALSCRIPTNAME
	               FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
	               WHERE DATATYPE = 'PERSON'
	                 AND NAMETYPE = 'Primary Name')) t1 ON t.id = t1.MAIN_ID
              LEFT JOIN -- 公司曾用名
              (SELECT MAIN_ID,
                      GROUP_CONCAT(DISTINCT CONVERT ( FROM_BASE64 ( ORIGINALSCRIPTNAME ) USING utf8mb4 )
                          ORDER BY CONVERT ( FROM_BASE64 ( ORIGINALSCRIPTNAME ) USING utf8mb4 ) SEPARATOR ';' ) AS ENTITYNAME4
               FROM lng.ods_zyhy_rm_enpr_namedetails_df_3
               WHERE DATATYPE = 'PERSON'
                 AND NAMETYPE = 'Formerly Known As'
               GROUP BY MAIN_ID) t2 ON t.id = t2.MAIN_ID
              LEFT JOIN (
SELECT DISTINCT
q.MAIN_ID,
q2.NMTOKEN AS description2_value_cn,
q3.NMTOKEN AS description3_value_cn,
CASE

WHEN q2.NMTOKEN = 'Sanctions Lists' THEN
'SAN' 
WHEN q2.NMTOKEN = 'Other Official Lists' THEN
'OOL' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Majority Owned', 'OFAC Related - Majority Owned', 'OFAC - Regional Sanctions Related - Majority Owned', 'EU - Regional Sanctions Related - Majority Owned', 'UK Related - Control', 'OFAC Related - Control', 'OFAC - Regional Sanctions Related - Control', 'EU - Regional Sanctions Related - Control', 'UK Related - Ownership Unknown', 'OFAC Related - Ownership Unknown', 'OFAC - Regional Sanctions Related - Ownership Unknown', 'EU - Regional Sanctions Related - Ownership Unknown' ) THEN
'SCO-高风险' 
WHEN q2.NMTOKEN = 'Sanctions Control and Ownership' 
AND q3.NMTOKEN IN ( 'UK Related - Minority Owned', 'OFAC Related - Minority Owned', 'OFAC - Regional Sanctions Related - Minority Owned', 'EU - Regional Sanctions Related - Minority Owned' ) THEN
'SCO-中风险' 
ELSE'无风险' 
END NMTOKEN_LEVEL,
start_time,
end_time,
CONCAT ( SANCTIONS_NM, '(', STATUS, ')' ) AS SANCTIONS_NM 
FROM
(
SELECT DESCRIPTIONS_ID, MAIN_ID, DESCRIPTION1_ID, DESCRIPTION2_ID, DESCRIPTION3_ID, DATATYPE FROM lng.ods_zyhy_rm_enpr_descriptions_df_3 
where DATATYPE='PERSON'
) q
LEFT JOIN ( SELECT DESCRIPTION1_ID, RECORDTYPE, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description1_df_3 ) q1 ON q.DESCRIPTION1_ID = q1.DESCRIPTION1_ID
LEFT JOIN ( SELECT DESCRIPTION2_ID, DESCRIPTION1_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description2_df_3 ) q2 ON q.DESCRIPTION2_ID = q2.DESCRIPTION2_ID
LEFT JOIN ( SELECT DESCRIPTION3_ID, DESCRIPTION2_ID, CONVERT ( FROM_BASE64 ( NMTOKEN ) USING utf8mb4 ) AS NMTOKEN FROM lng.ods_zyhy_rm_description3_df_4 ) q3 ON q.DESCRIPTION3_ID = q3.DESCRIPTION3_ID
LEFT JOIN (
SELECT
MAIN_ID,
CONCAT (CASE WHEN SINCEYEAR = 'null' THEN '' ELSE SINCEYEAR END,
CASE WHEN SINCEMONTH = 'null' THEN '' ELSE CONCAT ( '-', SINCEMONTH ) END,
CASE WHEN SINCEDAY = 'null' THEN '' ELSE CONCAT ( '-', SINCEDAY ) END ) AS START_time,
CONCAT ( CASE WHEN TOYEAR = 'null' THEN '' ELSE TOYEAR END,
CASE WHEN TOMONTH = 'null' THEN '' ELSE CONCAT ( '-', TOMONTH ) END,
CASE WHEN TODAY = 'null' THEN '' ELSE CONCAT ( '-', TODAY ) END ) AS end_time,
VALUE	
FROM
lng.ods_zyhy_rm_enpr_sanctisection_df_3 ) q4
on q.MAIN_ID=q4.MAIN_ID
LEFT JOIN ( 
SELECT 
SANCTIONS_CD, 
CONVERT ( FROM_BASE64 ( SANCTIONS_NM ) USING utf8mb4 ) AS SANCTIONS_NM, 
STATUS, 
DESCRIPTION2ID 
FROM lng.ods_zyhy_rm_sanctions_df_2 ) q5 
ON q2.DESCRIPTION2_ID=q5.DESCRIPTION2ID
and q4.VALUE= q5.SANCTIONS_CD

WHERE end_time = '' ) t3 ON t.id = t3.MAIN_ID
              LEFT JOIN -- 注册国
              (SELECT COMPANYDETAILS_ID,
                      MAIN_ID,
                      DATATYPE,
                      COUNTRYTYPE,
                      CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm1
               FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
               WHERE DATATYPE = 'PERSON'
                 AND COUNTRYTYPE = 'Country of Registration') t4 ON t.id = t4.MAIN_ID
              LEFT JOIN -- 所属国
              (SELECT COMPANYDETAILS_ID,
                      MAIN_ID,
                      DATATYPE,
                      COUNTRYTYPE,
                      CONVERT(FROM_BASE64(COUNTRYVALUE) USING utf8mb4) AS country_nm2
               FROM lng.ods_zyhy_rm_enpr_countrydetails_df_3
               WHERE DATATYPE = 'PERSON'
                 AND COUNTRYTYPE = 'Sanctioned Region') t5 ON t.id = t5.MAIN_ID
              LEFT JOIN -- 公司识别号码         idvalue1
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue1
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'Company Identification No.'
               GROUP BY MAIN_ID) t6 ON t.id = t6.MAIN_ID
              LEFT JOIN -- OFAC制裁项目代码	   idvalue6
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue6
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'OFAC Program ID'
               GROUP BY MAIN_ID) t7 ON t.id = t7.MAIN_ID
              LEFT JOIN -- OFAC唯一识别码		  idvalue7
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue7
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'OFAC Unique ID'
               GROUP BY MAIN_ID) t8 ON t.id = t8.MAIN_ID
              LEFT JOIN -- 国际海事组织（IMO）船舶编号		idvalue25
              (SELECT MAIN_ID, GROUP_CONCAT(DISTINCT IDVALUE ORDER BY IDVALUE SEPARATOR ';' ) AS idvalue25
               FROM lng.ods_zyhy_rm_enpr_idnumbertypes_df_3
               WHERE IDTYPE = 'International Maritime Organization (IMO) Ship No.'
               GROUP BY MAIN_ID) t9 ON t.id = t9.MAIN_ID) a
               
group by entity_id
        """
    ]

    # 创建数据库连接
    db = KingbaseConnector(**db_config)

    if not db.connect():
        sys.exit(1)

    try:
        # 执行两段 SQL
        for i, sql_stmt in enumerate(sql_statements, 1):
            print(f"正在执行第 {i} 段 SQL...")
            result = db.execute_sql(sql_stmt)

            if result is not None:
                if isinstance(result, list):
                    print(f"查询结果: {len(result)} 行")
                    # 如果需要，可以打印前几行结果
                    # for row in result[:5]:
                    #     print(row)
                else:
                    print(f"影响的行数: {result}")

            print(f"第 {i} 段 SQL 执行完成\n")

        print("所有 SQL 执行完成")

    except Exception as e:
        print(f"执行过程中发生错误: {e}")
    finally:
        # 关闭数据库连接
        db.close()


if __name__ == "__main__":
    main()