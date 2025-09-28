-- =============================================
-- 制裁风险数据处理 - Kingbase SQL实现
-- 功能：将Python代码逻辑转换为SQL查询
-- =============================================

-- 1. 基础风险数据查询（包含所有必要字段）
WITH base_risk_data AS (
    SELECT DISTINCT 
        t1.entity_id, 
        t1.entity_dt, 
        t1.activestatus, 
        t1.ENTITYNAME1, 
        t1.ENTITYNAME4, 
        t1.country_nm1, 
        t1.country_nm2, 
        t1.DATEVALUE1,
        t1.description2_value_cn, 
        t1.description3_value_cn, 
        t1.start_time, 
        t1.end_time,
        t1.SANCTIONS_NM, 
        t1.is_san, 
        t1.is_sco, 
        t1.is_ool, 
        t1.is_one_year, 
        t1.is_sanctioned_countries
        FROM lng.risk_dqs_test t1
        WHERE entity_id IN (
            SELECT entity_id FROM lng.risk_dqs_test 
            WHERE length(entity_id)=6 
            ORDER BY entity_id 
            LIMIT 1000
        )
),

-- 1.1 实体去重（用于最终聚合时避免笛卡尔重复）
entities AS (
    SELECT DISTINCT 
        entity_id, 
        entity_dt, 
        activestatus, 
        ENTITYNAME1, 
        ENTITYNAME4, 
        country_nm1, 
        country_nm2, 
        DATEVALUE1
    FROM base_risk_data
),

-- 1.2 实体级 is_one_year 判定（基于去重后的 DATEVALUE1）
one_year_eval AS (
    SELECT 
        e.entity_id,
        CASE 
            WHEN COUNT(*) FILTER (WHERE NULLIF(TRIM(b.DATEVALUE1), '') IS NOT NULL) = 0 THEN '无法判断'
            WHEN MAX(
                CASE 
                    WHEN NULLIF(TRIM(b.DATEVALUE1), '') IS NOT NULL
                         AND LENGTH(b.DATEVALUE1) = 11
                         AND SUBSTR(b.DATEVALUE1, 5, 1) = '-'
                         AND SUBSTR(b.DATEVALUE1, 9, 1) = '-'
                         AND UPPER(SUBSTR(b.DATEVALUE1, 6, 3)) IN ('JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC')
                         AND EXTRACT(DAY FROM (CAST(CURRENT_DATE AS TIMESTAMP) - TO_DATE(b.DATEVALUE1, 'YYYY-Mon-DD'))) <= 365
                    THEN 1 ELSE 0 END
            ) = 1 THEN '中风险'
            ELSE '无风险'
        END AS status
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    GROUP BY e.entity_id
),

-- 1.3 实体级 is_sanctioned_countries 判定（基于去重后的 country_nm1）
sanctioned_countries_eval AS (
    SELECT 
        e.entity_id,
        CASE 
            WHEN COUNT(*) FILTER (WHERE NULLIF(TRIM(b.country_nm1), '') IS NOT NULL) = 0 THEN '无法判断'
            WHEN MAX(CASE WHEN b.country_nm1 IN (SELECT Countryname FROM lng.contry_port) THEN 1 ELSE 0 END) = 1 THEN '中风险'
            ELSE '无风险'
        END AS status
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    GROUP BY e.entity_id
),

-- 2. 风险等级判定（严格优先级）
risk_level_determination AS (
    SELECT 
        e.entity_id,
        -- 检查是否存在高风险
        CASE 
            WHEN MAX(CASE WHEN is_san = '高风险' THEN 1 ELSE 0 END) = 1 
              OR MAX(CASE WHEN is_sco = '高风险' THEN 1 ELSE 0 END) = 1
              OR MAX(CASE WHEN is_ool = '高风险' THEN 1 ELSE 0 END) = 1
            THEN '高风险'
            -- 检查是否存在中风险
            WHEN MAX(CASE WHEN is_san = '中风险' THEN 1 ELSE 0 END) = 1 
              OR MAX(CASE WHEN is_sco = '中风险' THEN 1 ELSE 0 END) = 1
              OR MAX(CASE WHEN is_ool = '中风险' THEN 1 ELSE 0 END) = 1
              OR COALESCE(MAX(CASE WHEN oye.status = '中风险' THEN 1 ELSE 0 END),0) = 1
              OR COALESCE(MAX(CASE WHEN sce.status = '中风险' THEN 1 ELSE 0 END),0) = 1
            THEN '中风险'
            ELSE '无风险'
        END AS sanctions_lev
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    LEFT JOIN one_year_eval oye ON oye.entity_id = e.entity_id
    LEFT JOIN sanctioned_countries_eval sce ON sce.entity_id = e.entity_id
    GROUP BY e.entity_id
),

-- 3. 高风险数据聚合
high_risk_data AS (
    SELECT 
        entity_id,
        'is_san' as risk_type,
        is_san as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_san = '高风险'
    GROUP BY entity_id, is_san
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_sco' as risk_type,
        is_sco as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT('description3_value_cn', description3_value_cn)
        ) as tab_data
    FROM base_risk_data
    WHERE is_sco = '高风险' AND description3_value_cn IS NOT NULL AND description3_value_cn != ''
    GROUP BY entity_id, is_sco
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_ool' as risk_type,
        is_ool as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_ool = '高风险'
    GROUP BY entity_id, is_ool
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_one_year' as risk_type,
        is_one_year as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('DATEVALUE1', dv))
            FROM (
                SELECT DISTINCT DATEVALUE1 AS dv
                FROM base_risk_data b2
                WHERE b2.entity_id = base_risk_data.entity_id
                  AND b2.is_one_year = '高风险'
                ORDER BY dv
            ) s
        ) AS tab_data
    FROM base_risk_data
    WHERE is_one_year = '高风险'
    GROUP BY entity_id, is_one_year
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_sanctioned_countries' as risk_type,
        is_sanctioned_countries as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('country_nm1', cv))
            FROM (
                SELECT DISTINCT country_nm1 AS cv
                FROM base_risk_data b3
                WHERE b3.entity_id = base_risk_data.entity_id
                  AND b3.is_sanctioned_countries = '高风险'
                ORDER BY cv
            ) s
        ) AS tab_data
    FROM base_risk_data
    WHERE is_sanctioned_countries = '高风险'
    GROUP BY entity_id, is_sanctioned_countries
),

-- 4. 中风险数据聚合
mid_risk_data AS (
    SELECT 
        entity_id,
        'is_san' as risk_type,
        is_san as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_san = '中风险'
    GROUP BY entity_id, is_san
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_sco' as risk_type,
        is_sco as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT('description3_value_cn', description3_value_cn)
        ) as tab_data
    FROM base_risk_data
    WHERE is_sco = '中风险' AND description3_value_cn IS NOT NULL AND description3_value_cn != ''
    GROUP BY entity_id, is_sco
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_ool' as risk_type,
        is_ool as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_ool = '中风险'
    GROUP BY entity_id, is_ool
    
    UNION ALL
    
    SELECT 
        e.entity_id,
        'is_one_year' as risk_type,
        '中风险' as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('DATEVALUE1', dv))
            FROM (
                SELECT DISTINCT DATEVALUE1 AS dv
                FROM base_risk_data b2
                WHERE b2.entity_id = e.entity_id
                ORDER BY dv
            ) s
        ) AS tab_data
    FROM entities e
    JOIN one_year_eval oye ON oye.entity_id = e.entity_id AND oye.status = '中风险'
    
    UNION ALL
    
    SELECT 
        e.entity_id,
        'is_sanctioned_countries' as risk_type,
        '中风险' as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('country_nm1', cv))
            FROM (
                SELECT DISTINCT country_nm1 AS cv
                FROM base_risk_data b3
                WHERE b3.entity_id = e.entity_id
                ORDER BY cv
            ) s
        ) AS tab_data
    FROM entities e
    JOIN sanctioned_countries_eval sce ON sce.entity_id = e.entity_id AND sce.status = '中风险'
),

-- 5. 无法判断数据聚合
unknown_risk_data AS (
    SELECT 
        entity_id,
        'is_san' as risk_type,
        is_san as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_san = '无法判断'
    GROUP BY entity_id, is_san
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_sco' as risk_type,
        is_sco as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT('description3_value_cn', description3_value_cn)
        ) as tab_data
    FROM base_risk_data
    WHERE is_sco = '无法判断' AND description3_value_cn IS NOT NULL AND description3_value_cn != ''
    GROUP BY entity_id, is_sco
    
    UNION ALL
    
    SELECT 
        entity_id,
        'is_ool' as risk_type,
        is_ool as risk_value,
        JSON_ARRAYAGG(
            JSON_BUILD_OBJECT(
                'end_time', end_time,
                'start_time', start_time,
                'SANCTIONS_NM', SANCTIONS_NM,
                'description2_value_cn', description2_value_cn
            )
        ) as tab_data
    FROM base_risk_data
    WHERE is_ool = '无法判断'
    GROUP BY entity_id, is_ool
    
    UNION ALL
    
    SELECT 
        e.entity_id,
        'is_one_year' as risk_type,
        '无法判断' as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('DATEVALUE1', dv))
            FROM (
                SELECT DISTINCT DATEVALUE1 AS dv
                FROM base_risk_data b2
                WHERE b2.entity_id = e.entity_id
                ORDER BY dv
            ) s
        ) AS tab_data
    FROM entities e
    JOIN one_year_eval oye ON oye.entity_id = e.entity_id AND oye.status = '无法判断'
    
    UNION ALL
    
    SELECT 
        e.entity_id,
        'is_sanctioned_countries' as risk_type,
        '无法判断' as risk_value,
        (
            SELECT JSON_ARRAYAGG(JSON_BUILD_OBJECT('country_nm1', cv))
            FROM (
                SELECT DISTINCT country_nm1 AS cv
                FROM base_risk_data b3
                WHERE b3.entity_id = e.entity_id
                ORDER BY cv
            ) s
        ) AS tab_data
    FROM entities e
    JOIN sanctioned_countries_eval sce ON sce.entity_id = e.entity_id AND sce.status = '无法判断'
),

-- 6. 关联方信息查询（简化版，使用字符串拼接）
associated_parties AS (
    SELECT 
        t.associate_id,
        STRING_AGG(
            CONCAT('{"SORP_ID":"', COALESCE(t.SORP_ID::text, ''), 
                   '","nmtoken_level":"', COALESCE(IF(t.SOURCE_TYPE = 'PERSON', t3.nmtoken_level, t2.nmtoken_level), ''),
                   '","ENTITYNAME11":"', COALESCE(IF(t.SOURCE_TYPE = 'PERSON', t3.ENTITYNAME1, t2.ENTITYNAME1), ''),
                   '","SOURCE_TYPE":"', COALESCE(t.SOURCE_TYPE, ''),
                   '","RELATION_NM":"', COALESCE(t1.RELATION_NM, ''), '"}'), 
            ','
        ) as other_list_str
    FROM lng.ods_zyhy_rm_associate_df_3 t
    LEFT JOIN lng.ods_zyhy_rm_relation_df_3 t1 ON t.ASSOCIATE_CD = t1.RELATION_CD
    LEFT JOIN lng.dqs_entity_sanctions_test t2 ON t.SORP_ID = t2.entity_id
    LEFT JOIN lng.dqs_person_sanctions_test t3 ON t.SORP_ID = t3.entity_id
    WHERE t.associate_id IN (
        SELECT entity_id FROM lng.risk_dqs_test 
        WHERE length(entity_id)=6 
        ORDER BY entity_id 
        LIMIT 1000
    )
      AND t.SORP_ID IS NOT NULL
    GROUP BY t.associate_id
),

-- 6.1 风险描述信息（对齐Python逻辑中的映射）
san_desc AS (
    SELECT risk_type, risk_level AS risk_value, risk_desc, risk_desc_info, info
    FROM lng.sanctions_des_info
),

-- 6.1.1 无风险数据聚合（为未出现或评估为无风险的类型补齐对象，tab 为空数组）
no_risk_data AS (
    -- is_san 无风险：该实体不存在 any 高/中/无法判断 的 is_san 记录
    SELECT e.entity_id, 'is_san' AS risk_type, '无风险' AS risk_value, JSON_ARRAY() AS tab_data
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    GROUP BY e.entity_id
    HAVING COALESCE(MAX(CASE WHEN b.is_san IN ('高风险','中风险','无法判断') THEN 1 ELSE 0 END),0) = 0

    UNION ALL

    -- is_sco 无风险：该实体不存在 any 高/中/无法判断 的 is_sco 记录
    SELECT e.entity_id, 'is_sco' AS risk_type, '无风险' AS risk_value, JSON_ARRAY() AS tab_data
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    GROUP BY e.entity_id
    HAVING COALESCE(MAX(CASE WHEN b.is_sco IN ('高风险','中风险','无法判断') THEN 1 ELSE 0 END),0) = 0

    UNION ALL

    -- is_ool 无风险：该实体不存在 any 高/中/无法判断 的 is_ool 记录
    SELECT e.entity_id, 'is_ool' AS risk_type, '无风险' AS risk_value, JSON_ARRAY() AS tab_data
    FROM entities e
    LEFT JOIN base_risk_data b ON b.entity_id = e.entity_id
    GROUP BY e.entity_id
    HAVING COALESCE(MAX(CASE WHEN b.is_ool IN ('高风险','中风险','无法判断') THEN 1 ELSE 0 END),0) = 0

    UNION ALL

    -- is_one_year 无风险：实体级评估为无风险
    SELECT e.entity_id, 'is_one_year' AS risk_type, '无风险' AS risk_value, JSON_ARRAY() AS tab_data
    FROM entities e
    JOIN one_year_eval oye ON oye.entity_id = e.entity_id AND oye.status = '无风险'

    UNION ALL

    -- is_sanctioned_countries 无风险：实体级评估为无风险
    SELECT e.entity_id, 'is_sanctioned_countries' AS risk_type, '无风险' AS risk_value, JSON_ARRAY() AS tab_data
    FROM entities e
    JOIN sanctioned_countries_eval sce ON sce.entity_id = e.entity_id AND sce.status = '无风险'
),

-- 6.2 将各风险分组行转为包含描述信息的对象（高风险）- 字符串拼接版
high_risk_objects AS (
    SELECT 
        h.entity_id,
        CONCAT('{"risk_type":"', h.risk_type, 
               '","risk_value":"', h.risk_value,
               '","risk_desc":"', COALESCE(sd.risk_desc, ''),
               '","risk_desc_info":"', COALESCE(sd.risk_desc_info, ''),
               '","info":"', COALESCE(sd.info, ''),
               '","tab":', h.tab_data::text, '}') AS obj
    FROM high_risk_data h
    LEFT JOIN san_desc sd
      ON sd.risk_type = h.risk_type AND sd.risk_value = h.risk_value
),

-- 6.3 中风险对象 - 字符串拼接版
mid_risk_objects AS (
    SELECT 
        m.entity_id,
        CONCAT('{"risk_type":"', m.risk_type, 
               '","risk_value":"', m.risk_value,
               '","risk_desc":"', COALESCE(sd.risk_desc, ''),
               '","risk_desc_info":"', COALESCE(sd.risk_desc_info, ''),
               '","info":"', COALESCE(sd.info, ''),
               '","tab":', m.tab_data::text, '}') AS obj
    FROM mid_risk_data m
    LEFT JOIN san_desc sd
      ON sd.risk_type = m.risk_type AND sd.risk_value = m.risk_value
),

-- 6.4 无法判断对象 - 字符串拼接版
unknown_risk_objects AS (
    SELECT 
        u.entity_id,
        CONCAT('{"risk_type":"', u.risk_type, 
               '","risk_value":"', u.risk_value,
               '","risk_desc":"', COALESCE(sd.risk_desc, ''),
               '","risk_desc_info":"', COALESCE(sd.risk_desc_info, ''),
               '","info":"', COALESCE(sd.info, ''),
               '","tab":', u.tab_data::text, '}') AS obj
    FROM unknown_risk_data u
    LEFT JOIN san_desc sd
      ON sd.risk_type = u.risk_type AND sd.risk_value = u.risk_value
)

,

-- 6.5 无风险对象（补充无风险项，tab 为空）- 字符串拼接版
no_risk_objects AS (
    SELECT 
        n.entity_id,
        CONCAT('{"risk_type":"', n.risk_type, 
               '","risk_value":"', n.risk_value,
               '","risk_desc":"', COALESCE(sd.risk_desc, ''),
               '","risk_desc_info":"', COALESCE(sd.risk_desc_info, ''),
               '","info":"', COALESCE(sd.info, ''),
               '","tab":', n.tab_data::text, '}') AS obj
    FROM no_risk_data n
    LEFT JOIN san_desc sd
      ON sd.risk_type = n.risk_type AND sd.risk_value = n.risk_value
)

-- 7. 最终结果聚合
SELECT 
    e.entity_id,
    e.entity_dt,
    e.activestatus,
    e.ENTITYNAME1,
    e.ENTITYNAME4,
    e.country_nm1,
    e.country_nm2,
    e.DATEVALUE1,
    rld.sanctions_lev,
    
    -- 高风险列表（字符串拼接版）
    COALESCE(
        (
            SELECT CONCAT('[', STRING_AGG(o.obj, ','), ']')
            FROM high_risk_objects o
            WHERE o.entity_id = e.entity_id
        ),
        '[]'
    ) AS sanctions_list,
    
    -- 中风险列表（字符串拼接版）
    COALESCE(
        (
            SELECT CONCAT('[', STRING_AGG(o.obj, ','), ']')
            FROM mid_risk_objects o
            WHERE o.entity_id = e.entity_id
        ),
        '[]'
    ) AS mid_sanctions_list,
    
    -- 无风险列表（字符串拼接版）
    COALESCE(
        (
            SELECT CONCAT('[', STRING_AGG(o.obj, ','), ']')
            FROM no_risk_objects o
            WHERE o.entity_id = e.entity_id
        ),
        '[]'
    ) AS no_sanctions_list,
    
    -- 无法判断列表（字符串拼接版）
    COALESCE(
        (
            SELECT CONCAT('[', STRING_AGG(o.obj, ','), ']')
            FROM unknown_risk_objects o
            WHERE o.entity_id = e.entity_id
        ),
        '[]'
    ) AS unknown_risk_list,
    
    -- 关联方信息（字符串拼接版）
    COALESCE(
        CONCAT('[', ap.other_list_str, ']'), 
        '[]'
    ) AS other_list

FROM entities e
LEFT JOIN risk_level_determination rld ON e.entity_id = rld.entity_id
LEFT JOIN associated_parties ap ON e.entity_id = ap.associate_id

ORDER BY e.entity_id;

-- =============================================
-- 可选：将结果直接写入结果表
-- 说明：如需落表，取消以下两段注释。默认将 JSON 列转为 TEXT 以兼容表结构。
-- =============================================

-- TRUNCATE TABLE lng.sanctions_risk_result;
-- INSERT INTO lng.sanctions_risk_result (
--     entity_id,
--     entity_dt,
--     activestatus,
--     ENTITYNAME1,
--     ENTITYNAME4,
--     country_nm1,
--     country_nm2,
--     DATEVALUE1,
--     sanctions_lev,
--     sanctions_list,
--     mid_sanctions_list,
--     no_sanctions_list,
--     unknown_risk_list,
--     other_list
-- )
-- SELECT 
--     e.entity_id::text,
--     e.entity_dt,
--     e.activestatus,
--     e.ENTITYNAME1,
--     e.ENTITYNAME4,
--     e.country_nm1,
--     e.country_nm2,
--     e.DATEVALUE1,
--     rld.sanctions_lev,
--     (
--         SELECT COALESCE(JSON_ARRAYAGG(o.obj), JSON_ARRAY())
--         FROM high_risk_objects o
--         WHERE o.entity_id = e.entity_id
--     )::text AS sanctions_list,
--     (
--         SELECT COALESCE(JSON_ARRAYAGG(o.obj), JSON_ARRAY())
--         FROM mid_risk_objects o
--         WHERE o.entity_id = e.entity_id
--     )::text AS mid_sanctions_list,
--     (
--         SELECT COALESCE(JSON_ARRAYAGG(o.obj), JSON_ARRAY())
--         FROM no_risk_objects o
--         WHERE o.entity_id = e.entity_id
--     )::text AS no_sanctions_list,
--     (
--         SELECT COALESCE(JSON_ARRAYAGG(o.obj), JSON_ARRAY())
--         FROM unknown_risk_objects o
--         WHERE o.entity_id = e.entity_id
--     )::text AS unknown_risk_list,
--     COALESCE(ap.other_list, JSON_ARRAY())::text AS other_list
-- FROM entities e
-- LEFT JOIN risk_level_determination rld ON e.entity_id = rld.entity_id
-- LEFT JOIN associated_parties ap ON e.entity_id = ap.associate_id
-- ORDER BY e.entity_id;
