# import os
# os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"  # 国内镜像源
# #os.environ["HF_ENDPOINT"] = "https://mirrors.tuna.tsinghua.edu.cn/hugging-face-models"

import re
#import regex
import time
import logging
from typing import List, Dict, Tuple, Optional
from datetime import timedelta
from contextlib import asynccontextmanager
from functools import lru_cache
import pandas as pd
from rapidfuzz import fuzz, process
from fastapi import FastAPI, HTTPException, Query, APIRouter
import uvicorn
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import os

# from sentence_transformers import SentenceTransformer, util
# import torch

#from symspellpy import SymSpell, Verbosity

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------
# 数据库连接配置
# ------------------------------
KINGBASE_CONFIG = {
    'host': '10.11.142.145',
    'port': 54321,
    'database': 'lngdb',
    'user': 'system',
    'password': 'zV2,oB5%',
    'cursor_factory': RealDictCursor
}

# ------------------------------
# 响应模型
# ------------------------------
class MatchResponse(BaseModel):
    results: List[Dict]
    query: str
    timestamp: float

class RefreshResponse(BaseModel):
    status: str
    message: str

class HealthResponse(BaseModel):
    status: str
    timestamp: float
    database_status: str
    company_count: int

# ------------------------------
# 1. 公司名称标准化函数
# ------------------------------
@lru_cache(maxsize=100000)  # 缓存常见标准化结果
def normalize_company_name(text: str) -> str:
    """增强版公司名称标准化，支持多语言后缀和复杂格式处理"""
    if not text or not isinstance(text, str):
        return ""

    # 转为小写处理
    text = text.lower()

    # 第一步：移除多语言公司后缀
    suffixes = [
        # 中文后缀
        "有限公司", "股份有限公司", "有限责任公司", "集团有限公司", "集团股份有限公司",
        "合伙事务所", "股份公司", "责任公司",
        # 英文及国际通用后缀
        "limited liability company", "limited company", "company limited",
        "joint stock company", "joint-stock company",
        "s.a. de c.v.", "jsc", "ltda", "inc", "ltd", "llc", "gmbh", "corp",
        "co", "llp", "plc", "pty", "ag", "ohg", "bv", "nv", "sa", "pvt",
        # 日文后缀
        "株式会社", "合同会社",
        # 其他常见缩写
        "co., ltd", "corp.", "s.a.", "s.p.a"
    ]
    # 按长度倒序排序，确保长后缀优先匹配
    suffixes_sorted = sorted(suffixes, key=lambda x: len(x), reverse=True)
    suffix_pattern = r'\b(' + '|'.join(re.escape(s) for s in suffixes_sorted) + r')\b'

    cleaned_text = re.sub(
        suffix_pattern,
        '',
        text,
        flags=re.IGNORECASE
    )

    # 第二步：移除冗余前缀（如"The"、"A "等）
    prefixes = r'\b(the |a |an |la |le |el )'
    cleaned_text = re.sub(prefixes, '', cleaned_text, flags=re.IGNORECASE)

    # 第三步：移除所有特殊字符（保留字母、数字、空格和多语言字符）
    # \p{L}：匹配任意语言的字母（包括中文、俄文、阿拉伯文等）
    # \p{N}：匹配任意语言的数字（包括阿拉伯数字、中文数字等）
    # \s：匹配空格
    #cleaned_text = regex.sub(r'[^\p{L}\p{N}\s]', '', cleaned_text)

    # 第四步：清理空格并转为小写返回（统一格式）
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    return cleaned_text

# ------------------------------
# 2. 公司匹配器（带索引加速）
# ------------------------------
class CompanyMatcher:
    def __init__(self, company_data: pd.DataFrame, index_dir: str = "company_index"):
        """
        初始化匹配器，构建n-gram索引加速模糊匹配
        :param company_data: 包含MAIN_ID, original_name, normalized_name的DataFrame
        :param index_dir: 索引存储目录
        """
        self.company_data = company_data
        self.index_dir = index_dir
        self._build_index()  # 构建搜索索引

        # 构建快速映射字典
        self.normalized_to_details = {
            row['normalized_name']: {
                'original_name': row['original_name'],
                'main_id': row['MAIN_ID'],
                'entity_id': str(row['entity_id']),  # 确保转换为字符串
                'NAMETYPE': row['NAMETYPE'],
                # 可添加其他需要的字段如行业、热度等
            }
            for _, row in company_data.iterrows()
        }
        self.normalized_names = list(self.normalized_to_details.keys())

        # self.semantic_model = SentenceTransformer('all-MiniLM-L6-v2')  # 轻量级模型
        # self._build_semantic_vectors()  # 构建公司名称向量缓存

        # # 初始化拼写纠错器
        # self.spell_checker = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
        # self._build_spell_dictionary()  # 用公司名称构建词库

    # def _build_spell_dictionary(self):
    #     """用公司名称构建拼写纠错词库"""
    #     # 提取所有标准化名称作为词库
    #     names = self.company_data['normalized_name'].tolist()
    #     # 写入临时文件（symspellpy需要文件输入）
    #     with open("company_dictionary.txt", "w", encoding="utf-8") as f:
    #         for name in names:
    #             f.write(f"{name} 1\n")  # 1表示词频（默认即可）
    #     # 加载词库
    #     self.spell_checker.load_dictionary("company_dictionary.txt", term_index=0, count_index=1,encoding="utf-8")
    #     logger.info("拼写纠错词库构建完成")

    # def correct_spelling(self, query: str) -> str:
    #     """纠正查询词中的拼写错误"""
    #     suggestions = self.spell_checker.lookup(
    #         query,
    #         Verbosity.CLOSEST,  # 找最接近的匹配
    #         max_edit_distance=2  # 最大编辑距离（允许2个字符错误）
    #     )
    #     if suggestions:
    #         return suggestions[0].term
    #     return query  # 无纠错建议则返回原查询




    def _build_index(self):
        """构建Whoosh索引用于快速前缀匹配"""
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

        # 定义索引 schema
        schema = Schema(
            normalized_name=TEXT(stored=True),
            main_id=ID(stored=True),
            entity_id=ID(stored=True),
            NAMETYPE=TEXT(stored=True)
        )

        # 创建或打开索引
        if not os.listdir(self.index_dir):
            ix = create_in(self.index_dir, schema)
            writer = ix.writer()
            for _, row in self.company_data.iterrows():
                writer.add_document(
                    normalized_name=row['normalized_name'],
                    main_id=str(row['MAIN_ID']),
                    entity_id=str(row['entity_id']),
                    NAMETYPE=row['NAMETYPE']
                )
            writer.commit()
            logger.info(f"索引构建完成，共{len(self.company_data)}条记录")
        self.ix = open_dir(self.index_dir)

    #def _get_candidates(self, query: str) -> List[str]:
        # """通过索引获取候选名称，缩小模糊匹配范围"""
        # if len(query) < 2:
        #     return self.normalized_names  # 短查询返回全部候选

        # normalized_query = normalize_company_name(query)
        # candidates = set()

        # # 使用Whoosh进行前缀搜索
        # with self.ix.searcher() as searcher:
        #     parser = QueryParser("normalized_name", self.ix.schema)
        #     # 匹配包含查询词前缀的结果
        #     query = parser.parse(f"{normalized_query}*")
        #     results = searcher.search(query, limit=100)  # 限制候选数量
        #     for hit in results:
        #         candidates.add(hit['normalized_name'])

        # # 确保候选集不为空
        # return list(candidates) if candidates else self.normalized_names

    def _get_candidates(self, query: str) -> List[str]:
        normalized_query = normalize_company_name(query)
        if len(normalized_query) < 2:
            return self.normalized_names

        candidates = set()

        # 使用Whoosh进行前缀搜索（精确前缀匹配）
        with self.ix.searcher() as searcher:
            parser = QueryParser("normalized_name", self.ix.schema)
            # 匹配以查询词为前缀的结果（优先）
            prefix_query = parser.parse(f"{normalized_query}*")
            prefix_results = searcher.search(prefix_query, limit=200)  # 取更多前缀候选
            for hit in prefix_results:
                candidates.add(hit['normalized_name'])

        # 如果前缀候选不足，再补充普通模糊候选（避免漏检）
        if len(candidates) < 50:
            with self.ix.searcher() as searcher:
                parser = QueryParser("normalized_name", self.ix.schema)
                # 补充包含查询词任意部分的结果
                fuzzy_query = parser.parse(f"*{normalized_query}*")
                fuzzy_results = searcher.search(fuzzy_query, limit=100)
                for hit in fuzzy_results:
                    candidates.add(hit['normalized_name'])

        return list(candidates) if candidates else self.normalized_names

    def exact_match(self, query: str) -> List[Dict]:
        """精确匹配（原始名称和标准化名称）"""
        normalized_query = normalize_company_name(query)
        matches = self.company_data[
            (self.company_data['original_name'].str.lower() == query.lower()) |
            (self.company_data['normalized_name'] == normalized_query)
            ]

        return [
            {
                'main_id': row['MAIN_ID'],
                'name': row['original_name'],
                'entity_id': str(row['entity_id']),  # 确保转换为字符串
                'NAMETYPE': row['NAMETYPE'],
                'score': 100,
                'match_type': '精确匹配'
            }
            for _, row in matches.iterrows()
        ]
    

    #模糊匹配1.0算法
    # def fuzzy_match(self, query: str, top_n: int = 5) -> List[Dict]:
    #     """模糊匹配，结合多种算法和动态阈值"""
    #     normalized_query = normalize_company_name(query)
    #     if not normalized_query:
    #         return []

    #     # 动态阈值：输入越长，阈值越低（短输入需更精确）
    #     #threshold = max(40, 100 - len(normalized_query) * 2)
    #     threshold = max(30, 80 - len(normalized_query) * 1)

    #     # 通过索引获取候选名称，减少计算量
    #     candidates = self._get_candidates(query)
    #     if not candidates:
    #         return []

    #     # 多算法加权计算相似度
    #     scores = []
    #     for name in candidates:
    #         # 全量匹配得分
    #         ratio = fuzz.ratio(normalized_query, name)
    #         # 部分匹配得分（处理长名称包含短查询的情况）
    #         partial_ratio = fuzz.partial_ratio(normalized_query, name)
    #         # 忽略词序的匹配得分
    #         token_ratio = fuzz.token_sort_ratio(normalized_query, name)
    #         # 4. 子集匹配得分（强调交集部分，如"cosco"和"cosco shipping"的交集是"cosco"）
    #         token_set_ratio = fuzz.token_set_ratio(normalized_query, name)

    #         # 5. 前缀匹配加分：如果查询是候选词的前缀，额外加15分
    #         prefix_bonus = 15 if name.startswith(normalized_query) else 0


    #         # 加权总分（可根据实际效果调整权重）
    #         #total_score = 0.4 * ratio + 0.3 * partial_ratio + 0.3 * token_ratio
    #         #if total_score >= threshold:
    #         #scores.append((name, total_score))

    #         # 调整权重：提高部分匹配和子集匹配的权重，降低全量匹配权重
    #         total_score = (
    #         0.1 * ratio  # 降低全量匹配权重（因长度差异影响大）
    #         + 0.3 * partial_ratio  # 提高部分匹配权重
    #         + 0.2 * token_ratio
    #         + 0.3 * token_set_ratio  # 提高子集匹配权重
    #         + prefix_bonus  # 前缀匹配额外加分
    #         )

    #         if total_score >= threshold:
    #             scores.append((name, total_score))



    #     # 排序并返回结果
    #     scores.sort(key=lambda x: x[1], reverse=True)
    #     results = []
    #     seen = set()  # 去重
    #     for normalized_name, score in scores[:top_n * 2]:  # 多取一些再去重
    #         details = self.normalized_to_details.get(normalized_name)
    #         if not details or details['original_name'] in seen:
    #             continue
    #         seen.add(details['original_name'])
    #         results.append({
    #             'main_id': details['main_id'],
    #             'name': details['original_name'],
    #             'entity_id': str(details['entity_id']),  # 确保转换为字符串
    #             'NAMETYPE': details['NAMETYPE'],
    #             'score': round(score, 1),
    #             'match_type': '模糊匹配'
    #         })
    #         if len(results) >= top_n:
    #             break

    #     return results

    def fuzzy_match(self, query: str, top_n: int = 5) -> List[Dict]:
        """模糊匹配，结合多种算法和动态阈值，优化完整名称匹配"""
        normalized_query = normalize_company_name(query)
        if not normalized_query:
            return []

        # 动态阈值：输入越长，阈值越低（短输入需更精确）
        threshold = max(20, 50 - len(normalized_query) * 1)

        # 通过索引获取候选名称，减少计算量
        candidates = self._get_candidates(query)
        if not candidates:
            return []

        # 多算法加权计算相似度
        scores = []
        query_length = len(normalized_query)
        
        for name in candidates:
            name_length = len(name)
            
            # 基础匹配得分
            ratio = fuzz.ratio(normalized_query, name)
            partial_ratio = fuzz.partial_ratio(normalized_query, name)
            token_ratio = fuzz.token_sort_ratio(normalized_query, name)
            token_set_ratio = fuzz.token_set_ratio(normalized_query, name)

            # 1. 前缀匹配加分：根据查询长度占比动态调整加分
            prefix_bonus = 0
            if name.startswith(normalized_query):
                # 短查询匹配长名称前缀时，加分更少；长查询匹配时加分更多
                prefix_bonus = 5 + min(10, query_length * 0.8)
            
            # 2. 完全包含查询词加分：如果名称完全包含查询词
            contains_bonus = 0
            if normalized_query in name:
                # 包含完整查询词时给予额外加分，查询越长加分越多
                contains_bonus = min(10, query_length * 0.6)
            
            # 3. 精确子串匹配加分：如果存在完全匹配的子串
            exact_substring_bonus = 0
            if normalized_query.lower() in name.lower():
                # 找到精确匹配的子串，给予额外加分
                exact_substring_bonus = min(10, query_length * 0.5)
            
            # 4. 长度差异惩罚：如果候选名称比查询词短很多，适当减分
            length_penalty = 0
            if name_length < query_length * 0.7:
                # 候选名称太短，可能只是部分匹配
                length_penalty = -10

            # 调整权重：增强完整包含和精确匹配的权重
            total_score = (
                0.2 * ratio
                + 0.2 * partial_ratio
                + 0.2 * token_ratio
                + 0.2 * token_set_ratio  # 提高子集匹配权重，有利于完整包含的情况
                + prefix_bonus
                + contains_bonus         # 新增：完整包含查询词的加分
                + exact_substring_bonus  # 新增：精确子串匹配加分
                + length_penalty         # 新增：长度差异惩罚
            )

            # 确保分数不会超过100
            total_score = min(99, total_score)

            #if total_score >= 30:
            scores.append((name, total_score))

        # 排序并返回结果
        scores.sort(key=lambda x: x[1], reverse=True)
        results = []
        seen = set()  # 去重
        for normalized_name, score in scores[:top_n * 4]:  # 多取一些再去重
            details = self.normalized_to_details.get(normalized_name)
            if not details or details['original_name'] in seen:
                continue
            seen.add(details['original_name'])
            results.append({
                'main_id': details['main_id'],
                'name': details['original_name'],
                'entity_id': str(details['entity_id']),  # 确保转换为字符串
                'NAMETYPE': details['NAMETYPE'],
                'score': round(score, 1),
                'match_type': '模糊匹配'
            })
            if len(results) >= top_n:
                break

        return results
    
    # def _build_semantic_vectors(self):
    #     """为所有公司名称生成语义向量并缓存"""
    #     # 提取所有公司标准化名称
    #     names = self.company_data['normalized_name'].tolist()
    #     # 批量生成向量（模型支持批量处理，效率更高）
    #     self.name_vectors = self.semantic_model.encode(names, convert_to_tensor=True)
    #     # 建立名称到向量索引的映射
    #     self.name_to_idx = {name: i for i, name in enumerate(names)}
    #     logger.info(f"语义向量构建完成，共{len(names)}条")

    # def semantic_match(self, query: str, top_n: int = 5) -> List[Dict]:
    #     """基于语义向量的匹配"""
    #     normalized_query = normalize_company_name(query)
    #     if not normalized_query:
    #         return []
        
    #     # 生成查询词向量
    #     query_vector = self.semantic_model.encode(normalized_query, convert_to_tensor=True)
        
    #     # 计算与所有公司名称的余弦相似度
    #     cos_scores = util.cos_sim(query_vector, self.name_vectors)[0]
        
    #     # 取top_n个最相似的结果
    #     top_results = torch.topk(cos_scores, k=min(top_n * 4, len(cos_scores)))
        
    #     results = []
    #     seen = set()
    #     for score, idx in zip(top_results.values, top_results.indices):
    #         idx = idx.item()
    #         name = self.company_data.iloc[idx]['normalized_name']
    #         details = self.normalized_to_details.get(name)
    #         if not details or details['original_name'] in seen:
    #             continue
    #         seen.add(details['original_name'])
    #         results.append({
    #             'main_id': details['main_id'],
    #             'name': details['original_name'],
    #             'entity_id': details['entity_id'],
    #             'NAMETYPE': details['NAMETYPE'],
    #             'score': round(float(score) * 100, 1),  # 转换为百分比
    #             'match_type': '语义匹配'
    #         })
    #         if len(results) >= top_n:
    #             break
    #     return results




    def match(self, query: str, top_n: int = 5) -> List[Dict]:
        """综合匹配入口：先精确后模糊"""
        if not query or len(query.strip()) < 2:
            return []  # 过滤过短查询
        
        # # 拼写纠错
        # corrected_query = self.correct_spelling(query.strip())
        # if corrected_query != query.strip():
        #     logger.info(f"拼写纠错：{query.strip()} -> {corrected_query}")

        # 1. 同时执行三种匹配
        exact_matches = self.exact_match(query.strip())
        #semantic_matches = self.semantic_match(query.strip(), top_n * 4)  
        fuzzy_matches = self.fuzzy_match(query.strip(), top_n * 4)      

        # discounted_semantic = [
        # {**item, "score": item["score"] * 0.7}  # 分数打7折
        # for item in semantic_matches
        # ] 

        # 2. 合并所有结果（去重，保留最高分）
        merged = {}

        # 先加精确匹配（最高优先级，直接覆盖）
        for item in exact_matches:
            key = item["name"]
            merged[key] = item
        print("1",merged)

        # 再加模糊匹配（次高优先级，分数高于折扣后的语义匹配）
        for item in fuzzy_matches:
            key = item["name"]
            if key not in merged or item["score"] > merged[key]["score"]:
                merged[key] = item
        print("2",merged)
        # 最后加折扣后的语义匹配（仅补充未覆盖的结果）
        # for item in discounted_semantic:
        #     key = item["name"]
        #     if key not in merged:  # 只补充未被精确/模糊匹配覆盖的结果
        #         merged[key] = item


        # 3. 按分数排序，取前N个
        sorted_results = sorted(merged.values(), key=lambda x: x['score'], reverse=True)
        return sorted_results[:top_n]
    
# ------------------------------
# 3. 数据库交互类（带缓存）
# ------------------------------
class CompanyDatabase:
    def __init__(self, db_config, cache_ttl: int = 3600):
        """
        初始化数据库连接
        :param db_config: 数据库配置字典
        :param cache_ttl: 缓存过期时间（秒）
        """
        self.db_config = db_config
        self.cache_ttl = cache_ttl
        self._cache = {
            'company_data': None,
            'last_updated': 0
        }
        self.connection = None

    def connect(self) -> psycopg2.extensions.connection:
        """建立数据库连接（带重试）"""
        if self.connection and not self.connection.closed:
            return self.connection

        retry_count = 3
        for i in range(retry_count):
            try:
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config.get('port', 54321),
                    cursor_factory=self.db_config.get('cursor_factory', RealDictCursor)
                )
                logger.info("数据库连接成功")
                return self.connection
            except Exception as e:
                logger.error(f"数据库连接失败（第{i + 1}次重试）：{str(e)}")
                if i == retry_count - 1:
                    raise
                time.sleep(1)

    def close(self):
        """安全关闭连接"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.connection = None
            logger.info("数据库连接已关闭")

    def _load_company_data_from_db(self) -> pd.DataFrame:
        """从数据库加载公司数据"""
        conn = self.connect()
        try:
            # 使用 Kingbase 兼容的 SQL 语法
            query = """
                select  MAIN_ID,original_name,entity_id,NAMETYPE from
                (SELECT MAIN_ID,NAMETYPE, CONVERT(FROM_BASE64(ENTITYNAME) USING utf8mb4)         AS original_name 
                FROM lng.ods_zyhy_rm_enpr_namedetails_df_3 
                WHERE ENTITYNAME IS NOT NULL AND ENTITYNAME != ''
                  AND DATATYPE = 'ENTITY') t
                join 
                (
                select  id,entity_id from
                (select  id,entity_id,row_number() over (partition by entity_id order by entity_dt,create_time desc) as row_num from lng.ODS_ZYHY_RM_EN_ENTITY_DF_3 )
                where row_num=1
                ) t1
                on t.MAIN_ID=t1.id
                    """

            # 使用游标手动获取数据，避免 pandas 的 DBAPI2 警告
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

            # 调试：打印第一行的键，以了解实际返回的列名
            if rows:
                print(f"第一行的键: {list(rows[0].keys())}")

            # 手动构建DataFrame
            data = []

            for row in rows:
                # 调试：打印每行的键，确保我们访问正确的列名
                row_keys = list(row.keys())
                if 'main_id' not in row_keys and 'MAIN_ID' in row_keys:
                    # Kingbase 可能返回大写的列名
                    main_id = row['MAIN_ID']
                    original_name = row.get('original_name', row.get('ENTITYNAME', ''))
                    entity_id = row.get('entity_id', row.get('ENTITY_ID', ''))
                    NAMETYPE = row.get('NAMETYPE', row.get('NAMETYPE', ''))
                elif 'main_id' in row_keys:
                    main_id = row['main_id']
                    original_name = row.get('original_name', row.get('entityname', ''))
                    entity_id = row.get('entity_id', row.get('entity_id', ''))
                    NAMETYPE = row.get('NAMETYPE', row.get('NAMETYPE', ''))
                else:
                    # 如果列名不符合预期，跳过此行
                    print(f"跳过行，键不匹配: {row_keys}")
                    continue

                data.append({
                    'MAIN_ID': main_id,
                    'original_name': original_name,
                    'entity_id': str(entity_id),  # 确保转换为字符串
                    'NAMETYPE': NAMETYPE
                })

            df = pd.DataFrame(data)
            df['normalized_name'] = df['original_name'].apply(normalize_company_name)
            # 过滤无效标准化名称
            df = df[df['normalized_name'] != ''].reset_index(drop=True)
            logger.info(f"从数据库加载{len(df)}条有效公司数据")

            # 添加打印第一条数据的代码
            if not df.empty:
                first_row = df.iloc[0]
                print(
                    f"第一条数据示例 - MAIN_ID: {first_row['MAIN_ID']}, 原始名称: {first_row['original_name']}, 标准化名称: {first_row['normalized_name']}")
            else:
                print("数据为空，没有可打印的示例")

            return df
        except Exception as e:
            logger.error(f"加载公司数据失败：{str(e)}")
            raise
        finally:
            self.close()

    def get_company_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """获取公司数据（带缓存）"""
        now = time.time()
        # 缓存过期或强制刷新时重新加载
        if (now - self._cache['last_updated'] > self.cache_ttl) or force_refresh or not self._cache['company_data']:
            self._cache['company_data'] = self._load_company_data_from_db()
            self._cache['last_updated'] = now
        return self._cache['company_data']

    def get_company_details(self, company_id: int) -> Optional[Dict]:
        """获取公司详细信息"""
        try:
            conn = self.connect()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM lng.ods_zyhy_rm_enpr_namedetails_df_3 WHERE MAIN_ID = %s;",
                    (company_id,)
                )
                return cur.fetchone()
        except Exception as e:
            logger.error(f"获取公司详情失败（ID: {company_id}）：{str(e)}")
            return None
        finally:
            self.close()

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"数据库连接测试失败：{str(e)}")
            return False
        finally:
            self.close()

# ------------------------------
# 4. 创建FastAPI路由
# ------------------------------
cosco_router = APIRouter(
    prefix="/cosco",
    tags=["COSCO模型算法"],
    responses={404: {"description": "Not found"}},
)

# 全局变量：数据库和匹配器实例
db = None
matcher = None

def get_matcher():
    """获取匹配器实例"""
    global matcher
    if matcher is None:
        # 尝试初始化匹配器
        try:
            init_cosco_api()
        except Exception as e:
            logger.error(f"初始化匹配器失败: {e}")
            raise HTTPException(status_code=503, detail=f"匹配器初始化失败: {str(e)}")
    return matcher

def get_database():
    """获取数据库实例"""
    global db
    if db is None:
        # 尝试初始化数据库
        try:
            init_cosco_api()
        except Exception as e:
            logger.error(f"初始化数据库失败: {e}")
            raise HTTPException(status_code=503, detail=f"数据库初始化失败: {str(e)}")
    return db

def init_cosco_api():
    """初始化COSCO API（用于集成模式）"""
    global db, matcher
    if db is None or matcher is None:
        logger.info("正在初始化COSCO API...")
        db = CompanyDatabase(KINGBASE_CONFIG)
        company_data = db.get_company_data()
        matcher = CompanyMatcher(company_data)
        logger.info("COSCO API初始化完成")

@cosco_router.get("/", summary="COSCO模型算法API根路径")
async def root():
    """COSCO模型算法API服务根路径"""
    return {
        "service": "COSCO模型算法API",
        "version": "2.0.0",
        "description": "公司名称实时匹配服务",
        "endpoints": {
            "match": "/cosco/match - 公司名称匹配",
            "refresh": "/cosco/refresh-data - 刷新数据缓存",
            "health": "/cosco/health - 健康检查",
            "info": "/cosco/info - 服务信息"
        }
    }

@cosco_router.get("/match", response_model=MatchResponse, summary="公司名称匹配")
async def match_company(
    query: str = Query(..., min_length=1, description="公司名称查询词"),
    top_n: int = Query(5, ge=1, le=20, description="返回结果数量")
):
    """实时匹配公司名称接口（支持防抖前端调用）"""
    try:
        logger.info(f"开始处理匹配请求: query='{query}', top_n={top_n}")
        start_time = time.time()
        
        # 获取匹配器实例
        logger.info("正在获取匹配器实例...")
        matcher_instance = get_matcher()
        logger.info("匹配器实例获取成功")
        
        # 执行匹配
        logger.info("正在执行匹配...")
        results = matcher_instance.match(query, top_n)
        logger.info(f"匹配完成（耗时{time.time() - start_time:.3f}秒）：{query} -> {len(results)}条结果")
        
        return {
            "results": results,
            "query": query,
            "timestamp": time.time()
        }
    except HTTPException:
        # 重新抛出HTTP异常
        raise
    except Exception as e:
        logger.error(f"匹配接口错误：{str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"匹配过程发生错误: {str(e)}")

@cosco_router.get("/refresh-data", response_model=RefreshResponse, summary="刷新数据缓存")
async def refresh_company_data():
    """手动刷新公司数据缓存"""
    try:
        db_instance = get_database()
        db_instance.get_company_data(force_refresh=True)
        global matcher
        matcher = CompanyMatcher(db_instance.get_company_data())  # 重建匹配器
        return {"status": "success", "message": "数据已刷新"}
    except Exception as e:
        logger.error(f"刷新数据失败：{str(e)}")
        raise HTTPException(status_code=500, detail="数据刷新失败")

@cosco_router.get("/health", response_model=HealthResponse, summary="健康检查")
async def health_check():
    """服务健康检查"""
    try:
        db_instance = get_database()
        db_status = "healthy" if db_instance.test_connection() else "unhealthy"
        company_count = len(db_instance.get_company_data()) if db_status == "healthy" else 0
        
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "database_status": db_status,
            "company_count": company_count
        }
    except Exception as e:
        logger.error(f"健康检查失败：{str(e)}")
        return {
            "status": "unhealthy",
            "timestamp": time.time(),
            "database_status": "error",
            "company_count": 0
        }

@cosco_router.get("/info", summary="服务信息")
async def service_info():
    """获取服务详细信息"""
    try:
        db_instance = get_database()
        company_data = db_instance.get_company_data()
        
        return {
            "service_name": "COSCO模型算法API",
            "version": "2.0.0",
            "database": {
                "host": db_instance.db_config['host'],
                "port": db_instance.db_config['port'],
                "database": db_instance.db_config['database'],
                "status": "connected" if db_instance.test_connection() else "disconnected"
            },
            "company_data": {
                "total_count": len(company_data),
                "last_updated": db_instance._cache['last_updated'],
                "cache_ttl": db_instance.cache_ttl
            },
            "matcher": {
                "status": "initialized" if matcher else "not_initialized",
                "index_status": "ready" if matcher and hasattr(matcher, 'ix') else "not_ready"
            }
        }
    except Exception as e:
        logger.error(f"获取服务信息失败：{str(e)}")
        raise HTTPException(status_code=500, detail="获取服务信息失败")

# ------------------------------
# 5. 独立运行时的FastAPI应用
# ------------------------------
cosco_app = FastAPI(
    title="COSCO模型算法API服务",
    version="2.0.0",
    description="公司名称实时匹配服务，支持精确匹配和模糊匹配",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
from fastapi.middleware.cors import CORSMiddleware
cosco_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 将路由添加到独立应用
cosco_app.include_router(cosco_router)

# 独立应用的生命周期管理
@asynccontextmanager
async def cosco_lifespan(app: FastAPI):
    # 启动时初始化
    global db, matcher
    db = CompanyDatabase(KINGBASE_CONFIG)
    company_data = db.get_company_data()
    matcher = CompanyMatcher(company_data)
    logger.info("COSCO模型算法API服务启动完成")
    yield
    # 关闭时清理
    if db:
        db.close()
    logger.info("COSCO模型算法API服务关闭")

cosco_app.router.lifespan_context = cosco_lifespan

# ------------------------------
# 6. 运行入口
# ------------------------------
if __name__ == "__main__":
    # 开发环境运行（生产环境建议用Gunicorn等部署）
    uvicorn.run(cosco_app, host="0.0.0.0", port=8000, reload=False)
