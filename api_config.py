# API配置文件
# 用于管理各种API的超时、重试和其他网络设置

# 劳氏API配置
LLOYDS_API_CONFIG = {
    'base_url': 'https://api.lloydslistintelligence.com/v1',
    'timeout': 120,  # 120秒超时（翻倍）
    'max_retries': 3,  # 最大重试次数
    'retry_delay': 2,  # 初始重试延迟（秒）
    'exponential_backoff': True,  # 是否使用指数退避
    'connection_timeout': 20,  # 连接超时（翻倍）
    'read_timeout': 120,  # 读取超时（翻倍）
}

# Kpler API配置
KPLER_API_CONFIG = {
    'base_url': 'https://api.kpler.com/v2',
    'timeout': 60,  # 翻倍
    'max_retries': 2,
    'retry_delay': 1,
    'exponential_backoff': True,
    'connection_timeout': 10,  # 翻倍
    'read_timeout': 60,  # 翻倍
}

# 通用HTTP配置
HTTP_CONFIG = {
    'default_timeout': 60,  # 翻倍
    'max_retries': 3,
    'retry_delay': 2,
    'exponential_backoff': True,
    'user_agent': 'MaritimeDataProcessor/1.0',
    'verify_ssl': True,
}

# 网络配置
NETWORK_CONFIG = {
    'max_connections': 10,
    'connection_pool_size': 5,
    'keep_alive': True,
    'keep_alive_timeout': 60,  # 翻倍
}

def get_lloyds_session():
    """获取配置好的劳氏API会话"""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=LLOYDS_API_CONFIG['max_retries'],
        backoff_factor=LLOYDS_API_CONFIG['retry_delay'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    # 配置适配器
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=NETWORK_CONFIG['connection_pool_size'],
        pool_maxsize=NETWORK_CONFIG['max_connections']
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_kpler_session():
    """获取配置好的Kpler API会话"""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=KPLER_API_CONFIG['max_retries'],
        backoff_factor=KPLER_API_CONFIG['retry_delay'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    
    # 配置适配器
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=NETWORK_CONFIG['connection_pool_size'],
        pool_maxsize=NETWORK_CONFIG['max_connections']
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session
