import requests

# url = "https://api.kpler.com/v2/compliance/vessel-risks-v2/imo=9937115"
url = "https://api.kpler.com/v2/compliance/compliance-screening?vessels=9842190"
headers = {
    "Authorization": "Basic ejdXOEkzSGFKOEJWdno0ZzRIdEZJZzJZUzR1VmJQOVA6YWZEZ2d0NG9mZFJDX0Yyd1lQUlNhbXhMZFdjMVlJdnlsX1ctYW1QRnV3QmI2SFNaOWtwSFZ4NlpaYmVyaHJnbQ==",  # <-- Replace with your actual API key
    "Accept": "application/json"
}

# 发起GET请求
response = requests.get(url, headers=headers)

print(f"状态码: {response.status_code}")
print(f"响应头: {response.headers}")
print(f"响应内容: {response.text}")  

