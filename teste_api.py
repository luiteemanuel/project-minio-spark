import requests
import json
import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Ler configurações do .env
API_BASE_URL = os.getenv('API_BASE_URL')
API_KEY = os.getenv('API_KEY')  # Para quando precisar

print(f"Usando API Base URL: {API_BASE_URL}")
print(f"API Key configurada: {'Sim' if API_KEY else 'Não'}")
print()

# 1. Teste de ping
print("=== Teste 1: Ping da API ===")
url = f"{API_BASE_URL}/ping"
response = requests.get(url)

print(f"Status Code: {response.status_code}")
print(f"Resposta: {response.json()}")
print()

# 2. Buscar preço do Bitcoin
print("=== Teste 2: Preço do Bitcoin ===")
url = f"{API_BASE_URL}/simple/price"
params = {
    'ids': 'bitcoin',
    'vs_currencies': 'usd,brl'  # USD e Real
}

response = requests.get(url, params=params)
print(f"URL completa: {response.url}")
print(f"Status: {response.status_code}")
print(f"Preço Bitcoin: {response.json()}")
print()

# 3. Buscar top 5 cryptos
print("=== Teste 3: Top 5 Cryptos ===")
url = f"{API_BASE_URL}/coins/markets"
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 5,
    'page': 1
}

response = requests.get(url, params=params)
data = response.json()

print("Top 5 Criptomoedas:")
for i, coin in enumerate(data, 1):
    price = coin['current_price']
    change = coin['price_change_percentage_24h']
    print(f"{i}. {coin['name']} ({coin['symbol'].upper()}): ${price:,.2f} ({change:+.2f}%)")