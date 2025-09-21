import requests
import pandas as pd
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio
import io
import pyarrow as pa
import pyarrow.parquet as pq

# Carregar variáveis de ambiente
load_dotenv()

class CryptoDataPipeline:
    def __init__(self):
        # Suas configurações
        self.api_base_url = os.getenv('API_BASE_URL')
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
        
        # Adicionar cliente MinIO
        self.minio_client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )
        self.PRINCIPAIS_CRYPTOS = {
            'bitcoin': 'Bitcoin (BTC)',
            'ethereum': 'Ethereum (ETH)', 
            'binancecoin': 'BNB (BNB)',
            'solana': 'Solana (SOL)',
            'cardano': 'Cardano (ADA)',
            'avalanche-2': 'Avalanche (AVAX)',
            'polkadot': 'Polkadot (DOT)',
            'chainlink': 'Chainlink (LINK)',
            'polygon': 'Polygon (MATIC)',
            'litecoin': 'Litecoin (LTC)',
            'dogecoin': 'Dogecoin (DOGE)',
            'shiba-inu': 'Shiba Inu (SHIB)',
            'uniswap': 'Uniswap (UNI)',
            'near': 'NEAR Protocol (NEAR)',
            'cosmos': 'Cosmos (ATOM)'
        }
        
        print(f"Pipeline inicializado - API: {self.api_base_url}")
    
    def fazer_requisicao(self, url, params=None, max_tentativas=3):
        for tentativa in range(max_tentativas):
            try:
                print(f"Tentativa {tentativa + 1}: {url}")
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    print(f"Sucesso! Status: {response.status_code}")
                    return response.json()
                
                # Rate limit (muitas requisições)
                elif response.status_code == 429:
                    print("Rate limit! Aguardando 60 segundos...")
                    time.sleep(60)
                    continue
                else:
                    print(f"Erro: {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                print("Timeout! API demorou muito para responder")
            except requests.exceptions.ConnectionError:
                print("Erro de conexão!")
            except Exception as e:
                print(f"Erro: {e}")
            
            # Aguardar antes da próxima tentativa
            if tentativa < max_tentativas - 1:
                print(f"Aguardando 5 segundos...")
                time.sleep(5)
        
        print("Todas as tentativas falharam!")
        return None

            
    def get_raw_data_markets(self, vs_currency='usd', per_page=100, page=1):
        """
        Busca dados RAW do endpoint /coins/markets
        Salva em formato Parquet usando PyArrow direto
        """
        print("Iniciando coleta de dados do markets...")
        
        # Construir URL completa
        endpoint = "/coins/markets"
        url = f"{self.api_base_url}{endpoint}"
        
        # Parâmetros da requisição
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': page,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d,30d'
        }
        
        print(f"URL: {url}")
        print(f"Parametros: {params}")
        
        # Fazer requisição
        dados_raw = self.fazer_requisicao(url, params)
        
        if not dados_raw:
            print("Falha ao obter dados da API")
            return False
        
        print(f"Dados recebidos: {len(dados_raw)} registros")
        
        # Adicionar metadados diretamente no JSON
        timestamp_ingestao = datetime.now().isoformat()
        
        for registro in dados_raw:
            registro['_ingestion_timestamp'] = timestamp_ingestao
            registro['_data_source'] = 'coingecko_markets'
            registro['_endpoint'] = endpoint
            registro['_vs_currency'] = vs_currency
        
        # Converter JSON para PyArrow Table
        table = pa.Table.from_pylist(dados_raw)
        
        print(f"PyArrow Table criado: {table.num_rows} linhas, {table.num_columns} colunas")
        
        # Gerar caminho do arquivo com data como pasta única
        now = datetime.now()
        data_pasta = now.strftime('%Y-%m-%d')  # 2025-01-20
        hora_arquivo = now.strftime('%H%M%S')   # 143022
        
        caminho = f"crypto/markets/{data_pasta}/markets_raw_{hora_arquivo}.parquet"
        
        print(f"Caminho do arquivo: {caminho}")
        
        # Salvar no MinIO usando PyArrow
        try:
            # Converter Table para Parquet em memória
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression='snappy')
            parquet_buffer.seek(0)  # Voltar ponteiro para o início
            
            # Upload para MinIO
            self.minio_client.put_object(
                'raw-data',
                caminho,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            tamanho_mb = parquet_buffer.getbuffer().nbytes / (1024 * 1024)
            print(f"Arquivo salvo: raw-data/{caminho}")
            print(f"Tamanho: {tamanho_mb:.2f} MB")
            
            return {
                'status': 'success',
                'records': table.num_rows,
                'columns': table.num_columns,
                'file_path': caminho,
                'size_mb': round(tamanho_mb, 2)
            }
            
        except Exception as e:
            print(f"Erro ao salvar arquivo: {e}")
            return False

    def buscar_coin_id_por_nome(self, nome):
        """
        Busca o coin_id baseado no nome da criptomoeda
        
        Args:
            nome (str): Nome da moeda (ex: 'Bitcoin', 'Ethereum', 'BTC', 'ETH')
            
        Returns:
            str: coin_id correspondente ou None se não encontrar
        """
        nome_lower = nome.lower().strip()
        
        print(f"Buscando coin_id para: '{nome}'")
        
        for coin_id, nome_completo in self.PRINCIPAIS_CRYPTOS.items():
            nome_completo_lower = nome_completo.lower()
            
            # Extrair o nome da moeda (antes do parênteses)
            nome_moeda = nome_completo.split(' (')[0].lower()
            
            # Extrair o símbolo (entre parênteses)
            simbolo = nome_completo.split('(')[1].split(')')[0].lower()
            
            # Verificar se coincide com nome da moeda ou símbolo
            if (nome_lower == nome_moeda or 
                nome_lower == simbolo or
                nome_lower in nome_moeda):
                
                print(f"Encontrado: '{nome}' -> coin_id: '{coin_id}' ({nome_completo})")
                return coin_id
        
        print(f"Moeda '{nome}' não encontrada nas principais cryptos")
        return None
            
    def get_raw_data_coin_details(self, nome_moeda):
        """
        Busca dados RAW do endpoint /coins/{coin_id}
        Usa o nome da moeda para encontrar o coin_id
        
        Args:
            nome_moeda (str): Nome da moeda (ex: 'Bitcoin', 'Ethereum', 'BTC', 'SOL')
        """
        print(f"Iniciando coleta de dados para: {nome_moeda}")
        
        # Buscar o coin_id pelo nome
        coin_id = self.buscar_coin_id_por_nome(nome_moeda)
        
        if not coin_id:
            print(f"Não foi possível encontrar a moeda: {nome_moeda}")
            return False
        
        # Construir URL completa
        endpoint = f"/coins/{coin_id}"
        url = f"{self.api_base_url}{endpoint}"
        
        # Parâmetros da requisição
        params = {
            'localization': False,
            'tickers': True,
            'market_data': True,
            'community_data': True,
            'developer_data': True,
            'sparkline': False
        }
        
        print(f"URL: {url}")
        
        # Fazer requisição
        dados_raw = self.fazer_requisicao(url, params)
        
        if not dados_raw:
            print(f"Falha ao obter dados de {coin_id}")
            return False
        
        print(f"Dados recebidos para {coin_id}")
        
        # Adicionar metadados
        timestamp_ingestao = datetime.now().isoformat()
        dados_raw['_ingestion_timestamp'] = timestamp_ingestao
        dados_raw['_data_source'] = 'coingecko_coin_details'
        dados_raw['_endpoint'] = endpoint
        dados_raw['_coin_id'] = coin_id
        dados_raw['_nome_buscado'] = nome_moeda
        
        # Limpar structs vazios antes de converter para PyArrow
        def limpar_structs_vazios(obj):
            if isinstance(obj, dict):
                return {k: limpar_structs_vazios(v) for k, v in obj.items() if v is not None and v != {}}
            elif isinstance(obj, list):
                return [limpar_structs_vazios(item) for item in obj if item is not None and item != {}]
            else:
                return obj

        dados_limpos = limpar_structs_vazios(dados_raw)

        # Converter para PyArrow Table
        table = pa.Table.from_pylist([dados_limpos])

        print(f"PyArrow Table criado: {table.num_rows} linha, {table.num_columns} colunas")
        
        # Gerar caminho do arquivo
        now = datetime.now()
        data_pasta = now.strftime('%Y-%m-%d')
        hora_arquivo = now.strftime('%H%M%S')
        
        caminho = f"crypto/coin_details/{data_pasta}/coin_details_{coin_id}_{hora_arquivo}.parquet"
        
        print(f"Caminho do arquivo: {caminho}")
        
        # Salvar no MinIO
        try:
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression='snappy')
            parquet_buffer.seek(0)
            
            self.minio_client.put_object(
                'raw-data',
                caminho,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            tamanho_mb = parquet_buffer.getbuffer().nbytes / (1024 * 1024)
            print(f"Arquivo salvo: raw-data/{caminho}")
            print(f"Tamanho: {tamanho_mb:.2f} MB")
            
            return {
                'status': 'success',
                'nome_buscado': nome_moeda,
                'coin_id': coin_id,
                'columns': table.num_columns,
                'file_path': caminho,
                'size_mb': round(tamanho_mb, 2)
            }
            
        except Exception as e:
            print(f"Erro ao salvar arquivo: {e}")
            return False

    def get_raw_data_global(self):
        """
        Busca dados RAW do endpoint /global
        Salva dados globais do mercado de criptomoedas
        """
        print("Iniciando coleta de dados globais...")
        
        # Construir URL completa
        endpoint = "/global"
        url = f"{self.api_base_url}{endpoint}"
        
        print(f"URL: {url}")
        
        # Fazer requisição (sem parâmetros)
        dados_raw = self.fazer_requisicao(url)
        
        if not dados_raw:
            print("Falha ao obter dados globais")
            return False
        
        print("Dados globais recebidos")
        
        # Adicionar metadados
        timestamp_ingestao = datetime.now().isoformat()
        dados_raw['_ingestion_timestamp'] = timestamp_ingestao
        dados_raw['_data_source'] = 'coingecko_global'
        dados_raw['_endpoint'] = endpoint
        
        # Converter para PyArrow Table
        table = pa.Table.from_pylist([dados_raw])
        
        print(f"PyArrow Table criado: {table.num_rows} linha, {table.num_columns} colunas")
        
        # Gerar caminho do arquivo
        now = datetime.now()
        data_pasta = now.strftime('%Y-%m-%d')
        hora_arquivo = now.strftime('%H%M%S')
        
        caminho = f"crypto/global/{data_pasta}/global_{hora_arquivo}.parquet"
        
        print(f"Caminho do arquivo: {caminho}")
        
        # Salvar no MinIO
        try:
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression='snappy')
            parquet_buffer.seek(0)
            
            self.minio_client.put_object(
                'raw-data',
                caminho,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            tamanho_mb = parquet_buffer.getbuffer().nbytes / (1024 * 1024)
            print(f"Arquivo salvo: raw-data/{caminho}")
            print(f"Tamanho: {tamanho_mb:.2f} MB")
            
            return {
                'status': 'success',
                'columns': table.num_columns,
                'file_path': caminho,
                'size_mb': round(tamanho_mb, 2)
            }
            
        except Exception as e:
            print(f"Erro ao salvar arquivo: {e}")
            return False


if __name__ == "__main__":
    pipeline = CryptoDataPipeline()
    print("INICIANDO DUMP COMPLETO DE DADOS")
    
    # 1. Dados globais
    print("\n1. Coletando dados globais...")
    resultado_global = pipeline.get_raw_data_global()
    if resultado_global:
        print("   ✓ Dados globais coletados")
    else:
        print("   ✗ Erro nos dados globais")
    time.sleep(2)
    
    # 2. Markets - top 500 cryptos
    print("\n2. Coletando dados de mercado...")
    sucessos_market = 0
    for pagina in range(1, 6):
        print(f"   Pagina {pagina}/5")
        resultado = pipeline.get_raw_data_markets(per_page=100, page=pagina)
        if resultado:
            print(f"   ✓ Página {pagina}: {resultado['records']} registros")
            sucessos_market += 1
        else:
            print(f"   ✗ Erro na página {pagina}")
        time.sleep(3)  # Rate limit
    
    # 3. Detalhes das principais cryptos
    print("\n3. Coletando detalhes das principais cryptos...")
    sucessos_details = 0
    total_cryptos = len(pipeline.PRINCIPAIS_CRYPTOS)
    
    for i, coin_id in enumerate(pipeline.PRINCIPAIS_CRYPTOS.keys(), 1):
        nome_crypto = pipeline.PRINCIPAIS_CRYPTOS[coin_id]
        print(f"   [{i}/{total_cryptos}] Coletando {nome_crypto}")
        
        resultado = pipeline.get_raw_data_coin_details(coin_id)
        if resultado:
            print(f"   ✓ {coin_id}: {resultado['size_mb']} MB")
            sucessos_details += 1
        else:
            print(f"   ✗ Erro em {coin_id}")
        
        time.sleep(2)  # Rate limit
    
    # Resumo final
    print("\n" + "="*50)
    print("DUMP FINALIZADO")
    print(f"Global Data: {'✓' if resultado_global else '✗'}")
    print(f"Markets: {sucessos_market}/5 páginas")
    print(f"Coin Details: {sucessos_details}/{total_cryptos} cryptos")
    print(f"Total de arquivos esperados: ~{1 + sucessos_market + sucessos_details}")
    print("="*50)