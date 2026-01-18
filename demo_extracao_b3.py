# demo_extracao_b3.py
"""
Demonstração interativa de extração de dados B3
Mostra em tempo real o processo de scraping e salvamento
"""

import yfinance as yf
import boto3
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import time

s3 = boto3.client('s3')

# Configurações
BUCKET = 'pipeline-b3-lab-buzato'
TICKERS = [
    ('^BVSP', 'IBOV', 'Índice Bovespa'),
    ('PETR4.SA', 'PETR4', 'Petrobras PN'),
    ('VALE3.SA', 'VALE3', 'Vale ON')
]

def print_header(text):
    """Imprime cabeçalho formatado"""
    print("\n" + "="*80)
    print(f"  {text}")
    print("="*80)

def print_section(text):
    """Imprime seção formatada"""
    print("\n" + "-"*80)
    print(f"  {text}")
    print("-"*80)

def show_dataframe_sample(df, ticker_name):
    """Mostra amostra dos dados extraídos"""
    print(f"\n📊 Dados extraídos de {ticker_name}:")
    print(f"   Shape: {df.shape[0]} linhas x {df.shape[1]} colunas")
    print(f"\n   Colunas: {list(df.columns)}")
    print(f"\n   Primeiras linhas:")
    print(df.head().to_string(index=False))
    print(f"\n   Estatísticas:")
    print(f"   - Preço abertura: R$ {df['abertura'].iloc[0]:.2f}")
    print(f"   - Preço máximo: R$ {df['maxima'].iloc[0]:.2f}")
    print(f"   - Preço mínimo: R$ {df['minima'].iloc[0]:.2f}")
    print(f"   - Preço fechamento: R$ {df['fechamento'].iloc[0]:.2f}")
    print(f"   - Volume: {df['volume'].iloc[0]:,.0f}")

def main():
    print_header("DEMONSTRAÇÃO: EXTRAÇÃO DE DADOS B3")
    print(f"\n🕐 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # Para cada ticker
    for i, (ticker_yahoo, ticker_clean, ticker_desc) in enumerate(TICKERS, 1):
        
        print_section(f"TICKER {i}/3: {ticker_desc} ({ticker_clean})")
        
        print(f"\n[Etapa 1] 🌐 Conectando com Yahoo Finance...")
        print(f"   URL: https://finance.yahoo.com/quote/{ticker_yahoo}")
        time.sleep(0.5)
        
        try:
            print(f"\n[Etapa 2] 📥 Baixando dados históricos...")
            print(f"   Ticker: {ticker_yahoo}")
            print(f"   Período: 1 dia (último pregão)")
            print(f"   Intervalo: 1 dia (OHLCV)")
            
            # Download dos dados
            start_time = time.time()
            df = yf.download(
                ticker_yahoo, 
                period='1d', 
                interval='1d',
                progress=False
            )
            download_time = time.time() - start_time
            
            if df.empty:
                print(f"\n   ⚠️  AVISO: Sem dados disponíveis para {ticker_yahoo}")
                print(f"   Possível razão: Mercado fechado ou ticker inválido")
                continue
            
            print(f"   ✓ Download concluído em {download_time:.2f}s")
            print(f"   ✓ Registros obtidos: {len(df)}")
            
            # Processar dados
            print(f"\n[Etapa 3] 🔄 Processando dados...")
            df = df.reset_index()
            df.columns = ['data', 'abertura', 'maxima', 'minima', 'fechamento', 'volume']
            df['ticker'] = ticker_clean
            
            # Reordenar colunas
            df = df[['ticker', 'data', 'abertura', 'maxima', 'minima', 'fechamento', 'volume']]
            
            print(f"   ✓ Colunas padronizadas")
            print(f"   ✓ Ticker adicionado: {ticker_clean}")
            
            # Mostrar amostra dos dados
            show_dataframe_sample(df, ticker_desc)
            
            # Converter para Parquet
            print(f"\n[Etapa 4] 📦 Convertendo para formato Parquet...")
            buffer = BytesIO()
            start_time = time.time()
            df.to_parquet(
                buffer, 
                engine='pyarrow', 
                index=False,
                compression='snappy'
            )
            buffer.seek(0)
            conversion_time = time.time() - start_time
            
            parquet_size = len(buffer.getvalue())
            print(f"   ✓ Conversão concluída em {conversion_time:.3f}s")
            print(f"   ✓ Tamanho do arquivo: {parquet_size:,} bytes ({parquet_size/1024:.2f} KB)")
            
            # Calcular compressão (estimativa)
            csv_size = len(df.to_csv(index=False))
            compression_ratio = (1 - parquet_size/csv_size) * 100
            print(f"   ✓ Compressão vs CSV: {compression_ratio:.1f}% menor")
            
            # Upload para S3
            print(f"\n[Etapa 5] ☁️  Enviando para AWS S3...")
            date = datetime.now().strftime('%Y-%m-%d')
            key = f'raw/date={date}/{ticker_clean}_{date}.parquet'
            
            print(f"   Bucket: {BUCKET}")
            print(f"   Key: {key}")
            
            start_time = time.time()
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'ticker': ticker_clean,
                    'extraction_date': datetime.now().isoformat(),
                    'records': str(len(df))
                }
            )
            upload_time = time.time() - start_time
            
            print(f"   ✓ Upload concluído em {upload_time:.2f}s")
            print(f"   ✓ URL: s3://{BUCKET}/{key}")
            
            # Verificar upload
            print(f"\n[Etapa 6] ✅ Verificando upload no S3...")
            response = s3.head_object(Bucket=BUCKET, Key=key)
            print(f"   ✓ Arquivo confirmado no S3")
            print(f"   ✓ Tamanho: {response['ContentLength']:,} bytes")
            print(f"   ✓ Última modificação: {response['LastModified']}")
            print(f"   ✓ ETag: {response['ETag']}")
            
            # Sucesso
            results.append({
                'ticker': ticker_clean,
                'status': 'SUCCESS',
                'records': len(df),
                'file_size_kb': parquet_size/1024,
                's3_key': key,
                'download_time': download_time,
                'upload_time': upload_time
            })
            
            print(f"\n   ✅ {ticker_desc} processado com SUCESSO!")
            
        except Exception as e:
            print(f"\n   ❌ ERRO ao processar {ticker_yahoo}:")
            print(f"   {str(e)}")
            results.append({
                'ticker': ticker_clean,
                'status': 'FAILED',
                'error': str(e)
            })
        
        # Pausa entre tickers
        if i < len(TICKERS):
            print(f"\n   ⏳ Aguardando 2 segundos antes do próximo ticker...")
            time.sleep(2)
    
    # Resumo final
    print_header("RESUMO DA EXTRAÇÃO")
    
    success = [r for r in results if r['status'] == 'SUCCESS']
    failed = [r for r in results if r['status'] == 'FAILED']
    
    print(f"\n📊 Estatísticas:")
    print(f"   Total de tickers processados: {len(TICKERS)}")
    print(f"   ✅ Sucessos: {len(success)}")
    print(f"   ❌ Falhas: {len(failed)}")
    
    if success:
        total_records = sum(r['records'] for r in success)
        total_size = sum(r['file_size_kb'] for r in success)
        avg_download = sum(r['download_time'] for r in success) / len(success)
        avg_upload = sum(r['upload_time'] for r in success) / len(success)
        
        print(f"\n   Total de registros extraídos: {total_records}")
        print(f"   Tamanho total dos arquivos: {total_size:.2f} KB")
        print(f"   Tempo médio de download: {avg_download:.2f}s")
        print(f"   Tempo médio de upload: {avg_upload:.2f}s")
        
        print(f"\n📁 Arquivos criados no S3:")
        for r in success:
            print(f"   ✓ {r['s3_key']} ({r['file_size_kb']:.2f} KB)")
    
    if failed:
        print(f"\n❌ Tickers com falha:")
        for r in failed:
            print(f"   ✗ {r['ticker']}: {r.get('error', 'Erro desconhecido')}")
    
    print(f"\n🕐 Término: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print_header("EXTRAÇÃO CONCLUÍDA")
    
    print("\n🎯 Próximos passos:")
    print("   1. Lambda trigger será acionada automaticamente")
    print("   2. Job Glue iniciará o processamento ETL")
    print("   3. Dados refinados serão catalogados")
    print("   4. Consultas SQL estarão disponíveis no Athena")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()