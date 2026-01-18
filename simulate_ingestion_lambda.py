# simulate_ingestion_lambda.py
"""Simula a Lambda de ingestão localmente"""
import boto3
import pandas as pd
from datetime import datetime
import yfinance as yf

s3 = boto3.client('s3')
BUCKET = 'pipeline-b3-lab-buzato'
TICKERS = ['^BVSP', 'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'ITSA4.SA']

print("="*60)
print("SIMULAÇÃO: Lambda de Ingestão B3")
print("="*60)

for ticker in TICKERS:
    print(f"\nProcessando: {ticker}")
    
    # Download
    df = yf.download(ticker, period='1d', interval='1d', progress=False)
    df = df.reset_index()
    df.columns = ['data', 'abertura', 'maxima', 'minima', 'fechamento', 'volume']
    
    ticker_clean = ticker.replace('.SA', '').replace('^BVSP', 'IBOV')
    df['ticker'] = ticker_clean
    
    # Data
    date = datetime.now().strftime('%Y-%m-%d')
    
    # Salvar
    key = f'raw/date={date}/{ticker_clean}_{date}.parquet'
    df.to_parquet(f's3://{BUCKET}/{key}')
    
    print(f"  ✓ Salvo: {key} ({len(df)} registros)")

print("\n" + "="*60)
print("INGESTÃO CONCLUÍDA!")
print("="*60)