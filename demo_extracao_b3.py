"""
Demonstração interativa de extração de dados B3
VERSÃO MELHORADA - Com suporte a períodos customizados
"""

import yfinance as yf
import boto3
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import time
import argparse

s3 = boto3.client('s3')

# Configurações
BUCKET = 'pipeline-b3-lab-buzato'
TICKERS = [
    ('^BVSP', 'IBOV', 'Índice Bovespa'),
    ('PETR4.SA', 'PETR4', 'Petrobras PN'),
    ('VALE3.SA', 'VALE3', 'Vale ON'),
    ('ITUB4.SA', 'ITUB4', 'Itaú Unibanco PN'),
    ('ITSA4.SA', 'ITSA4', 'Itaúsa PN')
]

# OPÇÕES DE PERÍODO
PERIOD_OPTIONS = {
    '5d': '5 dias',
    '1mo': '1 mês (~21 dias úteis)',
    '3mo': '3 meses (~63 dias úteis)',
    '6mo': '6 meses (~126 dias úteis)',
    '1y': '1 ano (~250 dias úteis)',
    '2y': '2 anos (~500 dias úteis)',
    '5y': '5 anos (~1250 dias úteis)',
    '10y': '10 anos',
    'ytd': 'Do início do ano até hoje',
    'max': 'Máximo disponível (desde IPO)'
}

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
    print(f"\n   Período: {df['data'].min()} até {df['data'].max()}")
    print(f"   Total de dias: {df.shape[0]}")
    print(f"\n   Colunas: {list(df.columns)}")
    print(f"\n   Primeiras linhas:")
    print(df.head(3).to_string(index=False))
    print(f"\n   Últimas linhas:")
    print(df.tail(3).to_string(index=False))

def extract_with_custom_dates(ticker_yahoo, start_date, end_date):
    """
    Extrai dados com datas específicas
    
    Args:
        ticker_yahoo: Símbolo do Yahoo (ex: 'PETR4.SA')
        start_date: Data inicial (datetime ou string 'YYYY-MM-DD')
        end_date: Data final (datetime ou string 'YYYY-MM-DD')
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    print(f"   Período customizado: {start_date.date()} até {end_date.date()}")
    
    df = yf.download(
        ticker_yahoo,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        interval='1d',
        progress=False
    )
    
    return df

def main(period='1mo', custom_start=None, custom_end=None, tickers_filter=None):
    print_header("EXTRAÇÃO DE DADOS B3 - VERSÃO CUSTOMIZÁVEL")
    
    # Mostrar período selecionado
    if custom_start and custom_end:
        period_desc = f"Período customizado: {custom_start} até {custom_end}"
    else:
        period_desc = f"Período: {period} ({PERIOD_OPTIONS.get(period, 'Desconhecido')})"
    
    print(f"\n📅 {period_desc}")
    print(f"🕐 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Filtrar tickers se solicitado
    if tickers_filter:
        tickers_to_process = [t for t in TICKERS if t[1] in tickers_filter]
        if not tickers_to_process:
            print(f"\n⚠️  AVISO: Nenhum ticker encontrado em {tickers_filter}")
            print(f"Tickers disponíveis: {[t[1] for t in TICKERS]}")
            return
    else:
        tickers_to_process = TICKERS
    
    print(f"📈 Tickers selecionados: {[t[1] for t in tickers_to_process]}")
    
    results = []
    
    # Para cada ticker
    for i, (ticker_yahoo, ticker_clean, ticker_desc) in enumerate(tickers_to_process, 1):
        
        print_section(f"TICKER {i}/{len(tickers_to_process)}: {ticker_desc} ({ticker_clean})")
        
        print(f"\n[Etapa 1] 🌐 Conectando com Yahoo Finance...")
        time.sleep(0.5)
        
        try:
            print(f"\n[Etapa 2] 📥 Baixando dados históricos...")
            
            # Download dos dados
            start_time = time.time()
            
            if custom_start and custom_end:
                df = extract_with_custom_dates(ticker_yahoo, custom_start, custom_end)
            else:
                df = yf.download(
                    ticker_yahoo, 
                    period=period, 
                    interval='1d',
                    progress=False
                )
            
            download_time = time.time() - start_time
            
            if df.empty:
                print(f"\n   ⚠️  AVISO: Sem dados disponíveis para {ticker_yahoo}")
                continue
            
            print(f"   ✓ Download concluído em {download_time:.2f}s")
            print(f"   ✓ Registros obtidos: {len(df)}")
            
            # Processamento
            print(f"\n[Etapa 3] 🔄 Processando dados (SCHEMA SPARK-COMPATÍVEL)...")
            
            # Reset index e renomear colunas
            df = df.reset_index()
            
            # Padronizar nomes de colunas
            df.columns = ['data', 'abertura', 'maxima', 'minima', 'fechamento', 'volume']
            
            # Converter data para datetime64[ms] (Spark-compatível)
            df['data'] = pd.to_datetime(df['data']).dt.tz_localize(None)
            df['data'] = df['data'].astype('datetime64[ms]')
            
            # Garantir tipos corretos
            df['abertura'] = df['abertura'].astype('float64')
            df['maxima'] = df['maxima'].astype('float64')
            df['minima'] = df['minima'].astype('float64')
            df['fechamento'] = df['fechamento'].astype('float64')
            df['volume'] = df['volume'].astype('int64')
            
            # Adicionar ticker
            df['ticker'] = ticker_clean
            df['ticker'] = df['ticker'].astype('string')
            
            # Reordenar colunas
            df = df[['ticker', 'data', 'abertura', 'maxima', 'minima', 'fechamento', 'volume']]
            
            print(f"   ✓ Schema validado e otimizado para Spark")
            
            # Mostrar amostra
            show_dataframe_sample(df, ticker_desc)
            
            # Converter para Parquet
            print(f"\n[Etapa 4] 📦 Convertendo para formato Parquet...")
            buffer = BytesIO()
            start_time = time.time()
            
            df.to_parquet(
                buffer, 
                engine='pyarrow',
                index=False,
                compression='snappy',
                coerce_timestamps='ms',
                allow_truncated_timestamps=True,
                use_deprecated_int96_timestamps=False
            )
            
            buffer.seek(0)
            conversion_time = time.time() - start_time
            
            parquet_size = len(buffer.getvalue())
            print(f"   ✓ Conversão concluída em {conversion_time:.3f}s")
            print(f"   ✓ Tamanho do arquivo: {parquet_size:,} bytes ({parquet_size/1024:.2f} KB)")
            
            # Estatísticas de compressão
            csv_size = len(df.to_csv(index=False))
            compression_ratio = (1 - parquet_size/csv_size) * 100
            print(f"   ✓ Compressão vs CSV: {compression_ratio:.1f}% menor")
            
            # Upload para S3 (particionado por data)
            print(f"\n[Etapa 5] ☁️  Enviando para AWS S3...")
            
            # Agrupar por data para criar múltiplos arquivos se necessário
            dates_in_data = df['data'].dt.date.unique()
            files_uploaded = []
            
            for date in dates_in_data:
                date_str = date.strftime('%Y-%m-%d')
                df_date = df[df['data'].dt.date == date]
                
                # Criar buffer para esta data
                date_buffer = BytesIO()
                df_date.to_parquet(
                    date_buffer,
                    engine='pyarrow',
                    index=False,
                    compression='snappy',
                    coerce_timestamps='ms',
                    allow_truncated_timestamps=True
                )
                date_buffer.seek(0)
                
                # Key no S3
                key = f'raw/date={date_str}/{ticker_clean}_{date_str}.parquet'
                
                # Upload
                s3.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=date_buffer.getvalue(),
                    ContentType='application/octet-stream',
                    Metadata={
                        'ticker': ticker_clean,
                        'extraction_date': datetime.now().isoformat(),
                        'records': str(len(df_date)),
                        'period': period if not custom_start else 'custom'
                    }
                )
                files_uploaded.append(key)
            
            upload_time = time.time() - start_time
            
            print(f"   ✓ Upload concluído em {upload_time:.2f}s")
            print(f"   ✓ {len(files_uploaded)} arquivo(s) criado(s)")
            print(f"   ✓ Bucket: s3://{BUCKET}/")
            
            # Verificar uploads
            print(f"\n[Etapa 6] ✅ Verificando uploads no S3...")
            for key in files_uploaded[:3]:  # Mostrar apenas primeiros 3
                response = s3.head_object(Bucket=BUCKET, Key=key)
                print(f"   ✓ {key}")
                print(f"     Tamanho: {response['ContentLength']:,} bytes")
            
            if len(files_uploaded) > 3:
                print(f"   ... e mais {len(files_uploaded) - 3} arquivo(s)")
            
            # Sucesso
            results.append({
                'ticker': ticker_clean,
                'status': 'SUCCESS',
                'records': len(df),
                'files': len(files_uploaded),
                'date_range': f"{df['data'].min().date()} até {df['data'].max().date()}",
                'download_time': download_time,
                'upload_time': upload_time
            })
            
            print(f"\n   ✅ {ticker_desc} processado com SUCESSO!")
            
        except Exception as e:
            print(f"\n   ❌ ERRO ao processar {ticker_yahoo}:")
            print(f"   {str(e)}")
            import traceback
            print(traceback.format_exc())
            results.append({
                'ticker': ticker_clean,
                'status': 'FAILED',
                'error': str(e)
            })
        
        # Pausa entre tickers
        if i < len(tickers_to_process):
            print(f"\n   ⏳ Aguardando 2 segundos antes do próximo ticker...")
            time.sleep(2)
    
    # Resumo final
    print_header("RESUMO DA EXTRAÇÃO")
    
    success = [r for r in results if r['status'] == 'SUCCESS']
    failed = [r for r in results if r['status'] == 'FAILED']
    
    print(f"\n📊 Estatísticas:")
    print(f"   Total de tickers processados: {len(tickers_to_process)}")
    print(f"   ✅ Sucessos: {len(success)}")
    print(f"   ❌ Falhas: {len(failed)}")
    
    if success:
        total_records = sum(r['records'] for r in success)
        total_files = sum(r['files'] for r in success)
        avg_download = sum(r['download_time'] for r in success) / len(success)
        avg_upload = sum(r['upload_time'] for r in success) / len(success)
        
        print(f"\n   Total de registros extraídos: {total_records:,}")
        print(f"   Total de arquivos criados: {total_files}")
        print(f"   Tempo médio de download: {avg_download:.2f}s")
        print(f"   Tempo médio de upload: {avg_upload:.2f}s")
        
        print(f"\n📅 Períodos de dados:")
        for r in success:
            print(f"   {r['ticker']}: {r['date_range']} ({r['records']} dias)")
        
        print(f"\n📁 Arquivos criados no S3 (particionados por data):")
        print(f"   Padrão: s3://{BUCKET}/raw/date=YYYY-MM-DD/TICKER_YYYY-MM-DD.parquet")
    
    if failed:
        print(f"\n❌ Tickers com falha:")
        for r in failed:
            print(f"   ✗ {r['ticker']}: {r.get('error', 'Erro desconhecido')}")
    
    print(f"\n🕐 Término: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print_header("EXTRAÇÃO CONCLUÍDA - PRONTO PARA PROCESSAR NO GLUE!")
    
    print("\n🎯 Próximos passos:")
    print("   1. Lambda será acionada automaticamente pelo S3")
    print("   2. Glue Job processará os dados")
    print("   3. Dados refinados estarão disponíveis no Athena")
    print("\n💡 Monitorar:")
    print("   - CloudWatch Logs: /aws/lambda/b3-s3-trigger-lambda")
    print("   - Glue Jobs: AWS Console → Glue → ETL jobs → b3-etl-job")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Extração de dados B3 com períodos customizáveis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

  # Extrair últimos 5 dias (padrão)
  python demo_extracao_b3.py

  # Extrair 1 ano de dados
  python demo_extracao_b3.py --period 1y

  # Extrair 5 anos de dados
  python demo_extracao_b3.py --period 5y

  # Extrair máximo disponível
  python demo_extracao_b3.py --period max

  # Extrair apenas PETR4 e VALE3
  python demo_extracao_b3.py --period 1y --tickers PETR4 VALE3

  # Período customizado (datas específicas)
  python demo_extracao_b3.py --start 2020-01-01 --end 2025-12-31

  # Período customizado + tickers específicos
  python demo_extracao_b3.py --start 2023-01-01 --end 2024-12-31 --tickers IBOV PETR4

Períodos disponíveis:
  5d   - 5 dias
  1mo  - 1 mês (~21 dias úteis)
  3mo  - 3 meses (~63 dias úteis)
  6mo  - 6 meses (~126 dias úteis)
  1y   - 1 ano (~250 dias úteis) ✅ RECOMENDADO
  2y   - 2 anos (~500 dias úteis)
  5y   - 5 anos (~1250 dias úteis)
  10y  - 10 anos
  ytd  - Do início do ano até hoje
  max  - Máximo disponível (desde IPO)
        """
    )
    
    parser.add_argument(
        '--period',
        default='1mo',
        choices=list(PERIOD_OPTIONS.keys()),
        help='Período para extração (padrão: 1mo)'
    )
    
    parser.add_argument(
        '--start',
        type=str,
        help='Data inicial (formato: YYYY-MM-DD) - sobrescreve --period'
    )
    
    parser.add_argument(
        '--end',
        type=str,
        help='Data final (formato: YYYY-MM-DD) - requer --start'
    )
    
    parser.add_argument(
        '--tickers',
        nargs='+',
        help='Tickers específicos para extrair (ex: IBOV PETR4 VALE3)'
    )
    
    args = parser.parse_args()
    
    # Validações
    if args.end and not args.start:
        parser.error("--end requer --start")
    
    if args.start and args.end:
        try:
            start_dt = datetime.strptime(args.start, '%Y-%m-%d')
            end_dt = datetime.strptime(args.end, '%Y-%m-%d')
            if start_dt >= end_dt:
                parser.error("--start deve ser anterior a --end")
        except ValueError:
            parser.error("Datas devem estar no formato YYYY-MM-DD")
    
    # Executar
    main(
        period=args.period,
        custom_start=args.start,
        custom_end=args.end,
        tickers_filter=args.tickers
    )