import sys
from pathlib import Path
from datetime import datetime
import logging

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from b3_extractor import B3Extractor
from s3_uploader import S3Uploader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pipeline(ativos=None, period='1mo', upload_s3=True):
    """
    Executa pipeline completo: Extração -> Processamento -> Upload S3
    
    Args:
        ativos: Lista de ativos para extrair (None = todos)
        period: Período de dados ('1mo', '5d', etc)
        upload_s3: Se True, faz upload para S3
    """
    
    print('='*70)
    print('PIPELINE B3 - EXTRAÇÃO E UPLOAD')
    print('='*70)
    
    # 1. EXTRAÇÃO
    logger.info('FASE 1: Extração de dados')
    extractor = B3Extractor()
    
    if ativos is None:
        ativos = ['IBOV', 'PETR4', 'VALE3', 'ITUB4']
    
    df = extractor.extract_multiple(ativos=ativos, period=period, interval='1d')
    
    if df.empty:
        logger.error('Nenhum dado extraído!')
        return False
    
    logger.info(f'✓ Extração concluída: {len(df)} registros')
    
    # 2. SALVAR LOCALMENTE
    logger.info('FASE 2: Salvando localmente')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    local_parquet = f'data/b3_data_{timestamp}.parquet'
    local_csv = f'data/b3_data_{timestamp}.csv'
    
    extractor.save_to_parquet(df, local_parquet)
    extractor.save_to_csv(df, local_csv)
    
    logger.info(f'✓ Dados salvos: {local_parquet}')
    
    # 3. UPLOAD S3
    if upload_s3:
        logger.info('FASE 3: Upload para S3')
        
        try:
            uploader = S3Uploader()
            
            if not uploader.verify_bucket():
                logger.error('Bucket S3 não acessível!')
                return False
            
            uploaded = uploader.upload_parquet_partitioned(df, prefix='raw')
            logger.info(f'✓ Upload S3 concluído: {len(uploaded)} arquivo(s)')
            
        except Exception as e:
            logger.error(f'Erro no upload S3: {e}')
            logger.info('Dados salvos localmente mesmo com erro no S3')
            return False
    
    # 4. RESUMO
    print('\n' + '='*70)
    print('PIPELINE CONCLUÍDO COM SUCESSO!')
    print('='*70)
    print(f'Total de registros: {len(df)}')
    print(f'Ativos processados: {df["ticker"].unique().tolist()}')
    print(f'Período: {df["data"].min()} até {df["data"].max()}')
    print(f'Arquivos locais: {local_parquet}, {local_csv}')
    if upload_s3:
        print(f'Arquivos S3: {len(uploaded)} partições enviadas')
    print('='*70)
    
    return True

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline B3 - Extração e Upload')
    parser.add_argument('--ativos', nargs='+', help='Ativos para extrair (ex: IBOV PETR4)')
    parser.add_argument('--period', default='1mo', help='Período (1mo, 5d, 1y)')
    parser.add_argument('--no-upload', action='store_true', help='Não fazer upload S3')
    
    args = parser.parse_args()
    
    success = run_pipeline(
        ativos=args.ativos,
        period=args.period,
        upload_s3=not args.no_upload
    )
    
    sys.exit(0 if success else 1)
