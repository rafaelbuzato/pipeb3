import boto3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class S3Uploader:
    """Upload de dados para S3 com particionamento por data"""
    
    def __init__(self, bucket_name=None, region='us-east-1'):
        # Ler bucket name do .env ou usar default
        if bucket_name is None:
            env_file = Path('.env')
            if env_file.exists():
                for line in env_file.read_text().splitlines():
                    if line.startswith('S3_BUCKET_NAME='):
                        bucket_name = line.split('=')[1].strip()
                        break
        
        if bucket_name is None:
            bucket_name = 'pipeline-b3-data'
        
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
        self.region = region
        logger.info(f'S3Uploader inicializado para bucket: {bucket_name}')
        
        # Verificar/criar bucket automaticamente
        self._verify_or_create_bucket()
    
    def _verify_or_create_bucket(self):
        """Verifica se o bucket existe, cria se necessário"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f'Bucket {self.bucket_name} verificado')
        except:
            logger.warning(f'Bucket {self.bucket_name} não encontrado, criando...')
            try:
                if self.region == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                logger.info(f'Bucket {self.bucket_name} criado com sucesso')
            except Exception as e:
                logger.error(f'Erro ao criar bucket: {str(e)}')
                raise
    
    def verify_bucket(self):
        """Verifica se o bucket está acessível"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f'✓ Bucket {self.bucket_name} acessível')
            return True
        except Exception as e:
            logger.error(f'✗ Erro ao acessar bucket: {e}')
            return False
    
    def upload_parquet_partitioned(self, df, prefix='raw', partition_by='data'):
        """
        Upload de dados em Parquet com particionamento por data
        
        Args:
            df: DataFrame com os dados
            prefix: Prefixo do path no S3 (ex: 'raw', 'refined')
            partition_by: Coluna para particionamento
        """
        
        if df.empty:
            logger.warning('DataFrame vazio, nenhum upload')
            return []
        
        uploaded_files = []
        
        # Garantir que data é datetime
        df[partition_by] = pd.to_datetime(df[partition_by])
        
        # Agrupar por data
        for date, group_df in df.groupby(df[partition_by].dt.date):
            date_str = date.strftime('%Y-%m-%d')
            
            # Criar path particionado: raw/date=2025-12-26/
            partition_path = f"{prefix}/date={date_str}"
            
            # Nome do arquivo
            ticker = group_df['ticker'].iloc[0] if 'ticker' in group_df.columns else 'data'
            filename = f"{ticker}_{date_str}.parquet"
            s3_key = f"{partition_path}/{filename}"
            
            # Salvar temporariamente
            temp_dir = Path('temp')
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / filename
            
            group_df.to_parquet(temp_file, index=False, compression='snappy')
            
            # Upload para S3
            try:
                self.s3_client.upload_file(
                    str(temp_file),
                    self.bucket_name,
                    s3_key
                )
                
                s3_uri = f"s3://{self.bucket_name}/{s3_key}"
                logger.info(f'✓ Upload: {s3_uri}')
                uploaded_files.append(s3_key)
                
                # Remover arquivo temporário
                temp_file.unlink()
                
            except Exception as e:
                logger.error(f'✗ Erro no upload de {s3_key}: {e}')
        
        # Limpar pasta temp
        if temp_dir.exists() and not list(temp_dir.iterdir()):
            temp_dir.rmdir()
        
        logger.info(f'Upload concluído: {len(uploaded_files)} arquivo(s)')
        return uploaded_files
    
    def list_files(self, prefix=''):
        """Lista arquivos no bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            return files
        except Exception as e:
            logger.error(f'Erro ao listar arquivos: {e}')
            return []

if __name__ == '__main__':
    print('='*70)
    print('TESTE S3 UPLOADER')
    print('='*70)
    
    # Criar uploader
    uploader = S3Uploader()
    
    # Verificar bucket
    if not uploader.verify_bucket():
        print('\n✗ Erro: Bucket não acessível!')
        exit(1)
    
    # Carregar dados de exemplo
    data_file = Path('data/ibov_1m.parquet')
    
    if data_file.exists():
        print(f'\nCarregando dados de: {data_file}')
        df = pd.read_parquet(data_file)
        print(f'✓ {len(df)} registros carregados')
        
        # Upload
        print('\nIniciando upload para S3...')
        uploaded = uploader.upload_parquet_partitioned(df, prefix='raw')
        
        print(f'\n✓ Upload concluído: {len(uploaded)} arquivo(s)')
        
        # Listar arquivos
        print('\nArquivos no bucket (raw/):')
        files = uploader.list_files(prefix='raw/')
        for f in files[:10]:
            print(f'  - {f}')
    else:
        print(f'\n✗ Arquivo não encontrado: {data_file}')
    
    print('\n' + '='*70)
