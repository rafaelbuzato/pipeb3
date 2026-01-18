# fix_existing_parquet_files.py
import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')
BUCKET = "pipeline-b3-lab-buzato"

def fix_parquet_file(bucket, key):
    """Corrige timestamps de um arquivo Parquet"""
    print(f"\n[*] Processando: {key}")
    
    try:
        # Download
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        print(f"    Shape: {df.shape}")
        print(f"    Colunas: {list(df.columns)}")
        
        # Verificar e converter timestamps
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                print(f"    Convertendo: {col} ({df[col].dtype})")
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        # Salvar com configurações compatíveis com Spark
        buffer = BytesIO()
        df.to_parquet(
            buffer,
            engine='pyarrow',
            index=False,
            compression='snappy',
            coerce_timestamps='ms',
            allow_truncated_timestamps=True,
            use_deprecated_int96_timestamps=True  # Compatibilidade com Spark
        )
        
        # Upload
        buffer.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"    ✓ Corrigido com sucesso!")
        return True
        
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return False

# Main
print("="*60)
print("CORRIGINDO ARQUIVOS PARQUET")
print("="*60)

# Listar arquivos
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix='raw/date=')

fixed = 0
failed = 0

for page in pages:
    for obj in page.get('Contents', []):
        key = obj['Key']
        
        if key.endswith('.parquet'):
            if fix_parquet_file(BUCKET, key):
                fixed += 1
            else:
                failed += 1

print("\n" + "="*60)
print(f"RESUMO: {fixed} corrigidos, {failed} falharam")
print("="*60)