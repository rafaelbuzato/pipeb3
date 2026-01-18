
# show_s3_files.py
import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'pipeline-b3-lab-buzato'

print("="*70)
print("ARQUIVOS NO PIPELINE B3")
print("="*70)

# Mostrar arquivos RAW
print("\n📥 CAMADA RAW (últimos 10 arquivos):")
print("-"*70)

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix='raw/',
    MaxKeys=100
)

if 'Contents' in response:
    files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    for obj in files[-10:]:  # Últimos 10
        size_mb = obj['Size'] / (1024*1024)
        print(f"  ✓ {obj['Key']}")
        print(f"    Tamanho: {size_mb:.2f} MB | Data: {obj['LastModified']}")
else:
    print("  ⚠ Nenhum arquivo encontrado")

# Mostrar arquivos REFINED
print("\n📊 CAMADA REFINED (últimos 10 arquivos):")
print("-"*70)

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix='refined/',
    MaxKeys=100
)

if 'Contents' in response:
    files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    for obj in files[-10:]:  # Últimos 10
        size_mb = obj['Size'] / (1024*1024)
        print(f"  ✓ {obj['Key']}")
        print(f"    Tamanho: {size_mb:.2f} MB | Data: {obj['LastModified']}")
else:
    print("  ⚠ Nenhum arquivo encontrado")

# Estatísticas
print("\n" + "="*70)
print("RESUMO")
print("="*70)

# Contar partições
raw_dates = set()
refined_partitions = set()

response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/')
if 'Contents' in response:
    for obj in response['Contents']:
        if 'date=' in obj['Key']:
            date = obj['Key'].split('date=')[1].split('/')[0]
            raw_dates.add(date)

response = s3.list_objects_v2(Bucket=BUCKET, Prefix='refined/')
if 'Contents' in response:
    for obj in response['Contents']:
        if 'ticker=' in obj['Key']:
            # Extrai ano/mes/ticker
            parts = obj['Key'].split('/')
            partition = '/'.join([p for p in parts if '=' in p])
            refined_partitions.add(partition)

print(f"Datas com dados raw: {len(raw_dates)}")
print(f"Partições refined: {len(refined_partitions)}")
print(f"\nDatas disponíveis: {sorted(raw_dates)}")

print("\n" + "="*70)