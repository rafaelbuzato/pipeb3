import boto3
from datetime import datetime

# Configurações
BUCKET = "pipeline-b3-lab-buzato"
JOB_NAME = "b3-etl-job"

s3 = boto3.client('s3')
glue = boto3.client('glue')
logs = boto3.client('logs')

print("=" * 60)
print("DIAGNOSTICO COMPLETO")
print("=" * 60)

# [1] Verificar S3
print("\n[1] Verificando dados em S3...")
try:
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/', MaxKeys=10)
    if 'Contents' in response:
        print(f"  OK: {len(response['Contents'])} arquivos encontrados")
        for obj in response['Contents'][:5]:
            print(f"    - {obj['Key']}")
    else:
        print("  AVISO: Nenhum arquivo encontrado em raw/")
except Exception as e:
    print(f"  ERRO ao acessar S3: {e}")

# [2] Verificar última execução do Glue
print("\n[2] Analisando última execução do Glue...")
try:
    runs = glue.get_job_runs(JobName=JOB_NAME, MaxResults=1)
    
    if runs['JobRuns']:
        run = runs['JobRuns'][0]
        print(f"  Status: {run['JobRunState']}")
        print(f"  Inicio: {run['StartedOn']}")
        
        # Verificar se Arguments existe
        if 'Arguments' in run:
            print("  Argumentos:")
            for k, v in run['Arguments'].items():
                print(f"    {k}: {v}")
        else:
            print("  AVISO: Job iniciado sem argumentos customizados")
        
        # Mostrar erro se houver
        if run['JobRunState'] == 'FAILED':
            if 'ErrorMessage' in run:
                print(f"\n  ERRO: {run['ErrorMessage']}")
        
        # Mostrar tempo de execução
        if 'CompletedOn' in run:
            duracao = (run['CompletedOn'] - run['StartedOn']).total_seconds()
            print(f"  Duração: {duracao:.1f}s")
        elif run['JobRunState'] == 'RUNNING':
            duracao = (datetime.now(run['StartedOn'].tzinfo) - run['StartedOn']).total_seconds()
            print(f"  Tempo decorrido: {duracao:.1f}s")
            
    else:
        print("  AVISO: Nenhuma execução encontrada")
        
except glue.exceptions.EntityNotFoundException:
    print(f"  ERRO: Job '{JOB_NAME}' não encontrado!")
except Exception as e:
    print(f"  ERRO ao consultar Glue: {e}")

# [3] Verificar logs do CloudWatch
print("\n[3] Buscando logs do CloudWatch...")
try:
    log_group = f"/aws-glue/jobs/output"
    
    streams = logs.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=JOB_NAME,
        orderBy='LastEventTime',
        descending=True,
        limit=3
    )
    
    if streams['logStreams']:
        print(f"  Encontrados {len(streams['logStreams'])} streams")
        
        # Pegar últimas 20 linhas do log mais recente
        latest_stream = streams['logStreams'][0]['logStreamName']
        print(f"\n  Stream: {latest_stream}")
        
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=latest_stream,
            limit=20,
            startFromHead=False
        )
        
        if events['events']:
            print("\n  Últimas mensagens:")
            for event in events['events'][-10:]:  # Últimas 10 linhas
                timestamp = datetime.fromtimestamp(event['timestamp']/1000)
                message = event['message'].strip()
                print(f"    [{timestamp.strftime('%H:%M:%S')}] {message}")
    else:
        print("  AVISO: Nenhum log encontrado")
        
except logs.exceptions.ResourceNotFoundException:
    print(f"  AVISO: Log group '{log_group}' não encontrado")
except Exception as e:
    print(f"  ERRO ao buscar logs: {e}")

# [4] Verificar dados refined
print("\n[4] Verificando dados refinados...")
try:
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='refined/', MaxKeys=5)
    if 'Contents' in response:
        print(f"  OK: {len(response['Contents'])} arquivos encontrados")
        for obj in response['Contents']:
            print(f"    - {obj['Key']}")
    else:
        print("  AVISO: Nenhum dado processado ainda")
except Exception as e:
    print(f"  ERRO: {e}")

# [5] Status do Glue Data Catalog
print("\n[5] Verificando Glue Data Catalog...")
try:
    database = glue.get_database(Name='b3_database')
    print(f"  Database: {database['Database']['Name']}")
    
    tables = glue.get_tables(DatabaseName='b3_database')
    if tables['TableList']:
        for table in tables['TableList']:
            print(f"  Tabela: {table['Name']}")
            print(f"    Location: {table['StorageDescriptor']['Location']}")
            if 'Parameters' in table and 'last_modified_time' in table['Parameters']:
                print(f"    Atualizado: {table['Parameters']['last_modified_time']}")
    else:
        print("  AVISO: Nenhuma tabela catalogada")
        
except glue.exceptions.EntityNotFoundException as e:
    print(f"  AVISO: {e}")
except Exception as e:
    print(f"  ERRO: {e}")

print("\n" + "=" * 60)
print("DIAGNOSTICO CONCLUIDO")
print("=" * 60)