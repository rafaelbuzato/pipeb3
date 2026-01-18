
# diagnostico_lambda_trigger.py
import boto3
import json
from datetime import datetime, timedelta

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
logs = boto3.client('logs')
glue = boto3.client('glue')

BUCKET = 'pipeline-b3-lab-buzato'
LAMBDA_NAME = 'b3-s3-trigger-lambda'

print("="*80)
print("DIAGNÓSTICO: LAMBDA TRIGGER → GLUE JOB")
print("="*80)

# 1. Verificar configuração da Lambda
print("\n[1] Verificando configuração da Lambda...")
print("-"*80)

try:
    lambda_config = lambda_client.get_function(FunctionName=LAMBDA_NAME)
    
    print(f"  ✓ Lambda encontrada: {LAMBDA_NAME}")
    print(f"  Runtime: {lambda_config['Configuration']['Runtime']}")
    print(f"  Última modificação: {lambda_config['Configuration']['LastModified']}")
    
    # Verificar variáveis de ambiente
    env_vars = lambda_config['Configuration'].get('Environment', {}).get('Variables', {})
    if env_vars:
        print(f"\n  Variáveis de ambiente:")
        for k, v in env_vars.items():
            print(f"    {k}: {v}")
    
except Exception as e:
    print(f"  ❌ Erro ao buscar Lambda: {e}")

# 2. Verificar permissões da Lambda para S3
print("\n[2] Verificando permissões S3 → Lambda...")
print("-"*80)

try:
    # Verificar notificações do bucket
    notification_config = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    
    lambda_configs = notification_config.get('LambdaFunctionConfigurations', [])
    
    if lambda_configs:
        print(f"  ✓ {len(lambda_configs)} notificação(ões) configurada(s):")
        
        for i, config in enumerate(lambda_configs, 1):
            print(f"\n  Notificação #{i}:")
            print(f"    Lambda ARN: {config['LambdaFunctionArn']}")
            print(f"    Eventos: {config['Events']}")
            
            # Verificar filtros
            if 'Filter' in config:
                filter_rules = config['Filter'].get('Key', {}).get('FilterRules', [])
                print(f"    Filtros:")
                for rule in filter_rules:
                    print(f"      - {rule['Name']}: {rule['Value']}")
            
            # Verificar se é a Lambda correta
            if LAMBDA_NAME in config['LambdaFunctionArn']:
                print(f"    ✅ Lambda trigger configurada corretamente!")
            else:
                print(f"    ⚠️  Lambda ARN não corresponde")
    else:
        print(f"  ❌ NENHUMA notificação S3 configurada!")
        print(f"  → PROBLEMA ENCONTRADO: Lambda não está conectada ao S3")
        
except Exception as e:
    print(f"  ❌ Erro ao verificar notificações: {e}")

# 3. Verificar logs recentes da Lambda
print("\n[3] Verificando logs da Lambda (últimas 24h)...")
print("-"*80)

try:
    log_group = f'/aws/lambda/{LAMBDA_NAME}'
    
    # Buscar streams recentes
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    streams = logs.describe_log_streams(
        logGroupName=log_group,
        orderBy='LastEventTime',
        descending=True,
        limit=5
    )
    
    if streams['logStreams']:
        print(f"  ✓ Encontrados {len(streams['logStreams'])} streams recentes")
        
        # Pegar eventos do stream mais recente
        latest_stream = streams['logStreams'][0]
        print(f"\n  Stream mais recente: {latest_stream['logStreamName']}")
        print(f"  Último evento: {latest_stream.get('lastEventTime', 'N/A')}")
        
        # Buscar logs
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=latest_stream['logStreamName'],
            startFromHead=False,
            limit=20
        )
        
        if events['events']:
            print(f"\n  Últimas mensagens de log:")
            for event in events['events'][-10:]:
                timestamp = datetime.fromtimestamp(event['timestamp']/1000)
                message = event['message'].strip()
                print(f"    [{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {message}")
        else:
            print(f"  ⚠️  Nenhum log recente encontrado")
    else:
        print(f"  ⚠️  Nenhum stream de log encontrado nas últimas 24h")
        print(f"  → Lambda pode não ter sido invocada recentemente")
        
except logs.exceptions.ResourceNotFoundException:
    print(f"  ❌ Log group não encontrado: {log_group}")
    print(f"  → Lambda nunca foi executada ou logs não estão habilitados")
except Exception as e:
    print(f"  ❌ Erro ao buscar logs: {e}")

# 4. Verificar invocações recentes via métricas
print("\n[4] Verificando invocações da Lambda...")
print("-"*80)

try:
    cloudwatch = boto3.client('cloudwatch')
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    metrics = cloudwatch.get_metric_statistics(
        Namespace='AWS/Lambda',
        MetricName='Invocations',
        Dimensions=[
            {'Name': 'FunctionName', 'Value': LAMBDA_NAME}
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,  # 1 hora
        Statistics=['Sum']
    )
    
    total_invocations = sum(dp['Sum'] for dp in metrics['Datapoints'])
    
    if total_invocations > 0:
        print(f"  ✓ Lambda foi invocada {int(total_invocations)} vez(es) nas últimas 24h")
    else:
        print(f"  ⚠️  Lambda NÃO foi invocada nas últimas 24h")
        print(f"  → PROBLEMA: S3 não está disparando a Lambda")
        
except Exception as e:
    print(f"  ❌ Erro ao buscar métricas: {e}")

# 5. Verificar arquivos raw que deveriam ter disparado a Lambda
print("\n[5] Verificando arquivos raw recentes...")
print("-"*80)

try:
    # Listar arquivos das últimas 24h
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix='raw/'
    )
    
    recent_files = []
    cutoff = datetime.now() - timedelta(hours=24)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['LastModified'].replace(tzinfo=None) > cutoff:
                if obj['Key'].endswith('.parquet'):
                    recent_files.append(obj)
        
        if recent_files:
            print(f"  ✓ {len(recent_files)} arquivo(s) criado(s) nas últimas 24h:")
            for obj in recent_files[-5:]:  # Últimos 5
                print(f"    - {obj['Key']}")
                print(f"      Criado em: {obj['LastModified']}")
            
            print(f"\n  ⚠️  Esses arquivos DEVERIAM ter disparado a Lambda")
        else:
            print(f"  ℹ️  Nenhum arquivo novo nas últimas 24h")
    
except Exception as e:
    print(f"  ❌ Erro: {e}")

# 6. Testar Lambda manualmente
print("\n[6] Testando Lambda manualmente...")
print("-"*80)

try:
    # Criar evento de teste simulando S3
    test_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": BUCKET},
                "object": {"key": f"raw/date=2026-01-18/ITUB4_2026-01-18.parquet"}
            }
        }]
    }
    
    print(f"  Invocando Lambda com evento de teste...")
    
    response = lambda_client.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType='RequestResponse',
        Payload=json.dumps(test_event)
    )
    
    status_code = response['StatusCode']
    payload = json.loads(response['Payload'].read())
    
    if status_code == 200:
        print(f"  ✅ Lambda executada com sucesso!")
        print(f"  Response: {json.dumps(payload, indent=2)}")
    else:
        print(f"  ❌ Lambda retornou erro: {status_code}")
        print(f"  Response: {payload}")
        
except Exception as e:
    print(f"  ❌ Erro ao invocar Lambda: {e}")
    import traceback
    print(traceback.format_exc())

# 7. Verificar execuções do Glue Job
print("\n[7] Verificando execuções do Glue Job...")
print("-"*80)

try:
    runs = glue.get_job_runs(JobName='b3-etl-job', MaxResults=3)
    
    if runs['JobRuns']:
        print(f"  ✓ Últimas {len(runs['JobRuns'])} execuções:")
        
        for i, run in enumerate(runs['JobRuns'], 1):
            print(f"\n  Execução #{i}:")
            print(f"    Status: {run['JobRunState']}")
            print(f"    Início: {run['StartedOn']}")
            
            if 'CompletedOn' in run:
                duration = (run['CompletedOn'] - run['StartedOn']).total_seconds()
                print(f"    Duração: {duration:.1f}s")
            
            if 'Arguments' in run:
                print(f"    Argumentos: {run['Arguments']}")
    else:
        print(f"  ⚠️  Nenhuma execução encontrada")
        print(f"  → Glue Job nunca foi executado")
        
except Exception as e:
    print(f"  ❌ Erro: {e}")

# 8. Resumo e recomendações
print("\n" + "="*80)
print("RESUMO E AÇÕES RECOMENDADAS")
print("="*80)

print("\n🔍 Possíveis problemas identificados:")
print("  1. Lambda não conectada ao S3 (falta S3 Event Notification)")
print("  2. Filtro de prefix/suffix incorreto na notificação")
print("  3. Lambda sem permissão para ser invocada pelo S3")
print("  4. Lambda com erro no código")

print("\n🔧 Próximos passos:")
print("  1. Verificar resultado do teste manual (seção [6])")
print("  2. Se teste manual funcionar → problema está na notificação S3")
print("  3. Se teste manual falhar → problema está no código da Lambda")

print("\n" + "="*80)