import boto3
import json
import zipfile
from pathlib import Path
import time

print("\n" + "="*60)
print("CRIANDO LAMBDA COM LABROLE EXISTENTE")
print("="*60 + "\n")

# Ler configuracoes
env_file = Path('.env')
config = {}
if env_file.exists():
    for line in env_file.read_text(encoding='utf-8').splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            config[key.strip()] = value.strip()

BUCKET = config.get('S3_BUCKET_NAME', 'pipeline-b3-lab-buzato')
REGION = config.get('AWS_REGION', 'us-east-1')

# Clientes AWS
iam = boto3.client('iam', region_name=REGION)
lambda_client = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# ====================
# 1. Encontrar LabRole
# ====================
print("PASSO 1: Buscando LabRole...")

response = iam.list_roles()
lab_roles = [r for r in response['Roles'] if 'LabRole' == r['RoleName']]

if not lab_roles:
    # Tentar outras variações
    lab_roles = [r for r in response['Roles'] if 'Lab' in r['RoleName']]

if not lab_roles:
    print("ERRO: LabRole nao encontrada!")
    print("\nRoles disponiveis:")
    for role in response['Roles'][:10]:
        print(f"  - {role['RoleName']}")
    exit(1)

ROLE_ARN = lab_roles[0]['Arn']
ROLE_NAME = lab_roles[0]['RoleName']

print(f"OK: Usando role {ROLE_NAME}")
print(f"ARN: {ROLE_ARN}\n")

# ====================
# 2. Criar pacote Lambda
# ====================
print("PASSO 2: Criando pacote Lambda...")

lambda_dir = Path('lambda')
lambda_dir.mkdir(exist_ok=True)

# Codigo da Lambda (sem caracteres especiais)
lambda_code = """import json
import boto3
import os
from datetime import datetime

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")
    
    try:
        # Extract S3 information
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"New file detected: s3://{bucket}/{key}")
        
        # Extract date from partition
        if 'date=' in key:
            date_partition = key.split('date=')[1].split('/')[0]
        else:
            date_partition = datetime.now().strftime('%Y-%m-%d')
        
        # Get Glue job name
        glue_job_name = os.environ.get('GLUE_JOB_NAME', 'b3-etl-job')
        
        print(f"Starting Glue Job: {glue_job_name}")
        print(f"Date partition: {date_partition}")
        
        # Start Glue job
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--bucket': bucket,
                '--date': date_partition
            }
        )
        
        job_run_id = response['JobRunId']
        
        print(f"SUCCESS: Glue Job started!")
        print(f"JobRunId: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue Job started successfully',
                'jobRunId': job_run_id,
                'bucket': bucket,
                'file': key,
                'date': date_partition
            })
        }
        
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        print(error_msg)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error starting Glue Job',
                'error': str(e)
            })
        }
"""

# Salvar arquivo
lambda_file = lambda_dir / 'lambda_function.py'
lambda_file.write_text(lambda_code, encoding='utf-8')

# Criar ZIP
zip_file = lambda_dir / 'lambda_function.zip'
if zip_file.exists():
    zip_file.unlink()

with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write(lambda_file, 'lambda_function.py')

print(f"OK: Pacote criado em {zip_file}\n")

# ====================
# 3. Deletar Lambda existente (se houver)
# ====================
print("PASSO 3: Verificando Lambda existente...")

FUNCTION_NAME = 'b3-s3-trigger-lambda'

try:
    lambda_client.get_function(FunctionName=FUNCTION_NAME)
    print(f"Lambda {FUNCTION_NAME} existe, deletando...")
    lambda_client.delete_function(FunctionName=FUNCTION_NAME)
    print("Aguardando 5 segundos...")
    time.sleep(5)
except lambda_client.exceptions.ResourceNotFoundException:
    print("Lambda nao existe, criando nova...\n")

# ====================
# 4. Criar Lambda Function
# ====================
print("PASSO 4: Criando Lambda Function...")

try:
    with open(zip_file, 'rb') as f:
        zip_content = f.read()
    
    response = lambda_client.create_function(
        FunctionName=FUNCTION_NAME,
        Runtime='python3.11',
        Role=ROLE_ARN,
        Handler='lambda_function.lambda_handler',
        Code={'ZipFile': zip_content},
        Timeout=60,
        MemorySize=256,
        Environment={
            'Variables': {
                'GLUE_JOB_NAME': 'b3-etl-job'
            }
        },
        Description='Triggers Glue ETL job when new data arrives in S3'
    )
    
    LAMBDA_ARN = response['FunctionArn']
    print(f"OK: Lambda criada com sucesso!")
    print(f"Nome: {FUNCTION_NAME}")
    print(f"ARN: {LAMBDA_ARN}\n")
    
except Exception as e:
    print(f"ERRO ao criar Lambda: {e}\n")
    
    if 'InvalidParameterValueException' in str(e) and 'role' in str(e).lower():
        print("DIAGNOSTICO: LabRole nao tem trust policy para Lambda")
        print("\nSOLUCAO: Crie a Lambda manualmente no Console")
        print("1. AWS Console -> Lambda -> Create function")
        print("2. Use 'Create a new role with basic Lambda permissions'")
        print("3. Depois adicione as politicas inline (Glue e S3)")
        print("\nOu execute: create_lambda_manual_steps.txt")
    
    exit(1)

# ====================
# 5. Adicionar permissao para S3 invocar Lambda
# ====================
print("PASSO 5: Configurando permissao S3...")

try:
    lambda_client.add_permission(
        FunctionName=FUNCTION_NAME,
        StatementId='s3-invoke-lambda-permission',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=f'arn:aws:s3:::{BUCKET}'
    )
    print("OK: Permissao S3 configurada\n")
    
except Exception as e:
    if 'ResourceConflictException' in str(e):
        print("OK: Permissao S3 ja existe\n")
    else:
        print(f"AVISO: {e}\n")

# ====================
# 6. Configurar S3 Event Notification
# ====================
print("PASSO 6: Configurando S3 Event Notification...")

try:
    # Limpar notificacoes existentes
    try:
        s3.put_bucket_notification_configuration(
            Bucket=BUCKET,
            NotificationConfiguration={}
        )
        time.sleep(2)
    except:
        pass
    
    # Configurar nova notificacao
    notification_config = {
        'LambdaFunctionConfigurations': [
            {
                'Id': 's3-trigger-lambda-b3-pipeline',
                'LambdaFunctionArn': LAMBDA_ARN,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': 'raw/'},
                            {'Name': 'suffix', 'Value': '.parquet'}
                        ]
                    }
                }
            }
        ]
    }
    
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration=notification_config
    )
    
    print("OK: S3 Event Notification configurado!")
    print(f"Trigger: raw/*.parquet -> Lambda -> Glue\n")
    
except Exception as e:
    print(f"ERRO: {e}")
    print("\nConfigure manualmente:")
    print(f"1. S3 Console -> {BUCKET}")
    print("2. Properties -> Event notifications -> Create")
    print("3. Prefix: raw/ | Suffix: .parquet")
    print(f"4. Destination: Lambda -> {FUNCTION_NAME}\n")

# ====================
# RESUMO
# ====================
print("="*60)
print("PIPELINE CONFIGURADO COM SUCESSO!")
print("="*60)
print(f"Bucket S3: {BUCKET}")
print(f"Lambda: {FUNCTION_NAME}")
print(f"Role: {ROLE_NAME}")
print(f"Glue Job: b3-etl-job")
print(f"\nFluxo: S3 (raw/*.parquet) -> Lambda -> Glue Job -> S3 (refined/)")
print("\n" + "="*60)
print("PROXIMOS PASSOS:")
print("="*60)
print("1. Testar pipeline:")
print("   python src/main.py --ativos IBOV PETR4 --period 5d")
print("\n2. Ver logs Lambda:")
print("   aws logs tail /aws/lambda/b3-s3-trigger-lambda --follow")
print("\n3. Ver status Glue:")
print("   aws glue get-job-runs --job-name b3-etl-job --max-items 3")
print("\n4. Verificar dados refinados:")
print(f"   aws s3 ls s3://{BUCKET}/refined/ --recursive")
print("="*60 + "\n")