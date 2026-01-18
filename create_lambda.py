import boto3
import json
import zipfile
from pathlib import Path
import time

print("\n" + "="*60)
print("CRIANDO LAMBDA FUNCTION")
print("="*60 + "\n")

# Ler configurações
env_file = Path('.env')
config = {}
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if '=' in line:
            key, value = line.split('=', 1)
            config[key.strip()] = value.strip()

BUCKET = config.get('S3_BUCKET_NAME', 'pipeline-b3-data')
REGION = config.get('AWS_REGION', 'us-east-1')

print(f"Bucket: {BUCKET}")
print(f"Region: {REGION}\n")

# Clientes AWS
iam = boto3.client('iam', region_name=REGION)
lambda_client = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# ============================================================================
# PASSO 1: Encontrar Role para Lambda
# ============================================================================

print("PASSO 1: Procurando role do Lab...")

response = iam.list_roles()
lab_roles = [r for r in response['Roles'] if 'Lab' in r['RoleName']]

if lab_roles:
    ROLE_ARN = lab_roles[0]['Arn']
    ROLE_NAME = lab_roles[0]['RoleName']
    print(f"  ✓ Usando role: {ROLE_NAME}")
else:
    print("  ✗ Nenhuma role encontrada")
    exit(1)

# ============================================================================
# PASSO 2: Criar ZIP da Lambda
# ============================================================================

print("\nPASSO 2: Criando pacote Lambda...")

lambda_dir = Path('lambda')
lambda_dir.mkdir(exist_ok=True)

# Código da Lambda
lambda_code = '''import json
import boto3
import os
from datetime import datetime

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"New file: s3://{bucket}/{key}")
        
        if 'date=' in key:
            date_partition = key.split('date=')[1].split('/')[0]
        else:
            date_partition = datetime.now().strftime('%Y-%m-%d')
        
        glue_job_name = os.environ.get('GLUE_JOB_NAME', 'b3-etl-job')
        
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--bucket': bucket,
                '--date': date_partition
            }
        )
        
        job_run_id = response['JobRunId']
        print(f"Job started: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Job started',
                'jobRunId': job_run_id,
                'date': date_partition
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
'''

# Salvar código
lambda_file = lambda_dir / 'lambda_function.py'
lambda_file.write_text(lambda_code)

# Criar ZIP
zip_file = lambda_dir / 'lambda_function.zip'
with zipfile.ZipFile(zip_file, 'w') as zf:
    zf.write(lambda_file, 'lambda_function.py')

print(f"  ✓ ZIP criado: {zip_file}")

# ============================================================================
# PASSO 3: Criar Lambda Function
# ============================================================================

print("\nPASSO 3: Criando Lambda Function...")

FUNCTION_NAME = 'b3-s3-trigger-lambda'

# Deletar se existir
try:
    lambda_client.delete_function(FunctionName=FUNCTION_NAME)
    print("  Lambda existente deletada")
    time.sleep(2)
except:
    pass

# Criar função
try:
    with open(zip_file, 'rb') as f:
        zip_content = f.read()
    
    response = lambda_client.create_function(
        FunctionName=FUNCTION_NAME,
        Runtime='python3.11',
        Role=ROLE_ARN,
        Handler='lambda_function.lambda_handler',
        Code={'ZipFile': zip_content},
        Timeout=30,
        MemorySize=256,
        Environment={
            'Variables': {
                'GLUE_JOB_NAME': 'b3-etl-job'
            }
        }
    )
    
    LAMBDA_ARN = response['FunctionArn']
    print(f"  ✓ Lambda criada: {FUNCTION_NAME}")
    
except Exception as e:
    print(f"  ✗ Erro: {e}")
    exit(1)

# ============================================================================
# PASSO 4: Adicionar Permissão S3 para invocar Lambda
# ============================================================================

print("\nPASSO 4: Configurando permissão S3...")

try:
    lambda_client.add_permission(
        FunctionName=FUNCTION_NAME,
        StatementId='s3-invoke-lambda',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=f'arn:aws:s3:::{BUCKET}'
    )
    print("  ✓ Permissão configurada")
except Exception as e:
    if 'ResourceConflictException' in str(e):
        print("  ✓ Permissão já existe")
    else:
        print(f"  ⚠ Aviso: {e}")

# ============================================================================
# PASSO 5: Configurar S3 Event Notification
# ============================================================================

print("\nPASSO 5: Configurando S3 Event Notification...")

try:
    # Verificar notificações existentes
    try:
        current_notifications = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    except:
        current_notifications = {}
    
    # Adicionar nossa notificação
    notification_config = {
        'LambdaFunctionConfigurations': [
            {
                'Id': 's3-raw-to-lambda',
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
    
    print("  ✓ S3 Event Notification configurada")
    
except Exception as e:
    print(f"  ✗ Erro: {e}")
    print("  (Você pode configurar manualmente na console)")

# ============================================================================
# RESUMO
# ============================================================================

print("\n" + "="*60)
print("RESUMO")
print("="*60)
print(f"✓ Role: {ROLE_NAME}")
print(f"✓ Lambda: {FUNCTION_NAME}")
print(f"✓ S3 Trigger: raw/*.parquet → Lambda → Glue")
print("\nPara testar:")
print(f"  python src/main.py --period 5d --ativos IBOV")
print("="*60 + "\n")
