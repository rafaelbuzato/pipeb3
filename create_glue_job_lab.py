# create_glue_job_lab.py
import boto3
import json
import time
from pathlib import Path

print("\n" + "="*60)
print("CRIANDO GLUE JOB - B3 PIPELINE (LAB VERSION)")
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
glue = boto3.client('glue', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# ============================================================================
# PASSO 1: Encontrar Role Existente do Lab
# ============================================================================

print("PASSO 1: Procurando role existente do Lab...")

# Buscar roles do lab
response = iam.list_roles()
lab_roles = [r for r in response['Roles'] if 'Lab' in r['RoleName'] or 'Glue' in r['RoleName']]

if lab_roles:
    # Usar a primeira role do lab encontrada
    ROLE_NAME = lab_roles[0]['RoleName']
    ROLE_ARN = lab_roles[0]['Arn']
    print(f"  ✓ Usando role: {ROLE_NAME}")
else:
    print("  ✗ Nenhuma role do lab encontrada")
    print("\n  Roles disponíveis:")
    for role in response['Roles'][:10]:
        print(f"    - {role['RoleName']}")
    print("\n  Digite o nome da role a usar:")
    ROLE_NAME = input("  Role name: ").strip()
    
    role_info = iam.get_role(RoleName=ROLE_NAME)
    ROLE_ARN = role_info['Role']['Arn']
    print(f"  ✓ Usando role: {ROLE_NAME}")

# ============================================================================
# PASSO 2: Criar Database
# ============================================================================

print("\nPASSO 2: Criando Glue Database...")

DATABASE_NAME = "b3_database"

try:
    glue.get_database(Name=DATABASE_NAME)
    print("  ✓ Database já existe")
except:
    try:
        glue.create_database(
            DatabaseInput={
                'Name': DATABASE_NAME,
                'Description': 'Database for B3 stock market data'
            }
        )
        print("  ✓ Database criada")
    except Exception as e:
        print(f"  ⚠ Erro ao criar database: {e}")
        print("  (Você pode criar manualmente na console)")

# ============================================================================
# PASSO 3: Upload Script
# ============================================================================

print("\nPASSO 3: Upload script Glue...")

script_file = Path('glue/glue_etl_job.py')
if script_file.exists():
    try:
        s3.upload_file(
            str(script_file),
            BUCKET,
            'scripts/glue_etl_job.py'
        )
        print("  ✓ Script enviado")
    except Exception as e:
        print(f"  ✗ Erro no upload: {e}")
        exit(1)
else:
    print("  ✗ Arquivo glue/glue_etl_job.py não encontrado")
    exit(1)

# ============================================================================
# PASSO 4: Criar Glue Job
# ============================================================================

print("\nPASSO 4: Criando Glue Job...")

JOB_NAME = "b3-etl-job"

# Deletar se existir
try:
    glue.delete_job(JobName=JOB_NAME)
    print("  Job existente deletado")
    time.sleep(2)
except:
    pass

# Criar job
try:
    glue.create_job(
        Name=JOB_NAME,
        Role=ROLE_ARN,
        Command={
            'Name': 'glueetl',
            'ScriptLocation': f's3://{BUCKET}/scripts/glue_etl_job.py',
            'PythonVersion': '3'
        },
        DefaultArguments={
            '--job-language': 'python',
            '--bucket': BUCKET,
            '--date': '2025-12-26',
            '--enable-metrics': 'true',
            '--enable-spark-ui': 'true',
            '--TempDir': f's3://{BUCKET}/temp/',
            '--enable-glue-datacatalog': 'true'
        },
        GlueVersion='4.0',
        WorkerType='G.1X',
        NumberOfWorkers=2,
        Timeout=30,
        MaxRetries=0
    )
    print("  ✓ Glue Job criado")
except Exception as e:
    print(f"  ✗ Erro ao criar job: {e}")
    exit(1)

# ============================================================================
# RESUMO
# ============================================================================

print("\n" + "="*60)
print("RESUMO")
print("="*60)
print(f"✓ Role usada: {ROLE_NAME}")
print(f"✓ Database: {DATABASE_NAME}")
print(f"✓ Script S3: s3://{BUCKET}/scripts/glue_etl_job.py")
print(f"✓ Glue Job: {JOB_NAME}")
print("\nPara testar:")
print(f"  aws glue start-job-run --job-name {JOB_NAME}")
print("\nConsole:")
print("  AWS Glue → ETL jobs → b3-etl-job")
print("="*60 + "\n")