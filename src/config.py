
import os
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Criar diretórios se não existirem
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'pipeline-b3-data')
S3_RAW_PREFIX = 'raw'
S3_REFINED_PREFIX = 'refined'

# WINFUT Configuration
WINFUT_CONTRACTS = {
    'current': True,  # Usar contrato atual
    'num_contracts': 3,  # Número de contratos futuros
}

# Data extraction settings
DEFAULT_PERIOD = "1mo"
DEFAULT_INTERVAL = "1d"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
