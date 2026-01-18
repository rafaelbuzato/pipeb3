# trigger_glue_job.py
import boto3
from datetime import datetime

glue = boto3.client('glue')

response = glue.start_job_run(
    JobName='b3-etl-job',
    Arguments={
        '--bucket': 'pipeline-b3-lab-buzato',
        '--date': datetime.now().strftime('%Y-%m-%d')
    }
)

print(f"✓ Job iniciado!")
print(f"Job Run ID: {response['JobRunId']}")