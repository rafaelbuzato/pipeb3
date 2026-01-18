import json
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
