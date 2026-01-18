import json
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
