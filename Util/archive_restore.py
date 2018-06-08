# Sources:
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html
# http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html
# http://boto3.readthedocs.io/en/latest/reference/services/sns.html
# http://boto3.readthedocs.io/en/latest/reference/services/ses.html

import os
import boto3
import time
from datetime import datetime
from botocore.client import Config
from boto3.dynamodb.conditions import Key
import json
import hardcode

hardcodes = hardcode.Config

try:
	sqs = boto3.resource('sqs', region_name=hardcodes.AWS_REGION_NAME)
	glacier_queue = sqs.get_queue_by_name(QueueName=hardcodes.AWS_SQS_GLACIER_ARCHIVE)

except Exception as e:
	print (e)
	

while True:
	messages = glacier_queue.receive_messages(WaitTimeSeconds=5)

	if not messages:
		print('Waiting for message...for archive restore')
	else:
		for message in messages:
			try:
				if message:
					message = json.loads(json.loads(message[0].body)['Message'])

					# get the file to upload
					glacier_client = boto3.client('glacier', region_name=hardcodes.AWS_REGION_NAME)
					job_output = glacier_client.get_job_output(
											vaultName=hardcodes.AWS_GLACIER_VAULT,
											jobId=message['JobId']
										)


					# try to get the s3 key for upload
					description = json.loads(job_output['archiveDescription'])
					print(description['s3_key'] + "<- where you will find the file in s3")


					s3 = boto3.client('s3', 
					  region_name=hardcodes.AWS_REGION_NAME, 
					  config=Config(signature_version='s3v4'))

					upload_response = s3.put_object(
											Body=job_output['body'].read(),
											Bucket=hardcodes.AWS_S3_RESULTS_BUCKET,
											Key=description['s3_key'],
													)
					print(upload_response)

					# delete results_file_archive_id for this job in dynamodb
					dynamodb = boto3.resource('dynamodb', region_name=hardcodes.AWS_REGION_NAME)
					ann_table = dynamodb.Table(hardcodes.AWS_DYNAMODB_ANNOTATIONS_TABLE)
					update = {'results_file_archive_id': {'Action': 'DELETE'}}
					ann_table.update_item(Key={'job_id': description['job_id']}, AttributeUpdates=update)

					# delete the SQS message
					response[0].delete()
					print('Delete the handled message')

			except Exception as e:
				print(e)
				raise e
