from flask import Flask, request, render_template, jsonify
import boto3
from botocore.client import Config
import uuid
import os
from pathlib import Path
from subprocess import Popen, PIPE
import requests
import json
from botocore.exceptions import BotoCoreError
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)
import hardcode
import requests
#message

# Sources:
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html
# http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html
# http://boto3.readthedocs.io/en/latest/reference/services/sns.html

#get Hardcodes
hardcodes = hardcode.Config
#set waittime
WAIT_TIME = 30


try:
	#get all the necesssary pieces

	#SQS - http://boto3.readthedocs.io/en/latest/reference/services/sqs.html
	sqs = boto3.resource('sqs', region_name=hardcodes.AWS_REGION_NAME)
	queue = sqs.get_queue_by_name(QueueName=hardcodes.AWS_SQS_GLACIER)

	#DynamoDB - http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
	dynamodb = boto3.resource('dynamodb', region_name=hardcodes.AWS_REGION_NAME)
	ann_table = dynamodb.Table(hardcodes.AWS_DYNAMODB_ANNOTATIONS_TABLE)

	#S3 - http://boto3.readthedocs.io/en/latest/reference/services/s3.html
	s3 = boto3.client('s3',
					  region_name=hardcodes.AWS_REGION_NAME,
					  config=Config(signature_version='s3v4'))
	bucket_name = hardcodes.AWS_S3_RESULTS_BUCKET
	s3_resource = boto3.resource('s3')
	bucket = s3_resource.Bucket(bucket_name)

except Exception as e:
	print (e)
	 

	
while True:
	try:
		messages = queue.receive_messages(WaitTimeSeconds=5)

		if not messages:
			print("Using results_archive.py to archive free user data") 
		else:

			for message in messages:
				x = message.body
				x = json.loads(x)
				newMessage = x['Message']
				newMessage=json.loads(newMessage)
				print(newMessage['s3_key_input_file'])



				#Get the current DYNAMO entry to check the message
				job_id = newMessage['job_id']
				job = ann_table.query(Select='ALL_ATTRIBUTES',
									  KeyConditionExpression=Key('job_id').eq(job_id))
				job = job['Items'][0]

				if job['profile'] == "premium_user":
					message.delete()

				#will continue the loop if it is not time yet
				if (time.time() - float(job['complete_time'])) / 60 < WAIT_TIME:
					continue
				
				

				key = newMessage['s3_key_input_file'].replace('.vcf', '.annot.vcf').replace('~', '/')
				obj = bucket.Object(key)
				#Upload to Glacier
				glacier = boto3.client('glacier', hardcodes.AWS_REGION_NAME)
				glacier_response = glacier.upload_archive(vaultName=hardcodes.AWS_GLACIER_VAULT,
														  archiveDescription=json.dumps( {'s3_key': key, 'job_id': job_id,}), #this is how we will download s3 back after
														  body=obj.get()['Body'].read())

				glacier_job_id = glacier_response['archiveId']

				delete_from_s3 = s3.delete_object(Bucket=bucket_name, Key=key)

				ann_table.update_item(Key={'job_id':job_id},
									  AttributeUpdates={'results_file_archive_id':
									  					   {'Value':glacier_job_id, 'Action':'PUT'}
									  				   })

				message.delete()
				print("Deleted from queue and uploaded to glacier. Please check DynamoDB for further information")


	except Exception as e:
		print(e)

		

