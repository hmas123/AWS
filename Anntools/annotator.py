# Sources:
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html
# http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
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
import config

hardcodes = config.Config

sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName=hardcodes.AWS_SQS_REQUESTS_CAPSTONE)

while True:
	messages = queue.receive_messages(
		WaitTimeSeconds=10
		)

	if not messages:
		print("The Annotator Queue is Empty.")

	else:
		for message in messages:
			try:
				msg_body = eval(eval(message.body)['Message'])
				job_id = msg_body['job_id']
				user_id = msg_body['user_id']
				input_file_name = msg_body['input_file_name']
				s3_key_input_file = msg_body['s3_key_input_file']
				s3_inputs_bucket = msg_body['s3_inputs_bucket']
				submit_time = msg_body['submit_time']
				job_status = msg_body['job_status']
				profile = msg_body['profile']
				email = msg_body['email']
				message.delete()
				print("+----------------------------+")
				print("|Successful delete from queue|")
				print("+----------------------------+")
				


			except BotoCoreError as err:
				print(err)
				continue


			# Connect to Amazon S3
			s3 = boto3.resource('s3', config=Config(signature_version='s3v4'), region_name="us-east-1")

			#creates a filepath using the unique uuid
			filepath = "/home/ubuntu/anntools/files/" + user_id + "~" + job_id
			os.makedirs(filepath)

			#downloads and processes the file
			s3.Bucket(s3_inputs_bucket).download_file(s3_key_input_file, filepath + "/" + input_file_name)
			print("Now processing " + input_file_name +"...")
			print()
			process = Popen(['python3', '/home/ubuntu/anntools/run.py', '/home/ubuntu/anntools/files/' + user_id + "~" + job_id + '/' + input_file_name, job_id, user_id, input_file_name, s3_inputs_bucket, s3_key_input_file, str(submit_time), profile, email], stdout=PIPE, stderr=PIPE)

			