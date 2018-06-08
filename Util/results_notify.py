# Sources:
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html
# http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html
# http://boto3.readthedocs.io/en/latest/reference/services/sns.html
# http://boto3.readthedocs.io/en/latest/reference/services/ses.html

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
import hardcode
hardcodes = hardcode.Config
#message


##NEED TO UPDATE BODY AND SUBJECT IN THIS
def send_email_ses(recipients=None, 
	sender=None, subject=None, body=None):

	ses = boto3.client('ses', region_name=hardcodes.AWS_REGION_NAME)
	response = ses.send_email(
	Destination = {'ToAddresses': [recipients]},
	Message={
		'Body': {'Text': {'Charset': "UTF-8", 'Data': body}},
		'Subject': {'Charset': "UTF-8", 'Data': subject},
	},
	Source="hamadm@uchicago.edu")
	return response['ResponseMetadata']['HTTPStatusCode']





sqs = boto3.resource('sqs', region_name=hardcodes.AWS_REGION_NAME)
queue = sqs.get_queue_by_name(QueueName=hardcodes.AWS_SQS_JOB_RESULTS)

while True:
	messages = queue.receive_messages(
		MaxNumberOfMessages=10,
		WaitTimeSeconds=10
		)

	if not messages:
		print("The Queue is Empty. Waiting for job result")

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
				email = msg_body['email']
				message.delete()
				print(job_id)
				print("Successful delete from queue")

				body = "Your annotation job - ID: " + job_id + " has completed."
				subject = "GAS Job Complete!" 


			except BotoCoreError as err:
				print("Could not retrieve message from queue in the proper format")
				filename =""


			send_email_ses(body=body, subject=subject, recipients=email)

