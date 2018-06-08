# views.py
#
# Copyright (C) 2011-2018 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##

# Sources:
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html
# http://boto3.readthedocs.io/en/latest/reference/services/glacier.html
# http://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html
# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html
# http://boto3.readthedocs.io/en/latest/reference/services/sns.html

__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile
import requests
from botocore.exceptions import BotoCoreError



"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + str(uuid.uuid4()) + '~${filename}'

  # Redirect to a route that will call the annotator
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  # NOTE: We also must inlcude "x-amz-security-token" since we're
  # using temporary credentials via instance roles
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  expires_in = app.config['AWS_SIGNED_REQUEST_EXPIRATION']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  presigned_post = s3.generate_presigned_post(Bucket=bucket_name, 
    Key=key_name, Fields=fields, Conditions=conditions, ExpiresIn=expires_in)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
 # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  key = request.args.get('key')

  #Get user information
  user = session['primary_identity']
  profile = get_profile(identity_id=session.get('primary_identity'))
  role = profile.role
  email = profile.email

  # Extract the job ID from the S3 key
  fileinfo = key.split('/')[2]
  job_id = fileinfo.split('~')[0]
  filename = fileinfo.split('~')[1]


  # Persist job to database
  data = {"job_id": job_id,
          "user_id": user,
          "input_file_name": filename,
          "s3_inputs_bucket": 'gas-inputs',
          "s3_key_input_file": key,
          "submit_time": int(time.time()),
          "job_status": "PENDING",
          "profile": role,
          "email": email}

  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table('hamadm_annotations')
  ann_table.put_item(Item=data)

  # Send message to request queue
  sns = boto3.resource('sns', region_name=app.config['AWS_REGION_NAME'])
  topic = sns.Topic('arn')
  data1 = str(data)

  response = topic.publish(
  TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
  Message=data1
  )

  return render_template('annotate_confirm.html', job_id=job_id)


#List all annotations for a specific user
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  #get user
  user = session['primary_identity']
  #get DynamoDB database and get response from query
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 

  response = ann_table.query(IndexName='user_id_index', Select='SPECIFIC_ATTRIBUTES',
               ProjectionExpression='submit_time, job_id, job_status, input_file_name',
               KeyConditionExpression=Key('user_id').eq(user))

  #this changes the time(int) to the correct format
  for item in response['Items']:
    item['submit_time'] = datetime.fromtimestamp(int(item['submit_time'])).strftime('%Y-%m-%d %H:%M')
  # Get list off annotations to display
  
  return render_template('annotations.html', annotations=response['Items'])


#Displays details of a specific annotations job
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 

  response = ann_table.query(Select='ALL_ATTRIBUTES',
               KeyConditionExpression=Key('job_id').eq(id))

  #this is for passing to the html page. 
  for item in response['Items']:
    item['submit_time'] = datetime.fromtimestamp(int(item['submit_time'])).strftime('%Y-%m-%d %H:%M')
    if item['job_status'] == "COMPLETED":
      item['complete_time'] = datetime.fromtimestamp(int(item['complete_time'])).strftime('%Y-%m-%d %H:%M')

      s3 = boto3.client('s3', 
        region_name=app.config['AWS_REGION_NAME'], 
        config=Config(signature_version='s3v4'))

      url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
          'Bucket':app.config['AWS_S3_RESULTS_BUCKET'],
          'Key': response['Items'][0]['s3_key_result_file']
        })
      urlResponse = requests.get(url)
    else:
      url = 'None'

  profile = get_profile(identity_id=session.get('primary_identity'))
  if profile.role == "premium_user":
    premium = 'True'
  else:
    premium = 'False'

    #various variables are passed into the html to change what is shown on the html page
  return render_template('job_details.html', annotations=response['Items'], urlResponse=url, premium=premium)    

"""Display the log file for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 

  response = ann_table.query(Select='ALL_ATTRIBUTES',
                            KeyConditionExpression=Key('job_id').eq(id))

  s3 = boto3.resource('s3')

  obj = s3.Object(app.config['AWS_S3_RESULTS_BUCKET'], response['Items'][0]['s3_log_result_file'])
  stuff = obj.get()['Body'].read().decode('utf-8') 
  return render_template('log.html', stuff=stuff)


"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  user = session['primary_identity']
  if request.method == "POST":
    token = request.form.get('stripe_token')
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    cust = stripe.Customer.create(
              description="New customer" + session['primary_identity'],
              source=token
            )
    #update to premium user
    update_profile(
      identity_id=user,
      role="premium_user"
    )
    #Query the database for values that match the user
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    jobs = ann_table.query(
                        IndexName='user_id_index',
                        Select='SPECIFIC_ATTRIBUTES',
                        ProjectionExpression='results_file_archive_id, job_id, complete_time',
                        KeyConditionExpression=Key('user_id').eq(user)
                        )

    glacier_restore_jobs = jobs['Items']
    
    client=boto3.client('glacier', region_name=app.config['AWS_REGION_NAME'])
    for job in glacier_restore_jobs:
      response = client.initiate_job(
                    vaultName=app.config['AWS_GLACIER_VAULT'],
                    jobParameters={
                      'ArchiveId': job['results_file_archive_id'],
                      'Type': 'archive-retrieval',
                      'Tier': 'Expedited',
                      'SNSTopic': app.config['SNS_GLACIER_THAW']
                    }
      )
      update = {'profile': {'Value': 'premium_user', 'Action': 'PUT'}}
      ann_table.update_item(Key={'job_id': job_id}, AttributeUpdates=update)

      
    return render_template('subscribe_confirm.html', stripe_id=cust.id)


  return render_template('subscribe.html')

@app.route('/cancel_subscription', methods=['GET'])
@authenticated
def cancel_premium():
  user = session['primary_identity']
  profile = get_profile(identity_id=user)
  if profile.role == 'premium_user':
    update_profile(identity_id=user, role="free_user")
  return render_template('goodbye.html')




"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info('Login attempted from IP {0}'.format(request.remote_addr))
  # If user requested a specific page, save it to session for redirect after authentication
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. Please check the URL and try again."), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. If you think you deserve to be granted access, please contact the supreme leader of the mutating genome revolutionary party."), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; get your act together, hacker!"), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could not process your request."), 500

### EOF