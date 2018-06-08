

#Question on my uploading format below: would it be better practice to separate the whole process of creating filepaths and uploading for annot and log individually? Or should I do each step of the process together?

__author__ = 'Vas Vasil'
import os
import sys
import time
import driver
import boto3
import time
import json
from botocore.exceptions import BotoCoreError

REGION = 'us-east-1'
BUCKET = 'gas-results'
CNETID = 'hamadm'
DYNAMODB = 'hamadm_annotations'
SNS_RESULTS = 'arn:aws:sns:us-east-1:127134666975:hamadm_job_results'
SNS_GLACIER = 'arn:aws:sns:us-east-1:127134666975:hamadm_glacier'

class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print("Total runtime: {0:.6f} seconds".format(self.secs))


if __name__ == '__main__':
  if len(sys.argv) > 1:
    filepath = sys.argv[1]
    job_id = sys.argv[2]
    user_id = sys.argv[3]
    input_file_name = sys.argv[4]
    s3_inputs_bucket = sys.argv[5]
    s3_key_input_file = sys.argv[6]
    submit_time = sys.argv[7] 
    profile = sys.argv[8]
    email = sys.argv[9]

    data = {

        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": s3_inputs_bucket,
        "s3_key_input_file": s3_key_input_file,
        "submit_time": submit_time,
        "job_status": "RUNNING"

    }

    # updating dynamodDb instance with same job_id
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    ann_table = dynamodb.Table(DYNAMODB)
    ann_table.put_item(Item=data)

    with Timer() as t:
      driver.run(filepath, 'vcf')
    print ("Total runtime: %s seconds" % t.secs)

    ##we are looking to upload TWO of the generated files from the program
    s3 = boto3.resource('s3')

    ##finding and uploading annot
    annot_filepath = (filepath + '.annot').replace('.vcf.annot', '.annot.vcf')
    annot_s3_key = (s3_key_input_file.replace('~', '/') + '.annot').replace('.vcf.annot', '.annot.vcf')
    s3.meta.client.upload_file(annot_filepath, 'gas-results', annot_s3_key)
    print("annotations file uploaded to s3")

    ##finding and uploading log
    log_filepath = (filepath + '.count.log')
    log_s3_key = (s3_key_input_file.replace('~', '/')  + '.count.log')
    s3.meta.client.upload_file(log_filepath, 'gas-results', log_s3_key)
    print("log file uploaded to s3")



    #for DynamoDB input
    data = {

        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": s3_inputs_bucket,
        "s3_key_input_file": s3_key_input_file,
        "s3_results_bucket": 'gas-results',
        "s3_key_result_file": annot_s3_key,
        "s3_log_result_file": log_s3_key,
        "submit_time": int(submit_time),
        "complete_time": int(time.time()),
        "job_status": "COMPLETED",
        "profile": profile,
        "email": email

    }

    # updating dynamodDb instance with same job_id
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    ann_table = dynamodb.Table(DYNAMODB)
    ann_table.put_item(Item=data)

    #publish to SNS topic
    sns = boto3.resource('sns', region_name=REGION)
    topic = sns.Topic('arn')
    data1 = str(data) 
    response = topic.publish(
    TopicArn=SNS_RESULTS, 
    Message=data1
    )
    data2 = json.dumps(data)
    try:
        if profile == "free_user":
            response = topic.publish(
            TopicArn=SNS_GLACIER,
            Message=data2
            )


    except Exception as e:
        print(e)
     
    #remove all files from instance
    dir_path = os.path.dirname(filepath)
    os.remove(filepath)
    os.remove(annot_filepath)
    os.remove(log_filepath)
    os.rmdir(dir_path)




  else:
    print("A valid .vcf file must be provided as input to this program.")