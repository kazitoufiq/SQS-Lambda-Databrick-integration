import boto3
import requests
import json
from databricks_api import DatabricksAPI
import logging
from botocore.exceptions import ClientError

DOMAIN ='adb-xxxxxxx.9.azuredatabricks.net'
TOKEN  = 'xxxxx'
BASE_URL = 'https://%s/api/2.0/jobs/run-now' % (DOMAIN)


def lambda_handler(event, context):

    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s: %(levelname)s: %(message)s')


    client=boto3.client('sqs', region_name='ap-southeast-2')
    queue_url='https://sqs.ap-southeast-2.amazonaws.com/xxxxxxx/SQSInspectaQueue'

    def concurrent_user_available():
          
      db = DatabricksAPI(host=DOMAIN,
      token=TOKEN)

      job_details = db.jobs.get_job(
      10532,headers=None,)

      max_concurrent_runs = job_details['settings']['max_concurrent_runs']

      no_of_runs = db.jobs.list_runs(job_id=10532,active_only=True,
      completed_only=None,offset=None,limit=None,headers=None,)

      if no_of_runs['has_more'] == False and len(no_of_runs)==1:
          return True
      
      else:
          no_of_runs = len(no_of_runs['runs'])
          return int(max_concurrent_runs) > int(no_of_runs)


    capacity = concurrent_user_available()
    print('Concurrent user Availability bool :', capacity)
     
    def receive_queue_message(queue_url):
        """
        Retrieves one or more messages (up to 10), from the specified queue.
        """
        try:
            response = client.receive_message(QueueUrl=queue_url)
        except ClientError:
            logger.exception(
                f'Could not receive the message from the - {queue_url}.')
            raise
        else:
            return response
      


    def delete_queue_message(queue_url, receipt_handle):
        """
        Deletes the specified message from the specified queue.
        """
        try:
            response = client.delete_message(QueueUrl=queue_url,
                                                 ReceiptHandle=receipt_handle)
        except ClientError:
            logger.exception(
                f'Could not delete the meessage from the - {queue_url}.')
            raise
        else:
            return response



    # if enough capacity
    if(capacity == True):
    
      messages = receive_queue_message(queue_url)
      #print(messages)

      try:
            for msg in messages['Messages']:
              #print(msg)
              msg_body = msg['Body']
              print(msg_body)
              #print(eval(msg_body))
              receipt_handle = msg['ReceiptHandle']
            
              sent_api_request = requests.post(BASE_URL,

                                 json={'job_id': xxx,

                                        "notebook_params": eval(msg_body)},

                                 headers={'Authorization': 'Bearer %s' % TOKEN})
            
              print(sent_api_request.content)
              
              # delete message for the sent request
              delete_queue_message(queue_url, receipt_handle)
              
              logger.info(f'Received and deleted message(s) from {queue_url}.')
      except KeyError:
         print("No Message Found")
