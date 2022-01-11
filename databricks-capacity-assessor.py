import json
from databricks_api import DatabricksAPI
import boto3
from io import StringIO

client = boto3.client('lambda')

def lambda_handler(event, context):


  def concurrent_user_available():



    # import library

    from databricks_api import DatabricksAPI



    # Databricks creds, need to set in environment.

    db = DatabricksAPI(

        host="adb-xxxxx.9.azuredatabricks.net",

        token="xxxxx"

    )



    # Get job details.

    job_details = db.jobs.get_job(

        10532,

        headers=None,

    )



    # Get Max concurrent runs

    max_concurrent_runs = job_details['settings']['max_concurrent_runs']



    # Get No of active runs

    no_of_runs = db.jobs.list_runs(

        job_id=10532,

        active_only=True,

        completed_only=None,

        offset=None,

        limit=None,

        headers=None,

    )



    if no_of_runs['has_more'] == False:

      return True



    # Count active runs

    no_of_runs = len(no_of_runs['runs'])



    return int(max_concurrent_runs) > int(no_of_runs)



  result = concurrent_user_available()
  
  if(result == True):
    capacity = 'Yes'
  else:
    capacity = 'No'
  
  inputForInvoker = {'capacity_available': capacity}

  
  response = client.invoke(
    FunctionName='arn:aws:lambda:ap-southeast-2:xxxxx:function:ReceiveMessageFromSQS',
    InvocationType='RequestResponse',
    Payload= json.dumps(inputForInvoker))
  
  streamingBody = response["Payload"]
  jsonState = json.loads(streamingBody.read())
		

  print(jsonState)
  print('Concurrent user Availability bool :', capacity)

