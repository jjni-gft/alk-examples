import base64
from googleapiclient.discovery import build
from datetime import datetime
import json

def trigger_flow(msg, context):
    event = json.loads(base64.b64decode(msg['data']).decode('utf-8'))
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().flexTemplates().launch(
        projectId='<project_id>',
        location='europe-west3',
        body={
            'launch_parameter': {
                'container_spec_gcs_path':"gs://<bucket>/dataflow/templates/word_count.json",
                'job_name': f"super-word-count-{datetime.now().strftime('%Y%m%d-%H%M')}",
                'parameters':{
                    "input": f"gs://{event['bucket']}/{event['name']}",
                    "output":"gs://<bucket>/dataflow/output/function"
                }
            }
        }
      )
        
    response = request.execute()
    print(response)
