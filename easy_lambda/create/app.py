import os
import json
import time
import requests
import traceback


# Write your code here.


def work(args):
    result = {}


    
    return result



# ===You can ignore it below===



def slack(slack_uri, *args):
    message = "\n".join([str(sentence) for sentence in args])
    data = {"text": message}
    headers = {'Content-Type': 'application/json'}
    
    print(message)

    requests.post(slack_uri, json.dumps(
        data, ensure_ascii=False, default=str).encode("utf-8"), headers=headers)

    return message 
    
def main(event, context=None):

    status_code = 200

    try:
        body = work(event)

    except ValueError as e:
        status_code = 400
        body = str(e)
        if os.environ.get("SLACK_URI", ""):
            slack(os.environ["SLACK_URI"], body)
            
        traceback.print_exc()

    except Exception as e:
        status_code = 500
        body = traceback.format_exc()
        if os.environ.get("SLACK_URI", ""):
            slack(os.environ["SLACK_URI"], body)
        traceback.print_exc()

    return {
        'statusCode': status_code,
        'body': body
    }
