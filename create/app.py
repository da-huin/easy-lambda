from pprint import pprint
import time
import requests
import traceback
from layers import lambda_helper


class Worker():
    def __init__(self, handler: lambda_helper.LambdaHelper, event):
        self.handler = handler
        self.event = event

        
        
    def work(self):
        result = None


        return result

def work(handler, event):
    return Worker(handler, event).work()


def main(event, context=None):
    handler = lambda_helper.LambdaHelper()
    return handler.main(handler, work, event)

