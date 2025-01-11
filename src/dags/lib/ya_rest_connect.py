import requests

class YacloudRestReader:

    def __init__(self,object_name):
        self.count = 0
        self.object_name = object_name
    
    def count(self):
        return self.count
    
    def get_json(self,params):
        url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/" + self.object_name + "/"
        headers = {
            "X-Nickname": "serjb",
            "X-Cohort": "32",
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
        }
        response = requests.get(url, headers=headers,params=params)
        self.count = len(response.json())
        return response.json()
