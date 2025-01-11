import requests
from datetime import datetime

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


#params = {'sort_field': 'id', 'limit': 3, 'offset' : 2}
#objects = YacloudRestReader('deliveries')
objects = YacloudRestReader('couriers')
# json = objects.get_json(params=params)
# print(objects.count)
# print(len(json))
# print(json)
# params = {'sort_field': 'id', 'limit': 2, 'offset' : 1}
# objects = YacloudRestReader('couriers')

offset = 0
limit = 50
count = 1
while (count > 0):
    #params = {'sort_field': '_id', 'sort_direction': 'asc', 'limit': limit, 'offset' : offset, 'from' : '2022-01-10 19:22:27'}
    params = {'sort_field': '_id', 'sort_direction': 'desc', 'limit': limit, 'offset' : offset}
    json = objects.get_json(params=params)
    # print(json)
    print('before ' + str(offset))
    offset += objects.count
    count = objects.count
    # print(type(json))
    print('after ' + str(offset))
    for courier in json:
        #print(courier["order_id"] + '--' + courier["delivery_ts"] + '--' + courier["order_ts"])
        print(courier["_id"] + '--' + courier["name"])

#a = [{'order_id': '67573ac43ed2284f389fb85f', 'order_ts': '2024-12-09 18:45:24.233000', 'delivery_id': 'p04cd5k5rweabokrbqcoa34'
# , 'courier_id': 'd9py9w4karenlqf4s2i40sf', 'address': 'Ул. Набережная, 1, кв. 37', 'delivery_ts': '2024-12-09 19:24:28.610000'
# , 'rate': 4, 'sum': 7320, 'tip_sum': 1098}
# , {'order_id': '67573bee33299a6b7afbdb19', 'order_ts': '2024-12-09 18:50:22.646000'
# , 'delivery_id': 'ygau6x157rcfk1le2uw7i94', 'courier_id': 'cvri73uetjyqflq1xi70f82', 'address': 'Ул. Садовая, 13, кв. 85'
# , 'delivery_ts': '2024-12-09 20:25:31.369000', 'rate': 4, 'sum': 12098, 'tip_sum': 1814}
# , {'order_id': '67573d17f20c925937ae11e8', 'order_ts': '2024-12-09 18:55:19.669000', 'delivery_id': 'xfv843q8u2w7q4i591xkd4m', 'courier_id': 'fmsysa85ljp9vyisdu38aen', 'address': 'Ул. Лесная, 8, кв. 70', 'delivery_ts': '2024-12-09 19:19:21.490000', 'rate': 5, 'sum': 1048, 'tip_sum': 157}]
#print(a)
#print('--')
#print(a[-1]['delivery_ts'])
#print(datetime.strptime(a[-1]['delivery_ts'], '%Y-%m-%d %H:%M:%S.%f'))
#print(datetime.strptime('2024-12-09 19:24:28.610000', '%Y-%m-%d %H:%M:%S.%f'))
#print('2024-12-09 19:24:28.610000'.split('.')[0])
#print('2024-12-09 19:24:28'.split('.'))