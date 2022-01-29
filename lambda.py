import json
import urllib.request
import os
import boto3
import json
from pymongo import MongoClient

# Global vars
site_url = "https://www.ndbc.noaa.gov/data/realtime2/KIKT.txt"
s3 = boto3.resource('s3')
# s3_bucket = os.environ['s3_bucket']
# model = os.environ['model']
# model_path = '/tmp/' + model

# Atlas vars
password = os.environ['password']
print(password)
passw = "mongodb+srv://coghack:" + password + "@cognitedash.uadlm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
# cluster = MongoClient(passw)
# d = cluster.samples
# db = d["data3"]


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print("value1 = " + event['key1'])
    print("value2 = " + event['key2'])
    print("value3 = " + event['key3'])
    
    # 1) Fetch data, store in memory
    file = urllib.request.urlopen(site_url)
    file_as_is = []
    for line in file:
        decoded_line = line.decode("utf-8")
        #strip_content = decoded_line.split()
        #if (decoded_line )
        print(decoded_line)
    
    # 2) Run model and predict wind speed + direction for next 3 days
    for bucket in s3.buckets.all():
        print(bucket.name)
    
    # s3.download_file(s3_bucket, model, model_path)
    # with open(model_path, 'rb') as f:
    #     loaded_model = pickle.load(f)
        
    # Predict class
    # prediction = loaded_model.predict([[input]])[0]
    # print(prediction)
    
    req =  urllib.request.Request('https://cognite-deployed-model.herokuapp.com/forecast', method='POST')
    resp = urllib.request.urlopen(req)

    resp_json = json.loads(resp.read().decode(resp.info().get_param('charset') or 'utf-8'))
    print(resp_json)
    print(resp_json['prediction'])
    
    count = 0
    hour = 10
    minute = 20
    
    for prediction in resp_json['prediction']:
        row = {}
        row["_id"] = count
        row['year'] = '2022'
        row['month'] = '01'
        row['day'] = '29'
        row['hour'] = hour + int(minute / 60)
        row['minute'] = minute % 60
        row['WDIR_wind_direction'] = prediction
        row['WDIR_wind_speed'] = 3.6
        row['station_name'] = 'KIKT'
        print(row)
        # db.insert_one(row)
        count += 1
        minute += 20
    # 3) Store predictions in Atlas DB

    return event['key1']  # Echo back the first key value
    #raise Exception('Something went wrong')
