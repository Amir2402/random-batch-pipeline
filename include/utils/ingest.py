import requests as rq
from datetime import datetime
import random
import json
import uuid
from S3HelperFunctions import s3_resource, write_to_s3

def ingest(api_url):
    user_data = []

    for itr in range(1, 100):
        req = rq.get(api_url)
        user_record = json.loads(req.content.decode())

        user_id = str(uuid.uuid4())
        quantity_bought = random.randint(1, 10)
        unit_price = random.uniform(1, 100)

        user_record['id'] = user_id
        user_record['quantity'] = quantity_bought
        user_record['unit_price'] = unit_price
        
        user_data.append(user_record)

    write_to_s3(user_data, 'user_data', datetime.now(), 'bronze')


ingest('https://randomuser.me/api/')