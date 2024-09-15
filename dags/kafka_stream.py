from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 9, 14, 5, 10)
}

# get the data from the API
def get_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]
    
    return res

# format the data for Kafka
def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


# stream the data to Kafka
def stream_data():
    import json
    res = get_data()
    formatted_data = format_data(res)
    print(json.dumps(formatted_data, indent=3))


# create DAG
# with DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule_interval = '@daily',
#     catchup=False
#     ) as dag:
    
#     streaming_task = PythonOperator(
#     task_id = 'stream_data_from_api',
#     python_callable=stream_data
#     )

stream_data()