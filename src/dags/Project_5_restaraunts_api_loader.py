import requests
import json
import psycopg2

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
nickname = "yunikonius"
cohort = 14


headers = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": nickname,
        "X-Cohort": str(cohort)
}

method_url = '/restaurants'

payload = {'sort_field': '_id', 'sort_direction': 'asc', 'limit': 50, 'offset': 0}


def restaurants_loader_from_src():
        r = requests.get(url + method_url, params=payload, headers=headers)
        response_dict_restaurants = json.loads(r.content)
        return response_dict_restaurants


def restaurants_loader_to_stg(response_dict_restaurants):
        conn = psycopg2.connect(
                host="localhost",
                port="15432",
                database="de",
                user="jovyan",
                password="jovyan"
        )
        cursor = conn.cursor()

        for restaurant in response_dict_restaurants:
                src_id = restaurant["_id"]
                name = restaurant["name"]
                sql = f"INSERT INTO stg.courier_system_restaurants (src_id, name) VALUES ('{src_id}', '{name}') ON CONFLICT (src_id) DO NOTHING"
                cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()


response_dict_restaurants = restaurants_loader_from_src()
restaurants_loader_to_stg(response_dict_restaurants)


