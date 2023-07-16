import requests
import json
import psycopg2
from datetime import datetime

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
nickname = "yunikonius"
cohort = 14


headers = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": nickname,
        "X-Cohort": str(cohort)
}

method_url = '/deliveries'

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

payload = {'sort_field': '_id', 'sort_direction': 'asc', 'from': '2023-07-01 00:00:00', 'to': f'{current_datetime}'}


def deliveries_loader_from_src():
        r = requests.get(url + method_url, params=payload, headers=headers)
        response_dict_deliveries = json.loads(r.content)
        return response_dict_deliveries


def deliveries_loader_to_stg(response_dict_deliveries):
        conn = psycopg2.connect(
                host="localhost",
                port="15432",
                database="de",
                user="jovyan",
                password="jovyan"
        )
        cursor = conn.cursor()

        for delivery in response_dict_deliveries:
                order_id = delivery["order_id"]
                order_ts = delivery["order_ts"]
                delivery_id = delivery["delivery_id"]
                courier_id = delivery["courier_id"]
                address = delivery["address"]
                delivery_ts = delivery["delivery_ts"]
                rate = delivery["rate"]
                sum = delivery["sum"]
                tip_sum = delivery["tip_sum"]
                sql = f"INSERT INTO stg.courier_system_deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)" \
                      f" VALUES ('{order_id}', '{order_ts}', '{delivery_id}', '{courier_id}', '{address}', '{delivery_ts}', '{rate}', '{sum}', '{tip_sum}') " \
                      f"ON CONFLICT (delivery_id) DO UPDATE SET " \
                      f"order_id = EXCLUDED.order_id, " \
                      f"order_ts = EXCLUDED.order_ts, " \
                      f"delivery_id = EXCLUDED.delivery_id, " \
                      f"courier_id = EXCLUDED.courier_id, " \
                      f"address = EXCLUDED.address, " \
                      f"delivery_ts = EXCLUDED.delivery_ts, " \
                      f"rate = EXCLUDED.rate, " \
                      f"sum = EXCLUDED.sum, " \
                      f"tip_sum = EXCLUDED.tip_sum;"
                cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()


#response_dict_deliveries = deliveries_loader_from_src()
#deliveries_loader_to_stg(response_dict_deliveries)

