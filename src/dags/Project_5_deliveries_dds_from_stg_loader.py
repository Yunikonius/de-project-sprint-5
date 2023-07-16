import psycopg2

def deliveries_stg_to_dds_loader():
    conn = psycopg2.connect(
        host="localhost",
        port="15432",
        database="de",
        user="jovyan",
        password="jovyan"
    )
    cursor = conn.cursor()

    sql_timestamps = f"insert into dds.dm_timestamps(ts, year, month, day, time, date) " \
          f"select distinct " \
          f"delivery_ts as ts, " \
          f"extract (year from delivery_ts) as year, " \
          f"extract (month from delivery_ts) as month, " \
          f"extract (day from delivery_ts) as day, " \
          f"cast (delivery_ts as time) as time, " \
          f"cast (delivery_ts as date) as date " \
          f"from stg.courier_system_deliveries " \
          f"on conflict (ts) do nothing;"

    sql_deliveries = f"insert into dds.dm_deliveries (id, order_id, src_id, courier_id, address, timestamp_id, sum, tip_sum, rate) " \
          f"select t1.id, t1.order_id, t1.delivery_id as src_id, t1.courier_id, t1.address, t4.id as timestamp_id, t1.sum, t1.tip_sum, t1.rate " \
          f"from stg.courier_system_deliveries t1 " \
          f"join dds.dm_orders t2 " \
          f"on t1.order_id = t2.order_key " \
          f"join dds.dm_couriers t3 " \
          f"on t1.courier_id = t3.src_id " \
          f"join dds.dm_timestamps t4 " \
          f"on t1.delivery_ts = t4.ts " \
          f"on conflict (src_id) do update set " \
          f"id = EXCLUDED.id, " \
          f"order_id = EXCLUDED.order_id, " \
          f"courier_id = EXCLUDED.courier_id, " \
          f"address = EXCLUDED.address, " \
          f"timestamp_id = EXCLUDED.timestamp_id, " \
          f"sum = EXCLUDED.sum, " \
          f"tip_sum = EXCLUDED.tip_sum, " \
          f"rate = EXCLUDED.rate;"

    cursor.execute(sql_timestamps)
    cursor.execute(sql_deliveries)

    conn.commit()
    cursor.close()
    conn.close()