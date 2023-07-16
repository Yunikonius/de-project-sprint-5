import psycopg2

def couriers_stg_to_dds_loader():
    conn = psycopg2.connect(
        host="localhost",
        port="15432",
        database="de",
        user="jovyan",
        password="jovyan"
    )
    cursor = conn.cursor()

    sql = f"insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to) " \
    f"select " \
    f"src_id as restaurant_id, " \
    f"name as restaurant_name, " \
    f"current_timestamp as active_from, " \
    f"timestamp '2099-12-31 00:00:00.000' as active_to " \
    f"from stg.courier_system_restaurants " \
    f"on conflict (restaurant_id) do update set " \
    f"restaurant_name = excluded.restaurant_name, " \
    f"active_from = current_timestamp " \
    f"where dm_restaurants.restaurant_name <> excluded.restaurant_name;"

    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()