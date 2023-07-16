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

    sql = f"insert into dds.dm_couriers (id, src_id, name) " \
          f"(select id, src_id, name from stg.courier_system_couriers) " \
          f"on conflict (id, src_id) do update set " \
          f"name = excluded.name " \
          f"where dm_couriers.name <> excluded.name;"

    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()