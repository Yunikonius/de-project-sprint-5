import psycopg2

def mart_data_insert():
    conn = psycopg2.connect(
        host="localhost",
        port="15432",
        database="de",
        user="jovyan",
        password="jovyan"
    )
    cursor = conn.cursor()

    sql = "insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, " \
    f"rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum) " \
    f"select " \
    f"dc.id as courier_id, " \
    f"dc.name as courier_name, " \
    f"dt.year as settlement_year, " \
    f"dt.month as settlement_month, " \
    f"count(dd.order_id) as orders_count, " \
    f"sum(dd.sum) as orders_total_sum, " \
    f"avg(dd.rate) as rate_avg, " \
    f"(sum(dd.sum) * 0.25) as order_processing_fee, " \
    f"cs.courier_order_sum, " \
    f"sum(dd.tip_sum) as courier_tips_sum, " \
    f"(cs.courier_order_sum + sum(dd.tip_sum)*0.95) as courier_reward_sum " \
    f"from dds.dm_couriers dc " \
    f"join dds.dm_deliveries dd " \
    f"on dc.src_id = dd.courier_id " \
    f"left join dds.dm_orders ddo " \
    f"on dd.order_id = ddo.order_key " \
    f"left join dds.dm_timestamps dt " \
    f"on ddo.timestamp_id = dt.id " \
    f"left join " \
    f"(select dd.courier_id, " \
    f"case    when avg(dd.rate) < 4.00 then greatest((sum(dd.sum) * 0.05), count(dd.order_id)*100) " \
		    f"when avg(dd.rate) >= 4.00 and avg(dd.rate) < 4.5 then greatest((sum(dd.sum) * 0.07), count(dd.order_id)*150) " \
		    f"when avg(dd.rate) >= 4.5 and avg(dd.rate) < 4.9 then greatest((sum(dd.sum) * 0.08), count(dd.order_id)*175) " \
		    f"when avg(dd.rate) >= 4.9 then greatest((sum(dd.sum) * 0.1), count(dd.order_id)*200) " \
    f"end as courier_order_sum " \
    f"from dds.dm_deliveries dd " \
    f"group by dd.courier_id) cs " \
    f"on dc.src_id = cs.courier_id " \
    f"group by dc.id, dc.name, dt.year, dt.month, cs.courier_order_sum " \
    f"on conflict (courier_id, settlement_year, settlement_month) do update set " \ 
	    f"courier_name = excluded.courier_name, " \
	    f"orders_count = excluded.orders_count, " \
	    f"orders_total_sum = excluded.orders_total_sum, " \
	    f"rate_avg = excluded.rate_avg, " \
	    f"order_processing_fee = excluded.order_processing_fee, " \
	    f"courier_order_sum = excluded.courier_order_sum, " \
	    f"courier_tips_sum = excluded.courier_tips_sum, " \
	    f"courier_reward_sum = excluded.courier_reward_sum;"

    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()