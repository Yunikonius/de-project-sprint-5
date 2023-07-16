import psycopg2

def non_existing_tables_creation():
    conn = psycopg2.connect(
        host="localhost",
        port="15432",
        database="de",
        user="jovyan",
        password="jovyan"
    )
    cursor = conn.cursor()

    sql_stg_restaurans = f"CREATE TABLE if not exists stg.courier_system_restaurants ( " \
        f"id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE " \
        f"2147483647 START 1 CACHE 1 NO CYCLE), " \
        f"src_id varchar (255) NOT null unique, " \
        f"name varchar (255) not null, " \
        f"CONSTRAINT courier_system_restaurants_pkey PRIMARY KEY (id) );"

    sql_stg_couriers = f"CREATE TABLE if not exists stg.courier_system_couriers ( " \
        f"id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE " \
        f"2147483647 START 1 CACHE 1 NO CYCLE), " \
        f"src_id varchar (255) NOT null unique, " \
        f"name varchar (255) not null, " \
        f"CONSTRAINT courier_system_couriers_pkey PRIMARY KEY (id);"

    sql_stg_deliveries = f"create table if not exists stg.courier_system_deliveries ( " \
	    f"id int4 not null generated always as identity (increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1 no cycle), " \
	    f"order_id varchar (255) not null, " \
	    f"order_ts timestamp not null, " \
	    f"delivery_id varchar (255) not null unique, " \
	    f"courier_id varchar (255) not null, " \
	    f"address varchar (255) not null, " \
	    f"delivery_ts timestamp not null, " \
	    f"rate int4 not null, " \
	    f"sum numeric (14,2) not null, " \
	    f"tip_sum numeric (14,2) not null);"

    sql_dds_restaurants = f"CREATE TABLE dds.dm_restaurants ( " \
	    f"id serial4 NOT NULL, " \
	    f"restaurant_id varchar NOT NULL, " \
	    f"restaurant_name varchar NOT NULL, " \
	    f"active_from timestamp NOT NULL, " \
	    f"active_to timestamp NOT NULL, " \
	    f"CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id) ); " \
        f"CREATE INDEX idx_dm_restaurants__restaurant_id_active_from ON dds.dm_restaurants USING btree (restaurant_id, active_from);"

    sql_dds_couriers = f"CREATE TABLE dds.dm_couriers ( " \
	    f"id int4 NOT NULL, " \
	    f"src_id varchar(255) NOT NULL, " \
	    f"name varchar(255) NOT NULL, " \
	    f"CONSTRAINT dm_couriers_pkey PRIMARY KEY (id), " \
	    f"CONSTRAINT dm_couriers_unique_src_id UNIQUE (src_id), " \
	    f"CONSTRAINT unique_couriers_id_srcid UNIQUE (id, src_id) );"

    sql_dds_deliveries = f"CREATE TABLE if not exists dds.dm_deliveries ( " \
	    f"id serial4 NOT null primary key, " \
	    f"order_id varchar(255) NOT NULL, " \
	    f"src_id varchar(255) NOT null unique, " \
	    f"courier_id varchar(255) NOT NULL, " \
	    f"address varchar(255) NOT NULL, " \
	    f"timestamp_id int4 NOT NULL, " \
	    f"sum numeric(14,2) NOT NULL, " \
	    f"tip_sum numeric(14,2) not null, " \ 
		f"rate int2 default 0, " \ 
	    f"constraint dm_deliveries_order_fk foreign key (order_id) references dds.dm_orders (order_key), " \
	    f"constraint dm_deliveries_courier_fk foreign key (courier_id) references dds.dm_couriers (src_id), " \
	    f"constraint dm_deliveries_timestamp_fk foreign key (timestamp_id) references dds.dm_timestamps (id), " \
	    f"constraint dm_deliveries_sum_check check (sum >= 0.00), " \
	    f"constraint dm_deliveries_tip_sum_check check (tip_sum >= 0.00)); " \
        f"create index idx_dm_deliveries_id_srcid on dds.dm_deliveries using btree (id, src_id);"

    sql_cdm_courier_ledger = f"create table cdm.dm_courier_ledger ( " \
	    f"id serial4 primary key, " \
	    f"courier_id int4 unique not null, " \
	    f"courier_name varchar(255), " \
	    f"settlement_year int2 not null, " \
	    f"settlement_month int2 not null, " \
	    f"orders_count int4 not null, " \
	    f"orders_total_sum numeric(14,2) not null, " \
	    f"rate_avg numeric(14,2) not null, " \
	    f"order_processing_fee numeric(14,2) not null, " \
	    f"courier_order_sum numeric(14,2) not null, " \
	    f"courier_tips_sum numeric(14,2) not null, " \
	    f"courier_reward_sum numeric(14,2) not null, " \
	    f"constraint cdm_dm_courier_ledger_courier_fk foreign key (courier_id) references dds.dm_couriers (id), " \
	    f"constraint cdm_dm_courier_ledger_orders_count_check check (orders_count >= 0), " \
	    f"constraint cdm_dm_courier_ledger_orders_total_sum_check check (orders_total_sum >= 0.00), " \
	    f"constraint cdm_dm_courier_ledger_rate_avg_check check (rate_avg >= 0.00), " \
	    f"constraint cdm_dm_courier_ledger_order_processing_fee_check check (order_processing_fee >= 0.00), " \
	    f"constraint cdm_dm_courier_ledger_courier_order_sum_check check (courier_order_sum >= 0.00), " \
	    f"constraint cdm_dm_courier_ledger_courier_tips_sum_check check (courier_tips_sum >= 0.00), " \
	    f"constraint cdm_dm_courier_ledger_courier_reward_sum_check check (courier_reward_sum >= 0.00));"


    cursor.execute(sql_stg_restaurans)
    cursor.execute(sql_stg_couriers)
    cursor.execute(sql_stg_deliveries)
    cursor.execute(sql_dds_restaurants)
    cursor.execute(sql_dds_couriers)
    cursor.execute(sql_dds_deliveries)
    cursor.execute(sql_cdm_courier_ledger)

    conn.commit()
    cursor.close()
    conn.close()

