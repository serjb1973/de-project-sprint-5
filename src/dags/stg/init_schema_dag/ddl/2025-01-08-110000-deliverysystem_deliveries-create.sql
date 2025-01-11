--DROP TABLE IF EXISTS stg.ordersystem_orders;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
	id				serial PRIMARY key
	, object_id		varchar(100) NOT NULL
	, object_value	TEXT NOT NULL
	, update_ts		timestamp NOT NULL
	,CONSTRAINT deliverysystem_deliveries_object_id_key UNIQUE (object_id,update_ts)
	);
