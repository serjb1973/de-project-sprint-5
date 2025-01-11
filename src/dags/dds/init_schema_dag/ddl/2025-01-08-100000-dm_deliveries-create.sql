--DROP TABLE IF EXISTS dds.dm_deliveries;
CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
	id 					serial PRIMARY KEY
	,courier_id			integer NOT NULL
	,timestamp_id		integer NOT NULL
	,delivery_key		varchar NOT NULL
	,CONSTRAINT dm_deliveries_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers
	,CONSTRAINT dm_deliveries_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps
	,UNIQUE (delivery_key)
	);
