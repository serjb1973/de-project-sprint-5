CREATE TABLE IF NOT EXISTS dds.fct_order_deliveries(
	id 					serial PRIMARY KEY
	,delivery_id		integer NOT NULL
	,order_id			integer NOT NULL
	,tip_sum			numeric(14,2) NOT NULL DEFAULT 0 check(tip_sum>=0)
	,rate				numeric(4,2) NOT NULL DEFAULT 0
	,CONSTRAINT fct_order_deliveries_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders
	,CONSTRAINT fct_order_deliveries_delivery_id_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries
	,UNIQUE (delivery_id,order_id)
	);
