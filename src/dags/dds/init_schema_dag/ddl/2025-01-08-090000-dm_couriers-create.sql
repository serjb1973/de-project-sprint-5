--DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	id 					serial PRIMARY KEY
	,courier_id			varchar NOT NULL
	,courier_name		varchar NOT NULL
	,active_from		timestamp NOT NULL
	,active_to			timestamp NOT NULL default 'infinity'::timestamp
	);
