--DROP TABLE IF EXISTS stg.deliverysystem_couriers;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
	id				serial PRIMARY key
	, object_id		varchar(100) NOT NULL UNIQUE
	, object_value	TEXT NOT NULL
	);
