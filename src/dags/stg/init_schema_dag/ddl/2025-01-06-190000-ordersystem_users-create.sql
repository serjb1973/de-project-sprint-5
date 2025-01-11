CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id				serial PRIMARY key
	, object_id		varchar(100) NOT NULL UNIQUE
	, object_value	TEXT NOT NULL
	, update_ts		timestamp NOT NULL
	);
	