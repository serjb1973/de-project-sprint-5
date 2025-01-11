CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL DEFAULT 'infinity'::timestamp without time zone,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);
