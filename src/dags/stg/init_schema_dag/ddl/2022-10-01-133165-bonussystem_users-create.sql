CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);
