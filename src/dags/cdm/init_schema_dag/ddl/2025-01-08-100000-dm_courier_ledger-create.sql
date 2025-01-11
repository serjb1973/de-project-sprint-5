--drop TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
	(
		id							serial NOT NULL PRIMARY KEY
		, courier_id				varchar(100) NOT NULL
		, courier_name				varchar(100) NOT NULL
		, settlement_year			integer NOT NULL check (settlement_year>=2022 AND settlement_year<=2500)
		, settlement_month			integer NOT NULL check (settlement_month>=1 AND settlement_month<=12)
		, orders_count				integer NOT NULL DEFAULT 0 check (orders_count>=0)
		, orders_total_sum			numeric(14,2) NOT NULL DEFAULT 0 check (orders_total_sum>=0)
		, rate_avg					numeric(4,2) NOT NULL DEFAULT 0
		, order_processing_fee		numeric(14,2) NOT NULL DEFAULT 0 check (order_processing_fee>=0)
		, courier_order_sum			numeric(14,2) NOT NULL DEFAULT 0 check (courier_order_sum>=0)
		, courier_tips_sum			numeric(14,2) NOT NULL DEFAULT 0
		, courier_reward_sum		numeric(14,2) NOT NULL DEFAULT 0 check (courier_reward_sum>=0)
		, UNIQUE (courier_id,settlement_year,settlement_month)
	);
