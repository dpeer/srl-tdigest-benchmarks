CREATE TABLE transaction_dimensions_tdigest (
	load_test_run_id text,
	script_id text,
	transaction_name text,
	geo_location text,
	emulation text,
	ctrl_cre_date bigint DEFAULT (date_part('epoch'::text, now()) * (1000)::double precision),
	id uuid
);


CREATE TABLE tdigest_transaction_metrics (
	start_time bigint,
	end_time bigint,
	script_id text,
	transaction_name text,
	grouped boolean,
	tdigest_buf bytea
);

--CREATE INDEX tdigest_transaction_metrics_trans_index on tdigest_transaction_metrics (script_id,transaction_name);

--DROP INDEX tdigest_transaction_metrics_trans_index;