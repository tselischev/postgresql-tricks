-- sys_dwh.bdv_tbl_lst определение

-- Drop table

-- DROP TABLE sys_dwh.bdv_tbl_lst;

CREATE TABLE sys_dwh.bdv_tbl_lst (
	tbl_nm text NOT NULL,
	bdv_obj text NULL
)
DISTRIBUTED RANDOMLY;


-- sys_dwh.br_grnt определение

-- Drop table

-- DROP TABLE sys_dwh.br_grnt;

CREATE TABLE sys_dwh.br_grnt (
	rl_nm text NULL,
	scm_nm text NULL,
	tbl_nm text NULL,
	obj_tp text NULL,
	grnt text NULL,
	rvk text NULL,
	last_grant_dttm timestamp NULL,
	last_revoke_dttm timestamp NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED BY (rl_nm);


-- sys_dwh.collision_hist определение

-- Drop table

-- DROP TABLE sys_dwh.collision_hist;

CREATE TABLE sys_dwh.collision_hist (
	bk text NOT NULL,
	gk uuid NOT NULL,
	src_stm_id int4 NOT NULL,
	upd_dttm timestamp NOT NULL,
	new_bk text NOT NULL,
	new_gk uuid NOT NULL,
	s2t_col_nm text NOT NULL,
	hub_nm text NOT NULL,
	hub_src_stm_id int4 NOT NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.current_scn_hist определение

-- Drop table

-- DROP TABLE sys_dwh.current_scn_hist;

CREATE TABLE sys_dwh.current_scn_hist (
	current_scn numeric NULL,
	data_source text NULL,
	upload_dttm timestamp NULL,
	controlfile_time timestamp NULL,
	startup_time timestamp NULL
)
DISTRIBUTED BY (current_scn);


-- sys_dwh.dwh_err_ste определение

-- Drop table

-- DROP TABLE sys_dwh.dwh_err_ste;

CREATE TABLE sys_dwh.dwh_err_ste (
	log_id int8 NULL,
	cmnt text NULL,
	st text NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.log_dwh определение

-- Drop table

-- DROP TABLE sys_dwh.log_dwh;

CREATE TABLE sys_dwh.log_dwh (
	log_id bigserial NOT NULL,
	job_id text NULL,
	job_nm text NULL,
	job_prm text NULL,
	step_id text NULL,
	step_nm text NULL,
	step_prm text NULL,
	descr text NULL,
	start_dttm timestamp NULL,
	end_dttm timestamp NULL,
	ins_qty int8 NULL,
	upd_qty int8 NULL,
	del_qty int8 NULL,
	rej_qty int8 NULL,
	err text NULL,
	log_tp int2 NULL,
	debug_lvl int2 NULL,
	ret_json json NULL,
	env_nm varchar(20) NULL,
	env_runtime text DEFAULT 'airflow'::text NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=5
)
DISTRIBUTED BY (log_id);


-- sys_dwh.log_size_db определение

-- Drop table

-- DROP TABLE sys_dwh.log_size_db;

CREATE TABLE sys_dwh.log_size_db (
	database_name varchar(100) NULL,
	database_size int8 NULL,
	database_pretty_size text NULL,
	act_dt date NULL,
	upd_dttm timestamp NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.log_size_obj определение

-- Drop table

-- DROP TABLE sys_dwh.log_size_obj;

CREATE TABLE sys_dwh.log_size_obj (
	obj_schema varchar(100) NULL,
	obj_name varchar(100) NULL,
	obj_size int8 NULL,
	obj_pretty_size text NULL,
	act_dt date NULL,
	upd_dttm timestamp NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.log_size_schema определение

-- Drop table

-- DROP TABLE sys_dwh.log_size_schema;

CREATE TABLE sys_dwh.log_size_schema (
	schema_name varchar(100) NULL,
	schema_size int8 NULL,
	schema_pretty_size text NULL,
	act_dt date NULL,
	upd_dttm timestamp NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.mv_autorefresh определение

-- Drop table

-- DROP TABLE sys_dwh.mv_autorefresh;

CREATE TABLE sys_dwh.mv_autorefresh (
	mv_name text NOT NULL,
	is_refresh bool DEFAULT true NULL
)
DISTRIBUTED BY (mv_name);


-- sys_dwh.prm_gen_gk_bdv определение

-- Drop table

-- DROP TABLE sys_dwh.prm_gen_gk_bdv;

CREATE TABLE sys_dwh.prm_gen_gk_bdv (
	tbl_nm text NULL,
	json_param text NULL,
	eff_dt date NULL,
	end_dt date NULL,
	upd_dttm timestamp NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED BY (tbl_nm);


-- sys_dwh.prm_load_bdv определение

-- Drop table

-- DROP TABLE sys_dwh.prm_load_bdv;

CREATE TABLE sys_dwh.prm_load_bdv (
	tbl_name text NULL,
	dag_id text NULL,
	strt_dt date NULL,
	stop_dt date NULL,
	depth_load int2 NULL,
	upd_dttm timestamp NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_load_cntr определение

-- Drop table

-- DROP TABLE sys_dwh.prm_load_cntr;

CREATE TABLE sys_dwh.prm_load_cntr (
	src_stm_id int4 NOT NULL,
	last_ssn int8 NULL,
	last_act_dt date NULL,
	updated_dttm timestamp NULL,
	step_id int4 NOT NULL,
	CONSTRAINT prm_load_cntr_pkey PRIMARY KEY (src_stm_id, step_id)
)
WITH (
	appendonly=false
)
DISTRIBUTED BY (src_stm_id, step_id);


-- sys_dwh.prm_s2t_bdv_chain определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_chain;

CREATE TABLE sys_dwh.prm_s2t_bdv_chain (
	scnr_name text NULL,
	step_id int8 NULL,
	next_step_id int8 NULL,
	chain_is_active int8 NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_bdv_obj определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_obj;

CREATE TABLE sys_dwh.prm_s2t_bdv_obj (
	object_name text NULL,
	object_descr text NULL,
	object_is_active int8 NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_bdv_scnr определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_scnr;

CREATE TABLE sys_dwh.prm_s2t_bdv_scnr (
	scnr_name text NULL,
	scnr_descr text NULL,
	scnr_is_active int8 NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_bdv_scnr_line определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_scnr_line;

CREATE TABLE sys_dwh.prm_s2t_bdv_scnr_line (
	scnr_line_name text NULL,
	scnr_line_descr text NULL,
	scnr_line_owner text NULL,
	scnr_line_is_active int8 NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_bdv_scnr_line_scnr определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_scnr_line_scnr;

CREATE TABLE sys_dwh.prm_s2t_bdv_scnr_line_scnr (
	scnr_line_name text NULL,
	scnr_name text NULL,
	scnr_start_dt date NULL,
	scnr_stop_dt date NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_bdv_step определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_bdv_step;

CREATE TABLE sys_dwh.prm_s2t_bdv_step (
	src_nm varchar(50) NULL,
	table_name_bdv varchar(50) NULL,
	step_order_in_object int8 NULL,
	step_id int8 NULL,
	step_name text NULL,
	step_algorithm text NULL,
	object_name text NULL,
	step_is_active int8 NULL,
	schema_tgt varchar(50) NULL,
	table_name_tgt varchar(50) NULL,
	table_comment_tgt text NULL,
	column_name_tgt varchar(50) NULL,
	column_comment_tgt text NULL,
	datatype_tgt varchar(50) NULL,
	distribution_by_tgt int8 NULL,
	schema_src varchar(50) NULL,
	column_algorithm_src text NULL,
	main_table_src text NULL,
	table_algorithm_src text NULL,
	where_src text NULL,
	eff_dt date NULL,
	end_dt date NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_rdv определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_rdv;

CREATE TABLE sys_dwh.prm_s2t_rdv (
	schema_stg varchar(20) NULL,
	table_name_stg varchar(50) NULL,
	table_comment_stg varchar(255) NULL,
	column_name_stg varchar(50) NULL,
	column_comment_stg varchar(4000) NULL,
	datatype_stg varchar(50) NULL,
	algorithm varchar(4000) NULL,
	schema_rdv varchar(50) NULL,
	table_name_rdv varchar(50) NULL,
	column_name_rdv varchar(50) NULL,
	column_comment_rdv varchar(4000) NULL,
	datatype_rdv varchar(50) NULL,
	key_type_src varchar(20) NULL,
	ref_to_hub text NULL,
	ref_to_stg text NULL,
	src_stm_id int4 NULL,
	eff_dt date NULL,
	end_dt date NULL,
	datatype_stg_transform text NULL,
	upd_user text NULL,
	table_comment_rdv text NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_rdv_rule определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_rdv_rule;

CREATE TABLE sys_dwh.prm_s2t_rdv_rule (
	schema_stg varchar(20) NULL,
	table_name_stg varchar(50) NULL,
	schema_rdv varchar(50) NULL,
	table_name_rdv varchar(50) NULL,
	where_json text NULL,
	src_stm_id int4 NULL,
	eff_dt date NULL,
	end_dt date NULL,
	is_distinct int4 DEFAULT 0 NULL,
	upd_user text NULL,
	stm_ext_upload text NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_stg определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_stg;

CREATE TABLE sys_dwh.prm_s2t_stg (
	schema_src varchar(50) NULL,
	table_name_src varchar(50) NULL,
	table_comment_src varchar(255) NULL,
	column_name_src varchar(50) NULL,
	column_comment_src varchar(4000) NULL,
	datatype_src varchar(50) NULL,
	key_type_src varchar(20) NULL,
	connection_name_src varchar(50) NULL,
	schema_stg varchar(50) NULL,
	table_name_stg varchar(50) NULL,
	column_name_stg varchar(50) NULL,
	column_comment_stg varchar(4000) NULL,
	ref_to_stg text NULL,
	datatype_stg varchar(50) NULL,
	src_stm_id int4 NULL,
	eff_dt date NULL,
	end_dt date NULL,
	column_order_src int8 NULL,
	upd_user text NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_s2t_stg_src определение

-- Drop table

-- DROP TABLE sys_dwh.prm_s2t_stg_src;

CREATE TABLE sys_dwh.prm_s2t_stg_src (
	src_stm_id int4 NULL,
	schema_src varchar(50) NULL,
	table_name_src varchar(50) NULL,
	schema_stg varchar(50) NULL,
	table_name_stg varchar(50) NULL,
	pxf_name_src varchar(50) NULL,
	ext_pb varchar(50) NULL,
	ext_pb_itrv text NULL,
	act_dt_column varchar(50) NULL,
	pk_src text NULL,
	eff_dt date NULL,
	end_dt date NULL,
	ext_format text NULL,
	ext_encoding text NULL,
	column_fltr text NULL,
	column_fltr_from text NULL,
	column_fltr_to text NULL,
	ext_prd_pb text NULL,
	ext_prd_pb_itrv text NULL,
	ext_prd_pb_from text NULL,
	upd_user text NULL,
	cdc_tp text NULL,
	where_fltr_src text DEFAULT ''::text NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_src_stm определение

-- Drop table

-- DROP TABLE sys_dwh.prm_src_stm;

CREATE TABLE sys_dwh.prm_src_stm (
	src_stm_id int4 NOT NULL,
	prn_src_stm_id int4 NULL,
	nm text NULL,
	load_mode int4 DEFAULT 1 NOT NULL,
	eff_dt date NOT NULL,
	end_dt date NOT NULL,
	upd_dttm timestamp NULL,
	day_del int2 NULL,
	pxf_string varchar(255) NULL,
	ext_pb varchar(50) NULL,
	ext_pb_itrv text NULL,
	act_dt_column varchar(50) NULL,
	ext_location text NULL,
	ext_format text NULL,
	ext_encoding text NULL,
	load_mode_trgt int4 NOT NULL,
	flg_empty_rdv int2 DEFAULT 1 NULL,
	src_buf text NULL,
	load_mode_reload_cdc int4 NULL,
	src_jetlag int4 DEFAULT '-5'::integer NULL,
	dml_dttm_shift int4 DEFAULT 72 NULL,
	CONSTRAINT prm_src_stm_pk PRIMARY KEY (src_stm_id),
	CONSTRAINT prm_src_stm_un UNIQUE (nm, prn_src_stm_id)
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.prm_src_stm_speedtest определение

-- Drop table

-- DROP TABLE sys_dwh.prm_src_stm_speedtest;

CREATE TABLE sys_dwh.prm_src_stm_speedtest (
	src_stm_id int4 NOT NULL,
	prn_src_stm_id int4 NULL,
	nm text NULL,
	load_mode int4 DEFAULT 1 NOT NULL,
	eff_dt date NOT NULL,
	end_dt date NOT NULL,
	upd_dttm timestamp NULL,
	day_del int2 NULL,
	pxf_string varchar(255) NULL,
	ext_pb varchar(50) NULL,
	ext_pb_itrv text NULL,
	act_dt_column varchar(50) NULL,
	ext_location text NULL,
	ext_format text NULL,
	ext_encoding text NULL,
	load_mode_trgt int4 NOT NULL,
	flg_empty_rdv int2 DEFAULT 1 NULL,
	src_buf text NULL,
	load_mode_reload_cdc int4 NULL,
	src_jetlag int4 DEFAULT '-5'::integer NULL,
	dml_dttm_shift int4 DEFAULT 72 NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=5
)
DISTRIBUTED BY (src_stm_id);


-- sys_dwh.prm_step определение

-- Drop table

-- DROP TABLE sys_dwh.prm_step;

CREATE TABLE sys_dwh.prm_step (
	step_id int4 NOT NULL,
	step_name text NULL,
	step_nm text NULL,
	step_dscr text NULL,
	step_param text NULL,
	eff_dt date DEFAULT '1900-01-01'::date NOT NULL,
	end_dt date DEFAULT '9999-12-31'::date NOT NULL,
	upd_dttm timestamp DEFAULT now() NOT NULL,
	CONSTRAINT prm_step_pk PRIMARY KEY (step_id)
)
WITH (
	appendonly=false
)
DISTRIBUTED BY (step_id);


-- sys_dwh.prm_task определение

-- Drop table

-- DROP TABLE sys_dwh.prm_task;

CREATE TABLE sys_dwh.prm_task (
	id_task uuid NULL,
	dt date NULL,
	table_name text NULL,
	owner_rdv_table text NULL,
	column_name text NULL,
	backup_table_name text NULL,
	type_of_task text NULL,
	type_of_changes text NULL,
	success_fl bool DEFAULT false NULL,
	dttm_start timestamp NULL,
	dttm_end timestamp NULL,
	active_fl bool NULL
)
DISTRIBUTED REPLICATED;


-- sys_dwh.reg_dwh определение

-- Drop table

-- DROP TABLE sys_dwh.reg_dwh;

CREATE TABLE sys_dwh.reg_dwh (
	id bigserial NOT NULL,
	json_tp varchar(20) NOT NULL,
	json_value json NULL,
	upd_dttm timestamp DEFAULT now() NOT NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED BY (id);


-- sys_dwh.reg_dwh_back определение

-- Drop table

-- DROP TABLE sys_dwh.reg_dwh_back;

CREATE TABLE sys_dwh.reg_dwh_back (
	id int8 NULL,
	json_tp varchar(20) NULL,
	json_value json NULL,
	upd_dttm timestamp NULL
)
DISTRIBUTED RANDOMLY;


-- sys_dwh.reg_hub_sql определение

-- Drop table

-- DROP TABLE sys_dwh.reg_hub_sql;

CREATE TABLE sys_dwh.reg_hub_sql (
	reg_hub_sql_id serial4 NOT NULL,
	src_stm_id int4 NULL,
	hub_nm text NULL,
	tbl_nm_sdv text NULL,
	column_name_stg text NULL,
	src_stm_id_ref int4 NULL,
	set_sql text NULL,
	upd_dttm timestamp DEFAULT now() NOT NULL
)
WITH (
	appendonly=false
)
DISTRIBUTED REPLICATED;


-- sys_dwh.src_upload определение

-- Drop table

-- DROP TABLE sys_dwh.src_upload;

CREATE TABLE sys_dwh.src_upload (
	src_table text NULL,
	trgt_table text NULL,
	field_list text NULL,
	time_upload text NULL,
	column_fltr text NULL,
	column_fltr_from text NULL,
	last_upload_dttm timestamp NULL,
	sql_before text NULL,
	sql_after_on_success text NULL,
	sql_after_on_error text NULL,
	sql_before_conn text NULL,
	sql_after_conn text NULL,
	dag_id text NULL,
	upload_group text NULL
)
DISTRIBUTED BY (src_table);