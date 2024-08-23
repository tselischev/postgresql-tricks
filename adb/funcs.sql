-- DROP FUNCTION sys_dwh.bdv_add_prttn(text, json, bool);

CREATE OR REPLACE FUNCTION sys_dwh.bdv_add_prttn(p_table_name text, p_json json, p_debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_part_info record;
v_partitions record;
v_partitions_level_0 record;
v_partitionschemaname text;
v_partitiontablename text;
v_output_text text :='';
v_schema text;
v_table text;
v_dateStart text;
v_dateEnd text;
v_endPartition text;
v_cnt int8 := 0;
v_range_start text;
v_range_end text;
v_text_col text;
v_text_test text;
v_error_text text := '';
v_start_dttm text;

 begin
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select split_part(p_table_name,'.', 1) into v_schema;
	select split_part(p_table_name,'.', 2) into v_table;
	drop table if exists tmp_start_exist;
	create temporary table tmp_start_exist(start_col text);
	drop table if exists tmp_start_end;
	create temporary table tmp_start_end(text_col text);
	for v_part_info in (select * from json_populate_recordset(null::record,	p_json::json) as (partitionlevel text, partitionform text, columnname text, partitions text)) loop
		if v_part_info.partitionlevel = '1' then
			v_output_text = '';
			for v_partitions_level_0 in (select text_col from tmp_start_end)
				loop
					v_text_col = v_partitions_level_0.text_col || ' (subpartition ' || v_table ||'_subprttn ';
					for v_partitions in (select * from json_populate_recordset(null::record,	v_part_info.partitions::json) as (start_prttn text, end_prttn text, every_prttn text, with_prttn text)) loop
						begin
							v_dateStart := v_partitions.start_prttn;
							v_dateEnd := v_partitions.end_prttn;
							v_text_col := v_text_col || ' START(' || v_dateStart || ') inclusive END (' || v_dateEnd || ')';
							if v_partitions.every_prttn <> '' then 
								v_text_col := v_text_col || ' EVERY (' || v_partitions.every_prttn || ')';
							end if;
							if v_partitions.with_prttn <> '' then 
								v_text_col := v_text_col || ' WITH (' || v_partitions.with_prttn || ') ,';
							else
								v_text_col := v_text_col || ' ,';
							end if;	
						end;
					 end loop;	
			v_text_col := RTRIM(v_text_col,',')||' ';
			v_output_text := v_output_text || v_text_col ||');
';				
			end loop;
		end if;
			for v_partitions in (select * from json_populate_recordset(null::record,	v_part_info.partitions::json) as (start_prttn text, end_prttn text, every_prttn text, with_prttn text)) loop
			begin
				v_dateStart := v_partitions.start_prttn;
				v_dateEnd := v_partitions.end_prttn;
			--select partitionschemaname, partitiontablename into v_partitionschemaname, v_partitiontablename from pg_catalog.pg_partitions pp where pp.schemaname = v_schema and pp.tablename = v_table and pp.partitionrangestart = v_partitions.start and pp.partitionrangeend = v_partitions.end1;
			WHILE v_dateStart < v_dateEnd
			loop
				raise notice '%', v_dateStart;
				raise notice '%', v_dateEnd;
				if v_part_info.partitionlevel = '1' then
					v_dateEnd := replace(v_dateEnd, '::date','')::date;
					if v_partitions.every_prttn <> '' then 
						v_endPartition = (replace(v_dateStart, '::date','')::date + replace(v_partitions.every_prttn, '::interval','')::interval)::date; 
						v_dateStart = replace(v_dateStart, '::date','')::date;
					else 
						v_endPartition = replace(v_dateEnd, '::date','')::date;
						v_dateStart = replace(v_dateStart, '::date','')::date;
					end if;
					for v_partitions_level_0 in (select start_col from  tmp_start_exist)
					loop
						select partitionschemaname, partitiontablename into v_partitionschemaname, v_partitiontablename from pg_catalog.pg_partitions where  schemaname = v_schema and tablename = v_table and partitionlevel = 0 and partitionrangestart = v_partitions_level_0.start_col;
						v_range_start:= ''''||v_dateStart::text || '''::date';
						v_range_end:= ''''||v_endPartition::text || '''::date';
						select count(1) from pg_catalog.pg_partitions where schemaname = v_schema and tablename = v_table and partitionlevel = 1 and partitionrangestart = v_range_start and partitionrangeend = v_range_end and parentpartitiontablename = v_partitiontablename
							into v_cnt;
						if v_cnt = 0 then		
							v_text_col = 'alter table ' || p_table_name || ' alter PARTITION for'||v_partitions_level_0||' add partition START(''' || v_dateStart || '''::date) inclusive END(''' ||  v_endPartition || '''::date)';	
							if v_partitions.with_prttn <> '' then 
								v_text_col := v_text_col || ' WITH (' || v_partitions.with_prttn || ')';
							else
								v_text_col := v_text_col || ')';
							end if;
							v_output_text := v_output_text || v_text_col ||';
';		
						end if;
					end loop;
					if v_partitions.every_prttn <> '' then 
							v_dateStart = (replace(v_dateStart, '::date','')::date + replace(v_partitions.every_prttn, '::interval','')::interval)::date; 
						else 
							v_dateStart = v_dateEnd;
						end if;
				elsif v_part_info.partitionlevel = '0' then
					if v_partitions.every_prttn <> '' then 
					v_endPartition = v_dateStart + v_partitions.every_prttn; 
					else 
						v_endPartition = v_dateEnd;
					end if;
					select count(1) from pg_catalog.pg_partitions where schemaname = v_schema and tablename = v_table and partitionlevel = 0 and partitionrangestart = v_dateStart and partitionrangeend = v_endPartition
					into v_cnt;
					if v_cnt = 0 then
						v_text_col := 'alter table ' || p_table_name || ' add partition START(' || v_dateStart || ') inclusive END(' || v_endPartition || ')';
						if v_partitions.every_prttn <> '' then 
							v_text_col := v_text_col || ' EVERY (' || v_partitions.every_prttn || ')';
						end if;
						if v_partitions.with_prttn <> '' then 
							v_text_col := v_text_col || ' WITH (' || v_partitions.with_prttn || ')';
						end if;
						v_output_text := v_output_text || v_text_col ||' (subpartition ' || v_table || '_test START(''1900-01-01''::date) inclusive END (''1900-02-01''::date));
';				
						insert into tmp_start_end values (v_text_col);
					else
						insert into tmp_start_exist values (v_dateStart);
					end if;

					if v_partitions.every_prttn <> '' then 
						v_dateStart = v_dateStart + v_partitions.every_prttn; 
					else 
						v_dateStart = v_dateEnd;
					end if;
				else
					v_error_text := 'Такой partitionlevel отсутствует, возможно, не правильно передан json';
		    		raise exception '%', v_error_text;
				end if;
			end loop;
		end;
		end loop;
	end loop;
	if p_debug = true then
		return(v_output_text);
	else
		execute v_output_text;
		return('Success');
	end if;
end;
					

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.bdv_truncate_prttn(_text, bool);

CREATE OR REPLACE FUNCTION sys_dwh.bdv_truncate_prttn(prttns_list _text, p_debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_output_text text :='';
v_error_text text :='';
v_schema text;
v_table text;
v_cnt int8 := 0;
v_json_ret text := '';
v_start_dttm text;


 begin
	for prttn_name in 1..array_length(prttns_list,1) loop
		begin
			select split_part(prttns_list[prttn_name],'.', 1) into v_schema;
			select split_part(prttns_list[prttn_name],'.', 2) into v_table;
			select count(1) into v_cnt 
			from  pg_catalog.pg_tables  
		    where 1=1
		    and lower(tablename) = v_table
		    and lower(schemaname) = v_schema;
			if v_cnt = 0 then
		    v_error_text := 'The target table '||prttns_list[prttn_name]||' is missing on DB';
		    raise exception '%', v_error_text;
		    end if;

			v_output_text := v_output_text ||'truncate table ' || prttns_list[prttn_name] || ';
';

		end;
	end loop;
	if p_debug = true then
		return(v_output_text);
	else
		execute v_output_text;
		return('Success');

	end if;
end;
			

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.br_grnt_exec(text, int4);

CREATE OR REPLACE FUNCTION sys_dwh.br_grnt_exec(p_rl_nm text, p_mode int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_txt text;
v_return text;

begin

	if p_mode = 1 then 
	
		begin
			select
			string_agg(grnt, E'\n')
			into
				v_txt
			from
				sys_dwh.br_grnt
			where
				rl_nm = p_rl_nm;
			
			select
			string_agg(scm_nm||'.'||tbl_nm, ';')
			into
				v_return
			from
				sys_dwh.br_grnt
			where
				rl_nm = p_rl_nm;
		
		execute v_txt;
		update sys_dwh.br_grnt
		set last_grant_dttm = (now() at time zone 'utc')::timestamp;
		end;
	
	else 
		if p_mode = 2 then
	
			begin
				select
				string_agg(rvk, E'\n')
				into
					v_txt
				from
					sys_dwh.br_grnt
				where
					rl_nm = p_rl_nm;
				
				select
				string_agg(scm_nm||'.'||tbl_nm, ';')
				into
					v_return
				from
					sys_dwh.br_grnt
				where
					rl_nm = p_rl_nm;
			
			execute v_txt;
			update sys_dwh.br_grnt
			set last_revoke_dttm = (now() at time zone 'utc')::timestamp;
			end;
		end if;
	end if;

	return(v_return);
end;



$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.br_grnt_exec_multi(int4);

CREATE OR REPLACE FUNCTION sys_dwh.br_grnt_exec_multi(p_mode int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

p_rl_nm text :='';

v_json_ret text := '';

v_script_func text :='';

v_start_dttm text :='';

begin

	v_start_dttm := clock_timestamp() at time zone 'utc';

	for p_rl_nm in 

		(select rl_nm from sys_dwh.v_grant_br)

		loop			

			if (p_rl_nm <> '') then	

					select sys_dwh.br_grnt_exec(p_rl_nm,1) into v_script_func;

				

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,

					p_step := '1',

					p_descr := 'Выполнен скрипт грантования ',

					p_start_dttm := v_start_dttm,		

					p_val := 'Роль ' || p_rl_nm||', таблицы: '||v_script_func,

					p_log_tp := '1',

					p_debug_lvl := '3')

				into v_json_ret;

			end if;

end loop;

return(v_json_ret);

end;




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.br_grnt_fill();

CREATE OR REPLACE FUNCTION sys_dwh.br_grnt_fill()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

begin

truncate sys_dwh.br_grnt;

insert into sys_dwh.br_grnt

(select rolename,schemaname, tablename, objtype,

	'GRANT SELECT ON TABLE '||schemaname||'.'||tablename||' TO '||rolename||';' as grnt,

	'REVOKE SELECT ON TABLE '||schemaname||'.'||tablename||' FROM '||rolename||';' as rvk

from 

(select

	s[1] as rolename,

    n.nspname as schemaname,

    relname as tablename, 

    relkind as objtype, 

    coalesce(nullif(s[1], ''), 'public') as grantee, 

    regexp_split_to_table(s[2], '') as privs

from 

    pg_class c

    join pg_namespace n on n.oid = relnamespace

    join pg_roles r on r.oid = relowner,

    unnest(coalesce(relacl::text[], format('{%s=arwdDxt/%s}', rolname, rolname)::text[])) acl, 

    regexp_split_to_array(acl, '=|/') s

where 1=1

and n.nspname||'.'||relname not in (select partitionschemaname||'.'||partitiontablename from pg_catalog.pg_partitions)

and coalesce(nullif(s[1], ''), 'public') like 'br_%'

) s

where privs like '%r%'

order by rolename);

return('Success');

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.calc_size_db();

CREATE OR REPLACE FUNCTION sys_dwh.calc_size_db()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

v_cnt int8 := 0;

v_rec record;

begin

 --Вставляем данные в sys_dwh.log_size_db

 for v_rec in

 (select * from

  sys_service.v_log_size_db) loop

  insert into sys_dwh.log_size_db values(v_rec.database_name, v_rec.database_size, v_rec.database_pretty_size, v_rec.act_dt, v_rec.upd_dttm);

  v_cnt := v_cnt+1;

  end loop;

 --Собираем статистику

 analyze sys_dwh.log_size_db;

--Возвращаем лог

return('Success: Вставлено '||v_cnt||' новых строк');

--Регистрируем ошибки

exception when others then 

raise exception '%', SQLERRM;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.calc_size_obj();

CREATE OR REPLACE FUNCTION sys_dwh.calc_size_obj()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

v_cnt int8 := 0;

v_rec record;

begin

 --Вставляем данные в sys_dwh.log_size_obj

 for v_rec in

 (select * from

  sys_service.v_log_size_obj) loop

  insert into sys_dwh.log_size_obj values(v_rec.obj_schema, v_rec.obj_name, v_rec.obj_size, v_rec.obj_pretty_size, v_rec.act_dt, v_rec.upd_dttm);

  v_cnt := v_cnt+1;

  end loop;

 --Собираем статистику

 analyze sys_dwh.log_size_obj;

--Возвращаем лог

return('Success: Вставлено '||v_cnt||' новых строк');

--Регистрируем ошибки

exception when others then 

raise exception '%', SQLERRM;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.calc_size_schema();

CREATE OR REPLACE FUNCTION sys_dwh.calc_size_schema()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

v_cnt int8 := 0;

v_rec record;

begin

 --Вставляем данные в sys_dwh.log_size_schema

 for v_rec in

 (select * from

  sys_service.v_log_size_schema) loop

  insert into sys_dwh.log_size_schema values(v_rec.schema_name, v_rec.schema_size, v_rec.schema_pretty_size, v_rec.act_dt, v_rec.upd_dttm);

  v_cnt := v_cnt+1;

  end loop;

 --Собираем статистику

 analyze sys_dwh.log_size_schema;

--Возвращаем лог

return('Success: Вставлено '||v_cnt||' новых строк');

--Регистрируем ошибки

exception when others then 

raise exception '%', SQLERRM;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.check_lm_rdv();

CREATE OR REPLACE FUNCTION sys_dwh.check_lm_rdv()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

v_ddl text;

v_src_stm_id int8;

v_src_stm text := '';

v_cnt int2;

v_start_dttm text;

v_json_ret text := '';

begin

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

--Проверяем наличие данных в rdv-таблицах

for  v_ddl, v_src_stm_id in 

(select 'select count(1) from (select 1 from '||schema_rdv||'.'||table_name_rdv||' limit 1) as t;' as ddl, src_stm_id 

from sys_dwh.prm_s2t_rdv_rule r 

join information_schema.tables t 

on t.table_schema = r.schema_rdv 

and t.table_name = r.table_name_rdv) loop 

	execute v_ddl into v_cnt;

	if v_cnt = 0 then

	--Если rdv-таблица пустая, текущий тип загрузки load_mode принимает значение "1"

	update sys_dwh.prm_src_stm set load_mode = 1 where src_stm_id = v_src_stm_id;

	select (case when v_src_stm = '' then v_src_stm_id::text else v_src_stm||', '||v_src_stm_id::text end) into v_src_stm;

	raise notice '%', v_src_stm_id;

	end if;

end loop;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Обновляем lm для src_stm_id in ('||v_src_stm||')',

p_start_dttm := v_start_dttm,

p_val := v_ddl,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Обновляем lm',

p_val := v_ddl,

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Возвращаем результат

return(v_json_ret||']'); 

--Регистрируем ошибки

exception

when others then

if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

v_json_ret := v_json_ret||']';

end if;

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.check_s2t(text);

CREATE OR REPLACE FUNCTION sys_dwh.check_s2t(p_schema_stg text DEFAULT NULL::text)
	RETURNS TABLE (prv text, src_stm_id int4, column_name_stg varchar)
	LANGUAGE plpgsql
	IMMUTABLE
AS $$
	

#variable_conflict use_column

declare 

v_output text;

v_cnt int4;

v_sql text;

begin

--Проверяем русские символы в rdv-карте

select count(1) into v_cnt from

(select 

src_stm_id, 

column_name_stg, 

ref_to_stg

from sys_dwh.prm_s2t_rdv rdv 

where ref_to_stg ~* '[а-я]+'

and coalesce(p_schema_stg, schema_stg) = schema_stg

and now() at time zone 'utc' >= rdv.eff_dt and now() at time zone 'utc' <= rdv.end_dt) as t;

if v_cnt > 0 then

v_sql := 'select 

''Проверка на наличие русских символов в rdv-карте в поле ref_to_stg'' as prv,

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv 

where rdv.ref_to_stg ~* ''[а-я]+''

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt';

else 

v_sql := 'select 

''Проверка на наличие русских символов в rdv-карте в поле ref_to_stg'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

--Проверяем русские символы в stg-карте

select count(1) into v_cnt from

(select 

src_stm_id, 

column_name_stg, 

ref_to_stg

from sys_dwh.prm_s2t_stg stg 

where ref_to_stg ~* '[а-я]+'

and coalesce(p_schema_stg, schema_stg) = schema_stg

and now() at time zone 'utc' >= stg.eff_dt and now() at time zone 'utc' <= stg.end_dt) as t;

if v_cnt > 0 then

v_sql := v_sql||E'\n'||'union

select 

''Проверка на наличие русских символов в stg-карте в поле ref_to_stg'' as prv,

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg 

where ref_to_stg ~* ''[а-я]+''

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt';

else 

v_sql := v_sql||E'\n'||'union

select 

''Проверка на наличие русских символов в stg-карте в поле ref_to_stg'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

--Проверяем ключи

select count(1) into v_cnt from

(select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone 'utc' >= rdv.eff_dt and now() at time zone 'utc' <= rdv.end_dt

and rdv.src_stm_id is not null 

and coalesce(p_schema_stg, schema_stg) = schema_stg

and (upper(rdv.key_type_src) like '%FK%')

except

select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone 'utc' >= stg.eff_dt and now() at time zone 'utc' <= stg.end_dt

and stg.src_stm_id is not null 

and coalesce(p_schema_stg, schema_stg) = schema_stg

and (upper(stg.key_type_src) like '%FK%')) as t;

raise notice '%', v_cnt;

if v_cnt > 0 then

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие ключей FK в rdv- и stg-картах'' as prv,

src_stm_id, column_name_stg from

(

(select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and rdv.src_stm_id is not null 

and (upper(rdv.key_type_src) like ''%FK%'')

except

select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and stg.src_stm_id is not null 

and (upper(stg.key_type_src) <> ''''))

--Убрана проверка stg -> rdv

/*union all

(select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and stg.src_stm_id is not null 

and (upper(stg.key_type_src) like ''%FK%'')

except

select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and rdv.src_stm_id is not null 

and (upper(rdv.key_type_src) like ''%FK%''))*/

) as t';

else

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие ключей FK в rdv- и stg-картах'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

--Проверяем ключи

select count(1) into v_cnt from

(select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone 'utc' >= rdv.eff_dt and now() at time zone 'utc' <= rdv.end_dt

and coalesce(p_schema_stg, schema_stg) = schema_stg

and rdv.src_stm_id is not null 

and (upper(rdv.key_type_src) like '%PK%')

except

select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone 'utc' >= stg.eff_dt and now() at time zone 'utc' <= stg.end_dt

and coalesce(p_schema_stg, schema_stg) = schema_stg

and stg.src_stm_id is not null 

and (upper(stg.key_type_src) like '%PK%')) as t;

if v_cnt > 0 then

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие ключей PK в rdv- и stg-картах'' as prv,

src_stm_id, column_name_stg from

(

(select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and rdv.src_stm_id is not null 

and (upper(rdv.key_type_src) like ''%PK%'')

except

select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and stg.src_stm_id is not null 

and (upper(stg.key_type_src) like ''%PK%''))

--Убрана проверка stg -> rdv

/*union all

(select distinct

stg.src_stm_id, 

stg.column_name_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and stg.src_stm_id is not null 

and (upper(stg.key_type_src) like ''%PK%'')

except

select distinct

rdv.src_stm_id, 

rdv.column_name_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and rdv.src_stm_id is not null 

and (upper(rdv.key_type_src) like ''%PK%''))*/

) as t';

else

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие ключей PK в rdv- и stg-картах'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

--Проверяем соответствие ref_to_stg

select count(1) into v_cnt from

(select rdv.src_stm_id, rdv.column_name_stg 

from (select distinct

rdv.src_stm_id, 

rdv.column_name_stg,

coalesce(rdv.ref_to_stg, '') as ref_to_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone 'utc' >= rdv.eff_dt and now() at time zone 'utc' <= rdv.end_dt

and rdv.src_stm_id is not null 

and coalesce(p_schema_stg, schema_stg) = schema_stg

and coalesce(rdv.ref_to_stg, '') <> '' 

and (lower(rdv.ref_to_stg) not like '%[{"%')) as rdv

left join (select distinct

stg.src_stm_id, 

stg.column_name_stg,

coalesce(stg.ref_to_stg, '') as ref_to_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone 'utc' >= stg.eff_dt and now() at time zone 'utc' <= stg.end_dt

and stg.src_stm_id is not null 

and coalesce(p_schema_stg, schema_stg) = schema_stg

and lower(stg.ref_to_stg) not like '%[{"%') as stg 

on rdv.src_stm_id = stg.src_stm_id 

and rdv.column_name_stg = stg.column_name_stg 

where decode(lower(stg.ref_to_stg), lower(rdv.ref_to_stg), 1, 0) = 0) as t;

if v_cnt > 0 then

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие поля ref_to_stg в rdv- и stg-картах'' as prv,

rdv.src_stm_id, rdv.column_name_stg 

from (select distinct

rdv.src_stm_id, 

rdv.column_name_stg,

coalesce(rdv.ref_to_stg, '''') as ref_to_stg

from sys_dwh.prm_s2t_rdv rdv

where 1=1 

and now() at time zone ''utc'' >= rdv.eff_dt and now() at time zone ''utc'' <= rdv.end_dt

and rdv.src_stm_id is not null 

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and coalesce(rdv.ref_to_stg, '''') <> '''' 

and (lower(rdv.ref_to_stg) not like ''%[{"%'')) as rdv

left join (select distinct

stg.src_stm_id, 

stg.column_name_stg,

coalesce(stg.ref_to_stg, '''') as ref_to_stg

from sys_dwh.prm_s2t_stg stg

where 1=1 

and now() at time zone ''utc'' >= stg.eff_dt and now() at time zone ''utc'' <= stg.end_dt

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', schema_stg) = schema_stg

and stg.src_stm_id is not null 

and lower(stg.ref_to_stg) not like ''%[{"%'') as stg

on rdv.src_stm_id = stg.src_stm_id 

and rdv.column_name_stg = stg.column_name_stg 

where decode(lower(stg.ref_to_stg), lower(rdv.ref_to_stg), 1, 0) = 0';

else

v_sql := v_sql||E'\n'||'union

select 

''Проверка на несоответствие поля ref_to_stg в rdv- и stg-картах'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

--Проверяем column_fltr

select count(1) into v_cnt from

(select 

'Проверка на отсутствие поля column_fltr в rdv-карте' prv, 

ss.src_stm_id, s.column_name_stg 

from sys_dwh.prm_s2t_stg_src ss

join sys_dwh.prm_s2t_stg s on ss.src_stm_id = s.src_stm_id 

and lower(ss.column_fltr) = lower(s.column_name_src)

join sys_dwh.prm_s2t_rdv r on ss.src_stm_id = r.src_stm_id 

and s.column_name_stg = r.column_name_stg

where ss.column_fltr <> '' 

and coalesce(p_schema_stg, s.schema_stg) = s.schema_stg

and now() at time zone 'utc' >= ss.eff_dt and now() at time zone 'utc' <= ss.end_dt 

and now() at time zone 'utc' >= s.eff_dt and now() at time zone 'utc' <= s.end_dt 

and now() at time zone 'utc' >= r.eff_dt and now() at time zone 'utc' <= r.end_dt 

and ss.column_fltr is not null 

and (r.column_name_rdv = '' or r.column_name_rdv is null)) as t;

if v_cnt > 0 then

v_sql := v_sql||E'\n'||'union

select 

''Проверка на отсутствие поля column_fltr в rdv-карте'' prv, 

ss.src_stm_id, s.column_name_stg 

from sys_dwh.prm_s2t_stg_src ss

join sys_dwh.prm_s2t_stg s on ss.src_stm_id = s.src_stm_id 

and lower(ss.column_fltr) = lower(s.column_name_src)

join sys_dwh.prm_s2t_rdv r on ss.src_stm_id = r.src_stm_id 

and s.column_name_stg = r.column_name_stg

where ss.column_fltr <> '''' 

and coalesce('||coalesce(''''||p_schema_stg||'''', 'null')||', s.schema_stg) = s.schema_stg

and now() at time zone ''utc'' >= ss.eff_dt and now() at time zone ''utc'' <= ss.end_dt 

and now() at time zone ''utc'' >= s.eff_dt and now() at time zone ''utc'' <= s.end_dt 

and now() at time zone ''utc'' >= r.eff_dt and now() at time zone ''utc'' <= r.end_dt 

and ss.column_fltr is not null 

and (r.column_name_rdv = '''' or r.column_name_rdv is null)

';

else 

v_sql := v_sql||E'\n'||'union

select 

''Проверка на отсутствие поля column_fltr в rdv-карте'' as prv,

null::int4 src_stm_id, 

null::varchar(50) column_name_stg';

end if;

raise notice '%', v_sql; 

return query execute v_sql; 

--Регистрируем ошибки

exception

when others then

raise exception '%', sqlerrm;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.check_s2t_pr(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.check_s2t_pr(p_src_stm_id int4 DEFAULT NULL::integer, p_schema_stg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

v_output text;

v_cnt int4;

v_ssn text;

begin

select count(1) into v_cnt from sys_dwh.check_s2t(p_schema_stg) where src_stm_id = coalesce(p_src_stm_id, src_stm_id);

if v_cnt > 0 then

v_output := 'Обнаружены несоответствия в в stg- и rdv-картах';

raise exception '%', v_output;

end if;

v_output := 'Success: Расхождений между stg- и rdv-картой нет'||coalesce(' для schema_stg='||p_schema_stg, '')||coalesce(' для src_stm_id='||p_src_stm_id::text, '');

return(v_output); 

--Регистрируем ошибки

exception

when others then

raise exception '%', sqlerrm;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.check_s2t_pr_all();

CREATE OR REPLACE FUNCTION sys_dwh.check_s2t_pr_all()
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
AS $$
	
declare 
v_output text;
v_cnt int4;
v_except int8[] := array[66,13063,13071,13316,316,13338,13060,13065];
v_except_add int8[];
begin
 /*пропускаем тестовые s2t*/
select array_agg (src_stm_id)
into v_except_add	
from sys_dwh.prm_src_stm pss 
where prn_src_stm_id  = 20000;

select count(1) into v_cnt 
from sys_dwh.check_s2t()
where src_stm_id is not null
    and src_stm_id not in (select unnest(v_except || v_except_add));
if v_cnt > 0 then
select 'Обнаружены несоответствия в в stg- и rdv-картах: '||string_agg(pps.nm||'.'||pss.nm, ', ')
into v_output
from sys_dwh.check_s2t() cs
join sys_dwh.prm_src_stm pss on cs.src_stm_id = pss.src_stm_id 
join sys_dwh.prm_src_stm pps on pss.prn_src_stm_id = pps.src_stm_id 
where cs.src_stm_id is not null
and cs.src_stm_id not in (select unnest(v_except));
raise exception '%', v_output;
end if;
v_output := 'Success: Расхождений между stg- и rdv-картой нет';
return(v_output); 
--Регистрируем ошибки
exception
when others then
raise exception '%', sqlerrm;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_bdv_add_prttn(_text, json);

CREATE OR REPLACE FUNCTION sys_dwh.chk_bdv_add_prttn(p_table_name _text, p_json json)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_partitiontablename_lvl_0 text;
v_prttns_lvl_0 text[];
v_table_name text;
v_part_info record;
v_partitions record;
v_partitiontablename text;
v_output_text text :='';
v_schema text;
v_table text;
v_Start text;
v_End text;
v_start_query text;
v_end_query text;
v_endPartition text;
v_cnt int8 := 0;
v_error_text text := '';
v_parentpartitiontablename text;
v_ddl text;

 begin
	--Обрабатываем json, записываем данные о партициях в массив и проверяем партиции 0 уровня
	begin	
	for v_part_info in (select * from json_populate_recordset(null::record,	p_json::json) as (partitionlevel text, partitionform text, columnname text, partitions text)) loop
		for v_partitions in (select * from json_populate_recordset(null::record,	v_part_info.partitions::json) as (start_prttn text, end_prttn text, every_prttn text, with_prttn text)) loop
			if v_part_info.partitionlevel = '0' then 
				for v_table_name in select unnest(p_table_name) loop
				select split_part(v_table_name,'.', 1) into v_schema;
				select split_part(v_table_name,'.', 2) into v_table;
				if v_partitions.every_prttn = '' then 
				select count(*), partitiontablename into v_cnt, v_partitiontablename from pg_catalog.pg_partitions where tablename = lower(v_table) and
				schemaname = lower(v_schema) and partitionlevel = 0 and partitionrangestart = v_partitions.start_prttn and partitionrangeend = v_partitions.end_prttn group by partitiontablename;
				if v_cnt = 1 then
				v_prttns_lvl_0 = array_append(v_prttns_lvl_0, v_partitiontablename);
				else
					v_output_text = v_output_text || '
Не найдена партиция в таблице' || v_table_name || 'Start:'||v_partitions.start_prttn ||' End:' || v_partitions.end_prttn ;
				end if;
				raise notice '%', v_cnt;
				else
					v_Start = v_partitions.start_prttn;
					v_End = v_partitions.end_prttn;
					while  v_Start < v_End loop
						v_endPartition = v_Start + v_partitions.every_prttn;
						select count(*), partitiontablename into v_cnt, v_partitiontablename from pg_catalog.pg_partitions where tablename = lower(v_table) and
						schemaname = lower(v_schema) and partitionlevel = 0 and partitionrangestart = v_Start and partitionrangeend = v_endPartition group by partitiontablename;
						if v_cnt = 1 then
							v_prttns_lvl_0 = array_append(v_prttns_lvl_0, v_partitiontablename);
						else
							v_output_text = v_output_text || '
Не найдена партиция в таблице' || v_table_name || 'Start:'||v_Start ||' End:' || v_endPartition;
						end if;
						raise notice '%', v_cnt;
						v_Start = v_Start + v_partitions.every_prttn;
					end loop;
				end if;
				end loop;
			elsif v_part_info.partitionlevel = '1' and array_length(v_prttns_lvl_0,1) = 0 then 
				v_error_text = 'Нет партиций 0 уровня для партиций 1 уровня или P_json записан не правильно, партиции 0 уровня должны идти первыми';
				raise exception '%', v_error_text;
			elsif v_part_info.partitionlevel = '1' and array_length(v_prttns_lvl_0,1) > 0 then
				for v_table_name in select unnest(p_table_name) loop
					select split_part(v_table_name,'.', 1) into v_schema;
					select split_part(v_table_name,'.', 2) into v_table;
					for v_partitiontablename_lvl_0 in select unnest(v_prttns_lvl_0) loop
						if v_partitions.every_prttn = '' then 
							select count(*) into v_cnt from pg_catalog.pg_partitions where tablename = lower(v_table) and
							schemaname = lower(v_schema) and partitionlevel = 1 and partitionrangestart = v_partitions.start_prttn and partitionrangeend = v_partitions.end_prttn and parentpartitiontablename = v_partitiontablename_lvl_0;
							raise notice '%', v_cnt;
							raise notice '%', v_partitions.start_prttn;
							if v_cnt = 0 then
								v_output_text = v_output_text || '
Не найдена партиция в таблице' || v_table_name || 'Start:'||v_partitions.start_prttn ||' End:' || v_partitions.end_prttn  || ' parentprttn:'||v_partitiontablename_lvl_0;
							end if;
						elsif v_partitions.start_prttn like '%::date' then
							v_Start = replace(v_partitions.start_prttn, '::date','')::date;
							v_End = replace(v_partitions.end_prttn, '::date','')::date;
							while  v_Start < v_End loop
								v_endPartition = (replace(v_Start, '::date','')::date + replace(v_partitions.every_prttn, '::interval','')::interval)::date; 
								v_start_query = '''' || v_Start::text || '''::date';
								v_end_query = '''' || v_endPartition::text || '''::date';
								select count(*) into v_cnt from pg_catalog.pg_partitions where tablename = lower(v_table) and
								schemaname = lower(v_schema) and partitionlevel = 1 and partitionrangestart = v_start_query and partitionrangeend = v_end_query and parentpartitiontablename = v_partitiontablename_lvl_0;
								raise notice '%', v_cnt;
								raise notice '%', v_start_query;
								raise notice '%', v_end_query;
								raise notice '%', v_prttns_lvl_0;
								if v_cnt = 0 then
									v_output_text = v_output_text || '
Не найдена партиция в таблице' || v_table_name || 'Start:'||v_Start ||' End:' || v_endPartition || ' parentprttn:'||v_partitiontablename_lvl_0;
								end if;
								v_Start = (replace(v_Start, '::date','')::date + replace(v_partitions.every_prttn, '::interval','')::interval)::date; 
								end loop;
						elsif v_partitions.start_prttn like '%::timestamp without time zone' then
							v_Start = replace(v_partitions.start_prttn, '::timestamp without time zone','')::timestamp without time zone;
							v_End = replace(v_partitions.end_prttn, '::timestamp without time zone','')::timestamp without time zone;
							while  v_Start < v_End loop
								v_endPartition = (replace(v_Start, '::timestamp without time zone','')::timestamp without time zone + replace(v_partitions.every_prttn, '::interval','')::interval)::timestamp without time zone; 
								v_start_query = '''' || v_Start::text || '''::timestamp without time zone';
								v_end_query = '''' || v_endPartition::text || '''::timestamp without time zone';
								select count(*) into v_cnt from pg_catalog.pg_partitions where tablename = lower(v_table) and
								schemaname = lower(v_schema) and partitionlevel = 1 and partitionrangestart = v_start_query and partitionrangeend = v_end_query and parentpartitiontablename = v_partitiontablename_lvl_0;
								raise notice '%', v_cnt;
								raise notice '%', v_start_query;
								raise notice '%', v_end_query;
								raise notice '%', v_prttns_lvl_0;
								if v_cnt = 0 then
									v_output_text = v_output_text || '
Не найдена партиция в таблице' || v_table_name || 'Start:'||v_Start ||' End:' || v_endPartition || ' parentprttn:'||v_partitiontablename_lvl_0;
								end if;
								v_Start = (replace(v_Start, '::timestamp without time zone','')::timestamp without time zone + replace(v_partitions.every_prttn, '::interval','')::interval)::timestamp without time zone; 
								end loop;
							end if;
					end loop;
				end loop;
			end if;
		end loop;
	end loop;
    end;
	if v_output_text = '' then
		return('Все партиции на месте');
	else 
		return(v_output_text);
	end if;
end;
					

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_coll_bkgk(int4, text, bool);

CREATE OR REPLACE FUNCTION sys_dwh.chk_coll_bkgk(p_src_stm_id int4, p_json_ssn text DEFAULT NULL::text, debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	

	

declare

	v_coll_cnt int := 0;
	v_cnt int = 0;
	v_cnt_coll int2 = 0;
	v_src_stm_id_ref text;
	v_i text;
	v_i_gk_pk int2 = 0;
	v_i_json int2 = 0;
	--v_gk_text text;
	v_new_gk_text text;
	v_bk_text text;
	v_bk_str text;
	v_gk_pk_str_when text;
	v_col_nm text;
	v_gk_col_nm text;
	v_gk_pk_str text = '';
	v_src_stm_id_ref_json int2 = 0;
	v_crt_tmp_tbl_sql text = '';
	v_tbl_nm_sdv text;
	v_table_name_rdv text = '';
	v_rec_cr record;
	v_rec_cr_sql text;
	v_rec_json record;
	v_rec_gk_pk record;
	h_list record;
	v_upd_cur record;
	v_ssn int2 = 0;
	v_rec_json_ssn text;
	v_json_ret text = '';
	v_start_dttm text;

  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению

----

begin
	-- проверяем наличие значения, фиксируем название таблицы sdv-слоя
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg 
			into v_tbl_nm_sdv--, v_table_name_rdv
			from sys_dwh.prm_s2t_stg
				where src_stm_id = p_src_stm_id 
				and ((trim(ref_to_stg) <> '' 
				and ref_to_stg is not null)
				or (select count (*) 
				from information_schema.columns c
					join sys_dwh.prm_s2t_stg p on regexp_replace(c.table_schema, '^sdv', 'stg') = p.schema_stg
					and c.table_name = p.table_name_stg
					where src_stm_id = p_src_stm_id
						and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt
						and column_name = 'gk_pk') > 0
				)
				and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;
		
		if v_tbl_nm_sdv is null then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Поиск таблицы источника',
				p_start_dttm := v_start_dttm,
				p_err := 'Incorrect input parameter. Record with src_stm_id ' || p_src_stm_id || ' doesnt`t exist',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск таблицы источника',
			p_start_dttm := v_start_dttm,
			p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск таблицы источника',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	
	--Фиксируем номер сессии загрузки в таблице-источнике
	v_start_dttm := clock_timestamp() at time zone 'utc';
	if p_json_ssn is null then 
		execute 'select max(ssn) from '||v_tbl_nm_sdv into v_ssn;
	else
		for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,p_json_ssn)
			as (ssn int))
			loop
				if v_i = 0 then
					v_ssn := v_rec_json_ssn.ssn;
				else
					v_ssn := v_ssn||','||v_rec_json_ssn.ssn;
				end if;
				v_i = v_i + 1;
			end loop;
	end if;
	if v_ssn is null then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Поиск ssn',
			p_start_dttm := v_start_dttm,
			p_err := 'v_ssn is null',
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
	    into v_json_ret;
		raise exception '%', v_json_ret;
	end if;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Поиск ssn',
		p_start_dttm := v_start_dttm,
		p_val := 'ssn='''||v_ssn||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
	
	-- проверяем наличие значения в sys_dwh.prm_s2t_rdv
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select count(*)
			into v_cnt
			from sys_dwh.prm_s2t_rdv_rule 
			where src_stm_id = p_src_stm_id
				and (now() at time zone 'utc')::date between eff_dt and end_dt;
		
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '3',
				p_descr := 'Поиск таблицы приемника',
				p_start_dttm := v_start_dttm,
				p_err := 'Record with src_stm_id ' || p_src_stm_id || ' doesnt`t exist in sys_dwh.prm_s2t_rdv_rule.',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Поиск таблицы приемника',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Поиск таблицы приемника',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;

	--raise notice 'all checks are done';

	--проверяем на наличие gk_pk
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select count (*)
		into v_cnt
		from information_schema.columns c
			join sys_dwh.prm_s2t_rdv p on c.table_schema||'.'||c.table_name = regexp_replace(schema_stg,'^stg', 'sdv')||'.'||p.table_name_stg
		where c.table_schema||'.'||c.table_name = v_tbl_nm_sdv
			and column_name = 'gk_pk'
			and substring(trim(p.table_name_rdv) from 1 for 2) <> 'l_'
			and (now() at time zone 'utc')::date between p.eff_dt and p.end_dt;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Поиск наличия gk_pk',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Поиск наличия gk_pk',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	
	--собираем sql для цикла по полям
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		v_rec_cr_sql := 'select distinct schema_stg, table_name_stg, column_name_stg, ref_to_hub, coalesce(ref_to_stg, schema_stg||''.''||table_name_stg) ref_to_stg
			from sys_dwh.prm_s2t_rdv
			where src_stm_id = '||p_src_stm_id||'
			        and column_name_rdv like ''gk_%''
					and (now() at time zone ''utc'')::date between eff_dt and end_dt
					order by schema_stg, table_name_stg, column_name_stg)';
		if v_cnt <> 0 then
			v_rec_cr_sql := v_rec_cr_sql ||' union all
			select distinct schema_stg, table_name_stg, ''gk_pk'' column_name_stg, '''' ref_to_hub, coalesce(ref_to_stg, schema_stg||''.''||table_name_stg) ref_to_stg
			from sys_dwh.prm_s2t_rdv
			where src_stm_id = '||p_src_stm_id||'
			and (now() at time zone ''utc'')::date between eff_dt and end_dt';
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Поиск sql для цикла по полям',
			p_start_dttm := v_start_dttm,
			p_val := 'v_rec_cr_sql='''||v_rec_cr_sql||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Поиск sql для цикла по полям',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	-- основной цикл по всем gk входной таблицы
	for v_rec_cr in (
		select schema_stg, table_name_stg, column_name_stg, ref_to_hub, coalesce(ref_to_stg, schema_stg||'.'||table_name_stg) ref_to_stg
		from sys_dwh.prm_s2t_rdv
		where src_stm_id = p_src_stm_id
		        /*and trim(ref_to_stg) <> ''
				and ref_to_stg is not null*/
				and column_name_rdv like 'gk_%'
				and (now() at time zone 'utc')::date between eff_dt and end_dt
				order by schema_stg, table_name_stg, column_name_stg)
	loop
		v_bk_text := '';
		v_new_gk_text := '';
		v_bk_str := '';
		v_gk_pk_str_when := '';
		--если не json
		if substring(trim(v_rec_cr.ref_to_stg) from 1 for 3) = 'stg' and v_rec_cr.column_name_stg <> 'gk_pk' then
			--находим src_stm_id 
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					select src_stm_id into v_src_stm_id_ref 
				    		from sys_dwh.prm_src_stm 
				    			where nm = split_part(v_rec_cr.ref_to_stg, '.',2)
				    				and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
				    										where nm = regexp_replace(split_part(v_rec_cr.ref_to_stg, '.',1),'^stg_', ''));

				    /*if (v_i <> 0) or (v_i_json <> 0) then 
						v_exec_sql := v_exec_sql||', ';
				    end if;*/
					--v_bk_text := 'coalesce('||v_rec_cr.column_name_stg||'::text,''-1''))::uuid ';
				    v_bk_text := 'coalesce('||v_rec_cr.column_name_stg||'::text,''-1'') ';
					v_new_gk_text := 'coalesce('''||v_src_stm_id_ref||'::text||~''||'||v_rec_cr.column_name_stg||'::text||' || '''-#~''' ||',''-1'') ';
					v_col_nm := v_rec_cr.column_name_stg;
					v_gk_col_nm := 'gk_'||v_rec_cr.column_name_stg;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '6',
						p_descr := 'Сборка скрипта для не json',
						p_start_dttm := v_start_dttm,
						p_val := 'src_stm_id='''||v_src_stm_id_ref||''',v_col_nm='''||v_col_nm||''',v_bk_text='''||v_bk_text||''', v_new_gk_text='''||v_new_gk_text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
					into v_json_ret;						
			    exception when others then
					if v_ssn is null then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '6',
							p_descr := 'Сборка скрипта для не json',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
					end if;
					raise exception '%', v_json_ret;
			    end;
		elseif
		
		--json
		substring(trim(v_rec_cr.ref_to_stg) from 1 for 1) = '[' then
		  --begin
			v_i_json := 0;
			v_col_nm := v_rec_cr.column_name_stg;
			v_gk_col_nm := 'gk_'||v_rec_cr.column_name_stg;
			for v_rec_json in (select tbl_nm, fld_nm, val, ref_to_hub from json_populate_recordset(null::record,
						v_rec_cr.ref_to_stg::json)
						as (tbl_nm text, fld_nm text, val text, ref_to_hub text))
				    loop
						v_start_dttm := clock_timestamp() at time zone 'utc';
					    --находим src_stm_id
					    select src_stm_id into v_src_stm_id_ref_json 
				    		from sys_dwh.prm_src_stm 
				    			where nm = split_part(v_rec_json.tbl_nm, '.',2)
				    				and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
				    										where nm = regexp_replace(split_part(v_rec_json.tbl_nm, '.',1),'^stg_', ''));
				    	
				    	if v_i_json = 0 then 
						    v_bk_text := 'case ';
							v_new_gk_text := 'case ';
						end if; 								
				    		v_bk_text := v_bk_text||' when '||v_rec_json.fld_nm||' in( '||replace(replace(v_rec_json.val,'[',''),']','') || ') then coalesce(' || v_rec_cr.column_name_stg||',''-1'') ';
				    		v_new_gk_text := v_new_gk_text||' when '||v_rec_json.fld_nm||' in( '||replace(replace(v_rec_json.val,'[',''),']','')||') then coalesce('''||v_src_stm_id_ref_json||'||~''||'||v_rec_cr.column_name_stg|| '||' || '''-#~''' ||',''-1'') ';
						v_i_json := v_i_json + 1;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '7',
							p_descr := 'Сборка скрипта для json',
							p_start_dttm := v_start_dttm,
							p_val := 'v_bk_text='''||v_bk_text||''', v_new_gk_text='''||v_new_gk_text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				   end loop;
				   v_bk_text := v_bk_text || ' else  ''-1'' end ';
				   v_new_gk_text := v_new_gk_text ||' else  ''-1'' end ';
				   if v_bk_text is null or v_new_gk_text is null then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '7',
							p_descr := 'Сборка скрипта для json',
							p_start_dttm := v_start_dttm,
							p_val := 'v_col_nm='''||v_col_nm||''', v_bk_text='''||v_bk_text||''', v_new_gk_text='''||v_new_gk_text||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end if;
		end if;
		
		
		begin
			v_start_dttm := clock_timestamp() at time zone 'utc';
			if v_cnt <> 0 and v_rec_cr.column_name_stg = 'gk_pk' then
				v_i_gk_pk := 0;	
				v_gk_col_nm := 'gk_pk';
				for v_rec_gk_pk in (select column_name_stg  
						from sys_dwh.prm_s2t_rdv
							where src_stm_id = p_src_stm_id 
							and trim(key_type_src) like '%PK%'
							and (now() at time zone 'utc')::date between eff_dt and end_dt
							order by column_name_stg)
				loop
					if v_i_gk_pk = 0 then 
						v_gk_pk_str := 'coalesce('||v_rec_gk_pk.column_name_stg||'::text ,''-1'')';
						v_gk_pk_str_when := v_rec_gk_pk.column_name_stg||' is null ';
						v_bk_str := 'coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';
						v_col_nm := v_rec_gk_pk.column_name_stg;
					else
						v_gk_pk_str := v_gk_pk_str||'||''~''||coalesce('||v_rec_gk_pk.column_name_stg||'::text ,''-1'')';
						v_gk_pk_str_when := v_gk_pk_str_when||' and '||v_rec_gk_pk.column_name_stg||' is null ';
						v_bk_str := v_bk_str||'||''||''||coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';
						v_col_nm := v_col_nm||','||v_rec_gk_pk.column_name_stg;
					end if;
					v_i_gk_pk := v_i_gk_pk + 1;
				end loop;
				v_bk_text := 'case when '||v_gk_pk_str_when||' 
					then ''-1''
					else coalesce('||v_bk_str||'::text ,''-1'') end';
				v_new_gk_text := 'case when '||v_gk_pk_str_when||' 
					then ''-1''
					else coalesce('''||p_src_stm_id||'::text||~''||'||v_gk_pk_str||'::text ||' || '''-#~'',''-1'') end';
			end if;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '8',
				p_descr := 'Сборка скрипта для gk_pk',
				p_start_dttm := v_start_dttm,
				p_val := 'v_bk_text='''||v_bk_text||''', v_new_gk_text='''||v_new_gk_text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '8',
				p_descr := 'Сборка скрипта для gk_pk',
				p_start_dttm := v_start_dttm,
				p_val := 'v_bk_text='''||v_bk_text||''', v_new_gk_text='''||v_new_gk_text||'''',
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
	
		-- Цикл по всем хабам
		for h_list in (
			SELECT 'rdv.' || table_name tbl_hub
			FROM information_schema.tables
			WHERE table_schema = 'rdv'
				and table_name not like '%_prt_%'
		)
		loop 
			-- выявляем коллизии, фиксируем их в таблице, производим update sdv-таблицы
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
			
				v_crt_tmp_tbl_sql := 'create temp table collision_tmp as  
							select distinct new_bk, ' 
							|| ' old_gk, '
							|| ' new_gk,'
							|| ' bk as old_bk,'
							|| ' gk, rdv.src_stm_id hub_src_stm_id'
							|| ' from (
							select distinct ' || v_bk_text || ' new_bk, '
							|| v_col_nm ||', '
							|| v_gk_col_nm ||', '
							|| v_gk_col_nm ||' old_gk, '
							|| 'md5(' || v_new_gk_text || ')::uuid new_gk '
							|| ''
							|| 'from '
							|| v_tbl_nm_sdv ||' where ssn in ( '||v_ssn||'))'
							|| ' sdv join ' || h_list.tbl_hub || ' rdv on sdv.old_gk = rdv.gk'
							|| ' where
							' || v_bk_text || ' <> bk  
							and sdv.old_gk <> ''6bb61e3b7bce0931da574d19d1d82c88'' 
							DISTRIBUTED BY (old_bk, gk)';
				execute v_crt_tmp_tbl_sql;
				get diagnostics v_cnt = row_count;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '9',
					p_descr := 'Наполняем временную таблицу',
					p_start_dttm := v_start_dttm,
					p_val := 'v_crt_tmp_tbl_sql='''||v_crt_tmp_tbl_sql::text||'''',
					p_ins_qty := v_cnt::text,
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '9',
					p_descr := 'Наполняем временную таблицу',
					p_start_dttm := v_start_dttm,
					p_val := 'v_crt_tmp_tbl_sql='''||v_crt_tmp_tbl_sql::text||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						execute 'insert into sys_dwh.collision_hist (bk, gk, src_stm_id, upd_dttm, new_bk, new_gk, s2t_col_nm, hub_nm, hub_src_stm_id) select old_bk, old_gk,'||p_src_stm_id::text||', now(), new_bk, new_gk, 
							'''||v_rec_cr.column_name_stg||''' s2t_col_nm, ''' || h_list.tbl_hub || ''' hub_nm, hub_src_stm_id from collision_tmp t
							where not exists (select 1 from sys_dwh.collision_hist h where h.gk = t.gk and h.new_gk = t.new_gk)';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '10',
							p_descr := 'Вставка коллизии в sys_dwh.collision_hist',
							p_start_dttm := v_start_dttm,
							p_val := 'insert into sys_dwh.collision_hist (bk, gk, src_stm_id, upd_dttm, new_bk, new_gk, s2t_col_nm, hub_nm, hub_src_stm_id) select old_bk, old_gk,'||p_src_stm_id::text||', now(), new_bk, new_gk 
							'''||v_rec_cr.column_name_stg||''' s2t_col_nm, ''' || h_list.tbl_hub || ''' hub_nm, hub_src_stm_id from collision_tmp',
							p_ins_qty := v_cnt::text,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '10',
							p_descr := 'Вставка коллизии в sys_dwh.collision_hist',
							p_start_dttm := v_start_dttm,
							p_val := 'insert into sys_dwh.collision_hist (bk, gk, src_stm_id, upd_dttm, new_bk, new_gk, s2t_col_nm, hub_nm, hub_src_stm_id) select old_bk, old_gk,'||p_src_stm_id::text||', now(), new_bk, new_gk 
							'''||v_rec_cr.column_name_stg||''' s2t_col_nm, ''' || h_list.tbl_hub || ''' hub_nm, hub_src_stm_id from collision_tmp',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
				
				-- update только для debag = false
				if debug = false then
					for v_upd_cur in (
							select *
							from collision_tmp
						)
						loop
							begin
								v_start_dttm := clock_timestamp() at time zone 'utc';	
								execute 'update '||v_tbl_nm_sdv::text||' set gk_'|| v_rec_cr.column_name_stg::text||'='''|| v_upd_cur.new_gk::text||''' where gk_'||v_rec_cr.column_name_stg::text||'='''||v_upd_cur.old_gk::text||''' and '||v_rec_cr.column_name_stg::text||' ='''||v_upd_cur.new_bk::text||'''';
								
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '11',
									p_descr := 'Обновление таблицы sdv новым значением',
									p_start_dttm := v_start_dttm,
									p_val := 'update '||v_tbl_nm_sdv::text||' set gk_'|| v_rec_cr.column_name_stg::text||'='''|| v_upd_cur.new_gk::text||''' where gk_'||v_rec_cr.column_name_stg::text||'='''||v_upd_cur.old_gk::text||''' and '||v_rec_cr.column_name_stg::text||' ='''||v_upd_cur.new_bk::text||'''',
									p_upd_qty := v_cnt::text,
									p_log_tp := '1',
									p_debug_lvl := '3')
								into v_json_ret;
							exception when others then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '11',
									p_descr := 'Обновление таблицы sdv новым значением',
									p_start_dttm := v_start_dttm,
									p_val := 'update '||v_tbl_nm_sdv::text||' set gk_'|| v_rec_cr.column_name_stg::text||'='''|| v_upd_cur.new_gk::text||''' where gk_'||v_rec_cr.column_name_stg::text||'='''||v_upd_cur.old_gk::text||''' and '||v_rec_cr.column_name_stg::text||' ='''||v_upd_cur.new_bk::text||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
								into v_json_ret;
								raise exception '%', v_json_ret;
							end;						
						end loop;
				end if;	
				-- update только для debag = false	
					
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';	
				execute 'drop table if exists collision_tmp cascade';
				get diagnostics v_cnt = row_count;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '12',
					p_descr := 'Удаление collision_tmp',
					p_start_dttm := v_start_dttm,
					p_val := 'drop table if exists collision_tmp cascade',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '12',
					p_descr := 'Удаление collision_tmp',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
		end loop;
	end loop;
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		execute 'analyze ' || v_tbl_nm_sdv;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '13',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
	exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '13',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'analyze ' || v_tbl_nm_sdv,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
				raise exception '%', v_json_ret;
	end;
	--return('ok, found ' || v_coll_cnt || ' collisions');
	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
    --return (v_exec_sql); 
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '0',
					p_descr := 'Фатальная ошибка',
					p_start_dttm := v_start_dttm,
					
					p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
		end if;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    
end;



$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_rdv_pk_dbls(int4);

CREATE OR REPLACE FUNCTION sys_dwh.chk_rdv_pk_dbls(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
 
declare 
  v_rec_cr record;
  v_rec_rdv_tbl record;
  v_rec_json record;
  v_rec_rdv_col record;
  v_rec_json_ssn record;

  v_tbl_nm_sdv text;

  v_cnt bigint = 0;
  v_cnt_block int = 0;
  v_ssn text = '';
  v_max_ssn text = '';
  v_exec_sql text = '';
  v_sql_col text = '';
  v_sql_col_nvl text = '';
  
  v_i int = 2;
  
  v_json_ret text = '';
 
  v_start_dttm text;
  
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--таблица источник
	select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg
		--,schema_rdv
		--,table_name_rdv 
		into v_tbl_nm_sdv--, v_schema_rdv, v_table_name_rdv
		from sys_dwh.prm_s2t_rdv
			where src_stm_id = p_src_stm_id 
			and ((trim(ref_to_stg) <> '' 
			and ref_to_stg is not null)
			or (select count (*) 
			from information_schema.columns c
				join sys_dwh.prm_s2t_stg p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
				and c.table_name = p.table_name_stg
			  	where p.src_stm_id = p_src_stm_id
			  		and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date
			  		
			  		) > 0
			)
			and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		--p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||''', v_schema_rdv='''||v_schema_rdv||''', v_table_name_rdv='''||v_table_name_rdv||'''',
		p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
    into v_json_ret;
	raise exception '%', v_json_ret;
  end;
    v_start_dttm := clock_timestamp() at time zone 'utc';
	--Фиксируем номер сессии загрузки в таблице-источнике
	/*if p_json is null then 
		execute 'select max(ssn) from '||v_tbl_nm_sdv into v_max_ssn;
	end if;*/

	--цикл по ssn
	/*for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,coalesce(p_json,'[{"ssn":"'||v_max_ssn||'"}]')::json)
		as (ssn int))
		loop
			v_ssn := v_rec_json_ssn.ssn;*/
			--цикл по rdv таблицам
			for v_rec_rdv_tbl in (SELECT schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, is_distinct, upd_user
							FROM sys_dwh.prm_s2t_rdv_rule r
							where src_stm_id = p_src_stm_id 
							and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
							and exists (select 1 from sys_dwh.prm_s2t_rdv 
										where src_stm_id = r.src_stm_id
										and schema_rdv = r.schema_rdv 
										and table_name_rdv = r.table_name_rdv
										and (key_type_src in ('PK,FK','PK') or (table_name_rdv like('l_%') and column_name_rdv <> ''))))
				loop 
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_exec_sql := 'select count(*) from (select ';
					
					--v_sql_rdv := 'select count(*) from (select ';
					--if v_rec_rdv_tbl.is_distinct = 1 then v_sql_rdv := v_sql_rdv ||' distinct ';
					--end if;
					for v_rec_rdv_col in (SELECT schema_stg, table_name_stg, column_name_stg, datatype_stg, schema_rdv, table_name_rdv, column_name_rdv, datatype_rdv, key_type_src
											FROM sys_dwh.prm_s2t_rdv
											where src_stm_id = p_src_stm_id 
												and table_name_rdv = v_rec_rdv_tbl.table_name_rdv
												and schema_rdv = v_rec_rdv_tbl.schema_rdv
												and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
												and (key_type_src in ('PK,FK','PK') or (table_name_rdv like('l_%') and column_name_rdv <> ''))
												order by column_name_stg)
					loop 
						v_sql_col := v_sql_col ||v_rec_rdv_col.column_name_rdv||', ';
						v_sql_col_nvl := v_sql_col_nvl ||'coalesce('||v_rec_rdv_col.column_name_rdv||'::text, ''@~@''),';
					end loop;
					v_exec_sql := v_exec_sql || v_sql_col_nvl ||' 1 from 
					( select ';
					if v_rec_rdv_tbl.is_distinct = 1 then v_exec_sql := v_exec_sql ||' distinct ';
					end if;
					v_exec_sql := v_exec_sql || v_sql_col ||' 1 from '||v_rec_rdv_tbl.schema_rdv||'.'||v_rec_rdv_tbl.table_name_rdv;
					--v_exec_sql := v_exec_sql ||' where ssn = '||v_rec_json_ssn.ssn;
					v_exec_sql := v_exec_sql ||' where daterange(eff_dt,end_dt,''[]'')@>(now() at time zone ''utc'')::date ) t 
					group by '|| v_sql_col_nvl ||' 1 having count(*) > 1) t1';
					begin
						--if v_i = 3 then
						--return v_exec_sql;
						--end if;
						execute v_exec_sql into v_cnt;
						if v_cnt > 0 then
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := v_i::text,
							p_descr := 'Найдены дубли',
							p_start_dttm := v_start_dttm,
							p_val := 'schema_stg='''||v_rec_rdv_tbl.schema_stg||''', table_name_stg='''||v_rec_rdv_tbl.table_name_stg||''', schema_rdv='''||v_rec_rdv_tbl.schema_rdv||''', table_name_rdv='''||v_rec_rdv_tbl.table_name_rdv||'''',
							p_log_tp := '3',
							p_debug_lvl := '1')
					    	into v_json_ret;
						end if;
					v_sql_col:='';
					v_sql_col_nvl:='';
					exception when others then
					    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := v_i::text,
							p_descr := 'Поиск дублей',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
					    into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					v_i := v_i + 1;
				end loop;
		--end loop;
    v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '0',
					p_descr := 'Фатальная ошибка',
					p_start_dttm := v_start_dttm,
					
					p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
		end if;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 



$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_sdv_pk_dbls(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.chk_sdv_pk_dbls(p_src_stm_id int4, p_json text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
 
declare 
  v_rec_cr record;
  v_rec_rdv_tbl record;
  v_rec_json record;
  v_rec_rdv_col record;
  v_rec_json_ssn record;

  v_tbl_nm_sdv text;

  v_cnt bigint = 0;
  v_cnt_block int = 0;
  v_ssn text = '';
  v_max_ssn text = '';
  v_exec_sql text = '';
  v_sql_col text = '';
  v_sql_col_nvl text = '';
  
  v_i int = 2;
  
  v_json_ret text = '';
 
  v_start_dttm text;
  
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--таблица источник
	select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg
		--,schema_rdv
		--,table_name_rdv 
		into v_tbl_nm_sdv--, v_schema_rdv, v_table_name_rdv
		from sys_dwh.prm_s2t_rdv
			where src_stm_id = p_src_stm_id 
			and ((trim(ref_to_stg) <> '' 
			and ref_to_stg is not null)
			or (select count (*) 
			from information_schema.columns c
				join sys_dwh.prm_s2t_stg p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
				and c.table_name = p.table_name_stg
			  	where p.src_stm_id = p_src_stm_id
			  		and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date
			  		
			  		) > 0
			)
			and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		--p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||''', v_schema_rdv='''||v_schema_rdv||''', v_table_name_rdv='''||v_table_name_rdv||'''',
		p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
    into v_json_ret;
	raise exception '%', v_json_ret;
  end;
    v_start_dttm := clock_timestamp() at time zone 'utc';
	--Фиксируем номер сессии загрузки в таблице-источнике
	if p_json is null then 
		execute 'select max(ssn) from '||v_tbl_nm_sdv into v_max_ssn;
	end if;

	--цикл по ssn
	for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,coalesce(p_json,'[{"ssn":"'||v_max_ssn||'"}]')::json)
		as (ssn int))
		loop
			v_ssn := v_rec_json_ssn.ssn;
			--цикл по rdv таблицам
			for v_rec_rdv_tbl in (SELECT schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, is_distinct, upd_user
							FROM sys_dwh.prm_s2t_rdv_rule r
							where src_stm_id = p_src_stm_id 
							and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
							and exists (select 1 from sys_dwh.prm_s2t_rdv 
										where src_stm_id = r.src_stm_id
										and schema_rdv = r.schema_rdv 
										and table_name_rdv = r.table_name_rdv
										and (key_type_src in ('PK,FK','PK') or (table_name_rdv like('l_%') and column_name_rdv <> ''))))
				loop 
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_exec_sql := 'select count(*) from (select ';
					
					--v_sql_rdv := 'select count(*) from (select ';
					--if v_rec_rdv_tbl.is_distinct = 1 then v_sql_rdv := v_sql_rdv ||' distinct ';
					--end if;
					for v_rec_rdv_col in (SELECT schema_stg, table_name_stg, column_name_stg, datatype_stg, schema_rdv, table_name_rdv, column_name_rdv, datatype_rdv, key_type_src
											FROM sys_dwh.prm_s2t_rdv
											where src_stm_id = p_src_stm_id 
												and table_name_rdv = v_rec_rdv_tbl.table_name_rdv
												and schema_rdv = v_rec_rdv_tbl.schema_rdv
												and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
												and (key_type_src in ('PK,FK','PK') or (table_name_rdv like('l_%') and column_name_rdv <> ''))
												order by column_name_stg)
					loop 
						v_sql_col := v_sql_col ||v_rec_rdv_col.column_name_stg||', ';
						v_sql_col_nvl := v_sql_col_nvl ||'coalesce('||v_rec_rdv_col.column_name_stg||'::text, ''@~@''),';
					end loop;
					v_exec_sql := v_exec_sql || v_sql_col_nvl ||' 1 from 
					( select ';
					if v_rec_rdv_tbl.is_distinct = 1 then v_exec_sql := v_exec_sql ||' distinct ';
					end if;
					v_exec_sql := v_exec_sql || v_sql_col ||' 1 from '||v_tbl_nm_sdv;
					v_exec_sql := v_exec_sql ||' where ssn = '||v_rec_json_ssn.ssn;
					v_exec_sql := v_exec_sql ||' ) t 
					group by '|| v_sql_col_nvl ||' 1 having count(*) > 1) t1';
					begin
						--if v_i = 3 then
						--return v_exec_sql;
						--end if;
						execute v_exec_sql into v_cnt;
						if v_cnt > 0 then
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := v_i::text,
							p_descr := 'Найдены дубли',
							p_start_dttm := v_start_dttm,
							p_val := 'ssn='''||v_ssn||''', schema_stg='''||v_rec_rdv_tbl.schema_stg||''', table_name_stg='''||v_rec_rdv_tbl.table_name_stg||''', schema_rdv='''||v_rec_rdv_tbl.schema_rdv||''', table_name_rdv='''||v_rec_rdv_tbl.table_name_rdv||'''',
							p_log_tp := '3',
							p_debug_lvl := '1')
					    	into v_json_ret;
						end if;
					v_sql_col:='';
					v_sql_col_nvl:='';
					exception when others then
					    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := v_i::text,
							p_descr := 'Поиск дублей',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
					    into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					v_i := v_i + 1;
				end loop;
		end loop;
    v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '0',
					p_descr := 'Фатальная ошибка',
					p_start_dttm := v_start_dttm,
					
					p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
		end if;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_src_ready(int4);

CREATE OR REPLACE FUNCTION sys_dwh.chk_src_ready(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_cnt int8 := 0;
v_ddl text;
v_answr text;
v_start_dttm text;
v_json_ret text := '';
v_tbl_nm text;
v_db text;
v_nm text;
v_pnm text;
begin
--Проверяем наличие src_stm_id на БД
begin	
v_start_dttm := timeofday()::timestamp;
select count(1) into v_cnt 
from sys_dwh.prm_src_stm as p
where p.src_stm_id = p_src_stm_id
and now() at time zone 'utc' >= p.eff_dt 
and now() at time zone 'utc' <= p.end_dt;
if v_cnt <> 1
then raise exception '%', 'The src_stm_id is not correct in table sys_dwh.prm_src_stm';
end if;
--Определяем тип источника
select p.nm, s.nm 
into v_nm, v_pnm 
from sys_dwh.prm_src_stm as p
left join sys_dwh.prm_src_stm s on s.src_stm_id = p.prn_src_stm_id 
where p.src_stm_id = p_src_stm_id
and now() at time zone 'utc' >= p.eff_dt 
and now() at time zone 'utc' <= p.end_dt;
--Логируем
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '1',
p_descr := 'Проверка src_stm_id',
p_start_dttm := v_start_dttm,
p_val := 'src_stm_id='||p_src_stm_id::text||', '
||(case when v_pnm is null then 'DB-source ' else 'table-source ' end)||v_nm,
p_debug_lvl := '1')
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '1',
p_descr := 'Проверка src_stm_id',
p_start_dttm := v_start_dttm,
p_val := 'src_stm_id='||p_src_stm_id::text,
p_err := sqlerrm,
p_log_tp := '3',
p_debug_lvl := '1',
p_cls := ']')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Создаем внешнюю таблицу
begin
v_start_dttm := timeofday()::timestamp;
--Внешнаяя таблица для родительского источника
if v_pnm is null
then 
select sys_service.get_cctn(v_nm) into v_db;
select 'drop external table if exists ext_sys_dwh.chk_src_ready;
create external table ext_sys_dwh.chk_src_ready (n int4)
LOCATION (''pxf://query:'||(select current_database())||'/pxf_'||v_nm||'_'||v_db||'_check?PROFILE=JDBC&SERVER='||v_db||''') ON ALL FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
ENCODING ''UTF8'';' into v_ddl;
else
--Внешняя таблица для дочернего источника
select sys_dwh.ext_crt(p_src_stm_id, true) into v_ddl;
end if;
if lower(v_ddl) <> 'внутренний источник данных' then 
execute v_ddl;
end if;
--Логируем
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '2',
p_descr := 'Проверка src_stm_id',
p_start_dttm := v_start_dttm,
p_val := v_ddl,
p_debug_lvl := '1')
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '2',
p_descr := 'Проверка src_stm_id',
p_start_dttm := v_start_dttm,
p_val := v_ddl,
p_err := sqlerrm,
p_log_tp := '3',
p_debug_lvl := '1',
p_cls := ']')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Проверяем источник
begin
v_start_dttm := timeofday()::timestamp;
if lower(v_ddl) <> 'внутренний источник данных' then
select substring(lower(v_ddl) from '%drop external table if exists #"%#";%create%' for '#')
into v_tbl_nm;
else 
select substring(lower(sys_dwh.ext_get(p_src_stm_id, null, true)) from '%from #"%#";' for '#') 
into v_tbl_nm;
end if;
execute 'select count(1) from 
(select 1 from '||v_tbl_nm||' limit 1) s' into v_cnt;
if v_cnt > 0 then
--Логируем о success
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '3',
p_descr := 'Проверка доступа и данных на источнике',
p_start_dttm := v_start_dttm,
p_val := 'Success: источник доступен, данные есть',
p_debug_lvl := '1')
into v_json_ret;
--Логируем о warning
else 
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '3',
p_descr := 'Проверка доступа и данных на источнике',
p_start_dttm := v_start_dttm,
p_val := 'Warning: источник доступен, данных нет',
p_log_tp := '2',
p_debug_lvl := '1')
into v_json_ret;
end if;
--Логируем о error
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
p_step := '3',
p_descr := 'Проверка доступа и данных на источнике',
p_start_dttm := v_start_dttm,
p_val := 'Error: Источник недоступен',
p_err := sqlerrm,
p_log_tp := '3',
p_debug_lvl := '1',
p_cls := ']')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Возвращаем результат
v_json_ret := v_json_ret||']';
raise notice '%', v_ddl;
return(v_json_ret); 
--Регистрируем ошибки
exception
when others then
if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
v_json_ret := v_json_ret||']';
end if;
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.chk_strtr(int4);

CREATE OR REPLACE FUNCTION sys_dwh.chk_strtr(p_chk_num int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
v_chk_num int; -- цифровой номер проверки
v_chk_nm text; -- Имя проверки
v_schm text; -- слой проверки
v_tp text; -- тип проверки (в дальнейшем будет идентификатор, пока текст)
v_sql_chk text; -- sql-запрос проверки
v_jsn_chk json; -- json с необходимыми параметрами для проверки (например имя таблицы)
v_err_txt text; -- текст при ошибке 
v_err_txt_l text; -- текст при ошибке измененный в цикле
v_ok_txt text; -- тескт при успешном прохождении проверки
v_ok_txt_l text; -- тескт при успешном прохождении проверки измененный в цикле
v_eml_f int; -- флаг нужно ли отправлять отчет на email
v_email text; -- список email на которые необходимо сделать рассылку 
v_activ int; -- флаг активности проверки
v_tbl_nm text; -- имя таблицы
v_tbl_pk text; -- наименование поля/полей PK таблицы
v_i int = 0; -- кол-во итераций 
v_rec_json_prm record; -- список  для курсора
v_rslt_chk text; -- результат проверки
v_sql_tbl_nm text; -- текст запроса для вывода списка всех таблиц слоя
v_sql_chk_l text; -- sql-запрос проверки в цикле
v_sql_chk_h text; -- sql-запрос проверки для хэша
v_sql text; -- sql-запрос
v_ins text; -- скрипт insert
v_upd text; -- скрипт update 
v_rslt_num_hash text; --скрипт расчета id проверки
v_rslt_num uuid; -- id проверки
v_prm_func_tbl text; -- скрипт вывода параматров для функции
v_prm_func text; -- параметр для функции
v_prm_query text; -- параметр для запроса
v_case_num int ; -- номер кейса
v_origl_tb text ; -- таблица с исходными данными
v_intr_tb text ;-- промежуточная таблица
v_expc_tb text ; -- таблица с ожидаемыми данными (эталон)
v_actv_f int ; -- флаг активности кейса 
v_fin text = 'Success'; -- сообщение по окончанию работы функции
v_pre_sql text ; -- скрипт заполнения табилцы "in", который нужно выполнять перед тем как начать тестирование по кейсу
v_start_id uuid; --id запуска проверки (один на несколько кейсов)
v_cnt int;
v_rslt text;
v_schm_rdv text;
--тело процедуры
begin 
-- задаем переменные  
	select chk_num, chk_nm, schm, tp, sql_chk, jsn_chk,err_txt,ok_txt, eml_f, activ,  email, 
	md5(chk_num||chk_nm||schm||tp||sql_chk||eml_f||activ||(now()::timestamp)::text)::uuid as start_id
	into v_chk_num,v_chk_nm,v_schm,v_tp,v_sql_chk,v_jsn_chk, v_err_txt, v_ok_txt,v_eml_f,v_activ, v_email, v_start_id  
	from sys_tst_dwh.d_chk_dwh 
	where chk_num = p_chk_num ;
--проверяем есть проверка или нет	
if v_chk_num is not null then 
--проверяем активна проверка или нет
if v_activ = 0 then v_fin := 'Вы запустили не активированную проверку.';
else
-- смотрим тип проверки
-- если тип 'tbl_rdv_s' 
	if v_tp = 'tbl_rdv_s'  then
-- проверяем заполненность v_jsn_chk
-- если есть список таблиц 
		if v_jsn_chk::text like '[{"tbl_nm":%' 
			then v_tbl_nm := 'select distinct count(*) cn, concat(schema_rdv,''.'',table_name_rdv) tbl_nm  
			from sys_dwh.prm_s2t_rdv where end_dt = ''9999-12-31'' and key_type_src like ''%PK%'' 
			and concat(schema_rdv,''.'',table_name_rdv) in (select tbl_nm from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (tbl_nm text))
			group by table_name_rdv, schema_rdv';
-- запускаем цикл обработки списка таблиц
		for v_rec_json_prm in execute v_tbl_nm
		loop
			if v_rec_json_prm.cn = 1   
			then v_tbl_nm := v_rec_json_prm.tbl_nm;
			v_i := v_i + 1; -- счетчик отработкок для логов                
-- узнаем PK таблицы
			execute 'select column_name_rdv from sys_dwh.prm_s2t_rdv where end_dt = ''9999-12-31'' and key_type_src like ''%PK%'' and concat(schema_rdv,''.'',table_name_rdv) = '''||v_tbl_nm||''';'
			into  v_tbl_pk ;
			end if;
			if v_rec_json_prm.cn > 1
			then v_tbl_pk := 'gk_pk';
			 v_tbl_nm := v_rec_json_prm.tbl_nm;
			end if;
-- подставляем значения выполняем запрос
		v_sql_chk_l := replace(replace(v_sql_chk, '{{tbl_rdv}}', v_tbl_nm), '{{pk_rdv}}', v_tbl_pk);
		execute v_sql_chk_l
		into v_rslt_chk;
			if v_rslt_chk is null then
			v_rslt_chk := 'Проверка выдала нулевой результат.';
			end if;
-- собираем id результата проверки rslt_num
		v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';
		execute  v_rslt_num_hash
		into v_rslt_num;
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (rslt_num, start_id, chk_num,schm, tp, sql_chk,eml_txt,eml_f) values ('''||v_rslt_num||''', '''||v_start_id||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_email||''','''||v_eml_f||''');';
		execute v_ins;
-- сохраняем результат в таблицу результатов
			if v_rslt_chk::int = 0 
			then  v_ok_txt_l := replace(v_ok_txt, '{{tbl_rdv}}', v_tbl_nm);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 1, rslt_chk_txt = '''||v_ok_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';';
			else  v_err_txt_l := replace(replace(v_err_txt, '{{tbl_rdv}}', v_tbl_nm),'{{sql_chk}}', v_sql_chk_h);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 0, rslt_chk_txt = '''||v_err_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';'; 
			end if;
		execute v_upd;
		end loop;
		end if;
		
-- если есть схема
		if v_jsn_chk::text like '[{"schm_rdv":%' 	
			then 	
-- забираем переменную схемы
		execute 'select schm_rdv from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (schm_rdv text)'
		into v_schm_rdv;	
-- запускаем цикл обработки списка таблиц
		for v_rec_json_prm in (select count(*) cn, concat(schema_rdv,'.',table_name_rdv) tbl_nm  
								from sys_dwh.prm_s2t_rdv where end_dt ='9999-12-31' and key_type_src like '%PK%' and schema_rdv = v_schm_rdv
								group by table_name_rdv, schema_rdv)								
		loop		
			if v_rec_json_prm.cn = 1
			then v_tbl_nm := v_rec_json_prm.tbl_nm;	
			v_i := v_i + 1; -- счетчик отработкок для логов           
-- узнаем PK таблицы
			execute 'select column_name_rdv from sys_dwh.prm_s2t_rdv where end_dt = ''9999-12-31'' and key_type_src like ''%PK%'' and concat(schema_rdv,''.'',table_name_rdv) = '''||v_tbl_nm||''';'
			into  v_tbl_pk ; 		
			end if;
			if v_rec_json_prm.cn > 1
			then v_tbl_nm := v_rec_json_prm.tbl_nm;		
				v_tbl_pk := 'gk_pk';  
			end if;
-- подставляем значения выполняем запрос
		v_sql_chk_l := replace(replace(v_sql_chk, '{{tbl_rdv}}', v_tbl_nm), '{{pk_rdv}}', v_tbl_pk);  
		execute v_sql_chk_l 
		into v_rslt_chk;	
			if v_rslt_chk is null then
			v_rslt_chk := 'Проверка выдала нулевой результат.';			
			end if;  
		v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');			
-- собираем id результата проверки rslt_num
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';	
		execute  v_rslt_num_hash
		into v_rslt_num;	
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_email||''','''||v_eml_f||''');';	
		execute v_ins;
-- сохраняем результат в таблицу результатов
			if v_rslt_chk::int = 0 
			then  v_ok_txt_l := replace(v_ok_txt, '{{tbl_rdv}}', v_tbl_nm);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 1, rslt_chk_txt = '''||v_ok_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';';
			else   v_err_txt_l := replace(replace(v_err_txt, '{{tbl_rdv}}', v_tbl_nm),'{{sql_chk}}', v_sql_chk_h);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 0, rslt_chk_txt = '''||v_err_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';'; 			
			end if;
		execute v_upd;				
		end loop;
		end if;
		
-- если нет v_jsn_chk 
		if v_jsn_chk is null  
-- определяем список таблиц s2t rdv (сателлиты)
		then 
-- запускаем цикл обработки списка таблиц
		for v_rec_json_prm in (select count(*) cn, concat(schema_rdv,'.',table_name_rdv) tbl_nm  
		from sys_dwh.prm_s2t_rdv where end_dt ='9999-12-31' and key_type_src like '%PK%'
		group by table_name_rdv, schema_rdv)
		loop
			if v_rec_json_prm.cn = 1
			then v_tbl_nm := v_rec_json_prm.tbl_nm;
			v_i := v_i + 1; -- счетчик отработкок для логов           
-- узнаем PK таблицы
			execute 'select column_name_rdv from sys_dwh.prm_s2t_rdv where end_dt = ''9999-12-31'' and key_type_src like ''%PK%'' and concat(schema_rdv,''.'',table_name_rdv) = '''||v_tbl_nm||''';'
			into  v_tbl_pk ; 
			end if;
			if v_rec_json_prm.cn > 1
			then v_tbl_nm := v_rec_json_prm.tbl_nm;
			v_tbl_pk := 'gk_pk';   
			end if;
-- подставляем значения выполняем запрос
		v_sql_chk_l := replace(replace(v_sql_chk, '{{tbl_rdv}}', v_tbl_nm), '{{pk_rdv}}', v_tbl_pk);  
		execute v_sql_chk_l 
		into v_rslt_chk;
			if v_rslt_chk is null then
			v_rslt_chk := 'Проверка выдала нулевой результат.';
			end if;  
		v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');
-- собираем id результата проверки rslt_num
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';
		execute  v_rslt_num_hash
		into v_rslt_num;
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_email||''','''||v_eml_f||''');';
		execute v_ins;
-- сохраняем результат в таблицу результатов
			if v_rslt_chk::int = 0 
			then  v_ok_txt_l := replace(v_ok_txt, '{{tbl_rdv}}', v_tbl_nm);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 1, rslt_chk_txt = '''||v_ok_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';';
			else   v_err_txt_l := replace(replace(v_err_txt, '{{tbl_rdv}}', v_tbl_nm),'{{sql_chk}}', v_sql_chk_h);
			v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 0, rslt_chk_txt = '''||v_err_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';'; 
			end if;
		execute v_upd;
		end loop;
		end if;
	end if; 
-- смотрим тип проверки
-- если тип 'func' 
	if v_tp = 'func'  then 
		if v_jsn_chk is not null then
		v_prm_func_tbl := 'select src_stm_id, concat(schema_rdv,''.'',table_name_rdv) tbl_nm  
		from sys_dwh.prm_s2t_rdv where end_dt = ''9999-12-31'' 
		and concat(schema_rdv,''.'',table_name_rdv) in (select tbl_nm from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (tbl_nm text))
		group by src_stm_id, schema_rdv, table_name_rdv
		union all
		select src_stm_id, concat(regexp_replace(schema_stg,''^stg'', ''sdv''),''.'',table_name_stg) tbl_nm  
		from sys_dwh.prm_s2t_stg_src where end_dt = ''9999-12-31''  
		and concat(regexp_replace(schema_stg,''^stg'', ''sdv''),''.'',table_name_stg) in (select tbl_nm from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (tbl_nm text))
		group by src_stm_id, schema_stg, table_name_stg';
-- подставляем значения выполняем запрос
		for v_rec_json_prm in execute v_prm_func_tbl
		loop
		v_prm_func := v_rec_json_prm.src_stm_id;
		v_sql_chk_l :=replace(v_sql_chk, '{{prm_func}}', v_prm_func);            
		execute v_sql_chk_l 
		into v_rslt_chk;
			if v_rslt_chk is null then
			v_rslt_chk := 'Проверка выдала нулевой результат.';
			end if; 
		v_rslt_chk := replace(v_rslt_chk,'''', '''''');
		v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');
-- собираем id результата проверки rslt_num
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';
		execute  v_rslt_num_hash
		into v_rslt_num;
-- сохраняем результат в таблицу результатов
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''',1,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';
		execute v_ins;
		end loop;
		else v_fin := 'Для запуска проверки заполните поле jsn_chk у проверки chk_num = '||p_chk_num;
		end if;
	end if;
	
	

-- смотрим тип проверки
-- если тип 'query' и 'query_report' 
	if v_tp = 'query' 
		or v_tp = 'query_report' 
			or v_tp = 'sla'
		then 
-- проверяем наличие переменной в скрипте
	if v_jsn_chk is not null then
		v_prm_func_tbl := 'select prm_query from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (prm_query text)';
		for v_rec_json_prm in execute v_prm_func_tbl
		loop 
		v_prm_query := v_rec_json_prm.prm_query;
-- подставляем значения выполняем запрос
		v_sql_chk_l :=replace(v_sql_chk, '{{prm_query}}', v_prm_query);
		execute v_sql_chk_l 
		into v_rslt_chk;
			if v_rslt_chk is null then
			v_rslt_chk := 'Проверка выдала нулевой результат.';
			end if; 
		v_rslt_chk := replace(v_rslt_chk,'''', '''''');
		v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');
-- собираем id результата проверки rslt_num
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';  
		execute  v_rslt_num_hash
		into v_rslt_num;
-- сохраняем результат в таблицу результатов 
			if v_rslt_chk = 'Проверка выдала нулевой результат.' then 
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''', 1,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';	
		else v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''', 0,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';	
		end if; 
		execute v_ins;	
		end loop;
	end if;

		if v_jsn_chk is null then execute v_sql_chk
		into v_rslt_chk;
		if v_rslt_chk is null then
		v_rslt_chk := 'Проверка выдала нулевой результат.';	
		else v_rslt_chk := replace(v_rslt_chk,'''', '''''');
		end if;
		v_sql_chk_h := replace(v_sql_chk,'''', '''''');
-- собираем id результата проверки rslt_num
		v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';  
		execute  v_rslt_num_hash
		into v_rslt_num;	
-- сохраняем результат в таблицу результатов 
			if v_rslt_chk = 'Проверка выдала нулевой результат.' then 
		v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''', 1,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';
	else v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''', 0,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';
	end if;		
execute v_ins;
		end if;
	end if;

	
	
-- смотрим тип проверки
-- если тип 'chk_func' 
	if v_tp = 'chk_func'  then 
		if v_jsn_chk is not null then		
-- собираем перечень кейсов
		v_prm_func_tbl := 'select case_num from json_populate_recordset(null::record,'''||v_jsn_chk||'''::json) as (case_num text);';
		for v_rec_json_prm in execute v_prm_func_tbl
		loop 
			v_case_num := v_rec_json_prm.case_num;
-- задаем переменные  
				select case_num, origl_tb, intr_tb, expc_tb, prm_func, pre_sql, actv_f
				into v_case_num, v_origl_tb, v_intr_tb, v_expc_tb, v_prm_func, v_pre_sql, v_actv_f  
				from  sys_tst_dwh.d_chk_case 
				where case_num = v_case_num ;				
-- проверяем активен ли кейс
-- если кейс не активен
			if v_actv_f = 0 then 
			v_rslt_chk := 'Кейс '||v_case_num||' не активен.';
			v_sql_chk_h := replace(v_sql_chk,'''', '''''');
-- собираем id результата проверки rslt_num
			v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';  
			execute  v_rslt_num_hash
			into v_rslt_num;
-- сохраняем результат в таблицу результатов 
			v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,rslt_chk_txt,rslt_chk_f, upd_dttm,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql_chk_h||''','''||v_rslt_chk||''',1,now()::timestamp,'''||v_email||''','''||v_eml_f||''');';
			execute v_ins;
			end if;
-- если кейс активен		
			if v_actv_f = 1 then 
-- проверяем заполнено ли v_pre_sql, если заполнен - выполняем
				if v_pre_sql is not null then 			
				execute v_pre_sql;
				else 
-- копируем исходные тестовые данные в промежуточную таблицу 
			v_ins := 'truncate '||v_intr_tb||'; insert into '||v_intr_tb||' select * from  '||v_origl_tb||';';
			execute v_ins;
				end if;
-- подставляем значения выполняем запрос
			v_sql_chk_l := replace(v_sql_chk, '{{prm_func}}', v_prm_func); 		
			execute v_sql_chk_l;	
-- сравниваем промежуточную таблицу и эталонную
			v_sql := 'select string_agg(column_name,'', '') from information_schema.columns where concat(table_schema,''.'',table_name)  = '''||v_intr_tb||''' and column_name <> ''dml_dttm'' and column_name <> ''dml_tp'' and column_name <> ''ssn'' and column_name <> ''upd_dttm'' and column_name <> ''act_dt'' and column_name <> ''hash_diff'';';
			execute v_sql
			into v_prm_query;	
			v_sql := 'select count(1) from ((select '||v_prm_query||' from '||v_intr_tb||' except select '||v_prm_query||' from '||v_expc_tb||') union all (select '||v_prm_query||' from '||v_expc_tb||' except select '||v_prm_query||' from '||v_intr_tb||')) t1;';
			execute v_sql
			into v_rslt_chk;	
-- собираем id результата проверки rslt_num
			v_rslt_chk := replace(v_rslt_chk,'''', '''''');
			v_sql_chk_h := replace(v_sql_chk_l,'''', '''''');
			v_rslt_num_hash := 'select md5('''||v_chk_num||'''||'''||v_schm||'''||'''||v_tp||'''||'''||v_sql_chk_h||'''||'||'(now()::timestamp)::text)::uuid';
			execute  v_rslt_num_hash
			into v_rslt_num;	
			v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (start_id, rslt_num, chk_num,schm, tp, sql_chk,eml_txt,eml_f) values ('''||v_start_id||''', '''||v_rslt_num||''', '''||v_chk_num||''','''||v_schm||''','''||v_tp||''','''||v_sql||''','''||v_email||''','''||v_eml_f||''');';
			execute v_ins;
-- сохраняем результат в таблицу результатов
				if v_rslt_chk::int = 0 
				then  v_ok_txt_l := replace(v_ok_txt, '{{case_num}}', v_case_num::text);
				v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 1, rslt_chk_txt = '''||v_ok_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';';		
				else   v_err_txt_l := replace(replace(v_err_txt, '{{case_num}}', v_case_num::text),'{{sql_chk}}',v_sql);
				v_upd := 'update sys_tst_dwh.rslt_chk_dwh set rslt_chk_f = 0, rslt_chk_txt = '''||v_err_txt_l||''', upd_dttm = now()::timestamp where rslt_num = '''||v_rslt_num||''';'; 		
				end if;
			execute v_upd;
			end if;
		end loop;
		else v_fin := 'Для запуска проверки заполните поле jsn_chk у проверки chk_num = '||p_chk_num;
		end if;
	end if;

if v_tp != 'query' and v_tp != 'query_report' and v_tp != 'sla' 
		then 
execute 'select count(*) from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||''';' 
into v_cnt;		
if v_cnt > 0 then
execute 'select count(*) from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||''' and rslt_chk_f = 0;'
into v_cnt;
	if v_cnt = 0 then
	v_fin := 'Проверка пройдена успешно. Результат: select * from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||'''' ;
	else execute 'select (json_agg(rslt_num))::text from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||''' and rslt_chk_f = 0;'
	into v_rslt;
	v_fin := 'Проверка не пройдена. ID не пройденых проверок(rslt_num): '||v_rslt||' Результат: select * from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||'''' ;
	end if;
else v_fin := 'Сбой проверки ID:'||v_chk_num||' наименование:"'||v_chk_nm||'". Результат проверки не записан. '||E'\n'||'Причина: или ошибка в проверке или в select sys_dwh.chk_strtr() нет алгоритма под заданную проверку.';
end if;
end if;
if v_tp = 'query_report' then v_fin := 'Результат проверки: select * from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||'''' ;
end if;
if v_tp = 'query' or v_tp = 'sla' then v_fin := v_rslt_chk||' Результат проверки: select * from sys_tst_dwh.rslt_chk_dwh where start_id = '''||v_start_id||'''' ;
end if;
end if;
else v_fin := 'Такой проверки не существует';
end if;
return v_fin;
EXCEPTION WHEN OTHERS  
THEN
    RAISE EXCEPTION 'ERROR CODE: %. MESSAGE TEXT: %', SQLSTATE, SQLERRM;
/*	v_ins := 'insert into sys_tst_dwh.rslt_chk_dwh (rslt_num, upd_dttm, rslt_chk_txt) values (''-1'', now()::timestamp, ERROR CODE: '''||SQLSTATE||''' MESSAGE TEXT: '''||SQLERRM||''');';
	execute v_ins;*/
end; 





$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.cln_sdv_prttn_dbls(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.cln_sdv_prttn_dbls(p_src_stm_id int4, p_json text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

	

	

declare

v_rec_json_ssn record;

v_ddl text;

v_tmp_tbl text;

v_tbl_nm text;

v_sch_nm text;

v_prttn_tbl_nm text;

v_prttn_sch_nm text;

v_prttn text;

v_ssn int;

v_owner text;

v_answr text;

v_cnt int4;

v_json_ret text = '';

v_json_ssn text;

v_tmp_tbl_d text;

v_tmp_tbl_i text;

v_start_dttm text;

v_dist_clmn text;

v_dist text;

v_output_text text;



--Проверяем наличие sys_dwh.prm_src_stm в БД

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(1) into v_cnt from pg_catalog.pg_tables pt 

where lower(pt.schemaname) = 'sys_dwh'

and lower(pt.tablename) = 'prm_src_stm';

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Поиск prm_src_stm',

p_start_dttm := v_start_dttm,

p_val := 'v_cnt='''||v_cnt||'''',

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

if v_cnt = 0 then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Поиск prm_src_stm',

p_start_dttm := v_start_dttm,

p_err := 'The table sys_dwh.prm_src_stm is missing on DB',

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end if;



--Проверяем наличие sys_dwh.prm_s2t_stg в БД

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(1) into v_cnt from pg_catalog.pg_tables 

where lower(schemaname) = 'sys_dwh'

and lower(tablename) = 'prm_s2t_stg';

if v_cnt = 0 then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '2',

p_descr := 'Поиск метаданных',

p_start_dttm := v_start_dttm,

p_err := 'The table sys_dwh.prm_s2t_stg is missing on DB',

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end if;



--Проверяем наличие таблицы в sys_dwh.prm_src_stm

select sys_dwh.prv_tbl_id(p_src_stm_id) into v_answr;



--Проверяем наличие таблицы в sys_dwh.prm_s2t_stg 

select count(1) into v_cnt from sys_dwh.prm_s2t_stg t

where src_stm_id = p_src_stm_id;

if v_cnt = 0 then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '2',

p_descr := 'Поиск метаданных',

p_start_dttm := v_start_dttm,

p_err := 'The p_src_stm_id is missing as table in sys_dwh.prm_s2t_stg',

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end if;



--Фиксируем имя и схему таблицы

select 

'sdv_'||lower(pr.nm) scheme_name,

lower(substring(ch.nm from 1 for 50)) table_name

into v_sch_nm, v_tbl_nm

from sys_dwh.prm_src_stm as ch

join sys_dwh.prm_src_stm as pr

on ch.prn_src_stm_id = pr.src_stm_id 

where ch.src_stm_id = p_src_stm_id;	



--Фиксируем владельца таблицы

select pt.tableowner into v_owner from pg_catalog.pg_tables pt 

where lower(schemaname) = v_sch_nm and lower(tablename) = v_tbl_nm;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '2',

p_descr := 'Поиск метаданных',

p_start_dttm := v_start_dttm,

p_val := 'v_answr='''||v_answr||''', v_cnt='''||v_cnt||''', v_sch_nm='''||v_sch_nm||''', v_tbl_nm='''||v_tbl_nm||''', v_owner='''||v_owner||'''',

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '2',

p_descr := 'Поиск метаданных',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Фиксируем значение ssn

if p_json is null then 

execute 'select max(ssn) from '||v_sch_nm||'.'||v_tbl_nm into v_ssn;

v_json_ssn := '[{"ssn":"'||v_ssn::text||'"}]';

else

v_json_ssn := p_json;

end if;

for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,v_json_ssn::json)

as (ssn int))

loop

v_ssn := v_rec_json_ssn.ssn;



 --Проверяем отсутствие  партиции в  таблице источнике

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(1)  

into v_cnt

from pg_catalog.pg_partitions pp

where lower(pp.schemaname) = v_sch_nm

and lower(pp.tablename) = v_tbl_nm

and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_ssn;

if v_cnt = 0 then

v_output_text := 'Warning: Партиция '||v_ssn||' отсутствует!';

   select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

	p_descr := 'Проверка наличия партиции',

	p_val :=v_output_text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

else 

v_output_text := 'Success: партиция '||v_ssn||' существует.';

   select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

	p_descr := 'Проверка наличия партиции',

	p_val :=v_output_text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

--Фиксируем имя и схему партиции

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select pp.partitionschemaname, pp.partitiontablename, pp.partitionschemaname||'.'||pp.partitiontablename as prttn

into v_prttn_sch_nm, v_prttn_tbl_nm, v_prttn

from pg_catalog.pg_partitions pp

where lower(pp.schemaname) = v_sch_nm

and lower(pp.tablename) = v_tbl_nm

and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_ssn;



--Фиксируем имена временных таблиц

v_tmp_tbl_d := v_tbl_nm||to_char(now(),'ddHH24miSS')||v_ssn::text||'_d';

v_tmp_tbl_i := v_tbl_nm||to_char(now(),'ddHH24miSS')||v_ssn::text||'_i';

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '4',

p_descr := 'Фиксируем имя и схему партиции и временных таблицы',

p_start_dttm := v_start_dttm,

p_val := 'v_prttn_sch_nm='''||v_prttn_sch_nm||''', v_prttn_tbl_nm='''||v_prttn_tbl_nm||''', v_prttn='''||v_prttn||''', v_tmp_tbl='''||v_tmp_tbl||'''',

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '4',

p_descr := 'Фиксируем имя и схему партиции и временных таблицы',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Фиксируем дистрибьюцию таблицы

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select into v_dist_clmn

case 

when lower(pg_get_table_distributedby(c.oid)) in ('distributed randomly', 'distributed replicated')

then (select string_agg(column_name_stg, ', ') from sys_dwh.prm_s2t_stg where src_stm_id = p_src_stm_id and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt)

else replace(replace(lower(pg_get_table_distributedby(c.oid)), 'distributed by (', ''), ')', '')

end as dist

from pg_class as c

inner join pg_namespace as n

on c.relnamespace = n.oid

where 1=1

and c.relname = v_tbl_nm

and n.nspname = v_sch_nm;

select into v_dist

lower(pg_get_table_distributedby(c.oid))

from pg_class as c

inner join pg_namespace as n

on c.relnamespace = n.oid

where 1=1

and c.relname = v_tbl_nm

and n.nspname = v_sch_nm;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '5',

p_descr := 'Фиксируем дистрибьюцию таблицы',

p_start_dttm := v_start_dttm,

p_val := 'v_ddl='''||v_ddl||'''',

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '5',

p_descr := 'Фиксируем дистрибьюцию таблицы',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Создаем временную таблицу с дублями

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

v_ddl := 'create temporary table '||v_tmp_tbl_d||

' as select '||v_dist_clmn||' 

from '||v_prttn||'

group by '||v_dist_clmn||'

having count(1) > 1

'||v_dist;

execute v_ddl;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '6',

p_descr := 'Создаем временную таблицу с дублями',

p_start_dttm := v_start_dttm,

p_val := v_ddl, --'alter table '||v_tmp_tbl||' owner to '||v_owner,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '6',

p_descr := 'Создаем временную таблицу с дублями',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_val := v_ddl,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Заливаем во временную таблицу уникальные значения

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select 'create temporary table '||v_tmp_tbl_i||' as '||string_agg(sel.ddl_text, '')||

' '||v_dist into v_ddl 

from 

(select 'select '||string_agg(ff.ddl_text, ' ')||' from ' as ddl_text  

from

(select i.column_name||(case when i.ordinal_position <> 

(select max(ic.ordinal_position) from information_schema.columns ic where ic.table_schema = v_sch_nm and ic.table_name = v_tbl_nm) 

then ',' else '' end) as ddl_text, i.ordinal_position

from information_schema.columns i 

where i.table_schema = v_sch_nm and i.table_name = v_tbl_nm

order by i.ordinal_position) as ff 

union all

select '(select t.*, row_number() over (partition by t.'||

replace(v_dist_clmn, ', ', ', t.')

||' order by t.act_dt) as tmp_'||v_tbl_nm

||' from '||v_prttn||' as t

inner join '||v_tmp_tbl_d||' s on 1=1 '||(select coalesce(

(select string_agg('and s.'||pga.attname||'= t.'||pga.attname, E'\n')

from (select gdp.localoid,

unnest(gdp.distkey) attnum

from gp_distribution_policy gdp) as distrokey

join pg_class as pgc

on distrokey.localoid = pgc.oid

join pg_namespace pgn

on pgc.relnamespace = pgn.oid

left join pg_attribute pga

on distrokey.attnum = pga.attnum

and distrokey.localoid = pga.attrelid

where pgn.nspname = v_sch_nm

and pgc.relname = v_tbl_nm),

(select string_agg('and s.'||column_name_stg||'= t.'||column_name_stg, E'\n') 

from sys_dwh.prm_s2t_stg 

where src_stm_id = p_src_stm_id and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt)))

||') as tmp where tmp_'||v_tbl_nm||'= 1' as ddl_text

) as sel;

raise notice '%', v_ddl;

execute v_ddl;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '7',

p_descr := 'Заливаем во временную таблицу уникальные значения',

p_start_dttm := v_start_dttm,

p_val := 'v_ddl='''||v_ddl||'''',

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '7',

p_descr := 'Заливаем во временную таблицу уникальные значения',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Удаляем данные из партиции

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select 'delete from '||v_prttn||' t using '||v_tmp_tbl_d||' s 

where 1= 1 '||(select coalesce(

(select string_agg('and s.'||pga.attname||'= t.'||pga.attname, E'\n')

from (select gdp.localoid,

unnest(gdp.distkey) attnum

from gp_distribution_policy gdp) as distrokey

join pg_class as pgc

on distrokey.localoid = pgc.oid

join pg_namespace pgn

on pgc.relnamespace = pgn.oid

left join pg_attribute pga

on distrokey.attnum = pga.attnum

and distrokey.localoid = pga.attrelid

where pgn.nspname = v_sch_nm

and pgc.relname = v_tbl_nm),

(select string_agg('and s.'||column_name_stg||'= t.'||column_name_stg, E'\n') 

from sys_dwh.prm_s2t_stg 

where src_stm_id = p_src_stm_id and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt)))

into v_ddl;

raise notice '%', v_ddl;

execute v_ddl;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '8',

p_descr := 'Удаляем задублированные данные из партиции',

p_start_dttm := v_start_dttm,

p_val :=v_ddl,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '8',

p_descr := 'Удаляем задублированные данные из партиции',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;



--Вставляем уникальные данные

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select 'insert into '||v_prttn||' select * from '||v_tmp_tbl_i into v_ddl;

raise notice '%', v_ddl;

execute v_ddl;select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '9',

p_descr := 'Вставляем уникальные данные',

p_start_dttm := v_start_dttm,

p_val := v_ddl,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '9',

p_descr := 'Вставляем уникальные данные',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Собираем статистику

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

execute 'analyze '||v_sch_nm||'.'||v_tbl_nm;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '10',

p_descr := 'Сбор статистики',

p_start_dttm := v_start_dttm,

p_val := 'analyze '||v_sch_nm||'.'||v_tbl_nm,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '10',

p_descr := 'Сбор статистики',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end if;



exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '3',

p_descr := 'Проверка наличия партиции',

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;



end;

end loop;

v_json_ret := v_json_ret||']';

return(v_json_ret); 

--Регистрируем ошибки

exception

when others then

if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

v_json_ret := v_json_ret||']';

end if;

raise exception '%', v_json_ret;

end;








$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.crt_prttrn_hub(text);

CREATE OR REPLACE FUNCTION sys_dwh.crt_prttrn_hub(p_json_param text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	declare
		v_src_stm_id_ref bigint; 
		v_ref_to_hub text; 
		
		v_cnt bigint;
		v_json_ret text = '';
		
		
		v_exec_part_sql text;
		v_exec_sql text;
		v_where_sql_1 text = '';
		v_start_dttm text;

		err_code text; -- код ошибки
		msg_text text; -- текст ошибки
		exc_context text; -- контекст исключения
		msg_detail text; -- подробный текст ошибки
		exc_hint text; -- текст подсказки к исключению
	begin
		--Парсим json
		if (p_json_param is not null) then
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			for v_src_stm_id_ref, v_ref_to_hub in (
			select 
				j_src_stm_id_ref, 
				j_ref_to_hub
				from json_populate_recordset(null::record,
			p_json_param::json)
			as (j_src_stm_id_ref text, 
				j_ref_to_hub text)) loop
				v_where_sql_1 := v_where_sql_1 || case when v_ref_to_hub is not null then '(ss.ref_to_hub = '''||v_ref_to_hub||'''' else ' (1=1 ' end || case when v_src_stm_id_ref is not null then 'and ss.src_stm_id = '||v_src_stm_id_ref ||')'else ')' end ||'
		or';
				end loop;
				v_where_sql_1 := RTRIM(v_where_sql_1,'or');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'p_json_param='''||p_json_param::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		end if;
		--Собираем запрос для цикла
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			v_exec_sql := 'select ss.ref_to_hub, ss.src_stm_id from (select distinct r.ref_to_hub, ps.src_stm_id 
       from sys_dwh.prm_s2t_rdv r
             join sys_dwh.prm_src_stm ps on ps.nm = split_part(r.ref_to_stg, ''.'',2)
                                                             and ps.prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
                                                                                                     where nm = regexp_replace(split_part(r.ref_to_stg, ''.'',1),''^stg_'', ''''))
             where r.column_name_rdv like ''gk_%''
                    and r.ref_to_stg not like ''[%''
             and r.ref_to_hub <> ''''
             and daterange(r.eff_dt,r.end_dt,''[]'')@>(now() at time zone ''utc'')::date
union
select distinct ref_to_hub, ps.src_stm_id
       from 
(select distinct t.value ->> ''ref_to_hub'' as ref_to_hub,
       t.value ->> ''tbl_nm'' as ref_to_stg
       --ps.src_stm_id 
       from sys_dwh.prm_s2t_rdv r
             LEFT JOIN LATERAL json_array_elements(r.ref_to_stg::json) t(value) ON (true)
             where
             r.column_name_rdv like ''gk_%''
             and r.ref_to_stg like ''[%''
             and daterange(r.eff_dt,r.end_dt,''[]'')@>(now() at time zone ''utc'')::date ) s             
             
             join sys_dwh.prm_src_stm ps on ps.nm = split_part(s.ref_to_stg, ''.'',2)
                                                             and ps.prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
                                                                                                     where nm = regexp_replace(split_part(s.ref_to_stg, ''.'',1),''^stg_'', ''''))
      where ps.src_stm_id = ps.src_stm_id
       
union
select distinct ''rdv.h_''||(regexp_split_to_array(trim(table_name_rdv), ''\_+''))[2] ref_to_hub,
       ps.src_stm_id
       from sys_dwh.prm_s2t_rdv r
             join sys_dwh.prm_src_stm ps on ps.nm = r.table_name_stg
                                                             and ps.prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
                                                                                                     where nm = regexp_replace(r.schema_stg,''^stg_'', ''''))
       where daterange(r.eff_dt,r.end_dt,''[]'')@>(now() at time zone ''utc'')::date
             and substring(trim(table_name_rdv) from 1 for 2) <> ''l_''
                    and trim(key_type_src) like ''%PK%'') ss
					'|| (case when p_json_param is not null then 'where '||v_where_sql_1 else '' end) ||';';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Создание запроса для цикла',
				p_start_dttm := v_start_dttm,
				p_val := 'p_json_param='''||p_json_param::text||''' v_exec_sql='''|| v_exec_sql::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Создание запроса для цикла',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		raise notice '%', v_exec_sql;
		--открываем цикл для создания партиций
		for v_ref_to_hub, v_src_stm_id_ref in execute v_exec_sql loop
		raise notice '%', v_src_stm_id_ref;
		--Проверяем наличие партиции в целевой таблице
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
		  select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 
			into v_cnt
			from pg_catalog.pg_partitions pp
				where lower(pp.schemaname) = 'rdv'
					and lower(pp.tablename) = regexp_replace(v_ref_to_hub,'^rdv.', '')
					and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Поиск партиции в целевой таблице',
				p_start_dttm := v_start_dttm,
				p_val := 'cnt='''||v_cnt::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Поиск партиции в целевой таблице',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		if v_cnt = 0 then
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				--Создаем новую партицию
				v_exec_part_sql := 'alter table '||v_ref_to_hub||' add partition start ('||
				v_src_stm_id_ref||') inclusive end ('||(v_src_stm_id_ref::bigint+1)::text||') exclusive with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=5)';
				execute v_exec_part_sql;
				execute 'analyze ' || v_ref_to_hub;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '3',
					p_descr := 'Создание новой партиции',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_part_sql='''||v_exec_part_sql||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '3',
					p_descr := 'Создание новой партиции',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					--p_cls := ']',
					p_debug_lvl := '1')
					into v_json_ret;
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					PERFORM pg_sleep(240);
					select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 
					into v_cnt
					from pg_catalog.pg_partitions pp
						where lower(pp.schemaname) = 'rdv'
							and lower(pp.tablename) = regexp_replace(v_ref_to_hub,'^rdv.', '')
							and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '4',
									p_descr := 'Повторный поиск партиции в целевой таблице',
									p_start_dttm := v_start_dttm,
									p_val := 'cnt='''||v_cnt::text||'''',
									p_log_tp := '1',
									p_debug_lvl := '2')
								into v_json_ret;
				exception when others then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '4',
									p_descr := 'Повторный поиск партиции в целевой таблице',
									p_start_dttm := v_start_dttm,
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
								into v_json_ret;
				end;
				if v_cnt = 0 then
					raise exception '%', v_json_ret;
				end if;
			end;
		end if; 
		end loop;
		v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.crt_task(text);

CREATE OR REPLACE FUNCTION sys_dwh.crt_task(p_json_param text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	declare
		v_rec_sql record;
	
		v_src_stm_id bigint;
		v_type_of_changes text; 
		v_tbl_nm_sdv text; 
		v_tbl_nm_rdv text;
		v_sql text;
	
		v_start_dttm text;
		v_json_ret text;
	
		err_code text; -- код ошибки
		msg_text text; -- текст ошибки
		exc_context text; -- контекст исключения
		msg_detail text; -- подробный текст ошибки
		exc_hint text; -- текст подсказки к исключению
	begin
		--Парсим json
		
		begin
			v_start_dttm := clock_timestamp() at time zone 'utc';
			if p_json_param is null then
		
				v_sql := 'select src_stm_id,
						tbl_nm_rdv,
						max(change_ref_to_stg)::boolean change_ref_to_stg,
						max(change_algorithm)::boolean change_algorithm,
						max(add_column_rdv)::boolean add_column_rdv,
						false::boolean change_hash_diff,
						STRING_AGG(column_name_stg, '', '') columns
						from (
							select src_stm_id,
									tbl_nm_rdv,
									case when lag_ref_to_stg <> ref_to_stg and lag_ref_to_stg is not null then 1 else 0 end change_ref_to_stg,
									case when lag_algorithm <> algorithm and lag_algorithm is not null  then 1 else 0 end change_algorithm,
									case when lag_column_name_rdv = '''' and column_name_rdv <> '''' then 1 else 0 end add_column_rdv,
									column_name_stg
								from (
									select src_stm_id,
										schema_rdv||''.''||table_name_rdv tbl_nm_rdv,
										column_name_stg,
										c.ordinal_position,
										LAG(ref_to_stg, 1) OVER (PARTITION BY src_stm_id, schema_rdv||''.''||table_name_rdv, column_name_stg, c.ordinal_position ORDER BY eff_dt) lag_ref_to_stg,
										ref_to_stg,
										LAG(column_name_rdv, 1) OVER (PARTITION BY src_stm_id, schema_rdv||''.''||table_name_rdv, column_name_stg ORDER BY eff_dt) lag_column_name_rdv,
										column_name_rdv,
										LAG(algorithm, 1) OVER (PARTITION BY src_stm_id, schema_rdv||''.''||table_name_rdv, column_name_stg, c.ordinal_position ORDER BY eff_dt) lag_algorithm,
										algorithm,
										min (eff_dt) over (partition by src_stm_id, schema_rdv||''.''||table_name_rdv ORDER BY eff_dt) start_date,
										max (eff_dt) over (partition by src_stm_id, schema_rdv||''.''||table_name_rdv ORDER BY eff_dt) upd_date,
										eff_dt,end_dt
									from sys_dwh.prm_s2t_rdv r
										left join 
										information_schema.columns c on c.table_schema = r.schema_rdv and c.table_name = r.table_name_rdv and c.column_name = r.column_name_rdv
									--where src_stm_id = 20011
										) t
									
							where start_date <> now()::date at time zone ''utc'' and daterange(eff_dt,end_dt,''[]'')@>(now() at time zone ''utc'')::date
								and upd_date = now()::date at time zone ''utc''
								) tt
					where  change_ref_to_stg =1 or
						change_algorithm =1 or
						add_column_rdv =1
				
				group by src_stm_id,
						tbl_nm_rdv';
		
			elsif p_json_param is not null and p_json_param like '[%' then
				v_sql := 'select distinct 
						j_src_stm_id::bigint src_stm_id,
						j_tbl_nm_rdv::text tbl_nm_rdv,
						case when j_type_of_changes = ''change_ref_to_stg'' then true else false end change_ref_to_stg,
						case when j_type_of_changes = ''change_algorithm'' then true else false end change_algorithm,
						case when j_type_of_changes = ''add_column_rdv'' then true else false end add_column_rdv,
						case when j_type_of_changes = ''change_hash_diff'' then true else false end change_hash_diff,
						
						j_columns::text columns
					from
					(select t.value ->> ''j_src_stm_id'' as j_src_stm_id,
						t.value ->> ''j_tbl_nm_rdv'' as j_tbl_nm_rdv,
						t.value ->> ''j_type_of_changes'' as j_type_of_changes,
						t.value ->> ''j_columns'' as j_columns
					from json_array_elements('''||p_json_param||'''::json) t(value)) r';
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '1',
					p_descr := 'Получение запроса',
					p_start_dttm := v_start_dttm,
					/*p_val := 'v_src_stm_id='''||coalesce(v_src_stm_id::text,'')||''',
						v_type_of_changes='''||coalesce(v_type_of_task::changes,'')||''',
						v_tbl_nm_sdv='''||coalesce(v_tbl_nm_sdv::text,'')||''',
						v_tbl_nm_rdv='''||coalesce(v_tbl_nm_rdv::text,'')||'''',*/
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			else
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '1',
					p_descr := 'Получение параметров',
					p_start_dttm := v_start_dttm,
					p_val := 'Параметры не найдены',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			end if;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		for v_rec_sql in execute v_sql 
		loop
		--Создаём задания для change_hash_diff
			if v_rec_sql.change_hash_diff then
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					insert into sys_dwh.prm_task 
						with sel as
							(select unnest(array['rename_backup', 'create_new_with_like', 'set_owner', 'master_master', /*'updt_load_mode',*/ /*'copy_from_backup',*/'change_hash_diff']) 
								as type_of_task)
							select md5((now()::date)::text||schema_rdv||'_'||table_name_rdv||'change_hash_diff')::uuid as id_task,
							now()::date as dt,
							schema_rdv||'.'||table_name_rdv as table_name,
							(select tableowner from pg_catalog.pg_tables where schemaname||'.'||tablename = schema_rdv||'.'||table_name_rdv) as owner_rdv_table, 
							null as column_name,
							schema_rdv||'.'||table_name_rdv||to_char(now()::date, '_backup_yyyymmdd') as backup_table_name,
							type_of_task as type_of_task,
							'change_hash_diff' as type_of_changes,
							false as success_fl,
							null as dttm_start,
							null as dttm_end,
							true as active_fl
						from sys_dwh.prm_s2t_rdv_rule pstrr 
							join sel on true
						where schema_rdv||'.'||table_name_rdv = v_rec_sql.tbl_nm_rdv
							and pstrr.end_dt = '9999-12-31';
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '2',
							p_descr := 'Создание заданий для change_hash_diff',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '2',
						p_descr := 'Создание заданий для change_hash_diff',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		--Создаём задания для change_ref_to_stg
			if v_rec_sql.change_ref_to_stg then
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					insert into sys_dwh.prm_task 
						with sel as
							(select unnest(array['rename_backup', 'create_new_with_like', 'set_owner', 'master_master', 'updt_load_mode', 'put_bk_from_hub','change_ref_to_stg','change_hash_diff']) 
								as type_of_task)
							select md5((now()::date)::text||schema_rdv||'_'||table_name_rdv||'change_ref_to_stg')::uuid as id_task,
							now()::date as dt,
							schema_rdv||'.'||table_name_rdv as table_name,
							(select tableowner from pg_catalog.pg_tables where schemaname||'.'||tablename = schema_rdv||'.'||table_name_rdv) as owner_rdv_table, 
							v_rec_sql.columns as column_name,
							schema_rdv||'.'||table_name_rdv||to_char(now()::date, '_backup_yyyymmdd') as backup_table_name,
							type_of_task as type_of_task,
							'change_ref_to_stg' as type_of_changes,
							false as success_fl,
							null as dttm_start,
							null as dttm_end,
							true as active_fl
						from sys_dwh.prm_s2t_rdv_rule pstrr 
							join sel on true
						where schema_rdv||'.'||table_name_rdv = v_rec_sql.tbl_nm_rdv;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '3',
							p_descr := 'Создание заданий для change_ref_to_stg',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '3',
						p_descr := 'Создание заданий для change_ref_to_stg',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		--Создаём задания для add_column_rdv
			if v_rec_sql.add_column_rdv then
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					insert into sys_dwh.prm_task 
						with sel as
							(select unnest(array['rename_backup', 'create_new_with_like', 'set_owner', 'master_master', 'updt_load_mode', 'add_column_rdv','change_hash_diff']) 
								as type_of_task)
							select md5((now()::date)::text||schema_rdv||'_'||table_name_rdv||'add_column_rdv')::uuid as id_task,
							now()::date as dt,
							schema_rdv||'.'||table_name_rdv as table_name,
							(select tableowner from pg_catalog.pg_tables where schemaname||'.'||tablename = schema_rdv||'.'||table_name_rdv) as owner_rdv_table, 
							v_rec_sql.columns as column_name,
							schema_rdv||'.'||table_name_rdv||to_char(now()::date, '_backup_yyyymmdd') as backup_table_name,
							type_of_task as type_of_task,
							'add_column_rdv' as type_of_changes,
							false as success_fl,
							null as dttm_start,
							null as dttm_end,
							true as active_fl
						from sys_dwh.prm_s2t_rdv_rule pstrr 
							join sel on true
						where schema_rdv||'.'||table_name_rdv = v_rec_sql.table_nm_rdv;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '4',
							p_descr := 'Создание заданий для add_column_rdv',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '4',
						p_descr := 'Создание заданий для add_column_rdv',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		--Создаём задания для изменений в поле алгоритм
			if v_rec_sql.change_algorithm then
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					insert into sys_dwh.prm_task 
						with sel as
							(select unnest(array['rename_backup', 'create_new_with_like', 'set_owner', 'master_master', 'updt_load_mode', 'change_algorithm','change_hash_diff']) 
								as type_of_task)
							select md5((now()::date)::text||schema_rdv||'_'||table_name_rdv||'change_algorithm')::uuid as id_task,
							now()::date as dt,
							schema_rdv||'.'||table_name_rdv as table_name,
							(select tableowner from pg_catalog.pg_tables where schemaname||'.'||tablename = table_name_rdv) as owner_rdv_table, 
							v_rec_sql.columns as column_name,
							schema_rdv||'.'||table_name_rdv||to_char(now()::date, '_backup_yyyymmdd') as backup_table_name,
							type_of_task as type_of_task,
							'change_algorithm' as type_of_changes,
							false as success_fl,
							null as dttm_start,
							null as dttm_end,
							true as active_fl
						from sys_dwh.prm_s2t_rdv_rule pstrr 
							join sel on true
						where schema_rdv||'.'||table_name_rdv = v_rec_sql.table_nm_rdv;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '2',
							p_descr := 'Создание заданий для change_algorithm',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '2',
						p_descr := 'Получение параметров',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		end loop;
	return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.dbz_crt(int4, timestamp, bool);

CREATE OR REPLACE FUNCTION sys_dwh.dbz_crt(p_src_stm_id int4, p_dml_dttm_border timestamp, debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_load_mode int;

v_table_name_src varchar(50);

v_table_name_stg varchar(50);

v_output_text text;

v_json_ret text := '';

v_start_dttm text;

v_end_dttm text;

v_cdc_tp text;

v_template_mv text; --шаблон для mv - куда попадают данные из общей таблицы

v_template_v text; -- шаблон для view, которая преобразует данные из mv в вид, готовый для занесения в stg

v_source_name text; --имя источника (geb_3card, skb_ds и т.п.)v_e

v_fields_body text; --поля, вытащенные из json

v_ddl_crt text; --финальный скрипт

v_mv_name text;

v_view_name text;

v_pk text;

begin

	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	

	v_mv_name :=

	$query_v_mv_name$src_{{ v_source_name }}.vm_dbz_{{ v_table_name_stg }}$query_v_mv_name$;

	

	v_view_name := 

	$query_v_view_name$stg_{{ v_source_name }}.v_dbz_{{ v_table_name_stg }}$query_v_view_name$;	

	

	v_template_mv :=

	$query_v_template_mv$	

		DROP MATERIALIZED VIEW IF EXISTS {{ v_mv_name }} CASCADE;

		CREATE MATERIALIZED VIEW {{ v_mv_name }} AS 

		SELECT

		                CASE

		                    WHEN d.op = 'd'::text THEN d.before

		                    ELSE d.after

		                END AS json_value,

		                d.scn,

		                d.commit_scn,

		                d.offset,

		                CASE

		                    WHEN d.op = 'd'::text THEN 'D'::text

		                    WHEN d.op = 'c'::text THEN 'I'::text

		                    WHEN d.op = 'u'::text THEN 'U'::text

		                    ELSE d.op

		                END AS dml_tp,

		            dml_dttm,

		            hashdiff 

		           FROM src_{{ v_source_name }}.dbz_kafka_topic_listener d

		          WHERE d."table" = '{{ v_table_name_src }}'

		and dml_dttm > '{{ p_dml_dttm_border }}'

		WITH NO DATA

		DISTRIBUTED BY (hashdiff);

		

		GRANT SELECT ON TABLE {{ v_mv_name }} TO reader_stg_geb_3card;

	$query_v_template_mv$;	

	

	v_template_v := 

	$query_v_template_v$

	    DROP VIEW IF EXISTS {{ v_view_name }};

		CREATE OR REPLACE VIEW {{ v_view_name }}

		AS 

		select * from (

		select *,

		row_number() over (partition by {{ v_pk }} order by vp.commit_scn desc, vp.offset desc ) rnb

		from (SELECT

		{{ v_fields_body }},

		v.dml_tp,

		v.dml_dttm,

		v.commit_scn,

		v.offset

		FROM {{ v_mv_name }} v) vp) vr

		where vr.rnb = 1;

		

		GRANT SELECT ON {{ v_view_name }} TO reader_stg_geb_3card;  

	$query_v_template_v$;	



	--получение текущего lm

	 select load_mode into v_load_mode from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

	--получаем текущий cdc_tp

	 select cdc_tp into v_cdc_tp from sys_dwh.prm_s2t_stg_src pstss where pstss.src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

	--Проверяем наличие внутренних источников данных

	if v_load_mode != 2 and v_cdc_tp != 'dbz' then --если schema_src = schema_stg или сейчас будет загрузка через cdc dbz 

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,p_step := '1'::text,p_descr := ('У src_stm_id = % не выставлен признак dbz в таблице sys_dwh.prm_s2t_stg_src в поле cdc_tp, либо текущий load_mode не равен 2 ',p_src_stm_id::text)::text,p_start_dttm := v_start_dttm,p_val := 'v_ddl_crt='''||v_ddl_crt||'''',p_debug_lvl := '1') into v_json_ret;			

		return(v_json_ret||']');		

	else

	

		select pp.nm

		into v_source_name /*источник*/

		from sys_dwh.prm_src_stm p join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id and pp.end_dt = '9999-12-31'

		where p.src_stm_id = p_src_stm_id and p.end_dt = '9999-12-31';

	

	    select table_name_src,table_name_stg

		into v_table_name_src,v_table_name_stg /*таблица источника, таблица stg*/

		from sys_dwh.prm_s2t_stg_src

		where src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

	

	    /*Получение имен колонок как на источнике и типов данных stg*/

	    select sys_dwh.get_s2t_stg_clmntp_dbz(p_src_stm_id)

		into v_fields_body;

	

		/*Получение pk*/

		select pk_src from sys_dwh.prm_s2t_stg_src where src_stm_id = p_src_stm_id

						and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

		into v_pk;

		if v_pk is null then

			select replace(replace(array_agg(t.column_name_src order by t.column_order_src)::text, '{', ''), '}', '') 

			from (select distinct column_name_src, column_order_src from sys_dwh.prm_s2t_stg

				where src_stm_id = p_src_stm_id	

				and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date) t

			into v_pk;

		end if;

		v_pk := '"'||replace(v_pk,',','","')||'"';

		/* формирование скрипта mv */

		v_mv_name:=replace(v_mv_name,'{{ v_table_name_stg }}', v_table_name_stg);	   

		v_mv_name:=replace(v_mv_name,'{{ v_source_name }}', v_source_name);

		

		v_view_name:=replace(v_view_name,'{{ v_table_name_stg }}', v_table_name_stg);

		v_view_name:=replace(v_view_name,'{{ v_source_name }}', v_source_name);

		

		

		v_template_mv:=replace(v_template_mv,'{{ v_mv_name }}', v_mv_name);

	    v_template_mv:=replace(v_template_mv,'{{ v_table_name_src }}', v_table_name_src);	

	    v_template_mv:=replace(v_template_mv,'{{ v_table_name_stg }}', v_table_name_stg);

	    v_template_mv:=replace(v_template_mv,'{{ v_source_name }}',v_source_name);	    

	   	v_template_mv:=replace(v_template_mv,'{{ p_dml_dttm_border }}', p_dml_dttm_border::text);	   



	   	/* формирование скрипта v */

	    v_template_v:=replace(v_template_v,'{{ v_view_name }}',v_view_name);

	    v_template_v:=replace(v_template_v,'{{ v_mv_name }}',v_mv_name);

	    v_template_v:=replace(v_template_v,'{{ v_fields_body }}',v_fields_body);

	   	v_template_v:=replace(v_template_v,'{{ v_pk }}',v_pk);

	   

 	    v_ddl_crt:=v_template_mv || ' ' || v_template_v;



	    --Проверяем режим откладки

		if debug = true then

			return(v_ddl_crt);

			else

			--Выполняем DDL команды			

				execute v_ddl_crt;

			--Фиксируем лог

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,p_step := '1',p_descr := '{"v_mv_name":"'||v_mv_name||'", "v_view_name": "'||v_view_name || '", "dml_dttm_border":"'||p_dml_dttm_border::text||'"}'::text,p_start_dttm := v_start_dttm,p_val := 'v_ddl_crt='''||v_ddl_crt||'''',p_debug_lvl := '1') into v_json_ret;

				return(v_json_ret||']');

		end if;

	end if;



--Регистрируем ошибки

exception when others then

raise exception '%', sqlerrm; 

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.dbz_get(int4, text, bool);

CREATE OR REPLACE FUNCTION sys_dwh.dbz_get(p_src_stm_id int4, p_mv_name text, debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_sql text; --sql для выполнения

v_sql_debug text=''; --все sql, которые исполняются

v_json_ret text := ''; --json для лога

v_start_dttm text; --время старта

v_cnt int:=-1; --количество загруженных данных через mv





begin

	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	/*обновление mv*/

	v_sql := 'REFRESH MATERIALIZED VIEW {{ p_mv_name }}; ANALYZE {{ p_mv_name }};';

	v_sql:=replace(v_sql, '{{ p_mv_name }}',p_mv_name);

	v_sql_debug:=v_sql;

	

	if debug = false then

		execute v_sql;

	end if;

	/*фиксация количества загруженных данных*/

	v_sql := 'select count(*) from '|| p_mv_name||';';

	v_sql_debug:=v_sql_debug || v_sql;

	

	if debug = false then

		execute v_sql into v_cnt;

	end if;

	

	/*запись результатов в sys_dwh.reg_dwh*/

	

	v_sql := $query_reg_dwh$

	insert into sys_dwh.reg_dwh(json_tp,json_value,upd_dttm)

	values ('dbz_to_src','[{"src_stm_id":{{ p_src_stm_id }},"mv_name":"{{ p_mv_name }}", "cnt_rws":{{ v_cnt }}}]'::json,now() at time zone 'utc');

	$query_reg_dwh$;

	

	v_sql:=replace(v_sql, '{{ p_src_stm_id }}',p_src_stm_id::text);	

	v_sql:=replace(v_sql, '{{ p_mv_name }}',p_mv_name);

	v_sql:=replace(v_sql, '{{ v_cnt }}',v_cnt::text);

	v_sql_debug:=v_sql_debug || v_sql;

	

	if debug = false then

		execute v_sql;

	end if;	



	    --Проверяем режим откладки

		if debug = true then

			return(v_sql_debug);

			else			

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,p_step := '1',p_descr := '{"v_mv_name":"'||p_mv_name||'", "src_stm_id": "'||p_src_stm_id::text || '"}',p_start_dttm := v_start_dttm,p_val := 'v_sql_debug='''||v_sql_debug||'''',p_debug_lvl := '1') into v_json_ret;			

				return(v_json_ret||']');

		end if;

		

		



--Регистрируем ошибки

exception when others then

raise exception '%', sqlerrm; 

end;




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.drp_crt_prttn(text, text, int4);

CREATE OR REPLACE FUNCTION sys_dwh.drp_crt_prttn(p_scheme_name text, p_table_name text, p_deep_trim int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
	v_table text;
	v_cnt int8 := 0;
	v_src_stm_ids int[];
	v_dt_stg date;
	v_dml text;
	v_ddl text;
	v_rec_p record;
	v_json_ret text = '';
	v_start_dttm text;

begin
	begin
		v_start_dttm := timeofday()::timestamp;
		--Фиксируем полное наименование таблицы
		v_table := p_scheme_name||'.'||p_table_name;

		--Проверяем наличие таблицы на БД
		select count(1) into v_cnt 
			from  pg_catalog.pg_tables  
				where 1=1
					and lower(tablename) = p_table_name
					and lower(schemaname) = p_scheme_name;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Поиск метаданных',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt||'''',
				p_err := 'The target table '||v_table||' is missing on DB',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск метаданных',
			p_start_dttm := v_start_dttm,
			p_val := 'v_table='''||v_table||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск метаданных',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;

	--Получаем список src_stm_id, которые прогружаются на RDV из этого источника
	begin
		v_start_dttm := timeofday()::timestamp;
		select array_agg(src_stm_id)
		into v_src_stm_ids
		from(
			select s.src_stm_id
			from sys_dwh.prm_src_stm s
			where s.load_mode > 0 and s.src_buf = v_table
			union
			select s.src_stm_id
			from sys_dwh.prm_src_stm s
			 join sys_dwh.prm_src_stm sp on s.prn_src_stm_id = sp.src_stm_id
			where s.load_mode > 0 and sp.src_buf = v_table
		)t;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Получаем список src_stm_id, которые прогружаются на RDV из этого источника',
		p_start_dttm := v_start_dttm,
		p_val := 'v_src_stm_ids='||array_to_string(v_src_stm_ids,','),
		p_log_tp := '1',
		p_debug_lvl := '3')
	into v_json_ret;
	exception when others then	
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Получаем список src_stm_id, которые прогружаются на RDV из этого источника',
		p_start_dttm := v_start_dttm,
		p_val := 'v_src_stm_ids='||array_to_string(v_src_stm_ids,','),
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
	into v_json_ret;
	raise exception '%', v_json_ret;
	end;

	--Получаем дату предыдущей загрузки в STG
	begin
		v_start_dttm := timeofday()::timestamp;
		select min(r.upd_dttm)::date
		into v_dt_stg
		from (
			select j.src_stm_id::int src_stm_id,max(upd_dttm) upd_dttm
			from sys_dwh.reg_dwh,
			 lateral json_to_recordset(reg_dwh.json_value #> '{}'::text[]) j(src_stm_id text)
			where json_tp = 'src_ssn'
			group by j.src_stm_id::int)r
		 join (select unnest(v_src_stm_ids) src_stm_id)j on r.src_stm_id = j.src_stm_id;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Получаем дату предыдущей загрузки в STG',
		p_start_dttm := v_start_dttm,
		p_val := 'v_dt_stg='||coalesce(v_dt_stg::text,'NULL'),
		p_log_tp := '1',
		p_debug_lvl := '3')
	into v_json_ret;
	exception when others then	
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Получаем дату предыдущей загрузки в STG',
		p_start_dttm := v_start_dttm,
		p_val := 'v_dt_stg='||coalesce(v_dt_stg::text,'NULL'),
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
	into v_json_ret;
	raise exception '%', v_json_ret;
	end;

	--Генерируем даты для удаления и создания партиций
	begin
		v_start_dttm := timeofday()::timestamp;
		v_dml := '
	create temp table tmp_tbl_pt_act on commit drop as 
	select 
	 coalesce(dt.dt,pt.dt) as dt, 
	 case 
		when pt.dt<act_dt.dt and pt.dt<=stg_dt.dt and dt.dt is null then ''d'' 
		when dt.dt>=act_dt.dt and pt.dt is null then ''c'' 
	  end as pt_act 
	from (
		select (now() at time zone ''utc'')::date + generate_series(-'||p_deep_trim||',3) as dt
		)dt 
	 full join (
		select substring(partitionrangestart from 2 for 10)::date as dt 
		from pg_catalog.pg_partitions 
		where 
		 partitionlevel = 0
		 and schemaname = '''||p_scheme_name||''' 
		 and tablename = '''||p_table_name||''' 
		 and partitionisdefault is false
		)pt on dt.dt = pt.dt 
	 full join (
		select (now() at time zone ''utc'')::date as dt
		)act_dt on 1=1
	 full join (
		select '''||coalesce(v_dt_stg::text,'1900-01-01')||'''::date-'||p_deep_trim||' as dt
		)stg_dt on 1=1;';
		execute v_dml;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Генерируем даты для удаления и создания партиций',
		p_start_dttm := v_start_dttm,
		p_val := v_dml,
		p_log_tp := '1',
		p_debug_lvl := '3')
	into v_json_ret;
	exception when others then	
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Генерируем даты для удаления и создания партиций',
		p_start_dttm := v_start_dttm,
		p_val := v_dml,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
	into v_json_ret;
	raise exception '%', v_json_ret;
	end;

	if exists(select * from tmp_tbl_pt_act where pt_act = 'd')then
	--Удаляем партиции
	for v_rec_p in
		(	select dt, to_char(dt,'YYYY-MM-DD') start_p
			from tmp_tbl_pt_act 
			where pt_act = 'd'
			order by dt)

	--Динамически удаляем партиции
	loop
		begin
			v_start_dttm := timeofday()::timestamp;
		v_ddl := '
	ALTER TABLE '||v_table||' DROP PARTITION FOR ('''||v_rec_p.start_p||''');';
		execute v_ddl;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Динамически удаляем партиции',
			p_start_dttm := v_start_dttm,
			p_val := v_ddl,
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		exception when others then	
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Динамически удаляем партиции',
			p_start_dttm := v_start_dttm,
			p_val := v_ddl,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
		end;
	end loop;
	end if;

	if exists(select * from tmp_tbl_pt_act where pt_act = 'c')then
	--Создаем патиции
	for v_rec_p in
		(	select dt,
			 to_char(dt,'YYYY-MM-DD') start_p,
			 to_char(dt+1,'YYYY-MM-DD') end_p,
			 to_char(dt,'YYYYMMDD') name_p
			from tmp_tbl_pt_act 
			where pt_act = 'c'
			order by dt)

	--Динамически создаем партиции
	loop
		begin
			v_start_dttm := timeofday()::timestamp;
			v_ddl := '
	ALTER TABLE '||v_table||' SPLIT DEFAULT PARTITION 
	START ('''||v_rec_p.start_p||''') INCLUSIVE 
	END ('''||v_rec_p.end_p||''') EXCLUSIVE 
	INTO (PARTITION "'||v_rec_p.name_p||'", DEFAULT PARTITION);';
		execute v_ddl;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '6',
			p_descr := 'Динамически создаем партиции',
			p_start_dttm := v_start_dttm,
			p_val := 'v_ddl='''||v_ddl::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '6',
			p_descr := 'Динамически создаем партиции',
			p_start_dttm := v_start_dttm,
			p_val := 'v_ddl='''||v_ddl::text||'''',
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
		end;
	end loop;
	end if;

	--Собираем статистику
	begin
		v_start_dttm := timeofday()::timestamp;
	execute 'analyze '||v_table;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Сбор статистики',
		p_start_dttm := v_start_dttm,
		p_val := 'analyze '||v_table,
		p_log_tp := '1',
		p_debug_lvl := '3')
				into v_json_ret;
	exception when others then	
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Сбор статистики',
		p_start_dttm := v_start_dttm,
		p_val := 'analyze '||v_table,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
	into v_json_ret;
	raise exception '%', v_json_ret;
	end;


	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
    --return (v_exec_sql); 
    --Регистрируем ошибки
    exception
      when others then
		  if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		  end if;
      raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.drp_old_ssn(int4);

CREATE OR REPLACE FUNCTION sys_dwh.drp_old_ssn(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

	v_table_name text;

	v_scheme_name text;

	v_table text;

	v_cnt int8 := 0;

	v_output_text text;

	v_ddl text;

	v_answr text;

	v_day_del int2;

	v_ssn int4;

	v_prttn text;

	v_num text;

	v_json_ret text = '';

 

	v_start_dttm text;



begin

	begin

		--Проверяем наличие sys_dwh.prm_src_stm в БД

		v_start_dttm := clock_timestamp() at time zone 'utc';

		select count(1) into v_cnt from pg_catalog.pg_tables pt 

			where lower(pt.schemaname) = 'sys_dwh'

				and lower(pt.tablename) = 'prm_src_stm';

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '1',

			p_descr := 'Поиск prm_src_stm',

			p_start_dttm := v_start_dttm,

			p_val := 'v_cnt='''||v_cnt||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

		into v_json_ret;

		if v_cnt = 0 then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '1',

				p_descr := 'Поиск prm_src_stm',

				p_start_dttm := v_start_dttm,

				p_err := 'The table sys_dwh.prm_src_stm is missing on DB',

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

			into v_json_ret;

			raise exception '%', v_json_ret;

		end if;

	end;

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--Проверяем наличие таблицы в sys_dwh.prm_src_stm

		select sys_dwh.prv_tbl_id(p_src_stm_id) into v_answr;



		--Фиксируем имя и схему таблицы

		select 

				'sdv_'||lower(pr.nm) scheme_name,

				lower(ch.nm) table_name

			into v_scheme_name, v_table_name

			from sys_dwh.prm_src_stm as ch

				join sys_dwh.prm_src_stm as pr

					on ch.prn_src_stm_id = pr.src_stm_id 

			where ch.src_stm_id = p_src_stm_id;



		--Фиксируем полное наименование таблицы

		v_table := v_scheme_name||'.'||v_table_name;



		--Проверяем наличие таблицы на БД

		select count(1) into v_cnt 

			from  pg_catalog.pg_tables  

				where 1=1

					and lower(tablename) = v_table_name

					and lower(schemaname) = v_scheme_name;

		if v_cnt = 0 then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '2',

				p_descr := 'Поиск метаданных',

				p_start_dttm := v_start_dttm,

				p_val := 'v_cnt='''||v_cnt||'''',

				p_err := 'The target table '||v_table||' is missing on DB',

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

			into v_json_ret;

			raise exception '%', v_json_ret;

		end if;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '2',

			p_descr := 'Поиск метаданных',

			p_start_dttm := v_start_dttm,

			p_val := 'v_scheme_name='''||v_scheme_name||''', v_table='''||v_table||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

		into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '2',

			p_descr := 'Поиск метаданных',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	--Фиксируем количество партиций, которые необходимо оставить

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		select day_del into v_day_del 

			from sys_dwh.prm_src_stm

				where src_stm_id = p_src_stm_id;

		if v_day_del is null then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '3',

				p_descr := 'Поиск количества партиций, которые необходимо оставить',

				p_start_dttm := v_start_dttm,

				p_val := 'v_day_del='''||v_day_del||'''',

				p_err := 'Value day_del for table '||v_table||' is null',

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

			into v_json_ret;

			raise exception '%', v_json_ret;

		end if;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '3',

			p_descr := 'Поиск количества партиций, которые необходимо оставить',

			p_start_dttm := v_start_dttm,

			p_val := 'v_day_del='''||v_day_del::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

		into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '3',

			p_descr := 'Поиск количества партиций, которые необходимо оставить',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	--Открываем переменную для перечисления удаленных ssn

	v_num := '(';



	--Открываем цикл с именами партиций, которые необходимо удалить

	for v_ssn in

		(select ssn from

			(select replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint as ssn, 

				pp.partitionschemaname, pp.partitiontablename, 

				row_number() over(order by replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint desc) rn

			from pg_catalog.pg_partitions pp

				where replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint <> '0'::bigint

					and lower(pp.schemaname) = v_scheme_name

						and lower(pp.tablename) = v_table_name)  as sel

			where rn > v_day_del)



	--Динамически удаляем партиции

	loop

		begin

			v_start_dttm := clock_timestamp() at time zone 'utc';

			v_num := v_num||v_ssn||', ';

			v_ddl := 'alter table '||v_table||' drop partition for ('||v_ssn::text||')';

			execute v_ddl;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '4',

				p_descr := 'Динамически удаляем партиции',

				p_start_dttm := v_start_dttm,

				p_val := 'v_ddl='''||v_ddl::text||'''',

				p_log_tp := '1',

				p_debug_lvl := '3')

			into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '4',

				p_descr := 'Динамически удаляем партиции',

				p_start_dttm := v_start_dttm,

				p_val := 'v_ddl='''||v_ddl::text||'''',

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

			into v_json_ret;

			raise exception '%', v_json_ret;

		end;

	end loop;



	--Закрываем переменную для перечисления удаленных ssn

	v_num := v_num||')';

	v_num := replace(v_num, ', )' , ')');



	--Собираем статистику

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		execute 'analyze '||v_table;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '5',

						p_descr := 'Сбор статистики',

						p_start_dttm := v_start_dttm,

						p_val := 'analyze '||v_table,

						p_log_tp := '1',

						p_debug_lvl := '3')

				    into v_json_ret;

	exception when others then	

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,

					p_step := '7',

						p_descr := 'Сбор статистики',

						p_start_dttm := v_start_dttm,

						p_val := 'analyze '||v_table,

						p_err := SQLERRM,

						p_log_tp := '3',

						p_cls := ']',

						p_debug_lvl := '1')

				into v_json_ret;

				raise exception '%', v_json_ret;

	end;



	v_json_ret := v_json_ret||']';

    return(v_json_ret); 

    --return (v_exec_sql); 

    --Регистрируем ошибки

    exception

      when others then

		  if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

			v_json_ret := v_json_ret||']';

		  end if;

      raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.exec_load_step(int4, int4, int8, json, bool, bool);

CREATE OR REPLACE FUNCTION sys_dwh.exec_load_step(p_src_stm_id int4, p_step_id int4, p_ssn int8, p_param json DEFAULT NULL::json, p_debug bool DEFAULT false, p_is_switch_ssn bool DEFAULT true)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_step_func text;

v_cur_ssn int8;

v_last_ssn int8;

v_last_stg_ssn int8;

v_json_ssn text;

v_act_dt date;

v_rq text;

v_rs text;

v_json_ret text := '';

v_json_ret_tmp text := '';

v_rec_json_param record;

v_json_param_sql text := '';

v_start_dttm text;

v_end_dttm text;



BEGIN

--Проверяем наличие таблиц в sys_dwh.prm_src_stm

--select sys_dwh.prv_tbl_id(p_src_stm_id);



	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--получаем функцию исполнения шага

		select step_param

		into v_step_func

		from sys_dwh.prm_step

		where step_id = p_step_id;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '1',

			p_descr := 'Получаем функцию исполнения шага',

			p_start_dttm := v_start_dttm,

			p_val := 'v_step_func='''||v_step_func::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '1',

			p_descr := 'Получаем функцию исполнения шага',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--проверка есть ли параметр, если есть - раскручиваем

		if p_param is not null then

			for v_rec_json_param in (select param, val from json_populate_recordset(null::record,p_param::json)

				as (param text, val text))

				loop

					begin

						v_json_param_sql := v_json_param_sql || ',' || v_rec_json_param.param|| ':=''' ||v_rec_json_param.val||'''';

					end;

				end loop;

		end if;

		v_json_param_sql := coalesce(v_json_param_sql,'');

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '2',

			p_descr := 'Получаем параметр',

			p_start_dttm := v_start_dttm,

			--p_val := 'v_json_param_sql='''||v_json_param_sql::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '2',

			p_descr := 'Получаем параметр',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;



	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

			

		--получаем текущий ssn

		select COALESCE(p_ssn,last_value)

		into v_cur_ssn

		from sys_dwh.seq_ssn;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '3',

			p_descr := 'Получаем текущий ssn',

			p_start_dttm := v_start_dttm,

			p_val := 'v_cur_ssn='''||v_cur_ssn::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '3',

			p_descr := 'Получаем текущий ssn',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--получаем последний номер обработанной пачки (ssn)

		select last_ssn

		into v_last_ssn

		from sys_dwh.prm_load_cntr

		where src_stm_id = p_src_stm_id and step_id = p_step_id;

				

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '4',

			p_descr := 'Получаем последний номер обработанной пачки (ssn)',

			p_start_dttm := v_start_dttm,

			p_val := 'v_last_ssn='''||v_last_ssn::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '4',

			p_descr := 'Получаем последний номер обработанной пачки (ssn)',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--получаем последний номер обработанной пачки stg lm in (1,3) (ssn)

		select first_value(ssn) over w into v_last_stg_ssn

			from (

				select rd.json_value,

				t.value ->> 'src_stm_id' as src_stm_id,

				t.value ->> 'act_dt' as act_dt,

				t.value ->> 'upd_dttm' as ssn_upd_dttm,

				t.value ->> 'ssn' as ssn,

				t.value ->> 'load_mode' as load_mode

					from sys_dwh.reg_dwh rd

					LEFT JOIN LATERAL json_array_elements(rd.json_value) t(value) ON true

				where json_tp = 'src_ssn' 

				and t.value ->> 'src_stm_id' = ''||p_src_stm_id::text||''

					

				) dt

				where load_mode::int in (1,3)

				window w as (order by ssn::int desc)

							limit 1;

		v_last_stg_ssn := coalesce(v_last_stg_ssn,-1);

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '5',

			p_descr := 'Получаем последний номер обработанной пачки stg lm in (1,3) (ssn)',

			p_start_dttm := v_start_dttm,

			p_val := 'v_last_stg_ssn='''||v_last_stg_ssn::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '5',

			p_descr := 'Получаем последний номер обработанной пачки stg lm in (1,3) (ssn)',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

						

	

	/*if p_step_id > 5 then

		v_last_ssn := GREATEST(v_last_ssn, v_last_stg_ssn);

	end if;*/

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--проверяем, что ssn-ы не пустые и не равны друг другу

		if coalesce(v_cur_ssn,v_last_ssn,-1)=coalesce(v_last_ssn,v_cur_ssn,-1) and p_step_id < 5 then

			begin

				select sys_dwh.get_json4log(p_json_ret := '',

					p_step := '6',

					p_descr := 'Number of ssn='||coalesce(v_cur_ssn::text,'NULL')||' is invalid',

					p_start_dttm := v_start_dttm,

					p_err := SQLERRM,

					p_log_tp := '3',

					p_debug_lvl := '1',

					p_cls := ']')

				into v_json_ret;

				raise exception '%', v_json_ret;

				--raise exception '%', 'Number of ssn='||coalesce(v_cur_ssn::text,'NULL')||' is invalid';

			end;

		end if;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '6',

			p_descr := 'Проверяем, что ssn-ы не пустые и не равны друг другу',

			p_start_dttm := v_start_dttm,

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	end;

	

	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';

		--собираем ddl запуска шага по активным ssn

		--собираем все пачки загрузок по ssn для источника p_src_stm_id из sys_dwh.log_act_dt в json

		/*select array_to_json(array_agg(j),false)

		into v_json_ssn

		from(

						select ssn::int8 as ssn

						from(

							select p.ssn

							from sys_dwh.reg_dwh r

							 ,lateral json_to_recordset(r.json_value #> '{}'::text[]) p(ssn int8,src_stm_id int4)

							where r.json_tp = 'src_ssn'

							 and p.src_stm_id = p_src_stm_id

							 and p.ssn > v_last_ssn

							 and p.ssn >= v_last_stg_ssn

							 and p.ssn < v_cur_ssn

							group by ssn

							 union

							select v_cur_ssn as ssn)t

						order by ssn

			)j;*/

		select array_to_json(array_agg(j ORDER by ssn) ,false)

		into v_json_ssn

		from(

			select distinct 

			ssn::int8 ssn from

			(select rd.json_value,

							t.value ->> 'src_stm_id' as src_stm_id,

							t.value ->> 'ssn' as ssn

								from sys_dwh.reg_dwh rd

								LEFT JOIN LATERAL json_array_elements(rd.json_value) t(value) ON true

							where json_tp = 'src_ssn' 

							and t.value ->> 'src_stm_id' = ''||p_src_stm_id::text||''

							

							) r 

							where 

							 ssn::int8 > v_last_ssn

							and ssn::int8 >= v_last_stg_ssn

							and ssn::int8 < v_cur_ssn

							--order by ssn::int8

			union 

			select v_cur_ssn as ssn

			)j ;

		--собираем ddl запуска шага

		v_rq := 'select '||v_step_func||'(p_src_stm_id:='||p_src_stm_id||',p_json:='''||v_json_ssn||''''||v_json_param_sql||')';

	

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '7',

			p_descr := 'Cобираем все пачки загрузок по ssn для источника p_src_stm_id из sys_dwh.log_act_dt в json',

			p_start_dttm := v_start_dttm,

			p_val := 'v_json_ssn='''||v_json_ssn::text||'''',

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '7',

			p_descr := 'Cобираем все пачки загрузок по ssn для источника p_src_stm_id из sys_dwh.log_act_dt в json',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

 /*exception when others then

  select sys_dwh.get_json4log(p_json_ret := '',

		p_step := '1',

		p_descr := 'Подготовка ddl запуска шага',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

end;*/



	

	

	if p_debug = true then 

		return(v_rq);

	else

		begin

			v_start_dttm := clock_timestamp() at time zone 'utc';

			--выполняем запуск шага по активным ssn из списка

			execute v_rq into v_json_ret_tmp;

			v_json_ret := v_json_ret||','||coalesce(replace(replace(v_json_ret_tmp,']',''),'[',''),'');

		

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '8',

				p_descr := 'Запуск шага по активным ssn из списка',

				p_start_dttm := v_start_dttm,

				p_val := 'v_rq='''||replace(replace(v_rq::text,']',''),'[','')||'''',

				p_log_tp := '1',

				p_debug_lvl := '3')

				into v_json_ret;

			exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret||','||coalesce(replace(replace(v_json_ret_tmp,']',''),'[',''),''),

				p_step := '8',

				p_descr := 'Запуск шага по активным ssn из списка',

				p_start_dttm := v_start_dttm,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

			into v_json_ret;

			raise exception '%', v_json_ret;

		end;

	end if;



	begin

		v_start_dttm := clock_timestamp() at time zone 'utc';



		--получаем максимальную бизнес-дату пачки

		/*select max(p.act_dt)

			into v_act_dt

			from sys_dwh.reg_dwh r

				,LATERAL json_to_recordset(r.json_value #> '{}'::text[]) p(src_stm_id int4, ssn int8, act_dt date)

			where json_tp = 'src_ssn'

				and p.src_stm_id = p_src_stm_id

				and p.ssn = v_cur_ssn;*/

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '9',

				p_descr := 'Получаем бизнес-дату',

				p_start_dttm := v_start_dttm,

				p_val := 'v_act_dt='''||v_act_dt::text||'''',

				p_log_tp := '1',

				p_debug_lvl := '3')

				into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '9',

			p_descr := 'Получаем бизнес-дату',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	begin	 

		v_start_dttm := clock_timestamp() at time zone 'utc';

	

		--Фиксируем последнее значение ssn c максимальной бизнес-датой пачки

		if p_is_switch_ssn then --игнорируем шаг наполнения скриптов для хабов

			update sys_dwh.prm_load_cntr

				set last_ssn = v_cur_ssn, last_act_dt = v_act_dt, updated_dttm = now() at time zone 'utc'

					where src_stm_id = p_src_stm_id and 

						case when p_step_id <=8 then step_id = p_step_id

						else ((step_id = p_step_id) or (step_id = 8))

						end;

		end if;

		

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '10',

				p_descr := 'Фиксация ssn и бизнес-даты',

				p_start_dttm := v_start_dttm,

				p_val := 'v_cur_ssn='''||v_cur_ssn::text||''',v_act_dt='''||v_act_dt::text||'''',

				p_log_tp := '1',

				p_debug_lvl := '3')

				into v_json_ret;

		

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := '',

			p_step := '10',

			p_descr := 'Фиксация ssn и бизнес-даты',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	begin	 

		v_start_dttm := clock_timestamp() at time zone 'utc';

		analyze sys_dwh.prm_load_cntr;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '12',

				p_descr := 'analyze sys_dwh.prm_load_cntr;',

				p_start_dttm := v_start_dttm,

				p_log_tp := '1',

				p_debug_lvl := '3')

				into v_json_ret;

	end;

	v_json_ret := v_json_ret||']';

	return(v_json_ret);



	--Регистрируем ошибки

	--exception when others then 

	--raise exception '%', SQLERRM;



END;




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.execute_task(text);

CREATE OR REPLACE FUNCTION sys_dwh.execute_task(p_type_of_task text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

	v_start_dttm text; --переменная для логирования (время запуска)

	v_json_ret text := ''; --пременная для логирования (json)

	v_ddl text; --переменная для записи выполняемого ddl

	v_cnt int8; --пременная для записи количества записей

	v_rec record; --переменная для работы с циклом

	v_part text; --переменная для фиксации партиций

	v_src_stm_id int8;

	v_id_task uuid;

begin

	--Проверка наличия заданий для выполнения

	begin	

		v_start_dttm := clock_timestamp() at time zone 'utc';

		select count(1) into v_cnt

			from sys_dwh.prm_task 

				where active_fl and success_fl = false and type_of_task = p_type_of_task;

		--Проверяем наличие данных в rdv-таблицах

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '1',

			p_descr := 'Проверка наличия заданий для выполнения',

			p_start_dttm := v_start_dttm,

			p_val := 'Количество заданий для выполнения - '||v_cnt::text,

			p_log_tp := '1',

			p_debug_lvl := '3')

			into v_json_ret;

	exception when others then

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '1',

			p_descr := 'Проверка наличия заданий для выполнения',

			p_start_dttm := v_start_dttm,

			p_err := SQLERRM,

			p_log_tp := '3',

			p_debug_lvl := '1',

			p_cls := ']')

			into v_json_ret;

		raise exception '%', v_json_ret;

	end;

	--Возвращаем результат если заданий нет

	if v_cnt = 0 then 

		return(v_json_ret||']'); 

	end if;

	--Открываем цикл с заданиями

	for v_rec in (select distinct dt, table_name, owner_rdv_table, backup_table_name, type_of_task,	

		(select string_agg(distinct column_name, ', ') 

			from sys_dwh.prm_task p 

				where p.active_fl 

					and p.success_fl = false 

					and p.dt = t.dt 

					and p.table_name = t.table_name 

					--and p.type_of_changes = 'change_ref_to_stg'

			) as column_name

		from sys_dwh.prm_task t

		where active_fl and success_fl = false 

		and type_of_task = p_type_of_task) 

	loop 

		select distinct src_stm_id into v_src_stm_id from sys_dwh.prm_s2t_rdv_rule where end_dt = '9999-12-31'::date and schema_rdv||'.'||table_name_rdv = v_rec.table_name;

				

		--Выполняем rename_to_backup

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'rename_backup' then

				update sys_dwh.prm_task set dttm_start = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				v_ddl := 'ALTER TABLE '||v_rec.table_name||' RENAME TO '||substring(v_rec.backup_table_name from '%.#"%#"' for '#');

				execute v_ddl;

				update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '2',

				p_descr := 'Выполняем rename_to_backup',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '2',

				p_descr := 'Выполняем rename_to_backup',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

				raise exception '%', v_json_ret;

		end;

		--Выполняем create_new_with_like

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'create_new_with_like' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				v_ddl := 'create table '||v_rec.table_name||' (like '||v_rec.backup_table_name||' including all)';

				select E'\n'||'partition by '||pp.partitiontype||' ('||pc.columnname||')

					('||string_agg(replace(partitionboundary, ' WITH (appendonly=''true'')', ''), ','||E'\n')||')' 

					into v_part

					from pg_catalog.pg_partitions pp

					inner join  pg_catalog.pg_partition_columns pc 

						on lower(pp.schemaname)=lower(pc.schemaname) 

						and lower(pc.tablename)= lower(pp.tablename)

					where lower(pp.schemaname)||'.'||lower(pp.tablename) = v_rec.backup_table_name

					group by pp.partitiontype, pc.columnname;

					v_ddl := v_ddl||coalesce(v_part, '');

				execute v_ddl;

					update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '4',

				p_descr := 'Выполняем create_new_with_like',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '4',

				p_descr := 'Выполняем create_new_with_like',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

		--Выполняем set_owner

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'set_owner' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				v_ddl := 'alter table '||v_rec.table_name||' owner to '||v_rec.owner_rdv_table;

				execute v_ddl;

				update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '5',

				p_descr := 'Выполняем set_owner',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '5',

				p_descr := 'Выполняем set_owner',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

		--Выполняем copy_history

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'copy_history' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

					v_ddl := 'insert into '||v_rec.table_name||' select * from '||v_rec.backup_table_name||' s where s.end_dt <> ''9999-12-31''::date';

				execute v_ddl;

				update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '6',

				p_descr := 'Выполняем copy_history',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '6',

				p_descr := 'Выполняем copy_history',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

		--Выполняем updt_load_mode

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'updt_load_mode' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				v_ddl := 'select sys_dwh.updt_lm('||coalesce(v_src_stm_id::text, 'NULL')||')';

				execute v_ddl;

				update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '7',

				p_descr := 'Выполняем updt_load_mode',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '7',

				p_descr := 'Выполняем updt_load_mode',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

		--Выполняем add_column_sdv

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'add_column_sdv' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' 

					where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				select string_agg('alter table '||v_rec.backup_table_name||' add column '||column_name_stg||' '||datatype_stg||E';\n'||'alter table '||v_rec.backup_table_name||' add column gk_'||column_name_stg||E' uuid;\n', '')

					into v_ddl

					from sys_dwh.prm_s2t_rdv 

						where end_dt = '9999-12-31'

							and schema_rdv||'.'||table_name_rdv = v_rec.table_name

							and column_name_rdv in 

								(select regexp_split_to_table(v_rec.column_name, ', '));

			execute v_ddl;

			update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

		else v_ddl := 'Пропуск шага'; 

		end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '8',

				p_descr := 'Выполняем add_column_sdv',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_log_tp := '2',

				p_debug_lvl := '3')

				into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '8',

				p_descr := 'Выполняем add_column_sdv',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

	

	--Выполняем add_column_sdv

		begin 

			v_start_dttm := clock_timestamp() at time zone 'utc';

			if p_type_of_task = 'put_bk_from_hub' then

				update sys_dwh.prm_task s set dttm_start = now() at time zone 'utc' 

					where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

				v_ddl := 'select sys_dwh.put_bk_from_hub (''[{"v_tbl_nm":'''''||v_rec.backup_table_name||''''', "v_src_stm_id":'||v_src_stm_id||'}]''::json)';

				execute v_ddl;

				update sys_dwh.prm_task s set success_fl = true, dttm_end = now() at time zone 'utc' where table_name = v_rec.table_name and dt = v_rec.dt and type_of_task = p_type_of_task;

			else v_ddl := 'Пропуск шага'; 

			end if;

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,

					p_step := '9',

					p_descr := 'Выполняем put_bk_from_hub',

					p_start_dttm := v_start_dttm,

					p_val := v_ddl,

					p_log_tp := '2',

					p_debug_lvl := '3')

					into v_json_ret;

		exception when others then

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '9',

				p_descr := 'Выполняем put_bk_from_hub',

				p_start_dttm := v_start_dttm,

				p_val := v_ddl,

				p_err := SQLERRM,

				p_log_tp := '3',

				p_debug_lvl := '1',

				p_cls := ']')

				into v_json_ret;

			raise exception '%', v_json_ret;

		end;

	end loop;

	--Возвращаем результат

	return(v_json_ret||']'); 

	--Регистрируем ошибки

exception

	when others then

	if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

	v_json_ret := v_json_ret||']';

	end if;

	raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.ext_crt(int4, bool, int4);

CREATE OR REPLACE FUNCTION sys_dwh.ext_crt(p_src_stm_id int4, debug bool DEFAULT false, p_load_mode int4 DEFAULT NULL::integer)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_db_name text;

v_id_min text;

v_id_max text;

v_ext_minmax varchar(50);

v_ext_table varchar(50);

v_ext_column_type text;

v_schema_src varchar(50);

v_table_name_src varchar(50);

v_table_name_stg varchar(50);

v_load_mode int4;

v_ext_pb varchar(50);

v_ext_pb_itrv text;

v_ext_location text;

v_ext_format text;

v_ext_encoding text;

v_ddl_ext_crt text;

v_cnt int8 := 0;

v_output_text text;

v_json_ret text := '';

v_start_dttm text;

v_end_dttm text;

v_ext_prd_pb text;

v_ext_prd_pb_itrv text;

v_ext_prd_pb_from text;

v_ext_prd_pb_tp text;

v_prn_src_stm text;

v_db_src_stm text;

v_cdc_tp text;



begin

	--получение текущего lm

	 select load_mode into v_load_mode from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id and end_dt = '9999-12-31' ;

	--получаем текущий cdc_tp

	 select cdc_tp into v_cdc_tp from sys_dwh.prm_s2t_stg_src pstss where pstss.src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

	--Проверяем наличие внутренних источников данных

	

	

	select count(*)

		into v_cnt

	from sys_dwh.prm_s2t_stg_src

		where src_stm_id = p_src_stm_id

			and schema_src = schema_stg

			and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

	if v_cnt = 1 or (v_load_mode = 2 and v_cdc_tp = 'dbz') then --если schema_src = schema_stg или сейчас будет загрузка через cdc dbz 

		return('Внутренний источник данных');

	else

	--Проверяем наличие connection string в sys_dwh.prm_src_stm.ext_location

		select count(1) into v_cnt

			from sys_dwh.prm_src_stm as p

				where p.src_stm_id = p_src_stm_id

				and p.ext_location is not null

		and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt;

		if v_cnt = 0 then

			v_output_text := 'Empty column ext_location in sys_dwh.prm_src_stm for src_stm_id='||p_src_stm_id;

			raise exception '%', v_output_text;

		end if;

		begin

			v_start_dttm := clock_timestamp() at time zone 'utc';

			--Получаем параметры для источника

			select sys_dwh.get_s2t_stg_clmntp(p_src_stm_id)

				into v_ext_column_type;

			--Получаем имена и схему таблиц

			select schema_src,table_name_src,table_name_stg

				into v_schema_src,v_table_name_src,v_table_name_stg

				from sys_dwh.prm_s2t_stg_src

					where src_stm_id = p_src_stm_id

					and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

			--Фиксируем имя external_table

			select

				'ext_' || pp.nm || '.minmax_' || p.nm,

				'ext_' || pp.nm || '.' ||

				case

					when coalesce(p_load_mode, p.load_mode)=2 then 'cdc_'

					when coalesce(p_load_mode, p.load_mode)=3 then 'prd_'

					when p.ext_pb_itrv is not null then 'pll_'

					else ''

				end || p.nm,

				p.load_mode,

				p.ext_pb,

				p.ext_pb_itrv,

				p.ext_location,

				p.ext_format,

				p.ext_encoding,

				pp.nm

				into

				v_ext_minmax,

				v_ext_table,

				v_load_mode,

				v_ext_pb,

				v_ext_pb_itrv,

				v_ext_location,

				v_ext_format,

				v_ext_encoding,

				v_prn_src_stm

				from sys_dwh.prm_src_stm p

					join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id

						where p.src_stm_id = p_src_stm_id

							and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt;

			--Если p_load_mode <> null, то присваиваем его к v_load_mode

			if (p_load_mode is not null) then

				v_load_mode := p_load_mode;

			end if;

			--получаем имя БД источника для pxf

			select sys_service.get_cctn(v_prn_src_stm)

				into v_db_src_stm;

			--Получаем min/max из источника

			if (v_ext_pb_itrv is not null and v_load_mode in(1,3,4)) then

				execute 'select id_min,id_max from ' || v_ext_minmax

				into v_id_min,v_id_max;

			end if;

			--Фиксируем имя pxf при

			--Получаем поля для фильтра

			select

				nullif(ext_prd_pb,''),

				nullif(ext_prd_pb_itrv,''),

				nullif(ext_prd_pb_from,'')

				into v_ext_prd_pb, v_ext_prd_pb_itrv, v_ext_prd_pb_from

				from sys_dwh.prm_s2t_stg_src

					where 1=1

						and src_stm_id = p_src_stm_id

						and eff_dt <= now()

						and end_dt >= now();

			--Фиксируем тип данных для партицирования

			select

				case when lower(t.datatype_stg) in ('timestamp', 'date') then 'date'

					else 'int' end as tp into v_ext_prd_pb_tp

				from sys_dwh.prm_s2t_stg t

					where src_stm_id = p_src_stm_id

						and lower(t.column_name_stg) = lower(v_ext_prd_pb)

						and eff_dt <= now()

						and end_dt >= now();

			--Генерим ddl создания external table

			/*Примечание: замена location

			('pxf://dbo.toperpart?profile=jdbc&server=skb_etalon') on all

			('pxf://query:pxf_cdc_toperpart?profile=jdbc&server=skb_etalon') on all

			('pxf://dbo.toperpart?profile=jdbc&server=skb_etalon&partition_by=operationid:int&range=10000000065:10019248239&interval=10000') on all*/

			v_ddl_ext_crt :=

		'

		drop external table if exists '

		|| v_ext_table

		||';

		create external table '

		|| v_ext_table

		|| ' ('

		|| v_ext_column_type||(case when v_load_mode in (2,5) then ', dml_tp bpchar(1), dml_dttm timestamp' else '' end)

		|| ')

		LOCATION '

		||

			case

			when v_load_mode in (2,5) then

			(case when v_schema_src = 'pxf' then replace(v_ext_location,'pxf://query:pxf_ini_','pxf://query:pxf_cdc_')

			else replace(v_ext_location,'pxf://'||v_schema_src||'.','pxf://query:pxf_cdc_'||v_db_src_stm||'_'||v_schema_src||'_') end)

			when v_load_mode=3 then

			(case when v_ext_prd_pb is not null then replace(

			(case when v_schema_src = 'pxf' then replace(v_ext_location,'pxf://query:pxf_ini_','pxf://query:pxf_prd_') else

			replace(v_ext_location,'pxf://'||v_schema_src||'.'||v_table_name_src,'pxf://query:pxf_prd_'||v_db_src_stm||'_'||lower(v_schema_src)||'_'||v_table_name_stg)

			end),

			''') ON ALL','&PARTITION_BY='||v_ext_prd_pb|| ':'||v_ext_prd_pb_tp||'&RANGE='||v_ext_prd_pb_from||':'||

			case when v_ext_prd_pb_tp = 'int' then v_id_max else to_char(now() - interval '1 day', 'yyyy-mm-dd') end

			||'&INTERVAL='||v_ext_prd_pb_itrv||''') ON ALL'

			)

			else

			(case when v_schema_src = 'pxf' then replace(v_ext_location,'pxf://query:pxf_ini_','pxf://query:pxf_prd_') else

			replace(v_ext_location,'pxf://'||v_schema_src||'.'||v_table_name_src,'pxf://query:pxf_prd_'||v_db_src_stm||'_'||lower(v_schema_src)||'_'||v_table_name_stg) end)

			end)

			else

			case when v_ext_pb_itrv is null then v_ext_location else

			replace(v_ext_location,''') ON ALL','&PARTITION_BY='

			|| v_ext_pb

			|| '&RANGE='||v_id_min||':'||v_id_max

			|| '&INTERVAL='||v_ext_pb_itrv||''') ON ALL')

			end

			end

			|| '

			FORMAT '||v_ext_format

			|| '

			ENCODING '||v_ext_encoding||';

			alter table '

			|| v_ext_table

			|| ' owner to '

			|| (select sys_service.get_cctn('exec_owner'))

			||';';

			--добавляем имя базы в pxf

			select current_database() into v_db_name;

			v_ddl_ext_crt = replace(v_ddl_ext_crt, 'query:', 'query:' || v_db_name ||'/');

			--Проверяем режим откладки

			if debug = true then

				return(v_ddl_ext_crt);

				else

				--Выполняем DDL команды

				if v_ext_location not like '%file://%' then

					execute v_ddl_ext_crt;

				end if;

			end if;  

			--Фиксируем лог

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '1',

				p_descr := 'исполнение drop create external table',

				p_start_dttm := v_start_dttm,

				p_val := 'v_ddl_ext_crt='''||v_ddl_ext_crt||'''',

				p_debug_lvl := '1')

				into v_json_ret;

		exception when others then

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'исполнение drop create external table',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ddl_ext_crt='''||v_ddl_ext_crt||'''',

		p_err := sqlerrm,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

		into v_json_ret;

		raise exception '%', v_json_ret;

		end;

		--Возвращаем результат выполнения

		return(v_json_ret||']');

	end if;

--Регистрируем ошибки

exception when others then

raise exception '%', sqlerrm; 

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.ext_crt_pk(int4, bool);

CREATE OR REPLACE FUNCTION sys_dwh.ext_crt_pk(p_src_stm_id int4, p_debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
v_ddl_ext_crt text; 
v_table_nm text;

begin
select sys_dwh.ext_crt(p_src_stm_id, true, p_load_mode := 1) into v_ddl_ext_crt;
select
'ext_' || pp.nm || '.' ||
case
when p.ext_pb_itrv is not null then 'pll_'
else ''
end || p.nm from sys_dwh.prm_src_stm p join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id where p.src_stm_id = p_src_stm_id into v_table_nm;
v_ddl_ext_crt := REPLACE(v_ddl_ext_crt, v_table_nm, v_table_nm || '_pk');
if p_debug = true then 
return(v_ddl_ext_crt);
else EXECUTE v_ddl_ext_crt;
end if;
RETURN 'Success';
end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.ext_get(int4, json, bool, int4, bool);

CREATE OR REPLACE FUNCTION sys_dwh.ext_get(p_src_stm_id int4, p_json json, p_debug bool DEFAULT false, p_load_mode int4 DEFAULT NULL::integer, p_gen_data bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

	v_ext_table varchar(50);

	v_stg_table varchar(50);

	v_ext_column text;

	v_stg_column text;

	v_ssn varchar(50);

	v_act_dt varchar(50);

	v_ddl_ext_get text;

    v_ddl_ext_get_short text; 

	v_cnt int8 := 0;

	v_load_mode int4;

	v_int_table varchar(50);

	v_int int2 := 0;

	v_src_ssn json;

	v_src_ssn_act_dt json;

	v_json_ret text := '';

	v_start_dttm text;

	v_end_dttm text;

	v_json json;

    v_cdc_tp text;

    v_max_dml_dttm timestamp;



begin

--Проверяем наличие таблиц в sys_dwh.prm_src_stm

--select sys_dwh.prv_tbl_id(p_src_stm_id);

	

--получение текущего lm

select load_mode into v_load_mode from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id and end_dt = '9999-12-31' ;

--получаем текущий cdc_tp

select cdc_tp into v_cdc_tp from sys_dwh.prm_s2t_stg_src pstss where pstss.src_stm_id = p_src_stm_id and end_dt = '9999-12-31';



--получаем максимально загруженный max_dml_dttm

select  sys_dwh.get_max_dml_dttm(p_src_stm_id)

into v_max_dml_dttm;



--проверяем наличие внутренних источников данных

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	SELECT COUNT(*)

	INTO v_int

	FROM sys_dwh.prm_s2t_stg_src

	WHERE src_stm_id = p_src_stm_id

	 AND schema_src = schema_stg

	 AND now() at time zone 'utc' BETWEEN eff_dt AND end_dt;



	IF v_int = 1 THEN

		SELECT schema_src||'.'||table_name_src

		INTO v_int_table

		FROM sys_dwh.prm_s2t_stg_src

		WHERE src_stm_id = p_src_stm_id

		 AND now() at time zone 'utc' BETWEEN eff_dt AND end_dt;

	else

	    /*проверка на то, что сейчас загружается с lm2 через dbz*/

		if v_load_mode = 2 and v_cdc_tp = 'dbz' then

			SELECT schema_stg||'.'||'v_dbz_'||table_name_stg

			INTO v_int_table

			FROM sys_dwh.prm_s2t_stg_src

			WHERE src_stm_id = p_src_stm_id AND now() at time zone 'utc' BETWEEN eff_dt AND end_dt;		

     		v_int := 1; --пометить, что это внутренний источник

		end if;	

	END IF;



 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Проверка наличия внутренних источников данных',

		p_start_dttm := v_start_dttm,

		p_val := 'v_int='''||v_int||''', v_int_table='''||v_int_table||'''')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Проверка наличия внутренних источников данных',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;









--Получение ssn

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	--ssn

	--Фиксируем последнее значение ssn если на входе NULL

	v_json := (select coalesce(p_json,(select array_to_json(array_agg(j),false)from(select last_value::text as ssn from sys_dwh.seq_ssn)j)));

	--выбираем максимальный для фиксации новой пачки загрузки

	v_ssn := (select max(p.ssn) from (select v_json)j,LATERAL json_to_recordset(j.v_json #> '{}'::text[]) p(ssn text));

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '2',

		p_descr := 'Получение ssn',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ssn='''||v_ssn||'''')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '2',

		p_descr := 'Получение ssn',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;



--Получение списка полей таблиц для select-insert

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	select sys_dwh.get_s2t_src_clmn(p_src_stm_id)

	into v_ext_column;

	select sys_dwh.get_s2t_stg_clmn(p_src_stm_id)

	into v_stg_column;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

		p_descr := 'Получение списка полей таблиц для select-insert',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ext_column='''||v_ext_column||''', v_stg_column='''||v_stg_column||'''')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

		p_descr := 'Получение списка полей таблиц для select-insert',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;



--Получение имен таблиц и бизнес-даты

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	select

	'ext_' || pp.nm || '.' || case 

								when coalesce(p_load_mode, p.load_mode)=2 then 'cdc_'

								when coalesce(p_load_mode, p.load_mode)=3 then 'prd_'

								when p.ext_pb_itrv is not null then 'pll_'

								else ''

							  end || p.nm,

	'stg_' || pp.nm || '.' || p.nm,

	coalesce('"'||p.act_dt_column||'"','current_date-1'),

	p.load_mode

	into

	v_ext_table,

	v_stg_table,

	v_act_dt,

	v_load_mode

	from sys_dwh.prm_src_stm p

	join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id

	where p.src_stm_id = p_src_stm_id;

--Если p_load_mode <> null, то присваиваем его к v_load_mode

if (p_load_mode is not null) then

v_load_mode := p_load_mode;

end if;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '4',

		p_descr := 'Получение имен таблиц, бизнес-даты, типа загрузки',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ext_table='''||v_ext_table||''', v_stg_table='''||v_stg_table||''', v_act_dt='''||v_act_dt||''', v_load_mode='''||v_load_mode||'''')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '4',

		p_descr := 'Получение имен таблиц, бизнес-даты, типа загрузки',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;





--Создание ddl для select-insert

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';



--ddl создания новой партиции для указанного ssn в stg_table

v_ddl_ext_get := 'alter table '||v_stg_table||' add partition start ('||v_ssn||') inclusive end ('||((v_ssn::int)+1)||') exclusive with (appendonly=''true'');';

--raise notice '%', v_ddl_ext_get;

if p_gen_data = false then

--Создаем ddl по вставке значений

v_ddl_ext_get := v_ddl_ext_get || ' 

insert into '

|| v_stg_table

|| ' ('

|| v_stg_column

|| ', ssn, upd_dttm, act_dt, dml_tp, dml_dttm)

select '

|| v_ext_column

|| ', '

|| v_ssn

||' as ssn, now() at time zone ''utc'' as upd_dttm, ('

|| v_act_dt

||')::date as act_dt, '

|| (select case when v_load_mode=2 then 'dml_tp, dml_dttm'

				else '''I'' as dml_tp, now() at time zone ''utc'' as dml_dttm' end)||' 

from '

|| (select case when v_int=1 then v_int_table else v_ext_table end)

|| ';';

else 

select v_ddl_ext_get||E'\n'||sys_dwh.generate_data_stg(p_json:=('{"p_cnt":"1000", "p_ssn":"'||v_ssn::text||'", "p_src_stm_id":"'||p_src_stm_id::text||'"}')::json, p_debug:=true)

into v_ddl_ext_get;

end if;



v_ddl_ext_get_short := v_ddl_ext_get;

if p_gen_data = true then v_ddl_ext_get_short:=substring(v_ddl_ext_get,1,10); end if; --укорачиваем для вывода



 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '6',

		p_descr := 'Создание ddl для select-insert',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ddl_ext_get='''||v_ddl_ext_get_short||'''')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '6',

		p_descr := 'Создание ddl для select-insert',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;



--Выбираем значение для возврата (режим отладки/режим выполнения)

if p_debug = true

then

--Режим отладки без исполнения select-insert

	return(v_ddl_ext_get);

else

--Исполнение select-insert

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	execute v_ddl_ext_get;

	get diagnostics v_cnt = row_count;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '7',

		p_descr := 'Исполнение select-insert',

		p_start_dttm := v_start_dttm,

		p_ins_qty := v_cnt::text,

		p_val := 'v_ddl_ext_get='''||v_ddl_ext_get_short||'''',

		p_debug_lvl := '1')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '7',

		p_descr := 'Исполнение select-insert',

		p_start_dttm := v_start_dttm,

		p_val := 'v_ddl_ext_get='''||v_ddl_ext_get_short||'''',

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;





--Сбор статистики

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	execute 'analyze '||v_stg_table;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '8',

		p_descr := 'Сбор статистики',

		p_start_dttm := v_start_dttm,

		p_val := 'analyze '||v_stg_table,

		p_debug_lvl := '2')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '8',

		p_descr := 'Сбор статистики',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;





--Фиксация срезов src_ssn

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	--срез загруженных ssn

	execute '

	select COALESCE(array_to_json(array_agg(j),false),

		json_build_array(json_build_object(''src_stm_id'','||p_src_stm_id||',''ssn'','||v_ssn||',''cnt_rws'',0,''load_mode'','||v_load_mode||')))

	from(

		select '||p_src_stm_id||' src_stm_id, ssn, upd_dttm, min(dml_dttm) as min_dml_dttm, max(dml_dttm) as max_dml_dttm, count(*) cnt_rws, '||v_load_mode||' load_mode

		from '||v_stg_table||'

		where ssn='||v_ssn||'

		group by ssn, upd_dttm)j;'

	into v_src_ssn;



	--Фиксируем срезы

	insert into sys_dwh.reg_dwh(json_tp,json_value,upd_dttm)

	values

	('src_ssn',v_src_ssn,now() at time zone 'utc');

	get diagnostics v_cnt = row_count;

	analyze sys_dwh.reg_dwh;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '9',

		p_descr := 'Фиксация срезов src_ssn',

		p_start_dttm := v_start_dttm,

		p_ins_qty := v_cnt::text,

		p_debug_lvl := '3')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '9',

		p_descr := 'Фиксация срезов src_ssn',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;



--Фиксация срезов src_ssn_act_dt

BEGIN

 v_start_dttm := clock_timestamp() at time zone 'utc';

	--срез загруженных бизнес-дат

	execute '

	select (''{"src_stm_id": "'||p_src_stm_id||'", "ssn": "'||v_ssn||'", "act_dt_cnt": ''||array_to_json(array_agg(j),false)||''}'')::json

		from(

			select coalesce(act_dt::text,''NULL'') act_dt, count(*) cnt_rws

				from '||v_stg_table||'

				where ssn='||v_ssn||'

			group by coalesce(act_dt::text,''NULL''))j;'

	into v_src_ssn_act_dt;

	--Фиксируем срезы

	--Фиксируем срезы

	insert into dr.reg_dwh_slice(slice_tp,schema_name,table_name,slice_value)

	values

	('src_ssn_act_dt','src',v_ext_table,v_src_ssn_act_dt);

	get diagnostics v_cnt = row_count;

	analyze dr.reg_dwh_slice;

 select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '10',

		p_descr := 'Фиксация срезов src_ssn_act_dt',

		p_start_dttm := v_start_dttm,

		p_ins_qty := v_cnt::text,

		p_debug_lvl := '3')

 into v_json_ret;

 exception when others then

  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '10',

		p_descr := 'Фиксация срезов src_ssn_act_dt',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

  into v_json_ret;

 raise exception '%', v_json_ret;

END;



return(v_json_ret||']');



end if;



--Регистрируем ошибки

exception when others then 

raise exception '%', SQLERRM;



END;




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.ext_get_pk(int4, bool);

CREATE OR REPLACE FUNCTION sys_dwh.ext_get_pk(p_src_stm_id int4, p_debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
v_ddl_ext_get text; 
v_ext_table text;
v_stg_table text;
v_ext_column_old text;
v_stg_column_old text;
v_stg_columns RECORD;
v_src_columns RECORD;
v_json_columns RECORD;
v_fld_nm_columns RECORD;
v_ext_column_new text;
v_stg_column_new text;
tmp_column text;

begin
drop table if exists tmp_columns;
create temporary table tmp_columns(columns text);
select sys_dwh.ext_get(p_src_stm_id:=p_src_stm_id, p_debug:=true, p_json:=null, p_load_mode:=1) into v_ddl_ext_get;
select
	'ext_' || pp.nm || '.' || case
								when p.ext_pb_itrv is not null then 'pll_'
								else ''
							  end || p.nm,
	'stg_' || pp.nm || '.' || p.nm
	into
	v_ext_table,
	v_stg_table
	from sys_dwh.prm_src_stm p
	join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id
	where p.src_stm_id = p_src_stm_id;
v_ddl_ext_get := REPLACE(v_ddl_ext_get, v_ext_table, v_ext_table || '_pk');
v_ddl_ext_get := REPLACE(v_ddl_ext_get, v_stg_table, v_stg_table || '_pk');
select sys_dwh.get_s2t_src_clmn(p_src_stm_id)
into v_ext_column_old;
select sys_dwh.get_s2t_stg_clmn(p_src_stm_id)
into v_stg_column_old;
v_stg_column_new := '';
v_ext_column_new := '';


insert into tmp_columns (select column_name_stg from sys_dwh.prm_s2t_stg where 1=1 and 
src_stm_id = p_src_stm_id and 
key_type_src like 'PK%');--поля like PK грузим
--insert into tmp_columns (select ext_pb from sys_dwh.prm_s2t_stg_src where 1=1 and 
--src_stm_id = p_src_stm_id and ext_pb <> '');--если поле-источник есть в этих полях карты, то их тоже грузим
--insert into tmp_columns (select column_fltr from sys_dwh.prm_s2t_stg_src where 1=1 and 
--src_stm_id = p_src_stm_id and column_fltr <>'');--если поле-источник есть в этих полях карты, то их тоже грузим
--insert into tmp_columns (select ext_prd_pb from sys_dwh.prm_s2t_stg_src where 1=1 and 
--src_stm_id = p_src_stm_id and ext_prd_pb <> '');--если поле-источник есть в этих полях карты, то их тоже грузим

for v_json_columns in (select where_json::json from sys_dwh.prm_s2t_rdv_rule where src_stm_id =p_src_stm_id and where_json <> '')
		LOOP
			for v_fld_nm_columns in (select distinct fld_nm from json_populate_recordset(null::record,
v_json_columns.where_json) as (fld_nm text))
				loop
					insert into tmp_columns values (v_fld_nm_columns.fld_nm);
				end loop;
			end loop;
		
		
for v_stg_columns in (SELECT lower(column_name_stg) as column_name_stg, column_name_src
		FROM sys_dwh.prm_s2t_stg
		WHERE src_stm_id = p_src_stm_id
		 AND now() BETWEEN eff_dt AND end_dt and column_name_stg in (select distinct lower(columns) from tmp_columns))
	loop
			v_stg_column_new := v_stg_column_new || v_stg_columns.column_name_stg || ',';
			v_ext_column_new := v_ext_column_new ||' "'|| v_stg_columns.column_name_src || '",';
	end loop;
v_stg_column_new := RTRIM(v_stg_column_new,',')||'';
v_ext_column_new := RTRIM(v_ext_column_new,',')||'';
v_ddl_ext_get := REPLACE(v_ddl_ext_get, v_stg_column_old, v_stg_column_new);
v_ddl_ext_get := REPLACE(v_ddl_ext_get, v_ext_column_old, v_ext_column_new);
	
if p_debug = true then 
return(v_ddl_ext_get);
else EXECUTE v_ddl_ext_get;
end if;
RETURN 'Success';
end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.generate_data_stg(json, bool);

CREATE OR REPLACE FUNCTION sys_dwh.generate_data_stg(p_json json, p_debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

v_rec record; --Переменная для работы с циклом

v_val text; --Переменная для сбора полей вставки в строку

v_val_all text; --Переменная для фиксации строки вставки

v_ins text; --Переменная для сбора полей таблицы

v_ins_all text := ''; --Переменная для фиксации строки таблицы

v_all text; --Переменная для формирования нескольких строк вставки

v_all_union text := ''; --Переменная для формирования нескольких строк вставки

v_i int8; --Перменная для формирования нескольких строк вставки

v_schema text; --Имя схемы

v_table text; --Имя таблицы

v_cnt int8;--Количество строк

v_full_table_name text := ''; --Полное имя таблицы

v_ssn int8; --Переменная для фиксации ssn

v_ins_begin text; --Начало полей

v_src_stm_id int8; --Переменная для фиксации src_stm_id

v_load_mode int8; --Переменная для load_mode

v_act_dt text; --Переменная для поле с датой

v_act_dt_synth text; --Переменная для даты

begin

--Парсим json

select (p_json ->>'p_cnt')::int8 into v_cnt;

select (p_json ->>'p_ssn')::int8 into v_ssn;

select (p_json ->>'p_src_stm_id')::int8 into v_src_stm_id;

if v_src_stm_id is null

then raise '%', 'Заданый ssn is NULL';

end if;

--Ищем таблицу в s2t-карте

select schema_stg, table_name_stg

into v_schema, v_table

from sys_dwh.prm_s2t_stg_src

where end_dt = '9999-12-31'::date

and src_stm_id = v_src_stm_id;

if v_schema is null or v_table is null

then raise '%', 'Таблица по заданному ssn отсутствует в таблице sys_dwh.prm_s2t_stg_src';

end if;

--Фиксируем полное имя

v_full_table_name := v_schema||'.'||v_table;

--Фиксируем load_mode

select distinct s.load_mode, ss.column_name_stg

into v_load_mode, v_act_dt

from sys_dwh.prm_src_stm s

left join sys_dwh.prm_s2t_stg ss on ss.src_stm_id = s.src_stm_id 

and s.act_dt_column = ss.column_name_src

and ss.end_dt = '9999-12-31'::date

where s.src_stm_id = v_src_stm_id;

if v_load_mode is null

then raise '%', 'Load mode по заданному ssn отсутствует в таблице sys_dwh.prm_src_stm';

end if;

--Фиксируем начало полей для вставки

v_ins_begin := '(ssn, upd_dttm, dml_dttm, dml_tp, '||case when v_act_dt is null then 'act_dt, ' else '' end;

--Фиксируем начало вставки

v_val_all := '('||v_ssn||', now() at time zone ''utc'', now(), ''I'''||case when v_act_dt is null then ', (now()-interval ''1 days'')::date' else '' end;

--Открываем цикл со сбором полей

for v_rec in (select c.column_name, s.datatype_stg as data_type, c.character_maximum_length, case when c.numeric_precision > 2 then c.numeric_precision-1 else c.numeric_precision end as numeric_precision

				from sys_dwh.prm_s2t_stg s 

			 inner join information_schema.columns c 

				on s.table_name_stg = c.table_name 

				and s.schema_stg = c.table_schema 

				and s.column_name_stg = c.column_name

				and s.src_stm_id = v_src_stm_id

			where s.end_dt = '9999-12-31'::date) loop 

	v_act_dt_synth := ''''||((now() - interval '100 days' + interval '1 days'*trunc(random()*100))::date)::text||'''::date';

    v_val := case when v_rec.column_name = v_act_dt then v_act_dt_synth||', '||v_act_dt_synth 

		   when v_rec.data_type similar to 'char%|varchar%|text%' then 'substr(md5(random()::text), 0, '||coalesce(v_rec.character_maximum_length, 25)::text||')'

           when v_rec.data_type similar to 'date%|timestamp%' then '(now() - interval ''100 days'' + interval ''1 days''*trunc(random()*100))::'||v_rec.data_type

           when v_rec.data_type similar to 'int%|smallint%|numeric%|bigint%|double precision%|decimal%|real%|float%|money%' then '(random()*10*'||v_rec.numeric_precision::text||')::'||v_rec.data_type

           when v_rec.data_type in ('uuid') then 'md5(random()::text)::uuid' 

           else 'NULL' end;

     v_ins := case when v_rec.column_name = v_act_dt then 'act_dt, '||v_rec.column_name else v_rec.column_name end;

     v_val_all := v_val_all||', '||v_val;

     v_ins_all := v_ins_all||case when v_ins_all='' then v_ins else ', '||v_ins end;

end loop;

--Формируем нужное количество записей

for v_i in 1..v_cnt loop

    v_all_union := v_all_union||case when v_all_union='' then v_val_all else E'),\n'||v_val_all end;

end loop;

--Финальный запрос

v_ins_all := v_ins_begin||v_ins_all||')';

v_all := 'insert into '||v_full_table_name||v_ins_all||E'\n values\n'||

v_all_union||');'; 

if p_debug then

   return v_all;

end if;

execute v_all;

return  'SUCCESS';

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_hash_diff_rdv(text);

CREATE OR REPLACE FUNCTION sys_dwh.get_hash_diff_rdv(p_rdv_table_name text)
	RETURNS TABLE (sql_field_hash text, sql_field_hash_s text, sql_field_hash_chg text)
	LANGUAGE plpgsql
	IMMUTABLE
AS $$
	

declare

	v_tbl_nm_rdv text = p_rdv_table_name;

	v_exec_sql text = '';

	v_sql_field_hash text = 'md5(';

	v_sql_field_hash_s text = 'md5(';

	v_sql_field_hash_chg text = 'md5(';

	v_cnt_add int = 0;

	v_cnt_rts int = 0;

	v_cnt_alg int = 0;

	v_cnt_hsh int = 0;

	v_is_cnt_pk int = 0;

	v_algorithm text = '';

	v_rec_cr record;

	p_err text;

	err_code text; -- код ошибки

    msg_text text; -- текст ошибки

    exc_context text; -- контекст исключения

    msg_detail text; -- подробный текст ошибки

    exc_hint text; -- текст подсказки к исключению

	begin

		--флаг наличия полей помимо pk

		select distinct count(1) - count(1) filter (where key_type_src similar to '(%PK%)')

			into v_is_cnt_pk

			from sys_dwh.prm_s2t_rdv

				where schema_rdv||'.'||table_name_rdv = v_tbl_nm_rdv

				--and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;

				and (now() at time zone 'utc')::date between eff_dt and end_dt;

		begin

			--блок предобработки

			select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv')) t into v_cnt_add;

			select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg')) t into v_cnt_rts;

			select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm')) t into v_cnt_alg;

			select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff')) t into v_cnt_hsh;

			if v_cnt_hsh = 0 then

				select count (*) 

					into v_cnt_hsh

					from sys_dwh.prm_task

						where success_fl = true

							and type_of_task = 'change_hash_diff'

							and table_name = v_tbl_nm_rdv;

			end if;

			/*case when v_cnt_add > 0 then

				select backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'copy_from_backup') into v_backup_table_name;

			when v_cnt_rts > 0 then

				select backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'copy_from_backup') into v_backup_table_name;

			when v_cnt_alg > 0 then

				select backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'copy_from_backup') into v_backup_table_name;

			when v_cnt_hsh > 0 then

				select backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'copy_from_backup') into v_backup_table_name;

			end case;*/

		end;

		--Открываем цикл по полям

		for v_rec_cr in (select schema_stg, table_name_stg, column_name_stg, column_name_rdv, datatype_stg, datatype_rdv, key_type_src, replace(datatype_stg_transform,'''','''''') datatype_stg_transform, algorithm,

				coalesce(ad.column_name_add, coalesce(rts.column_name_rts, coalesce(alg.column_name_alg, hsh.column_name_hsh))) column_name_chg ,

				case when rts.column_name_rts is not null then true end is_rts,

				case when ad.column_name_add is not null then true end is_add,

				case when alg.column_name_alg is not null then true end is_alg,

				case when hsh.column_name_hsh is not null then true end is_hsh

			from sys_dwh.prm_s2t_rdv r

				join information_schema.columns o on o.table_schema = r.schema_rdv and o.table_name = r.table_name_rdv and o.column_name = r.column_name_rdv

				left join (select trim(string_agg(column_name, ',')) column_name_add 

						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv') 

							) ad on ad.column_name_add = r.column_name_rdv

				left join (select trim(string_agg(column_name, ',')) column_name_rts 

						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg') 

							) rts on rts.column_name_rts = r.column_name_rdv

				left join (select trim(string_agg(column_name, ',')) column_name_alg 

						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm') 

							) alg on alg.column_name_alg = r.column_name_rdv

				left join (select trim(string_agg(column_name, ',')) column_name_hsh 

						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff') 

							) hsh on hsh.column_name_hsh = r.column_name_rdv

				where column_name_rdv is not null

					and column_name_rdv <> ''

					--and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date

					and (now() at time zone 'utc')::date between r.eff_dt and r.end_dt

					and schema_rdv||'.'||table_name_rdv = v_tbl_nm_rdv

					order by o.ordinal_position)

		loop

			--Выбираем поля, не являющиеся ключами

			if (v_rec_cr.key_type_src not in ('PK','PK,FK','PK,FK,GU','PK,GU') or v_rec_cr.key_type_src is null or v_rec_cr.key_type_src = '' or v_is_cnt_pk = 0) then 

				--Фиксируем внутрь переменной хэш полей

				if v_rec_cr.column_name_rdv like 'gk_%' then

					v_sql_field_hash := v_sql_field_hash||'gk_'||v_rec_cr.column_name_stg||'::text||''''!~@#''''||';

					v_sql_field_hash_s := v_sql_field_hash_s||'s.gk_'||v_rec_cr.column_name_stg||'::text||''''!~@#''''||';

					

					if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 then

						if v_rec_cr.is_add or v_rec_cr.is_rts or v_rec_cr.is_alg then

							if v_rec_cr.is_rts then

								v_sql_field_hash_chg := v_sql_field_hash_chg||'t.gk_'||v_rec_cr.column_name_stg||'::text||''''!~@#''''||';

							elsif v_rec_cr.is_add or v_rec_cr.is_alg then

								v_sql_field_hash_chg := v_sql_field_hash_chg||'s.gk_'||v_rec_cr.column_name_stg||'::text||''''!~@#''''||';

							else

								v_sql_field_hash_chg := v_sql_field_hash_chg||'t.'||v_rec_cr.column_name_rdv||'::text||''''!~@#''''||';

							end if;

						end if;

					elsif v_cnt_hsh <> 0 then

						v_sql_field_hash_chg := v_sql_field_hash_chg||'t.'||v_rec_cr.column_name_rdv||'::text||''''!~@#''''||';

					end if;

					

				elsif v_rec_cr.algorithm is not null and v_rec_cr.algorithm <> '' then

					begin

						with sel as

							(select num, when_val, val, query_type, grp from json_populate_recordset(null::record, v_rec_cr.algorithm::json)

								as (num int4, when_val text, val text, query_type int4, grp int4)),

							sel_2 as (select 

								(case when num = 1 and query_type = 2

										then 'case '

									  when num = 1 and query_type = 1

										then ''

								else '' end)||

								(case when query_type = 1 then coalesce(val,'') when query_type = 2 then coalesce(when_val,'') end)||' '||

								(case when query_type = 2 then coalesce(val,'') else '' end)||' '||

								(case when ((query_type = 2) and (lead(num) over(partition by grp order by num) is null)) 

									then 'end'

								else ''

								end) as f

							from sel)

							select ''||string_agg(replace(f,'''',''''''), E'\n') into v_algorithm

							from sel_2;

						if v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then

							v_sql_field_hash := v_sql_field_hash||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg',v_algorithm)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

							v_sql_field_hash_s := v_sql_field_hash_s||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg',v_algorithm)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

							if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 then

								if v_rec_cr.column_name_rdv = v_rec_cr.column_name_chg then

									v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg',v_algorithm)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

								else

									v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

								end if;

							elsif v_cnt_hsh <> 0 then

								v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

							end if;

						else

							v_sql_field_hash := v_sql_field_hash||'coalesce('||v_algorithm||'::text, ''''!~@#'''')||''''!~@#''''||';

							v_sql_field_hash_s := v_sql_field_hash_s||'coalesce('||v_algorithm||'::text, ''''!~@#'''')||''''!~@#''''||';

							if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 then

								if v_rec_cr.column_name_rdv = v_rec_cr.column_name_chg then

									v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||v_algorithm||'::text, ''''!~@#'''')||''''!~@#''''||';

								else

									v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

								end if;

							elsif v_cnt_hsh <> 0 then

								v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

							

							end if;

						end if;

							

					end;

				elsif v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then

					v_sql_field_hash := v_sql_field_hash||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg',''||v_rec_cr.column_name_stg)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

					v_sql_field_hash_s := v_sql_field_hash_s||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg','s.'||v_rec_cr.column_name_stg)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

					

					if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 then

						if v_rec_cr.column_name_rdv = v_rec_cr.column_name_chg then

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg','s.'||v_rec_cr.column_name_stg)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						else

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(t.'||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						end if;

					elsif v_cnt_hsh <> 0 then

						if v_rec_cr.column_name_rdv = v_rec_cr.column_name_chg then

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg','s.'||v_rec_cr.column_name_stg)||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						else

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(t.'||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						end if;

					end if;

				else

					v_sql_field_hash := v_sql_field_hash||'coalesce('||v_rec_cr.column_name_stg||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

					v_sql_field_hash_s := v_sql_field_hash_s||'coalesce(s.'||v_rec_cr.column_name_stg||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

					if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 then

						if v_rec_cr.is_rts then

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(t.'||v_rec_cr.column_name_stg||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						elsif v_rec_cr.is_add or v_rec_cr.is_alg then

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(s.'||v_rec_cr.column_name_stg||'::'||v_rec_cr.datatype_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						else

							v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(t.'||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

						end if;

					elsif v_cnt_hsh <> 0 then

						v_sql_field_hash_chg := v_sql_field_hash_chg||'coalesce(t.'||v_rec_cr.column_name_rdv||'::text, ''''!~@#'''')||''''!~@#''''||';

					

					end if;

					

				end if;



				if v_sql_field_hash is null or v_sql_field_hash_s is null then

					raise exception '%', 'Хэш не найден или пустой';

				end if;

			end if;

		end loop;

		v_sql_field_hash := v_sql_field_hash||'''''!~@#'''')';

		v_sql_field_hash_s := v_sql_field_hash_s||'''''!~@#'''')';   

		v_sql_field_hash_chg := v_sql_field_hash_chg||'''''!~@#'''')';

		--Возвращаем результат выполнения           

	v_exec_sql := 'select '''|| v_sql_field_hash||'''::text sql_field_hash,'''||v_sql_field_hash_s||'''::text sql_field_hash_s, '''||v_sql_field_hash_chg||'''::text sql_field_hash_chg';

	--raise exception '%', v_exec_sql;

	return query

	execute v_exec_sql;

exception

      when others then

		GET STACKED DIAGNOSTICS

			err_code = RETURNED_SQLSTATE, -- код ошибки

			msg_text = MESSAGE_TEXT, -- текст ошибки

			exc_context = PG_CONTEXT, -- контекст исключения

			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки

			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению

			p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'';

			raise exception '%', p_err;		

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_json4log(text, text, text, text, text, text, text, text, text, text, text, text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.get_json4log(p_json_ret text, p_step text, p_descr text, p_start_dttm text, p_end_dttm text DEFAULT NULL::text, p_ins_qty text DEFAULT ''::text, p_upd_qty text DEFAULT ''::text, p_del_qty text DEFAULT ''::text, p_rej_qty text DEFAULT ''::text, p_val text DEFAULT ''::text, p_err text DEFAULT ''::text, p_log_tp text DEFAULT 1, p_debug_lvl text DEFAULT 3, p_cls text DEFAULT ''::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	





declare

  v_output_text text;

begin



if coalesce(p_json_ret,'')=''

then

	v_output_text := '[{';

else

	v_output_text := p_json_ret||',{';

end if;



v_output_text := v_output_text||

	'"step":'||to_json(coalesce(p_step,''))||

	',"descr":'||to_json(coalesce(p_descr,''))||

	',"start_dttm":'||to_json(coalesce(p_start_dttm,''))||

	',"end_dttm":'||to_json(coalesce(p_end_dttm,((clock_timestamp() at time zone 'utc')::text)))||

	',"ins_qty":'||to_json(coalesce(p_ins_qty,''))||

	',"upd_qty":'||to_json(coalesce(p_upd_qty,''))||

	',"del_qty":'||to_json(coalesce(p_del_qty,''))||

	',"rej_qty":'||to_json(coalesce(p_rej_qty,''))||

	',"val":'||to_json(coalesce(p_val,''))||

	',"err":'||to_json(coalesce(p_err,''))||

	',"log_tp":'||to_json(coalesce(p_log_tp,''))||

	',"debug_lvl":'||to_json(coalesce(p_debug_lvl,''))||'}'||

	p_cls;





    return(v_output_text);   

    --Регистрируем ошибки

    exception

      when others then

      raise exception '%', sqlerrm;   

end;

  


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_max_dml_dttm(int4, int4);

CREATE OR REPLACE FUNCTION sys_dwh.get_max_dml_dttm(p_src_stm_id int4, p_time_shift int4 DEFAULT NULL::integer)
	RETURNS timestamp
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare



v_start_dttm text;

v_time_shift int;

v_time_shift_interval interval;

v_max_dml_dttm timestamp; --финальный скрипт



begin



/*получение смещения для конкретной таблицы*/

if p_time_shift is NULL then

	select dml_dttm_shift

	into v_time_shift

	from sys_dwh.prm_src_stm pss

	where src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

else

	v_time_shift:=p_time_shift;

end if;



	v_time_shift = coalesce(v_time_shift,72); --по-умолчанию 72 часа

	v_time_shift_interval:= (v_time_shift::text||' hour')::interval;

	

	/*вычисление смещения по src_stm_id*/

	

	select  max(max_dml_dttm)

	into v_max_dml_dttm

	from 

	(		

	     select coalesce((t.value ->> 'max_dml_dttm'), (now() at time zone 'utc')::text)::timestamp as max_dml_dttm

	                from sys_dwh.reg_dwh rd

	                LEFT JOIN LATERAL json_array_elements(rd.json_value) t(value) ON true

	            where json_tp = 'src_ssn'  

	            and   rd.upd_dttm  > ((now() at time zone 'utc') -  interval '30 days')

	            and t.value ->> 'src_stm_id' = p_src_stm_id::text

		 order by upd_dttm desc

         limit 1

	)s;

	

	v_max_dml_dttm:=coalesce(v_max_dml_dttm, (now() at time zone 'utc') );

	

return v_max_dml_dttm - v_time_shift_interval;	 

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_s2t_src_clmn(int4);

CREATE OR REPLACE FUNCTION sys_dwh.get_s2t_src_clmn(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE
	v_rec_s2t_src_clmn record;
	v_cnt int8 := 0;
	v_output_text text;

BEGIN
--Проверки:
--Проверяем наличие таблиц в sys_dwh.prm_src_stm
	SELECT sys_dwh.prv_tbl_id(p_src_stm_id)
	INTO v_output_text;
--Проверяем наличие p_src_stm_id в sys_dwh.prm_s2t_stg
	select count(1) into v_cnt
	from sys_dwh.prm_s2t_stg
    where src_stm_id = p_src_stm_id;
    if v_cnt = 0 then
    v_output_text := 'The TABLE is missing in sys_dwh.prm_s2t_stg';
    raise exception '%', v_output_text;
    end if;

--получаем параметры для источника
	v_output_text := '';
	FOR v_rec_s2t_src_clmn IN(
		SELECT column_name_src
		FROM sys_dwh.prm_s2t_stg
		WHERE src_stm_id = p_src_stm_id
		 AND now() BETWEEN eff_dt AND end_dt
		ORDER BY column_name_src
		)LOOP
			v_output_text := v_output_text||' "'||v_rec_s2t_src_clmn.column_name_src||'",';
		END LOOP;

	v_output_text := RTRIM(v_output_text,',')||' ';

	RETURN(v_output_text);

	--Регистрируем ошибки
	EXCEPTION
	WHEN OTHERS THEN 
	--RETURN('Error: ' || SQLERRM);
	RAISE EXCEPTION '%', 'Error: ' || SQLERRM;   

END;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_s2t_stg_clmn(int4);

CREATE OR REPLACE FUNCTION sys_dwh.get_s2t_stg_clmn(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE
	v_rec_s2t_stg_clmn record;
	v_cnt int8 := 0;
	v_output_text text;

BEGIN
--Проверки:
--Проверяем наличие таблиц в sys_dwh.prm_src_stm
	SELECT sys_dwh.prv_tbl_id(p_src_stm_id)
	INTO v_output_text;
--Проверяем наличие p_src_stm_id в sys_dwh.prm_s2t_stg
	select count(1) into v_cnt
	from sys_dwh.prm_s2t_stg
    where src_stm_id = p_src_stm_id;
    if v_cnt = 0 then
    v_output_text := 'The TABLE is missing in sys_dwh.prm_s2t_stg';
    raise exception '%', v_output_text;
    end if;

--получаем параметры для источника
	v_output_text := '';
	FOR v_rec_s2t_stg_clmn IN(
		SELECT column_name_stg
		FROM sys_dwh.prm_s2t_stg
		WHERE src_stm_id = p_src_stm_id
		 AND now() BETWEEN eff_dt AND end_dt
		ORDER BY column_name_stg
		)LOOP
			v_output_text := v_output_text||' '||v_rec_s2t_stg_clmn.column_name_stg||',';
		END LOOP;

	v_output_text := RTRIM(v_output_text,',')||' ';

	RETURN(v_output_text);

	--Регистрируем ошибки
	EXCEPTION
	WHEN OTHERS THEN 
	--RETURN('Error: ' || SQLERRM);
	RAISE EXCEPTION '%', 'Error: ' || SQLERRM;   

END;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_s2t_stg_clmntp(int4);

CREATE OR REPLACE FUNCTION sys_dwh.get_s2t_stg_clmntp(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE
	v_rec_s2t_stg_clmn record;
	v_cnt int8 := 0;
	v_output_text text;

BEGIN
--Проверки:
--Проверяем наличие таблиц в sys_dwh.prm_src_stm
	SELECT sys_dwh.prv_tbl_id(p_src_stm_id)
	INTO v_output_text;
--Проверяем наличие p_src_stm_id в sys_dwh.prm_s2t_stg
	select count(1) into v_cnt
	from sys_dwh.prm_s2t_stg
    where src_stm_id = p_src_stm_id;
    if v_cnt = 0 then
    v_output_text := 'The TABLE is missing in sys_dwh.prm_s2t_stg';
    raise exception '%', v_output_text;
    end if;

--получаем параметры для источника
	v_output_text := '';
	FOR v_rec_s2t_stg_clmn IN(
		SELECT column_name_src,datatype_stg
		FROM sys_dwh.prm_s2t_stg
		WHERE src_stm_id = p_src_stm_id
		 AND now() BETWEEN eff_dt AND end_dt
		ORDER BY column_name_src
		)LOOP
			v_output_text := v_output_text||' "'||v_rec_s2t_stg_clmn.column_name_src||'" '||v_rec_s2t_stg_clmn.datatype_stg||',';
		END LOOP;

	v_output_text := RTRIM(v_output_text,',')||' ';

	RETURN(v_output_text);

	--Регистрируем ошибки
	EXCEPTION
	WHEN OTHERS THEN 
	--RETURN('Error: ' || SQLERRM);
	RAISE EXCEPTION '%', 'Error: ' || SQLERRM;   

END;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_s2t_stg_clmntp_dbz(int4);

CREATE OR REPLACE FUNCTION sys_dwh.get_s2t_stg_clmntp_dbz(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE
	v_rec_s2t_stg_clmn record;
	v_cnt int8 := 0;
	v_output_text text;

BEGIN
--Проверки:
--Проверяем наличие таблиц в sys_dwh.prm_src_stm
	SELECT sys_dwh.prv_tbl_id(p_src_stm_id)
	INTO v_output_text;
--Проверяем наличие p_src_stm_id в sys_dwh.prm_s2t_stg
	select count(1) into v_cnt
	from sys_dwh.prm_s2t_stg
    where src_stm_id = p_src_stm_id;
    if v_cnt = 0 then
    v_output_text := 'The TABLE is missing in sys_dwh.prm_s2t_stg';
    raise exception '%', v_output_text;
    end if;

--получаем параметры для источника
	v_output_text := '';
	FOR v_rec_s2t_stg_clmn IN(
		SELECT column_name_src,datatype_stg
		FROM sys_dwh.prm_s2t_stg
		WHERE src_stm_id = p_src_stm_id
		 AND now() BETWEEN eff_dt AND end_dt
		ORDER BY column_name_src
		)loop			
		    
			IF v_rec_s2t_stg_clmn.datatype_stg != 'date' and v_rec_s2t_stg_clmn.datatype_stg != 'timestamp' then
				v_output_text := v_output_text||' '|| '(json_value::jsonb ->> ''' || v_rec_s2t_stg_clmn.column_name_src || '''::text)::' || v_rec_s2t_stg_clmn.datatype_stg || ' AS "'|| v_rec_s2t_stg_clmn.column_name_src || '",';
			else
				v_output_text := v_output_text||' '|| 'to_timestamp(((json_value::jsonb ->> ''' || v_rec_s2t_stg_clmn.column_name_src || '''::text)::bigint)/1000)::' || v_rec_s2t_stg_clmn.datatype_stg || ' AS "'|| v_rec_s2t_stg_clmn.column_name_src || '",';
			end if;
		END LOOP;

	v_output_text := RTRIM(v_output_text,',')||' ';

	RETURN(v_output_text);

	--Регистрируем ошибки
	EXCEPTION
	WHEN OTHERS THEN 
	--RETURN('Error: ' || SQLERRM);
	RAISE EXCEPTION '%', 'Error: ' || SQLERRM;   

END;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.get_task_prev_rdv(text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.get_task_prev_rdv(p_rdv_table_name text, p_type_of_changes text, p_type_of_task text)
	RETURNS TABLE (backup_table_name text, column_name text)
	LANGUAGE plpgsql
	IMMUTABLE
AS $$
	

declare

	v_exec_sql text;

	v_rec record;

begin

	v_exec_sql := 'select distinct backup_table_name, column_name from sys_dwh.prm_task s where active_fl = true and success_fl = false and table_name = '''||p_rdv_table_name||'''

		and type_of_changes = '''||p_type_of_changes||''' and type_of_task = '''||p_type_of_task||'''

		and (select distinct success_fl from sys_dwh.prm_task t where t.id_task=s.id_task and t.type_of_task = ''master_master'')';

	return query

	execute v_exec_sql;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_bdv_prttn(text, text, int4, date, date, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_bdv_prttn(p_trg_tbl text, p_src_tbl text, p_stm_id int4, p_dt_strt date, p_dt_end date, p_schm text DEFAULT 'bdv'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
v_cnt int8 := 0;
v_output_text text;
v_ddl text;
v_answr text;
v_owner text;
v_start_dttm text;
v_json_ret text := '';
v_prnt_prttn text;
 begin
   --Проверяем наличие таблиц на БД
	begin	
	v_start_dttm := timeofday()::timestamp;
	select count(1) into v_cnt 
	from  pg_catalog.pg_tables  
    where 1=1
    and lower(tablename) = p_trg_tbl
    and lower(schemaname) = p_schm;
    if v_cnt = 0 then
    v_output_text := 'The target table '||p_trg_tbl||' is missing on DB';
    raise exception '%', v_output_text;
    end if;
	select count(1) into v_cnt 
	from  pg_catalog.pg_tables  
    where 1=1
    and lower(tablename) = p_src_tbl
    and lower(schemaname) = p_schm;
    if v_cnt = 0 then
    v_output_text := 'The source table '||p_src_tbl||' is missing on DB';
    raise exception '%', v_output_text;
    end if;    
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверка наличия таблиц на БД',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверка наличия таблиц на БД',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем структуры таблиц
	begin	
	v_start_dttm := timeofday()::timestamp;
    select coalesce(count(1), 1) into v_cnt
    from  
   (select i.column_name, i.ordinal_position  
 	from information_schema.columns i 
 	where i.table_schema = p_schm and i.table_name = p_trg_tbl) t1
	full outer join 
	(select i.column_name, i.ordinal_position  
 	from information_schema.columns i 
 	where i.table_schema = p_schm and i.table_name = p_src_tbl) t2
  	 on t1.column_name = t2.column_name
 	and t1.ordinal_position = t2.ordinal_position
 	where t1.column_name is null 
       or t2.column_name is null;
    if v_cnt > 0 then
    v_output_text := 'The table structures vary';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Проверка структуры таблиц',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Проверка структуры таблиц',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем owners у таблиц
	begin	
	v_start_dttm := timeofday()::timestamp;
    select coalesce(count(1), 1) into v_cnt from
    (select tableowner from pg_catalog.pg_tables pt 
     where lower(schemaname) = p_schm
     and lower(tablename) = p_trg_tbl
	 except
	 select tableowner from pg_catalog.pg_tables pt 
	 where lower(schemaname) = p_schm
	 and lower(tablename) = p_src_tbl) as t;
    if v_cnt > 0 then
    v_output_text := 'The owners of the source table and the target table are different';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Проверка владельцев таблиц',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Проверка владельцев таблиц',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
   --Проверяем наличие соответствующих партиций
	begin	
	v_start_dttm := timeofday()::timestamp;   
   --Проверяем партицию
    select count(1) into v_cnt 
    from pg_catalog.pg_partitions
    where lower(schemaname) = p_schm
    and lower(tablename) = p_trg_tbl
    and parentpartitiontablename is null
    and partitionrangestart = p_stm_id::text;
    if v_cnt <> 1 then
    v_output_text := 'The partition for stm_id= '||p_stm_id::text||' is missing in the target table';
    raise exception '%', v_output_text;
    end if;
    select partitiontablename into v_prnt_prttn 
    from pg_catalog.pg_partitions
    where lower(schemaname) = p_schm
    and lower(tablename) = p_trg_tbl
    and parentpartitiontablename is null
    and partitionrangestart = p_stm_id::text;
    --Проверяем субпартицию
    select count(1) into v_cnt
    from pg_catalog.pg_partitions
    where lower(schemaname) = p_schm
    and lower(tablename) = p_trg_tbl
    and parentpartitiontablename = v_prnt_prttn
    and replace(replace(replace(partitionrangestart, '''', ''), '::date', ''), ' 00:00:00::timestamp without time zone', '') = to_char(p_dt_strt, 'yyyy-mm-dd')
    and replace(replace(replace(partitionrangeend, '''', ''), '::date', ''), ' 00:00:00::timestamp without time zone', '') = to_char(p_dt_end, 'yyyy-mm-dd');
    if v_cnt <> 1 then
    v_output_text := 'The subpartition for stm_id= '||p_stm_id::text||' and act_dt between'||to_char(p_dt_strt, 'yyyy-mm-dd')||
    ' and '||to_char(p_dt_end, 'yyyy-mm-dd')||' is missing in the target table';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Проверка наличия соответствующих партиций',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Проверка наличия соответствующих партиций',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем таблицу-источник с партицией
    begin
	v_start_dttm := timeofday()::timestamp;	
    v_ddl := 'alter table '||p_schm||'.'||p_trg_tbl||' alter partition for ('||p_stm_id::text||') 
    exchange partition for ('''||to_char(p_dt_strt, 'yyyy-mm-dd')||'''::date)
    with table '||p_schm||'.'||p_src_tbl;  
    execute v_ddl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_val := v_ddl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := v_ddl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Оповещаем об окончании
    return(v_json_ret||']');   
    --Регистрируем ошибки
    exception
    when others then 
    raise exception '%', v_json_ret;   
    end 

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_hub(int4, text, text, json);

CREATE OR REPLACE FUNCTION sys_dwh.load_hub(p_src_stm_id int4, p_hub text DEFAULT NULL::text, p_json text DEFAULT NULL::text, p_json_param json DEFAULT NULL::json)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
 
declare 
  v_rec_cr record;
  v_rec_json record;
  v_rec_gk_pk record;
  v_rec_json_ssn record;
  v_rec_gk_pk_table_name_rdv record;
  v_json_param json;
  j_is_load bool;
  v_src_stm_id_ref int;
  v_src_stm_id_ref_json int;
  j_tbl_nm_sdv text;
  v_tbl_nm_sdv text;
  v_table_name_rdv text;	
  v_schema_rdv text;  
  v_ref_to_hub text;
  v_hub_nm text;
  v_partitiontablename text;
  v_gk_pk_str text;
  v_gk_pk_str_when text;
  v_cnt bigint = 0;
  v_cnt_block int = 0;
  v_ssn text = '';
  v_exec_sql text;
  v_exec_part_sql text;
  v_exec_block_sql text;
  v_rec_cr_sql text;
  v_rule text = '';
  v_i_gk_pk int = 0;
  v_i int = 0;
  v_res int = -1;
  v_max_ret int = 1200;
  v_ret int = v_max_ret;
  v_json_ret text = '';
  v_json_ret_tmp text = '';
 
  v_start_dttm text;
  
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--таблица источник
	if p_json_param::text not like '[%' then
		v_json_param := ('['||replace(p_json_param::text,'\\','')||']')::json;
	end if;
	--return p_json_param::text;
	select distinct tbl_nm::text, is_load::bool from
			(select t.value ->> 'tbl_nm' as tbl_nm, t.value ->> 'is_load' as is_load from json_array_elements(v_json_param::json) t(value)) r  into j_tbl_nm_sdv, j_is_load;
	j_is_load := coalesce(j_is_load,false);
	if j_tbl_nm_sdv is null then 
	
		select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg
			--,schema_rdv
			--,table_name_rdv 
			into v_tbl_nm_sdv--, v_schema_rdv, v_table_name_rdv
			from sys_dwh.prm_s2t_rdv
				where src_stm_id = p_src_stm_id 
				and ((trim(ref_to_stg) <> '' 
				and ref_to_stg is not null)
				or (select count (*) 
				from information_schema.columns c
					join sys_dwh.prm_s2t_stg p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
					and c.table_name = p.table_name_stg
				  	where p.src_stm_id = p_src_stm_id
				  		and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date
				  		and column_name = 'gk_pk') > 0
				)
				and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
		v_rec_cr_sql := 'select distinct schema_stg, table_name_stg, column_name_stg, ref_to_hub, ref_to_stg  
		from sys_dwh.prm_s2t_rdv 
			where src_stm_id = '||p_src_stm_id::text||'
				and trim(ref_to_stg) <> ''''
				and ref_to_stg is not null
				and daterange(eff_dt,end_dt,''[]'')@>(now() at time zone ''utc'')::date';
	else
		v_tbl_nm_sdv := j_tbl_nm_sdv;
		v_rec_cr_sql := 'select distinct schema_stg, table_name_stg, column_name_stg, ref_to_hub, ref_to_stg  
		from sys_dwh.prm_s2t_rdv
			join information_schema.columns b on b.table_schema||''.''||b.table_name = '||v_tbl_nm_sdv||'
			and b.column_name = column_name_stg
			where src_stm_id = '||p_src_stm_id::text||'
				and trim(ref_to_stg) <> ''''
				and ref_to_stg is not null
				and daterange(eff_dt,end_dt,''[]'')@>(now() at time zone ''utc'')::date';
	end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		--p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||''', v_schema_rdv='''||v_schema_rdv||''', v_table_name_rdv='''||v_table_name_rdv||'''',
		p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Поиск таблицы источника',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
    into v_json_ret;
	raise exception '%', v_json_ret;
  end;
    v_start_dttm := clock_timestamp() at time zone 'utc';
	--Фиксируем номер сессии загрузки в таблице-источнике
	if p_json is null then 
		execute 'select max(ssn) from '||v_tbl_nm_sdv into v_ssn;
	else
		for v_rec_json_ssn in (select distinct ssn::int8 ssn from
			(select t.value ->> 'ssn' as ssn from json_array_elements(p_json::json) t(value)) r order by ssn::int8)
				loop
					if v_i = 0 then
						v_ssn := v_rec_json_ssn.ssn::text;
					else
						v_ssn := v_ssn||','||v_rec_json_ssn.ssn::text;
					end if;
					v_i = v_i + 1;
			end loop;
	end if;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Поиск ssn',
		p_start_dttm := v_start_dttm,
		p_val := 'ssn='''||v_ssn||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
	if v_ssn is null then
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Поиск ssn',
			p_start_dttm := v_start_dttm,
			p_val := 'Warning: ssn не найден',
			p_log_tp := '2',
			p_debug_lvl := '1',
			p_cls := ']')
	    into v_json_ret;
	return(v_json_ret);
	end if;
    
    --цикл по gk полям
	for v_rec_cr in execute v_rec_cr_sql
		loop
			
			--если не json
			if substring(trim(v_rec_cr.ref_to_stg) from 1 for 3) = 'stg' 
				and v_rec_cr.ref_to_hub = 
				(case when (p_hub is not null) 
					then p_hub 
					else v_rec_cr.ref_to_hub end ) then
				begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				  --находим src_stm_id для хаба
				  select src_stm_id into v_src_stm_id_ref 
							from sys_dwh.prm_src_stm 
								where nm = split_part(v_rec_cr.ref_to_stg, '.',2)
									and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
															where nm = regexp_replace(split_part(v_rec_cr.ref_to_stg, '.',1),'^stg_', ''));
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '3',
					p_descr := 'Поиск src_stm_id',
					p_start_dttm := v_start_dttm,
					p_val := 'src_stm_id='''||v_src_stm_id_ref::text||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
				if v_src_stm_id_ref is null then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '3',
						p_descr := 'Поиск src_stm_id',
						p_start_dttm := v_start_dttm,
						p_err := 'src_stm_id не найден v_rec_cr.ref_to_stg='''||coalesce(v_rec_cr.ref_to_stg,'')||'''',
						p_log_tp := '3',
						--p_cls := ']',
						p_debug_lvl := '1')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end if;
				exception when others then
				--if v_ssn is null then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '3',
						p_descr := 'Поиск src_stm_id',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
				--end if;
				raise exception '%', v_json_ret;
				end;
				--вносим в hub
				select ','||replace(replace(sys_dwh.load_set_hub('[{"j_tbl_nm_sdv":"'||v_tbl_nm_sdv||'",
						"j_src_stm_id_ref":"'||v_src_stm_id_ref::text||'", 
						"j_ref_to_hub":"'||v_rec_cr.ref_to_hub||'", 
						"j_column_name_stg":"'||v_rec_cr.column_name_stg||'",
						"j_ssn":"'||v_ssn||'",
						"j_is_load":'||j_is_load||',
						"j_src_stm_id":'||p_src_stm_id||'}]'), '[', ''), ']', '') into v_json_ret_tmp;
				v_json_ret := v_json_ret||coalesce(v_json_ret_tmp,'');
			end if;
			--json
			if substring(trim(v_rec_cr.ref_to_stg) from 1 for 1) = '[' then
				begin
					for v_rec_json in ( select tbl_nm, fld_nm, val, ref_to_hub from (
						select t.value ->> 'tbl_nm' as tbl_nm,
								t.value ->> 'fld_nm' as fld_nm,
								t.value ->> 'val' as val,
								t.value ->> 'ref_to_hub' as ref_to_hub
									from json_array_elements(v_rec_cr.ref_to_stg::json) t(value)
								) r 
								where 
									ref_to_hub = case when p_hub is not null then p_hub 
										else ref_to_hub end
					)
				    loop
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							--находим src_stm_id
							select src_stm_id into v_src_stm_id_ref_json 
								from sys_dwh.prm_src_stm 
									where nm = split_part(v_rec_json.tbl_nm, '.',2)
										and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
																where nm = regexp_replace(split_part(v_rec_json.tbl_nm, '.',1),'^stg_', ''));
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
											p_step := '15',
											p_descr := 'Поиск src_stm_id json',
											p_start_dttm := v_start_dttm,
											p_val := 'src_stm_id='''||v_src_stm_id_ref_json||'''',
											p_ins_qty := v_cnt_block::text,
											p_log_tp := '1',
											p_debug_lvl := '3')
										into v_json_ret;
							if v_src_stm_id_ref_json is null then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
											p_step := '15',
											p_descr := 'Поиск src_stm_id json',
											p_start_dttm := v_start_dttm,
											p_err := 'src_stm_id не найден v_rec_json.tbl_nm='''||coalesce(v_rec_json.tbl_nm,'')||'''',
											p_log_tp := '3',
											--p_cls := ']',
											p_debug_lvl := '1')
										into v_json_ret;
										raise exception '%', v_json_ret;
							end if;
						exception when others then	
										select sys_dwh.get_json4log(p_json_ret := v_json_ret,
											p_step := '15',
											p_descr := 'Поиск src_stm_id json',
											p_start_dttm := v_start_dttm,
											p_err := SQLERRM,
											p_log_tp := '3',
											p_cls := ']',
											p_debug_lvl := '1')
										into v_json_ret;
										raise exception '%', v_json_ret;
						end;
						--вносим в hub
						select ','||replace(replace(sys_dwh.load_set_hub('[{"j_tbl_nm_sdv":"'||v_tbl_nm_sdv||'",
							"j_src_stm_id_ref":"'||v_src_stm_id_ref_json::text||'", 
							"j_ref_to_hub":"'||v_rec_json.ref_to_hub||'", 
							"j_column_name_stg":"'||v_rec_cr.column_name_stg||'",
							"j_ssn":"'||v_ssn::text||'",
							"j_json_fld_nm":"'||v_rec_json.fld_nm||'", 
							"j_json_val":"'||v_rec_json.val||'",
							"j_is_load":'||j_is_load||',
							"j_src_stm_id":'||p_src_stm_id||'}]'), '[', ''), ']', '') into v_json_ret_tmp;
						v_json_ret := v_json_ret||coalesce(v_json_ret_tmp,'');
 				    end loop;
			    end;
			end if; 
		end loop;
	--проверяем на наличие gk_pk
	if j_tbl_nm_sdv is null then
	for v_rec_gk_pk_table_name_rdv in
		(
		select distinct p.schema_rdv, p.table_name_rdv, r.where_json
				from sys_dwh.prm_s2t_rdv p
					join information_schema.columns c
						on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
							and c.table_name = p.table_name_stg
							and column_name = 'gk_pk'
							and substring(trim(p.table_name_rdv) from 1 for 2) <> 'l_'
					join sys_dwh.prm_s2t_rdv_rule r
						on r.src_stm_id = p_src_stm_id
							and p.schema_rdv = r.schema_rdv
							and p.table_name_rdv = r.table_name_rdv
							and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date
					where p.src_stm_id = p_src_stm_id 
					and trim(key_type_src) like '%PK%'
					and 'rdv.h_'||(regexp_split_to_array(trim(p.table_name_rdv), '\_+'))[2] = case when p_hub is not null then p_hub 
											else 'rdv.h_'||(regexp_split_to_array(trim(p.table_name_rdv), '\_+'))[2] end
					and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date
		) loop
			v_i_gk_pk := 0;
			v_gk_pk_str := '';
		begin
	 		for v_rec_gk_pk in (select distinct column_name_stg  
				from sys_dwh.prm_s2t_rdv
					where src_stm_id = p_src_stm_id
					and v_rec_gk_pk_table_name_rdv.schema_rdv = schema_rdv
					and v_rec_gk_pk_table_name_rdv.table_name_rdv = table_name_rdv
					and trim(key_type_src) like '%PK%'
					and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
					order by column_name_stg)
				loop
					if v_i_gk_pk = 0 then 
						v_gk_pk_str := 'coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';	
						v_gk_pk_str_when := v_rec_gk_pk.column_name_stg||' is null ';	
					else	
						v_gk_pk_str := v_gk_pk_str||'||''||''||coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';	
						v_gk_pk_str_when := v_gk_pk_str_when||' and '||v_rec_gk_pk.column_name_stg||' is null ';
					end if;
					v_i_gk_pk := v_i_gk_pk + 1;
				end loop;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '27',
						p_descr := 'Сборка составного ключа',
						p_start_dttm := v_start_dttm,
						p_val := 'v_gk_pk_str='''||v_gk_pk_str||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
				    into v_json_ret;
		exception when others then
				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '27',
						p_descr := 'Сборка составного ключа',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
		end;
		--Парсим правила в переменную
				if v_rec_gk_pk_table_name_rdv.where_json is not null and v_rec_gk_pk_table_name_rdv.where_json <> '' then
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						with sel as
						(select distinct num::int4 num, 
							predicate::text predicate, 
							fld_nm::text fld_nm, 
							equal_type::text equal_type, 
							val::text val, 
							func_nm::text func_nm, 
							where_type::int4 where_type, 
							grp::int4 grp from
							(select t.value ->> 'num' as num,
								t.value ->> 'predicate' as predicate,
								t.value ->> 'fld_nm' as fld_nm,
								t.value ->> 'equal_type' as equal_type,
								t.value ->> 'val' as val,
								t.value ->> 'func_nm' as func_nm,
								t.value ->> 'where_type' as where_type,
								t.value ->> 'grp' as grp
								from json_array_elements(v_rec_gk_pk_table_name_rdv.where_json::json) t(value)) r order by grp::int4, num::int4),
						sel_2 as (select 
						(case when num = 1 then predicate||' (' else predicate||' ' end)||
						(case when where_type = 1 then ' '||fld_nm when where_type = 2 then func_nm end)||' '||
						' '||equal_type||' '||val||
						(case when lead(num) over(partition by grp order by num) is null then ')' else '' end) as f
						from sel)
						select ' and 1=1'||E' '||string_agg(f, E' ')||E' ' into v_rule
						from sel_2;
						
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '28',
							p_descr := 'Парсинг json-правила',
							p_start_dttm := v_start_dttm,
							p_val := 'v_rule='''||v_rule::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '28',
							p_descr := 'Парсинг json-правила',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
				end if;
				v_rule := coalesce(v_rule, '');
			
			v_exec_part_sql := '[{"j_tbl_nm_sdv":"'||v_tbl_nm_sdv||'",
							"j_src_stm_id_ref":"'||p_src_stm_id::text||'", 
							"j_ref_to_hub":"rdv.h_'||(regexp_split_to_array(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv), '\_+'))[2]||'", 
							"j_column_name_stg":"'||v_gk_pk_str||'",
							"j_ssn":"'||v_ssn::text||'",
							"j_rule":"'||v_rule||'",
							"j_gk_pk_str_when":"'||v_gk_pk_str_when||'",
							"j_is_load":'||j_is_load||',
							"j_src_stm_id":'||p_src_stm_id||'}]';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '29',
							p_descr := 'Составление json параметра',
							p_start_dttm := v_start_dttm,
							--p_val := 'v_exec_part_sql='''||v_exec_part_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
			--вносим в hub
			select ','||replace(replace(sys_dwh.load_set_hub(v_exec_part_sql), '[', ''), ']', '') into v_json_ret_tmp;
			v_json_ret := v_json_ret||coalesce(v_json_ret_tmp,'');
	end loop;
	end if;
    v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_prm_s2t_stg_2_dwh(text);

CREATE OR REPLACE FUNCTION sys_dwh.load_prm_s2t_stg_2_dwh(p_tbl_stg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_rec_s2t record;

v_rec_s2t_dwh record;

v_cnt int = 0;

v_cnt_ins int = 0;

v_cnt_upd int = 0;

v_cnt_del int = 0;

v_src_stm_id int;

v_hash_diff text;

v_schema_stg text;

v_table_name_stg text;

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

v_user text; --Перемнная для фиксирования пользователя

begin

--Проверяем источники в таблице stg_sys_dwh.prm_s2t_stg_src

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(*)

into v_cnt

from (select distinct schema_stg

from stg_sys_dwh.prm_s2t_stg_src) s;

--Логируем ошибку о большом количестве источников

if v_cnt > 1 then 

raise exception '%', 'Too many sources';

end if;

--Фиксируем источники в таблице stg_sys_dwh.prm_s2t_stg_src

select schema_stg

into v_schema_stg

from stg_sys_dwh.prm_s2t_stg_src

group by schema_stg;

--Логируем ошибку о пустом поле с источником

if v_schema_stg is null then 

raise exception '%', 'Source is null';

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg_src',

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg_src',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (SELECT schema_src, table_name_src, schema_stg, table_name_stg, pxf_name_src, ext_pb, ext_pb_itrv, act_dt_column, pk_src, ext_format, ext_encoding, column_fltr, column_fltr_from, column_fltr_to, ext_prd_pb, ext_prd_pb_itrv, ext_prd_pb_from, upd_user, cdc_tp,

md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(schema_stg,'-1')||

coalesce(table_name_stg,'-1')||

coalesce(pxf_name_src,'-1')||

coalesce(ext_pb,'-1')||

coalesce(ext_pb_itrv,'-1')||

coalesce(act_dt_column,'-1')||

coalesce(pk_src,'-1')||

coalesce(ext_format,'-1')||

coalesce(ext_encoding,'-1')||

coalesce(column_fltr,'-1')||

coalesce(column_fltr_from,'-1')||

coalesce(column_fltr_to,'-1')||

coalesce(ext_prd_pb,'-1')||

coalesce(ext_prd_pb_itrv,'-1')||

coalesce(ext_prd_pb_from,'-1')||

coalesce(cdc_tp,'-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_stg_src

where apply_f = 0

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), table_name_stg) = table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), schema_stg) = schema_stg

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_stg_src

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(*)

into v_cnt

from sys_dwh.prm_s2t_stg_src s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_stg_src

if v_cnt > 0 then 

select count(*), src_stm_id, 

md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(schema_stg,'-1')||

coalesce(table_name_stg,'-1')||

coalesce(pxf_name_src,'-1')||

coalesce(ext_pb,'-1')||

coalesce(ext_pb_itrv,'-1')||

coalesce(act_dt_column,'-1')||

coalesce(pk_src,'-1')||

coalesce(ext_format,'-1')||

coalesce(ext_encoding,'-1')||

coalesce(column_fltr,'-1')||

coalesce(column_fltr_from,'-1')||

coalesce(column_fltr_to,'-1')||

coalesce(ext_prd_pb,'-1')||

coalesce(ext_prd_pb_itrv,'-1')||

coalesce(ext_prd_pb_from,'-1')||

coalesce(cdc_tp,'-1'))::text as hash_diff

into v_cnt, v_src_stm_id, v_hash_diff 

from sys_dwh.prm_s2t_stg_src s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

group by src_stm_id, md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(schema_stg,'-1')||

coalesce(table_name_stg,'-1')||

coalesce(pxf_name_src,'-1')||

coalesce(ext_pb,'-1')||

coalesce(ext_pb_itrv,'-1')||

coalesce(act_dt_column,'-1')||

coalesce(pk_src,'-1')||

coalesce(ext_format,'-1')||

coalesce(ext_encoding,'-1')||

coalesce(column_fltr,'-1')||

coalesce(column_fltr_from,'-1')||

coalesce(column_fltr_to,'-1')||

coalesce(ext_prd_pb,'-1')||

coalesce(ext_prd_pb_itrv,'-1')||

coalesce(ext_prd_pb_from,'-1')||

coalesce(cdc_tp,'-1'))::text;

end if;

--Фиксируем src_stm_id для записи, отсутствующей в sys_dwh.prm_s2t_stg_src

if v_cnt = 0 then

select s.src_stm_id

into v_src_stm_id

from sys_dwh.prm_src_stm s

where s.nm = v_rec_s2t.table_name_stg

 and v_rec_s2t.schema_stg = (select 'stg_'||p.nm from sys_dwh.prm_src_stm p where s.prn_src_stm_id = p.src_stm_id);

--Уведомляем, если sys_dwh.prm_src_stm не заполнена для записи, отсутствующей в sys_dwh.prm_s2t_rdv

if v_src_stm_id is null then

raise exception '%', 'The src_stm_id in sys_dwh.prm_src_stm is empty';

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_stg_src запись

insert into sys_dwh.prm_s2t_stg_src(

schema_src,

table_name_src,

schema_stg,

table_name_stg,

pxf_name_src,

ext_pb,

ext_pb_itrv,

act_dt_column,

pk_src,

ext_format,

ext_encoding,

column_fltr,

column_fltr_from,

column_fltr_to,

ext_prd_pb, 

ext_prd_pb_itrv, 

ext_prd_pb_from,

upd_user,

cdc_tp,

src_stm_id,

eff_dt,

end_dt)

values(

v_rec_s2t.schema_src,

v_rec_s2t.table_name_src,

v_rec_s2t.schema_stg,

v_rec_s2t.table_name_stg,

v_rec_s2t.pxf_name_src,

v_rec_s2t.ext_pb,

v_rec_s2t.ext_pb_itrv,

v_rec_s2t.act_dt_column,

v_rec_s2t.pk_src,

v_rec_s2t.ext_format,

v_rec_s2t.ext_encoding,

v_rec_s2t.column_fltr,

v_rec_s2t.column_fltr_from,

v_rec_s2t.column_fltr_to,

v_rec_s2t.ext_prd_pb, 

v_rec_s2t.ext_prd_pb_itrv, 

v_rec_s2t.ext_prd_pb_from,

v_rec_s2t.upd_user,

v_rec_s2t.cdc_tp,

v_src_stm_id,

v_rec_s2t.eff_dt,

v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_stg_src s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_stg_src, сверяем атрибуты

if v_cnt > 0 then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_stg_src

if v_hash_diff <> v_rec_s2t.hash_diff then

update sys_dwh.prm_s2t_stg_src s set end_dt = current_date-1 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_stg_src(

schema_src,

table_name_src,

schema_stg,

table_name_stg,

pxf_name_src,

ext_pb,

ext_pb_itrv,

act_dt_column,

pk_src,

ext_format,

ext_encoding,

column_fltr,

column_fltr_from,

column_fltr_to,

ext_prd_pb, 

ext_prd_pb_itrv, 

ext_prd_pb_from,

upd_user,

cdc_tp,

src_stm_id,

eff_dt,

end_dt)

values(

v_rec_s2t.schema_src,

v_rec_s2t.table_name_src,

v_rec_s2t.schema_stg,

v_rec_s2t.table_name_stg,

v_rec_s2t.pxf_name_src,

v_rec_s2t.ext_pb,

v_rec_s2t.ext_pb_itrv,

v_rec_s2t.act_dt_column,

v_rec_s2t.pk_src,

v_rec_s2t.ext_format,

v_rec_s2t.ext_encoding,

v_rec_s2t.column_fltr,

v_rec_s2t.column_fltr_from,

v_rec_s2t.column_fltr_to,

v_rec_s2t.ext_prd_pb, 

v_rec_s2t.ext_prd_pb_itrv, 

v_rec_s2t.ext_prd_pb_from,

v_rec_s2t.upd_user,

v_rec_s2t.cdc_tp,

v_src_stm_id,

v_rec_s2t.eff_dt,

v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_stg_src s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_ssn='||v_src_stm_id||' v_table_name_stg='||v_rec_s2t.table_name_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_ssn='||v_src_stm_id||' v_table_name_stg='||v_rec_s2t.table_name_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_stg_src

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_stg_src;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_stg_src

if v_cnt > 0 then

select max(upd_user) into v_user from stg_sys_dwh.prm_s2t_stg_src;

update sys_dwh.prm_s2t_stg_src p set end_dt = now()::date-1, upd_user = v_user

where 

p.schema_stg = v_schema_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), p.table_name_stg) = p.table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), p.schema_stg) = p.schema_stg

and not exists (select 1 from stg_sys_dwh.prm_s2t_stg_src s

where s.schema_stg = v_schema_stg

and s.schema_stg = p.schema_stg and s.table_name_stg = p.table_name_stg);

GET DIAGNOSTICS v_cnt_del = ROW_COUNT;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_stg_src is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_stg_src where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_val := 'v_schema_stg='||v_schema_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_schema_stg='||v_schema_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_ref_hub(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_ref_hub(p_src_stm_id int4, p_json text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

 

declare 

  v_rec_cr record;

  v_rec_json record;

  v_rec_gk_pk record;

  v_rec_json_ssn record;

  v_rec_gk_pk_table_name_rdv record;

  --v_src_stm_id int;

  v_src_stm_id_ref int;

  v_src_stm_id_ref_json int;

  v_tbl_nm_sdv text;

  v_table_name_rdv text;

  v_schema_rdv text;

  v_ref_to_hub text;

  v_hub_nm text;

  v_partitiontablename text;

  v_gk_pk_str text;

  v_gk_pk_str_when text;

  v_rule text;

  v_cnt bigint = 0;

  v_cnt_block int = 0;

  v_ssn text = '';

  v_exec_sql text;

  v_exec_part_sql text;

  v_exec_block_sql text;

  --v_output_text text;

  v_i_gk_pk int = 0;

  v_i int = 0;

  v_res int = -1;

  v_max_ret int = 1200;

  v_ret int = v_max_ret;

  v_json_ret text = '';

 

  v_start_dttm text;

  

  err_code text; -- код ошибки

  msg_text text; -- текст ошибки

  exc_context text; -- контекст исключения

  msg_detail text; -- подробный текст ошибки

  exc_hint text; -- текст подсказки к исключению

begin

  begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	--таблица источник

	select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg--,

		--schema_rdv,

		--table_name_rdv 

		into v_tbl_nm_sdv--, v_schema_rdv, v_table_name_rdv

		from sys_dwh.prm_s2t_rdv

			where src_stm_id = p_src_stm_id 

			and ((trim(ref_to_stg) <> '' 

			and ref_to_stg is not null)

			or (select count (*) 

			from information_schema.columns c

				join sys_dwh.prm_s2t_stg p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 

				and c.table_name = p.table_name_stg

			  	where p.src_stm_id = p_src_stm_id

			  		and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date

			  		and column_name = 'gk_pk') > 0

			)

			and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Поиск таблицы источника',

		p_start_dttm := v_start_dttm,

		--p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||''', v_schema_rdv='''||v_schema_rdv||''', v_table_name_rdv='''||v_table_name_rdv||'''',

		p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',

		p_log_tp := '1',

		p_debug_lvl := '3')

    into v_json_ret;

  exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Поиск таблицы источника',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_debug_lvl := '1',

		p_cls := ']')

    into v_json_ret;

	raise exception '%', v_json_ret;

  end;

    v_start_dttm := clock_timestamp() at time zone 'utc';

	--Фиксируем номер сессии загрузки в таблице-источнике

	if p_json is null then 

		execute 'select max(ssn) from '||v_tbl_nm_sdv into v_ssn;

	else

		for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,p_json::json)

			as (ssn int))

			loop

				if v_i = 0 then

					v_ssn := v_rec_json_ssn.ssn;

				else

					v_ssn := v_ssn||','||v_rec_json_ssn.ssn;

				end if;

				v_i = v_i + 1;

			end loop;

	end if;

	select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '2',

		p_descr := 'Поиск ssn',

		p_start_dttm := v_start_dttm,

		p_val := 'ssn='''||v_ssn||'''',

		p_log_tp := '1',

		p_debug_lvl := '3')

    into v_json_ret;

	if v_ssn is null then

	select sys_dwh.get_json4log(p_json_ret := v_json_ret,

			p_step := '2',

			p_descr := 'Поиск ssn',

			p_start_dttm := v_start_dttm,

			p_val := 'Warning: ssn не найден',

			p_log_tp := '2',

			p_debug_lvl := '1',

			p_cls := ']')

	    into v_json_ret;

	return(v_json_ret);

	end if;

    

    --цикл по gk полям

	for v_rec_cr in (select distinct schema_stg, table_name_stg, column_name_stg, ref_to_hub, ref_to_stg  

	from sys_dwh.prm_s2t_rdv 

		where src_stm_id = p_src_stm_id 

			and trim(ref_to_stg) <> '' 

			and ref_to_stg is not null

			and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date)

		loop

			

			--если не json

			if substring(trim(v_rec_cr.ref_to_stg) from 1 for 3) = 'stg' then

			  begin

				v_start_dttm := clock_timestamp() at time zone 'utc';

				  --находим src_stm_id для хаба

				  select src_stm_id into v_src_stm_id_ref 

				    		from sys_dwh.prm_src_stm 

				    			where nm = split_part(v_rec_cr.ref_to_stg, '.',2)

				    				and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm

				    										where nm = regexp_replace(split_part(v_rec_cr.ref_to_stg, '.',1),'^stg_', ''));

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,

					p_step := '3',

					p_descr := 'Поиск src_stm_id',

					p_start_dttm := v_start_dttm,

					p_val := 'src_stm_id='''||v_src_stm_id_ref||'''',

					p_log_tp := '1',

					p_debug_lvl := '3')

			    into v_json_ret;

				if v_src_stm_id_ref is null then

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '3',

						p_descr := 'Поиск src_stm_id',

						p_start_dttm := v_start_dttm,

						p_err := 'src_stm_id не найден v_rec_cr.ref_to_stg='''||coalesce(v_rec_cr.ref_to_stg,'')||'''',

						p_log_tp := '3',

						--p_cls := ']',

						p_debug_lvl := '1')

				    into v_json_ret;

					raise exception '%', v_json_ret;

				end if;

			  exception when others then

			    --if v_ssn is null then

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '3',

						p_descr := 'Поиск src_stm_id',

						p_start_dttm := v_start_dttm,

						p_err := SQLERRM,

						p_log_tp := '3',

						p_debug_lvl := '1',

						p_cls := ']')

				    into v_json_ret;

				--end if;

				raise exception '%', v_json_ret;

			  end;	  

				  --собираем sql

				  --!!!!!!!!! не реализован нормально поиск последнего ssn

				  --!!!!!!!!! после этого убрать distinct

				  --Проверяем наличие партиции в целевой таблице

			 	v_start_dttm := clock_timestamp() at time zone 'utc';

			 	begin

				  select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

				    into v_cnt

				    from pg_catalog.pg_partitions pp

				    	where lower(pp.schemaname) = 'rdv'

				    		and lower(pp.tablename) = regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')

				    		and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref;

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '4',

						p_descr := 'Поиск партиции в целевой таблице',

						p_start_dttm := v_start_dttm,

						p_val := 'cnt='''||v_cnt::text||'''',

						p_log_tp := '1',

						p_debug_lvl := '3')

				    into v_json_ret;

				exception when others then

				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '4',

						p_descr := 'Поиск партиции в целевой таблице',

						p_start_dttm := v_start_dttm,

						p_err := SQLERRM,

						p_log_tp := '3',

						p_debug_lvl := '1',

						p_cls := ']')

				    into v_json_ret;

					raise exception '%', v_json_ret;

				end;  

				    if v_cnt = 0 then

				  		--проверяем права на создание партиции

				  		--Проверяем owners у таблиц

						begin	

							v_start_dttm := clock_timestamp() at time zone 'utc';

						    WITH RECURSIVE cte AS (

								SELECT pg_roles.oid,

									pg_roles.rolname

								   FROM pg_roles

								  WHERE pg_roles.rolname = CURRENT_USER

								UNION

								 SELECT m.roleid, pgr.rolname

								    FROM cte cte_1

										JOIN pg_auth_members m ON m.member = cte_1.oid

										JOIN pg_roles pgr ON pgr.oid = m.roleid

								),

								ow as (select tableowner 

									from pg_catalog.pg_tables pt 

										where lower(schemaname) = 'rdv'

										and lower(tablename) =  regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', ''))

								select coalesce(count(1), 0) into v_cnt from (			

							SELECT cte.rolname

								FROM cte

									where cte.rolname in (select tableowner from ow)) as t;

						    if v_cnt < 1 then

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '5',

									p_descr := 'Проверка прав на создание партиции',

									p_start_dttm := v_start_dttm,

									p_err := 'Нет прав на создание партиции',

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;



								raise exception '%', v_json_ret;

						    end if;

						    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '5',

								p_descr := 'Проверка прав на создание партиции',

								p_start_dttm := v_start_dttm,

								p_val := 'cnt='''||v_cnt::text||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

						    into v_json_ret;

						exception when others then

						    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '5',

								p_descr := 'Проверка прав на создание партиции',

								p_start_dttm := v_start_dttm,

								p_err := SQLERRM,

								p_log_tp := '3',

								p_cls := ']',

								p_debug_lvl := '1')

						    into v_json_ret;

						    raise exception '%', v_json_ret;

					    end;

				  		begin

	   					    v_start_dttm := clock_timestamp() at time zone 'utc';

					     	--Создаем новую партицию

	   					    v_exec_part_sql := 'alter table '||v_rec_cr.ref_to_hub||' add partition start ('||

					     	v_src_stm_id_ref::text||') inclusive end ('||(v_src_stm_id_ref+1)::text||') exclusive with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=5)';

	   					    execute v_exec_part_sql;

							execute 'analyze ' || v_rec_cr.ref_to_hub;

	   					    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '6',

								p_descr := 'Создание новой партиции',

								p_start_dttm := v_start_dttm,

								p_val := 'v_exec_part_sql='''||v_exec_part_sql||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

						    into v_json_ret;

					    exception when others then	

					       	select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '6',

								p_descr := 'Создание новой партиции',

								p_start_dttm := v_start_dttm,

								p_err := SQLERRM,

								p_log_tp := '3',

								--p_cls := ']',

								p_debug_lvl := '1')

								into v_json_ret;

							begin

								v_start_dttm := clock_timestamp() at time zone 'utc';

								PERFORM pg_sleep(240);

								select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

								into v_cnt

								from pg_catalog.pg_partitions pp

									where lower(pp.schemaname) = 'rdv'

										and lower(pp.tablename) = regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')

										and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref;

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

												p_step := '7',

												p_descr := 'Повторный поиск партиции в целевой таблице',

												p_start_dttm := v_start_dttm,

												p_val := 'cnt='''||v_cnt::text||'''',

												p_log_tp := '1',

												p_debug_lvl := '2')

											into v_json_ret;

							exception when others then

											select sys_dwh.get_json4log(p_json_ret := v_json_ret,

												p_step := '7',

												p_descr := 'Повторный поиск партиции в целевой таблице',

												p_start_dttm := v_start_dttm,

												p_err := SQLERRM,

												p_log_tp := '3',

												p_debug_lvl := '1',

												p_cls := ']')

											into v_json_ret;

							end;

							if v_cnt = 0 then

								raise exception '%', v_json_ret;

							end if;

				        end;

				    end if; 

				    

				    --вносим в hub  

				    --Улейко Е.В. 06.09.2022 добавила coalesce

				   

				  

				    begin

	   				    v_start_dttm := clock_timestamp() at time zone 'utc';

				        --ищем партицию в хабе

					    select partitiontablename 

					        into v_partitiontablename

					        from pg_catalog.pg_partitions pp

					    	    where lower(pp.schemaname) = 'rdv'

					    		    and lower(pp.tablename) = regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')

					    		    and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref;

				  	    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '8',

								p_descr := 'Поиск партиции в хабе',

								p_start_dttm := v_start_dttm,

								p_val := 'v_partitiontablename='''||v_partitiontablename||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

						    into v_json_ret;

				    exception when others then	

					       	select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '8',

								p_descr := 'Поиск партиции в хабе',

								p_start_dttm := v_start_dttm,

								p_err := SQLERRM,

								p_log_tp := '3',

								p_cls := ']',

								p_debug_lvl := '1')

						    into v_json_ret;

								raise exception '%', v_json_ret;

					     	 

				    end;

					    	

				    --проверяем на блокировку и блокируем

				    v_exec_block_sql := '(select count(*) AS tablename FROM pg_locks l1 JOIN pg_stat_user_tables t ON l1.relation = t.relid

							where t.schemaname || ''.'' || t.relname = ''rdv.'||v_partitiontablename||''' and mode = ''AccessExclusiveLock'' 

								and mppsessionid not in (select sess_id from pg_stat_activity where pid!=pg_backend_pid() OR query!=''<IDLE>'' 

															OR waiting=''t'') and mppsessionid != 0)';	  

				    v_res = -1;

				    v_max_ret = 1200;

				    v_ret = v_max_ret;

				    WHILE v_ret > 0 AND v_res < 0

					    loop						

					        v_ret :=  v_ret - 1;

					        begin

								v_start_dttm := clock_timestamp() at time zone 'utc';

								execute v_exec_block_sql into v_cnt_block;

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '9',

									p_descr := 'Проверка блокировки хаба',

									p_start_dttm := v_start_dttm,

									p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

									p_ins_qty := v_cnt_block::text,

									p_log_tp := '1',

									p_debug_lvl := '3')

								into v_json_ret;

				  		    exception when others then	

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '9',

									p_descr := 'Проверка блокировки хаба',

									p_start_dttm := v_start_dttm,

									p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

									p_err := SQLERRM,

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;

									raise exception '%', v_json_ret;

						    end;

					        -- block function

					        if v_cnt_block = 0 then

						        v_exec_block_sql := 'lock table rdv.'||v_partitiontablename;

								v_start_dttm := clock_timestamp() at time zone 'utc';

						        begin	

							        execute v_exec_block_sql;

							        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '10',

										p_descr := 'Блокировка хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_ins_qty := v_cnt_block::text,

										p_log_tp := '1',

										p_debug_lvl := '3')

								    into v_json_ret;

						  		exception when others then	

							       	select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '10',

										p_descr := 'Блокировка хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_err := SQLERRM,

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

								    into v_json_ret;

										raise exception '%', v_json_ret;

								end;

								v_start_dttm := clock_timestamp() at time zone 'utc';  	

								  

									begin

										execute 'drop table if exists tmp'||regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref;

										v_exec_sql := 'create temporary table tmp'||regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||'

											(bk text, gk uuid) with (appendonly=true) on commit drop 

											distributed by (bk, gk)';

										execute v_exec_sql;	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '11',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

									exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '11',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin

										v_exec_sql := 'insert into tmp'||regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||' (bk, gk)														

															select distinct coalesce('||v_rec_cr.column_name_stg||'::text,''-1''), gk_'||v_rec_cr.column_name_stg||'

											from '||v_tbl_nm_sdv||' p where ssn in( '||v_ssn||')';

										execute v_exec_sql;

										execute 'analyze tmp'||regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '12',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

								    exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '12',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin

										v_exec_sql := 'insert into '||v_rec_cr.ref_to_hub||' (bk, gk, src_stm_id, upd_dttm)

											select p.bk, 

												p.gk,

												'||v_src_stm_id_ref||',

												now() at time zone ''utc''

												from tmp'||regexp_replace(v_rec_cr.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||' p 

													left join '||v_rec_cr.ref_to_hub||' h on h.gk = p.gk and h.bk = p.bk and h.src_stm_id = '||v_src_stm_id_ref||'

														where h.bk is null';

										execute v_exec_sql;

										get diagnostics v_cnt = row_count;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '13',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

									exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '13',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;    

  						  		    -- Собираем статистику

								    v_start_dttm := clock_timestamp() at time zone 'utc';  	

								    begin	

										v_exec_block_sql := 'analyze rdv.'||v_partitiontablename;

										execute v_exec_block_sql;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '14',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

						  		    exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '14',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

								    end;

					            --PERFORM pg_sleep(1);

								v_res := 0;

					            exit;

					        end if;

					        if v_cnt_block <> 0 then

					           PERFORM pg_sleep(1);

							   if v_ret = 0 then

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '9',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_err := 'Хаб был заблокирован дольше '||v_max_ret||' секунд' ,

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;

										raise exception '%', v_json_ret;

							   end if;

					           --v_ret := v_ret - 1;

					           --RAISE exception '%', 'Error: Despite having made '||v_max_ret||' retries. Lock on '||p_src_stm_id||' failed';--v_msg;

      				        end if;

				    end loop;



			  

			end if;

			--json

			if substring(trim(v_rec_cr.ref_to_stg) from 1 for 1) = '[' then

			  begin

				  for v_rec_json in (select tbl_nm, fld_nm, val, ref_to_hub from json_populate_recordset(null::record,

						v_rec_cr.ref_to_stg::json)

						as (tbl_nm text, fld_nm text, val text, ref_to_hub text)

				    )

				    loop

						begin

							v_start_dttm := clock_timestamp() at time zone 'utc';

							--находим src_stm_id

							select src_stm_id into v_src_stm_id_ref_json 

								from sys_dwh.prm_src_stm 

									where nm = split_part(v_rec_json.tbl_nm, '.',2)

										and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm

																where nm = regexp_replace(split_part(v_rec_json.tbl_nm, '.',1),'^stg_', ''));

							select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '15',

											p_descr := 'Поиск src_stm_id json',

											p_start_dttm := v_start_dttm,

											p_val := 'src_stm_id='''||v_src_stm_id_ref_json||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

							if v_src_stm_id_ref_json is null then

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '15',

											p_descr := 'Поиск src_stm_id json',

											p_start_dttm := v_start_dttm,

											p_err := 'src_stm_id не найден v_rec_json.tbl_nm='''||coalesce(v_rec_json.tbl_nm,'')||'''',

											p_log_tp := '3',

											--p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

										raise exception '%', v_json_ret;

							end if;

						exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '15',

											p_descr := 'Поиск src_stm_id json',

											p_start_dttm := v_start_dttm,

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

										raise exception '%', v_json_ret;

						end;

						v_start_dttm := clock_timestamp() at time zone 'utc';

					 	--Проверяем наличие партиции в целевой таблице

						begin

						  select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

								    into v_cnt

								    from pg_catalog.pg_partitions pp

								    	where lower(pp.schemaname) = 'rdv'

								    		and lower(pp.tablename) = regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')

								    		and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref_json;

							select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '16',

								p_descr := 'Поиск партиции в целевой таблице',

								p_start_dttm := v_start_dttm,

								p_val := 'cnt='''||v_cnt::text||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

							into v_json_ret;

						exception when others then

							select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '16',

								p_descr := 'Поиск партиции в целевой таблице',

								p_start_dttm := v_start_dttm,

								p_err := SQLERRM,

								p_log_tp := '3',

								p_debug_lvl := '1',

								p_cls := ']')

							into v_json_ret;

							raise exception '%', v_json_ret;

						end;

				    	

						   if v_cnt = 0 then

							 begin

								v_start_dttm := clock_timestamp() at time zone 'utc';

								WITH RECURSIVE cte AS (

									SELECT pg_roles.oid,

										pg_roles.rolname

									   FROM pg_roles

									  WHERE pg_roles.rolname = CURRENT_USER

									UNION

									 SELECT m.roleid, pgr.rolname

										FROM cte cte_1

											JOIN pg_auth_members m ON m.member = cte_1.oid

											JOIN pg_roles pgr ON pgr.oid = m.roleid

									),

									ow as (select tableowner 

										from pg_catalog.pg_tables pt 

											where lower(schemaname) = 'rdv'

											and lower(tablename) = regexp_replace(v_rec_json.ref_to_hub,'^rdv.', ''))

									select coalesce(count(1), 0) into v_cnt from (			

								SELECT cte.rolname

									FROM cte

										where cte.rolname in (select tableowner from ow)) as t;

								

								if v_cnt < 1 then

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '17',

										p_descr := 'Проверка прав на создание партиции',

										p_start_dttm := v_start_dttm,

										p_err := 'Нет прав на создание партиции',

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;



									raise exception '%', v_json_ret;

								end if;

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '17',

									p_descr := 'Проверка прав на создание партиции',

									p_start_dttm := v_start_dttm,

									p_val := 'cnt='''||v_cnt::text||'''',

									p_log_tp := '1',

									p_debug_lvl := '3')

								into v_json_ret;

							exception when others then

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '17',

									p_descr := 'Проверка прав на создание партиции',

									p_start_dttm := v_start_dttm,

									p_err := SQLERRM,

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;

								raise exception '%', v_json_ret;

							end;

						     	--Создаем новую партицию

								v_start_dttm := clock_timestamp() at time zone 'utc';

								v_exec_part_sql := 'alter table '||v_rec_json.ref_to_hub||' add partition start ('||

								v_src_stm_id_ref_json::text||') inclusive end ('||(v_src_stm_id_ref_json+1)::text||') exclusive with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=5)';

								begin  	

									execute v_exec_part_sql;

									execute 'analyze ' || v_rec_json.ref_to_hub;

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '18',

											p_descr := 'Создание новой партиции',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_part_sql='''||v_exec_part_sql||'''',

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

								exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '18',

											p_descr := 'Создание новой партиции',

											p_start_dttm := v_start_dttm,

											p_err := SQLERRM,

											p_log_tp := '3',

											--p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

										begin

											v_start_dttm := clock_timestamp() at time zone 'utc';

											PERFORM pg_sleep(240);

											select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

											into v_cnt

											from pg_catalog.pg_partitions pp

												where lower(pp.schemaname) = 'rdv'

													and lower(pp.tablename) = regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')

													and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref_json;

											select sys_dwh.get_json4log(p_json_ret := v_json_ret,

															p_step := '19',

															p_descr := 'Повторный поиск партиции в целевой таблице',

															p_start_dttm := v_start_dttm,

															p_val := 'cnt='''||v_cnt::text||'''',

															p_log_tp := '1',

															p_debug_lvl := '2')

														into v_json_ret;

										exception when others then

														select sys_dwh.get_json4log(p_json_ret := v_json_ret,

															p_step := '19',

															p_descr := 'Повторный поиск партиции в целевой таблице',

															p_start_dttm := v_start_dttm,

															p_err := SQLERRM,

															p_log_tp := '3',

															p_debug_lvl := '1',

															p_cls := ']')

														into v_json_ret;

										end;

										if v_cnt = 0 then

											raise exception '%', v_json_ret;

										end if;

								end;

						  end if;

				    	--ищем партицию в хабе

						begin

						  v_start_dttm := clock_timestamp() at time zone 'utc';

						  select partitiontablename 

						    into v_partitiontablename

						    from pg_catalog.pg_partitions pp

						    	where lower(pp.schemaname) = 'rdv'

						    		and lower(pp.tablename) = regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')

						    		and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref_json;

						  select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '20',

								p_descr := 'Поиск партиции в хабе',

								p_start_dttm := v_start_dttm,

								p_val := 'v_partitiontablename='''||v_partitiontablename||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

						    into v_json_ret;

					    exception when others then	

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '20',

									p_descr := 'Поиск партиции в хабе',

									p_start_dttm := v_start_dttm,

									p_err := SQLERRM,

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;

									raise exception '%', v_json_ret;

								 

					    end;		

						  

						  --вносим в hub

						  --проверяем на блокировку и блокируем

						  

						  v_exec_block_sql := '(select count(*) AS tablename FROM pg_locks l1 JOIN pg_stat_user_tables t ON l1.relation = t.relid

									where t.schemaname || ''.'' || t.relname = ''rdv.'||v_partitiontablename||''' and mode = ''AccessExclusiveLock''

										and mppsessionid not in (select sess_id from pg_stat_activity where pid!=pg_backend_pid() OR query!=''<IDLE>'' 

															OR waiting=''t'') and mppsessionid != 0)';

							  

						  --return v_exec_block_sql;	  

						  v_res = -1;

						  v_max_ret = 1200;

						  v_ret = v_max_ret;

						  WHILE v_ret > 0 AND v_res < 0

							loop  

							    v_ret :=  v_ret - 1;

							    v_start_dttm := clock_timestamp() at time zone 'utc';

							    begin

							        execute v_exec_block_sql into v_cnt_block;

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '21',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_ins_qty := v_cnt_block::text,

										p_log_tp := '1',

										p_debug_lvl := '3')

									into v_json_ret;

								exception when others then	

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '21',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_err := SQLERRM,

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;

										raise exception '%', v_json_ret;

								end;

							    -- block function

							    if v_cnt_block = 0 then

							      	  v_exec_block_sql := 'lock table rdv.'||v_partitiontablename;

									  v_start_dttm := clock_timestamp() at time zone 'utc';

									  begin	

										execute v_exec_block_sql;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '22',

											p_descr := 'Блокировка хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '22',

											p_descr := 'Блокировка хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;

									v_start_dttm := clock_timestamp() at time zone 'utc';  	

									begin

										execute 'drop table if exists tmp'||regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref_json;

										v_exec_sql := 'create temporary table tmp'||regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref_json||'

											(bk text, gk uuid) with (appendonly=true) on commit drop 

											distributed by (bk, gk)';

										execute v_exec_sql;	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '23',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

									exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '23',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin	

										v_exec_sql := 'insert into tmp'||regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref_json||' (bk, gk)														

															select distinct coalesce('||v_rec_cr.column_name_stg||'::text,''-1''), gk_'||v_rec_cr.column_name_stg||'

											from '||v_tbl_nm_sdv||' p 

												where ssn in( '||v_ssn||') and '||v_rec_json.fld_nm||' in( '||replace(replace(v_rec_json.val,'[',''),']','')||')';

										execute v_exec_sql;

										execute 'analyze tmp'||regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref_json;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '24',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

								    exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '24',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin	

										v_exec_sql := 'insert into '||v_rec_json.ref_to_hub||' (bk, gk, src_stm_id, upd_dttm)

											select p.bk, 

												p.gk,

												'||v_src_stm_id_ref_json||',

												now() at time zone ''utc''

												from tmp'||regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref_json||' p 

													left join '||v_rec_json.ref_to_hub||' h on h.gk = p.gk and h.bk = p.bk and h.src_stm_id = '||v_src_stm_id_ref_json||'

														where h.bk is null';

										execute v_exec_sql;

										get diagnostics v_cnt = row_count;

								        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '25',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '25',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;    

	  						  		  -- Собираем статистику

									  v_start_dttm := clock_timestamp() at time zone 'utc';  	

									  begin	

								        v_exec_block_sql := 'analyze rdv.'||v_partitiontablename;

										execute v_exec_block_sql;

								        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '26',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '26',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;

									  v_res := 0;

							          exit;

							      else

							           PERFORM pg_sleep(1);

									   if v_ret = 0 then

											select sys_dwh.get_json4log(p_json_ret := v_json_ret,

												p_step := '21',

												p_descr := 'Проверка блокировки хаба',

												p_start_dttm := v_start_dttm,

												p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

												p_err := 'Хаб был заблокирован дольше '||v_max_ret||' секунд' ,

												p_log_tp := '3',

												p_cls := ']',

												p_debug_lvl := '1')

											into v_json_ret;

												raise exception '%', v_json_ret;

									   end if;

							           --v_ret := -1;

		      				      end if;

						  end loop;

 				    end loop;

			  end;

			end if; 

		end loop;

	--проверяем на наличие gk_pk

	/*begin

	    v_start_dttm := clock_timestamp() at time zone 'utc'; 

		select count (*) 

		into v_cnt

		from information_schema.columns c

			join sys_dwh.prm_s2t_rdv p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 

			and c.table_name = p.table_name_stg

			where p.src_stm_id = p_src_stm_id

				and column_name = 'gk_pk'

				and substring(trim(table_name_rdv) from 1 for 2) <> 'l_';

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '27',

						p_descr := 'Проверка на наличие gk_pk',

						p_start_dttm := v_start_dttm,

						p_val := 'cnt='''||v_cnt::text||'''',

						p_log_tp := '1',

						p_debug_lvl := '3')

				    into v_json_ret;

	exception when others then

				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '27',

						p_descr := 'Проверка на наличие gk_pk',

						p_start_dttm := v_start_dttm,

						p_err := SQLERRM,

						p_log_tp := '3',

						p_debug_lvl := '1',

						p_cls := ']')

				    into v_json_ret;

					raise exception '%', v_json_ret;

	end;

	if v_cnt <> 0 then*/

		--Собираем строку для составного ключа

	for v_rec_gk_pk_table_name_rdv in

		(

		select distinct p.schema_rdv, p.table_name_rdv, r.where_json

				from sys_dwh.prm_s2t_rdv p

					join information_schema.columns c

						on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 

							and c.table_name = p.table_name_stg

							and column_name = 'gk_pk'

							and substring(trim(p.table_name_rdv) from 1 for 2) <> 'l_'

					join sys_dwh.prm_s2t_rdv_rule r

						on r.src_stm_id = p_src_stm_id

							and p.schema_rdv = r.schema_rdv

							and p.table_name_rdv = r.table_name_rdv

							and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date

					where p.src_stm_id = p_src_stm_id 

					and trim(key_type_src) like '%PK%'

					and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date

		) loop

			v_i_gk_pk := 0;

			v_gk_pk_str := '';

		begin

	 		for v_rec_gk_pk in (select distinct column_name_stg  

				from sys_dwh.prm_s2t_rdv

					where src_stm_id = p_src_stm_id

					and v_rec_gk_pk_table_name_rdv.schema_rdv = schema_rdv

					and v_rec_gk_pk_table_name_rdv.table_name_rdv = table_name_rdv

					and trim(key_type_src) like '%PK%'

					and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date

					order by column_name_stg)

				loop

					if v_i_gk_pk = 0 then 

						v_gk_pk_str := 'coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';

						v_gk_pk_str_when := v_rec_gk_pk.column_name_stg||' is null ';

					else

						v_gk_pk_str := v_gk_pk_str||'||''||''||coalesce('||v_rec_gk_pk.column_name_stg||'::text ,'''')';

						v_gk_pk_str_when := v_gk_pk_str_when||' and '||v_rec_gk_pk.column_name_stg||' is null ';

					end if;

					v_i_gk_pk := v_i_gk_pk + 1;

				end loop;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '27',

						p_descr := 'Сборка составного ключа',

						p_start_dttm := v_start_dttm,

						p_val := 'v_gk_pk_str='''||v_gk_pk_str||'''',

						p_log_tp := '1',

						p_debug_lvl := '3')

				    into v_json_ret;

		exception when others then

				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '27',

						p_descr := 'Сборка составного ключа',

						p_start_dttm := v_start_dttm,

						p_err := SQLERRM,

						p_log_tp := '3',

						p_debug_lvl := '1',

						p_cls := ']')

				    into v_json_ret;

					raise exception '%', v_json_ret;

		end;

		--Парсим правила в переменную

				if v_rec_gk_pk_table_name_rdv.where_json is not null and v_rec_gk_pk_table_name_rdv.where_json <> '' then

					begin

						v_start_dttm := clock_timestamp() at time zone 'utc';

						with sel as

						(select num, predicate, fld_nm , equal_type, val, func_nm, where_type, grp from json_populate_recordset(null::record, v_rec_gk_pk_table_name_rdv.where_json::json)

									as (num int4, predicate text, fld_nm text, equal_type text, val text, func_nm text, where_type int4, grp int4)),

						sel_2 as (select 

						(case when num = 1 then predicate||' (' else predicate||' ' end)||

						(case when where_type = 1 then ' '||fld_nm when where_type = 2 then func_nm end)||' '||

						' '||equal_type||' '||val||

						(case when lead(num) over(partition by grp order by num) is null then ')' else '' end) as f

						from sel)

						select ' and 1=1'||E'\n'||string_agg(f, E'\n')||E'\n' into v_rule

						from sel_2;

						

						select sys_dwh.get_json4log(p_json_ret := v_json_ret,

							p_step := '28',

							p_descr := 'Парсинг json-правила',

							p_start_dttm := v_start_dttm,

							p_val := 'v_rule='''||v_rule::text||'''',

							p_log_tp := '1',

							p_debug_lvl := '3')

							into v_json_ret;

					exception when others then

						select sys_dwh.get_json4log(p_json_ret := v_json_ret,

							p_step := '28',

							p_descr := 'Парсинг json-правила',

							p_start_dttm := v_start_dttm,

							p_err := SQLERRM,

							p_log_tp := '3',

							p_debug_lvl := '1',

							p_cls := ']')

						into v_json_ret;

						raise exception '%', v_json_ret;

					end;

				end if;

				v_rule := coalesce(v_rule, '');

			

			--Проверяем наличие партиции в целевой таблице

			begin

				v_start_dttm := clock_timestamp() at time zone 'utc';

				select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

				into v_cnt

				from pg_catalog.pg_partitions pp

					where lower(pp.schemaname) = 'rdv'

						 and lower(pp.tablename) = 'h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2)--regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')

						 and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = p_src_stm_id::bigint;

				select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '29',

								p_descr := 'Поиск партиции в целевой таблице',

								p_start_dttm := v_start_dttm,

								p_val := 'cnt='''||v_cnt::text||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

							into v_json_ret;

			exception when others then

							select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '29',

								p_descr := 'Поиск партиции в целевой таблице',

								p_start_dttm := v_start_dttm,

								p_err := SQLERRM,

								p_log_tp := '3',

								p_debug_lvl := '1',

								p_cls := ']')

							into v_json_ret;

							raise exception '%', v_json_ret;

			end;

			if v_cnt = 0 then

				--Проверяем права на создание партиции

				begin

								v_start_dttm := clock_timestamp() at time zone 'utc';

								

								WITH RECURSIVE cte AS (

									SELECT pg_roles.oid,

										pg_roles.rolname

									   FROM pg_roles

									  WHERE pg_roles.rolname = CURRENT_USER

									UNION

									 SELECT m.roleid, pgr.rolname

										FROM cte cte_1

											JOIN pg_auth_members m ON m.member = cte_1.oid

											JOIN pg_roles pgr ON pgr.oid = m.roleid

									),

									ow as (select tableowner 

										from pg_catalog.pg_tables pt 

											where lower(schemaname) = 'rdv'

											and lower(tablename) = 'h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2))

									select coalesce(count(1), 0) into v_cnt from (			

								SELECT cte.rolname

									FROM cte

										where cte.rolname in (select tableowner from ow)) as t;

							

								if v_cnt < 1 then

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '30',

										p_descr := 'Проверка прав на создание партиции',

										p_start_dttm := v_start_dttm,

										p_err := 'Нет прав на создание партиции',

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;



									raise exception '%', v_json_ret;

								end if;

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '30',

									p_descr := 'Проверка прав на создание партиции',

									p_start_dttm := v_start_dttm,

									p_val := 'cnt='''||v_cnt::text||'''',

									p_log_tp := '1',

									p_debug_lvl := '3')

								into v_json_ret;

				exception when others then

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '30',

									p_descr := 'Проверка прав на создание партиции',

									p_start_dttm := v_start_dttm,

									p_err := SQLERRM,

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;

								raise exception '%', v_json_ret;

				end;

		   		v_start_dttm := clock_timestamp() at time zone 'utc';

   					  

				--Создаем новую партицию

				v_exec_part_sql := 'alter table rdv.h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2)||' add partition start ('||

				p_src_stm_id::text||') inclusive end ('||(p_src_stm_id+1)::text||') exclusive with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=5)';

				begin

				       		execute v_exec_part_sql;

							execute 'analyze rdv.h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2);

				       		select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '31',

											p_descr := 'Создание новой партиции',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_part_sql='''||v_exec_part_sql||'''',

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

				exception when others then

						    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '31',

											p_descr := 'Создание новой партиции',

											p_start_dttm := v_start_dttm,

											p_err := SQLERRM,

											p_log_tp := '3',

											--p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

						begin

							v_start_dttm := clock_timestamp() at time zone 'utc';

							PERFORM pg_sleep(240);

							select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 

							into v_cnt

							from pg_catalog.pg_partitions pp

								where lower(pp.schemaname) = 'rdv'

									 and lower(pp.tablename) = 'h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2)--regexp_replace(v_rec_json.ref_to_hub,'^rdv.', '')

									 and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = p_src_stm_id::bigint;

							select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '32',

											p_descr := 'Повторный поиск партиции в целевой таблице',

											p_start_dttm := v_start_dttm,

											p_val := 'cnt='''||v_cnt::text||'''',

											p_log_tp := '1',

											p_debug_lvl := '2')

										into v_json_ret;

						exception when others then

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '32',

											p_descr := 'Повторный поиск партиции в целевой таблице',

											p_start_dttm := v_start_dttm,

											p_err := SQLERRM,

											p_log_tp := '3',

											p_debug_lvl := '1',

											p_cls := ']')

										into v_json_ret;

										raise exception '%', v_json_ret;

						end;

						if v_cnt = 0 then

							raise exception '%', v_json_ret;

						end if;

				end;

				

			end if;

		    --вносим в hub  

										

			--ищем партицию в хабе

			    begin

					select partitiontablename 

						into v_partitiontablename

						from pg_catalog.pg_partitions pp

							where lower(pp.schemaname) = 'rdv'

								and lower(pp.tablename) = regexp_replace('rdv.h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2),'^rdv.', '')

								and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = p_src_stm_id;

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

								p_step := '33',

								p_descr := 'Поиск партиции в хабе',

								p_start_dttm := v_start_dttm,

								p_val := 'v_partitiontablename='''||v_partitiontablename||'''',

								p_log_tp := '1',

								p_debug_lvl := '3')

						    into v_json_ret;

				exception when others then	

								select sys_dwh.get_json4log(p_json_ret := v_json_ret,

									p_step := '33',

									p_descr := 'Поиск партиции в хабе',

									p_start_dttm := v_start_dttm,

									p_err := SQLERRM,

									p_log_tp := '3',

									p_cls := ']',

									p_debug_lvl := '1')

								into v_json_ret;

									raise exception '%', v_json_ret;

								 

				end;

				  --проверяем на блокировку и блокируем

				  

				  v_exec_block_sql := '(select count(*) AS tablename FROM pg_locks l1 JOIN pg_stat_user_tables t ON l1.relation = t.relid

							where t.schemaname || ''.'' || t.relname = ''rdv.'||v_partitiontablename||''' and mode = ''AccessExclusiveLock'' 

								and mppsessionid not in (select sess_id from pg_stat_activity where pid!=pg_backend_pid() OR query!=''<IDLE>'' 

															OR waiting=''t'') and mppsessionid != 0)';

					  

				  --return v_exec_block_sql;	  

				  v_res = -1;

				  v_max_ret = 1200;

				  v_ret = v_max_ret;

				  WHILE v_ret > 0 AND v_res < 0

					  loop



					      v_ret :=  v_ret - 1;



					      v_start_dttm := clock_timestamp() at time zone 'utc';

						  begin

							        execute v_exec_block_sql into v_cnt_block;

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '34',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_ins_qty := v_cnt_block::text,

										p_log_tp := '1',

										p_debug_lvl := '3')

									into v_json_ret;

								  exception when others then	

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '34',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_err := SQLERRM,

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;

										raise exception '%', v_json_ret;

								  end;

							    -- block function

							      if v_cnt_block = 0 then

							      	  v_exec_block_sql := 'lock table rdv.'||v_partitiontablename;

									  v_start_dttm := clock_timestamp() at time zone 'utc';

									  begin	

										execute v_exec_block_sql;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '35',

											p_descr := 'Блокировка хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '35',

											p_descr := 'Блокировка хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;

									  v_start_dttm := clock_timestamp() at time zone 'utc';  	

									begin

										execute 'drop table if exists tmp'||v_partitiontablename||'_'||p_src_stm_id::text;

										v_exec_sql := 'create temporary table tmp'||v_partitiontablename||'_'||p_src_stm_id::text||'

											(bk text, gk uuid) with (appendonly=true) on commit drop 

											distributed by (bk, gk)';

										execute v_exec_sql;	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '36',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

									exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '36',

											p_descr := 'Создание временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin

										v_exec_sql := 'insert into tmp'||v_partitiontablename||'_'||p_src_stm_id::text||' (bk, gk)														

															select distinct case when '||v_gk_pk_str_when||' then ''-1''::text else '||v_gk_pk_str||'::text end, gk_pk

											from '||v_tbl_nm_sdv||' p 

												where ssn in( '||v_ssn||') '||v_rule;

										execute v_exec_sql;

										execute 'analyze tmp'||v_partitiontablename||'_'||p_src_stm_id::text;

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '37',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											--p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '1')

										into v_json_ret;

								    exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '37',

											p_descr := 'Наполнение временной таблицы',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									end;

									v_start_dttm := clock_timestamp() at time zone 'utc';

									begin

										v_exec_sql := 'insert into rdv.h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2)||' (bk, gk, src_stm_id, upd_dttm)

											select p.bk, 

												p.gk,

												'||p_src_stm_id::text||',

												now() at time zone ''utc''

												from tmp'||v_partitiontablename||'_'||p_src_stm_id::text||' p 

													left join rdv.h_'||substring(trim(v_rec_gk_pk_table_name_rdv.table_name_rdv) from 3 for 2)||' h on h.gk = p.gk and h.bk = p.bk and h.src_stm_id = '||p_src_stm_id::text||'

														where h.bk is null';

											

								        execute v_exec_sql;

								        get diagnostics v_cnt = row_count;

								        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '38',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_ins_qty := v_cnt::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '38',

											p_descr := 'Заполнение хаба',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_sql='''||v_exec_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;    

	  						  		  -- Собираем статистику

									  v_start_dttm := clock_timestamp() at time zone 'utc';  	

									  begin	

								        v_exec_block_sql := 'analyze rdv.'||v_partitiontablename;

										execute v_exec_block_sql;

								        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '39',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_ins_qty := v_cnt_block::text,

											p_log_tp := '1',

											p_debug_lvl := '3')

										into v_json_ret;

									  exception when others then	

										select sys_dwh.get_json4log(p_json_ret := v_json_ret,

											p_step := '39',

											p_descr := 'Сбор статистики',

											p_start_dttm := v_start_dttm,

											p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

											p_err := SQLERRM,

											p_log_tp := '3',

											p_cls := ']',

											p_debug_lvl := '1')

										into v_json_ret;

											raise exception '%', v_json_ret;

									  end;

								v_res := 0;

					           exit;

					      end if;

					      if v_cnt_block <> 0 then

					           PERFORM pg_sleep(1);

							   if v_ret = 0 then

									select sys_dwh.get_json4log(p_json_ret := v_json_ret,

										p_step := '34',

										p_descr := 'Проверка блокировки хаба',

										p_start_dttm := v_start_dttm,

										p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',

										p_err := 'Хаб был заблокирован дольше '||v_max_ret||' секунд' ,

										p_log_tp := '3',

										p_cls := ']',

										p_debug_lvl := '1')

									into v_json_ret;

										raise exception '%', v_json_ret;

							   end if;

					           --v_ret := -1;

					      end if;

				  end loop;

				  

	--end if;	

	end loop;

    v_json_ret := v_json_ret||']';

    return(v_json_ret);   

    --Регистрируем ошибки

    exception

      when others then

		GET STACKED DIAGNOSTICS

			err_code = RETURNED_SQLSTATE, -- код ошибки

			msg_text = MESSAGE_TEXT, -- текст ошибки

			exc_context = PG_CONTEXT, -- контекст исключения

			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки

			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению

		if v_json_ret is null then

			v_json_ret := '';

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

					p_step := '0',

					p_descr := 'Фатальная ошибка',

					p_start_dttm := v_start_dttm,

					

					p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',

					p_log_tp := '3',

					p_cls := ']',

					p_debug_lvl := '1')

				into v_json_ret;

		end if;

		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

			v_json_ret := v_json_ret||']';

		end if;

		raise exception '%', v_json_ret;   

    end;

 






$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_ref_hub(int4, text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_ref_hub(p_src_stm_id int4 DEFAULT NULL::integer, p_hub text DEFAULT NULL::text, p_json text DEFAULT NULL::text, p_json_param text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	declare
		v_rec_cr record;
		v_rec_json_ssn record;
		
		v_ssn text;
		v_ref_to_hub text; 
		v_src_stm_id bigint;
		v_reg_hub_sql_id bigint;
		v_json_param text;
		
		v_partitiontablename text;
		v_cnt bigint;
		v_i int = 0;

		v_json_ret text = '';
		
		v_exec_sql text;
		v_rec_cr_sql text;

		v_start_dttm text;

		err_code text; -- код ошибки
		msg_text text; -- текст ошибки
		exc_context text; -- контекст исключения
		msg_detail text; -- подробный текст ошибки
		exc_hint text; -- текст подсказки к исключению
	begin
		--Парсим json
		v_start_dttm := clock_timestamp() at time zone 'utc';
		--Фиксируем номер сессии загрузки в таблице-источнике
		for v_rec_json_ssn in (select distinct ssn::int8 ssn from
			(select t.value ->> 'ssn' as ssn from json_array_elements(p_json::json) t(value)) r order by ssn::int8)
				loop
					if v_i = 0 then
						v_ssn := v_rec_json_ssn.ssn::text;
					else
						v_ssn := v_ssn||','||v_rec_json_ssn.ssn::text;
					end if;
					v_i = v_i + 1;
		end loop;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск ssn',
			p_start_dttm := v_start_dttm,
			p_val := 'ssn='''||v_ssn||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		if v_ssn is null then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Поиск ssn',
				p_start_dttm := v_start_dttm,
				p_val := 'Warning: ssn не найден',
				p_log_tp := '2',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
		return(v_json_ret);
		end if;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			v_json_param := p_json_param;
			if v_json_param not like '[%' then
				v_json_param := '['||v_json_param||']';
			end if;
			
			select distinct reg_hub_sql_id::bigint reg_hub_sql_id from
			(select t.value ->> 'reg_hub_sql_id' as reg_hub_sql_id from json_array_elements(v_json_param::json) t(value)) r
			 into
				v_reg_hub_sql_id;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'v_reg_hub_sql_id='''||coalesce(v_reg_hub_sql_id::text,'')||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
	
		v_rec_cr_sql := 'select set_sql from sys_dwh.reg_hub_sql where 1 = 1 ';
		if p_src_stm_id is not null then
			v_rec_cr_sql := v_rec_cr_sql||' and src_stm_id = '||p_src_stm_id::text;
		end if;
		if p_hub is not null then
			v_rec_cr_sql := v_rec_cr_sql||' and hub_nm = '''||p_hub||'''';
		end if;
		if v_reg_hub_sql_id is not null then
			v_rec_cr_sql := v_rec_cr_sql||' and reg_hub_sql_id = '||v_reg_hub_sql_id::text;
		end if;
		
		for v_rec_cr in execute v_rec_cr_sql 
			loop						
			 	v_start_dttm := clock_timestamp() at time zone 'utc';  	
				begin
					execute replace(replace(v_rec_cr.set_sql,'{{ var_ssn }}',v_ssn),'\','');
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '3',
						p_descr := 'Применение записи в reg_hub_sql',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_rec_cr.set_sql||'''',
						--p_ins_qty := v_cnt_block::text,
						p_log_tp := '1',
						p_debug_lvl := '3')
					into v_json_ret;
				exception when others then	
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '3',
						p_descr := 'Применение записи в reg_hub_sql',
						p_start_dttm := v_start_dttm,
						--p_val := 'v_exec_sql='''||v_exec_sql||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_cls := ']',
						p_debug_lvl := '1')
					into v_json_ret;
						raise exception '%', v_json_ret;
				end;	
		end loop;
		v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_ref_satellete(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_ref_satellete(p_src_stm_id int4, p_ssn text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare 
  v_rec_tb record; --переменная для работы с циклом по таблицам
  v_rec_cr record; --переменная для работы с циклом по полям
  v_tbl_nm_sdv text; --переменная для фиксации имени sdv-таблицы
  v_tbl_nm_rdv text; --переменная для фиксации имени rdv-таблицы со схемой
  v_table_name_rdv text; --переменная для фиксации имени rdv-таблицы
  v_schema_rdv text; --переменная для фиксации схемы rdv-таблицы
  v_backup_table_name text; --переменная для фиксации backup таблицы для добавления полей
  v_load_mode int; --переменная для фиксации типа загрузки
  v_reg_load_mode text; --переменная для фиксации типа загрузки из dwh_reg по ssn
  v_sql_fld_rdv_gk text; --переменная для фиксации ключей
  v_sql_field_gu text; --переменная для фиксации полей для определения delete lm5
  v_sql_field_stg_s text; --переменная для фиксации полей stg-таблицы с алиасом
  v_sql_field_hash_s text; --переменная для фиксации хэша с алиасом
  v_datatype_stg text; --переменная типа данных stg
  v_ssn_upd_dttm timestamp; --переменная времени загрузки по ssn со смещением
  v_src_jetlag text; --переменная смещения времени на источнике
  v_valid_dt date; --переменная дат eff_dt и end_dt
  v_cnt bigint = 0; --переменная для фиксации количества записей и инкремента
  v_ins bigint = 0; --переменная для фиксации количества записей вставки
  v_upd bigint = 0; --переменная для фиксации количества записей обновления
  v_del bigint = 0; --переменная для фиксации количества записей удаления
  v_ssn text; --переменная для фиксации ssn-таблицы
  v_exec_sql text; --переменная для фиксации sql-скрипта
  v_sql_field_stg text; --переменная для фиксации полей stg-таблицы
  v_sql_field_hash text; --переменная для фиксации хэша
  v_sql_field_rdv text; --переменная для фиксации полей rdv-таблицы
  v_sql_field_chg text; --переменная для фиксации полей rdv-таблицы и sdv таблицы для добавления полей
  
  v_cnt_gu_gk bigint; --переменная для фиксации наличия ключа на gu поле
  v_cnt_add bigint; --переменная для фиксации количества записей для добавления полей
  v_cnt_rts bigint; --переменная для фиксации количества записей для ref_to_stg
  v_cnt_alg bigint;
  v_cnt_is_new_hash bigint;
  v_cnt_hsh bigint; --переменная для фиксации количества записей для ref_to_stg
  v_sql_field_hash_chg text;
  v_sql_field_rdv_type text; --переменная для фиксации типов полей rdv-таблицы (для создания временной)
  v_sql_distributed text; --переменная для фиксации ключа дистрибьюции (для создания временной)
  v_sql_distributed_t text; --переменная для фиксации ключа дистрибьюции из словаря данных (для создания временной)
  v_sql_gk_pk text; --переменная для фиксации ключа (для связок)
  v_sql_pk_where text; --переменная для фиксации условий соединения (для связок)
  v_sql_rdv_pk_where text; --переменная для фиксации условий соединения (для связок)
  v_sql_rdv_pk_s_where text; --переменная для фиксации условий соединения (для связок)
  v_sql_fltr_s text; --переменная stg для условия фильтрации для load_mode 3
  v_sql_fltr_t text; --переменная rdv для условия фильтрации для load_mode 3
  v_output_text text; --переменная для фиксации возвращаемого сообщения
  v_i int = 0; --переменная для работы цикла
  v_i_gk int = 0; --переменная для работы цикла
  v_i_hash int = 0; --переменная для работы цикла
  v_i_gk_pk int = 0; --переменная для работы цикла
  v_is_cnt_pk int = 0; --переменная для определения составляют ли все поля pk
  v_answr text; --переменная для записи ответа
  v_json_rule text; --переменная для записи json-правил заполнения сателлита
  v_is_distinct int; --переменная для указания distinct в select
  v_column_fltr_stg text = ''; --второй лист s2t stg, поле для load_mode 3
  v_column_fltr_rdv text = ''; --второй лист s2t stg, поле для load_mode 3
  v_column_fltr_from text = ''; --второй лист s2t stg, поле фильтрации начала диапазона load_mode 3
  v_column_fltr_to text = ''; --второй лист s2t stg, поле фильтрации окончания диапазона для load_mode 3
  v_rule text; --переменная для записи правил заполнения сателлита
  v_algorithm text; --переменная для записи json-правил заполнения поля
  --v_json_algorithm text; --переменная для записи правил заполнения поля
  v_json_ret text = ''; --переменная для return

  v_start_dttm text; --время старта шага
  
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
	--Проверяем наличие sys_dwh.prm_src_stm в БД
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		select count(1) into v_cnt from pg_catalog.pg_tables pt 
			where lower(pt.schemaname) = 'sys_dwh'
			and lower(pt.tablename) = 'prm_src_stm';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Проверка наличия sys_dwh.prm_src_stm в БД',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Таблица sys_dwh.prm_src_stm отсутствует в БД',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt::text||'''', 
				p_err := 'Таблица sys_dwh.prm_src_stm отсутствует в БД',
				p_log_tp := '3',
				p_debug_lvl := '3',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Проверка наличия sys_dwh.prm_src_stm в БД',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	--Проверяем наличие sys_dwh.prm_s2t_stg_src в БД
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		select count(1) into v_cnt from pg_catalog.pg_tables pt 
			where lower(pt.schemaname) = 'sys_dwh'
				and lower(pt.tablename) = 'prm_s2t_stg_src';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_stg_src в БД',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Таблица sys_dwh.prm_s2t_stg_src отсутствует в БД',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt::text||'''', 
				p_err := 'Таблица sys_dwh.prm_s2t_stg_src отсутствует в БД',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_stg_src в БД',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	--Проверяем наличие sys_dwh.prm_s2t_rdv в БД
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		select count(1) into v_cnt from pg_catalog.pg_tables pt 
			where lower(pt.schemaname) = 'sys_dwh'
				and lower(pt.tablename) = 'prm_s2t_rdv';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_rdv в БД',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '3',
				p_descr := 'Таблица sys_dwh.prm_s2t_rdv отсутствует в БД',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt::text||'''', 
				p_err := 'Таблица sys_dwh.prm_s2t_rdv отсутствует в БД',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_rdv в БД',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	--Проверяем наличие sys_dwh.prm_s2t_rdv_rule в БД
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		select count(1) into v_cnt from pg_catalog.pg_tables pt 
			where lower(pt.schemaname) = 'sys_dwh'
				and lower(pt.tablename) = 'prm_s2t_rdv_rule';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_rdv_rule в БД',
			p_start_dttm := v_start_dttm,
			p_val := 'v_cnt='''||v_cnt::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '4',
				p_descr := 'Таблица sys_dwh.prm_s2t_rdv_rule отсутствует в БД',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt::text||'''', 
				p_err := 'Таблица sys_dwh.prm_s2t_rdv_rule отсутствует в БД',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Проверка наличия sys_dwh.prm_s2t_rdv_rule в БД',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	--Проверяем наличие таблицы в sys_dwh.prm_src_stm и параметров загрузки
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		select sys_dwh.prv_tbl_id(p_src_stm_id) into v_answr;
		
		if v_answr is null then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '5',
				p_descr := 'Таблица отсуствует в sys_dwh.prm_src_stm',
				p_start_dttm := v_start_dttm,
				p_val := 'v_answr='''||coalesce(v_answr::text,'')||'''',
				p_err := 'Таблица отсуствует в sys_dwh.prm_src_stm',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Проверка наличия таблицы в sys_dwh.prm_src_stm',
			p_start_dttm := v_start_dttm,
			p_val := 'v_answr='''||coalesce(v_answr::text,'')||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
			into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'Проверка наличия таблицы в sys_dwh.prm_src_stm',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	--Открываем цикл по таблицам
	for v_rec_tb in (select distinct table_name_rdv
	from sys_dwh.prm_s2t_rdv
		where src_stm_id = p_src_stm_id
		and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date) 
		loop	
		    --Фиксируем таблицы источника и приемника
		    v_start_dttm := clock_timestamp() at time zone 'utc';
		    begin
				--таблицы
			    select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg,
					schema_rdv||'.'||table_name_rdv,
					table_name_rdv,
					schema_rdv
					into v_tbl_nm_sdv, v_tbl_nm_rdv, v_table_name_rdv, v_schema_rdv
					from sys_dwh.prm_s2t_rdv
						where src_stm_id = p_src_stm_id 
						and table_name_rdv = v_rec_tb.table_name_rdv
						and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
				--флаг наличия полей помимо pk
				select distinct count(1) - count(1) filter (where key_type_src similar to '(%PK%)')
					into v_is_cnt_pk
					from sys_dwh.prm_s2t_rdv
						where src_stm_id = p_src_stm_id 
						and table_name_rdv = v_rec_tb.table_name_rdv
						and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '6',
					p_descr := 'Поиск таблицы источника и приемника',
					p_start_dttm := v_start_dttm,
					p_val := 'v_is_cnt_pk='''||v_is_cnt_pk::text||''', v_tbl_nm_sdv='''||v_tbl_nm_sdv||''', v_tbl_nm_rdv='''||v_tbl_nm_rdv||''', v_table_name_rdv='''||v_table_name_rdv||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
		    exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '6',
					p_descr := 'Поиск таблицы источника и приемника',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
		    end;
			--Фиксируем номер сессии загрузки в таблице-источнике
			v_start_dttm := clock_timestamp() at time zone 'utc';
			if p_ssn is null then
				execute 'select max(ssn) from '||v_tbl_nm_sdv into v_ssn;
			else 
				execute 'select distinct ssn from '||v_tbl_nm_sdv||' where ssn = '||p_ssn::text into v_ssn;
			--v_ssn := p_ssn;
			end if;
			if v_ssn is null then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '7',
					p_descr := 'Поиск ssn и параметров загрузки',
					p_start_dttm := v_start_dttm,
					p_val := 'Warning: ssn не найден',
					p_log_tp := '2',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
			return(v_json_ret);
			end if;
			select src_jetlag::text into v_src_jetlag
				from sys_dwh.prm_src_stm 
					where src_stm_id = p_src_stm_id;
			v_exec_sql = 'select coalesce(ssn_upd_dttm::timestamp, upd_dttm) 
				+ interval '''||v_src_jetlag||''' hour, load_mode::text 
					from (
						select rd.json_value,
						rd.upd_dttm,
						t.value ->> ''src_stm_id'' as src_stm_id,
						
						t.value ->> ''upd_dttm'' as ssn_upd_dttm,
						t.value ->> ''ssn'' as ssn,
						t.value ->> ''load_mode'' as load_mode
							from sys_dwh.reg_dwh rd
							LEFT JOIN LATERAL json_array_elements(rd.json_value) t(value) ON true
						where json_tp = ''src_ssn'' and t.value ->> ''ssn'' = '''||v_ssn::text||''' and t.value ->> ''src_stm_id'' = '''||p_src_stm_id::text||''') dt';
			execute v_exec_sql into v_ssn_upd_dttm, v_reg_load_mode;
			v_valid_dt := v_ssn_upd_dttm::date;
		    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '7',
				p_descr := 'Поиск ssn и параметров загрузки',
				p_start_dttm := v_start_dttm,
				p_val := 'ssn='''||v_ssn||''', v_src_jetlag='''||coalesce(v_src_jetlag::text,'')||''', v_ssn_upd_dttm='''||coalesce(v_ssn_upd_dttm::text,'')||''', v_valid_dt='''||coalesce(v_valid_dt::text,'')||''', v_reg_load_mode='''||coalesce(v_reg_load_mode::text,'')||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				--блок предобработки
				v_backup_table_name := '';
				select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv')) t into v_cnt_add;
				select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg')) t into v_cnt_rts;
				select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm')) t into v_cnt_alg;
				select count(*) from (select backup_table_name, column_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff')) t into v_cnt_hsh;
				if v_cnt_add > 0 then
					select distinct backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv') into v_backup_table_name;
				end if;
				if v_cnt_rts > 0 then
					select distinct backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg') into v_backup_table_name;
				end if;
				if v_cnt_alg > 0 then
					select distinct backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm') into v_backup_table_name;
				end if;
				if v_cnt_hsh > 0 then
					select distinct backup_table_name from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff') into v_backup_table_name;
				end if;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '8',
						p_descr := 'Поиск признака предобработки',
						p_start_dttm := v_start_dttm,
						p_val := 'v_cnt_add='''||v_cnt_add::text||''', v_cnt_rts='''||v_cnt_rts::text||''', v_cnt_alg='''||v_cnt_alg::text||''', v_cnt_hsh='''||v_cnt_hsh::text||''', v_backup_table_name='''||v_backup_table_name::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
					
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '8',
					p_descr := 'Поиск признака предобработки',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
		    --Фиксируем тип загрузки
			v_start_dttm := clock_timestamp() at time zone 'utc';
			begin
				select load_mode into v_load_mode 
					 from sys_dwh.prm_src_stm 
							where src_stm_id = p_src_stm_id;
				execute 'select count(*) from '||v_tbl_nm_rdv into v_cnt;
				
				v_load_mode := coalesce(v_reg_load_mode::int, v_load_mode);
				if v_cnt = 0 and v_cnt_add = 0 and v_cnt_rts = 0 and v_cnt_alg = 0 and v_cnt_hsh = 0 then v_load_mode := 4;
				end if;
				
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '9',
					p_descr := 'Поиск типа загрузки',
					p_start_dttm := v_start_dttm,
					p_val := 'v_load_mode='''||v_load_mode::text||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
				if v_load_mode = 0 then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '9',
						p_descr := 'Поиск типа загрузки',
						p_start_dttm := v_start_dttm, 
						p_val := 'v_load_mode='''||v_load_mode::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					return(v_json_ret);
				end if;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '9',
					p_descr := 'Поиск типа загрузки',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			if v_load_mode = 0 then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '9',
							p_descr := 'Поиск типа загрузки',
							p_start_dttm := v_start_dttm, 
							p_val := 'v_load_mode='''||v_load_mode::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						return(v_json_ret);
			end if;
			--Фиксируем диапазоны загрузки для load_mode = 3
			if v_load_mode = 3 then
				v_start_dttm := clock_timestamp() at time zone 'utc';
				begin
					select r.column_name_rdv, r.column_name_stg, r.datatype_stg, p.column_fltr_from,
							case when (p.column_fltr_to is null or p.column_fltr_to = '') and lower(s.datatype_stg) in ('int2', 'smallint')
									then '32767'
								when (p.column_fltr_to is null or p.column_fltr_to = '') and lower(s.datatype_stg) in ('int4', 'int')
									then '2147483647'
								when (p.column_fltr_to is null or p.column_fltr_to = '') and lower(s.datatype_stg) in ('int8', 'bigint')
									then '9223372036854775807'
								when (p.column_fltr_to is null or p.column_fltr_to = '') and lower(s.datatype_stg) like 'timestamp%' or lower(s.datatype_stg) = 'date'
									then '9999-12-31'
							end
						into v_column_fltr_rdv, v_column_fltr_stg, v_datatype_stg, v_column_fltr_from, v_column_fltr_to 
							from sys_dwh.prm_s2t_stg_src p
								join sys_dwh.prm_s2t_stg s on p.src_stm_id = s.src_stm_id and p.column_fltr = s.column_name_src and daterange(s.eff_dt,s.end_dt,'[]')@>(now() at time zone 'utc')::date 
								join sys_dwh.prm_s2t_rdv r on p.src_stm_id = r.src_stm_id and r.column_name_stg = s.column_name_stg and r.column_name_rdv not like 'gk_%' 
									and daterange(r.eff_dt,r.end_dt,'[]')@>(now() at time zone 'utc')::date
								where p.src_stm_id = p_src_stm_id
									and daterange(p.eff_dt,p.end_dt,'[]')@>(now() at time zone 'utc')::date;
						if v_datatype_stg in ('int2', 'smallint', 'int4', 'int', 'int8', 'bigint') then
							v_sql_fltr_t := coalesce(' and coalesce(t.'||v_column_fltr_rdv||','||v_column_fltr_from||') between '||v_column_fltr_from||' and '||v_column_fltr_to, '');
							v_sql_fltr_s := coalesce(' and coalesce(s.'||v_column_fltr_stg||','||v_column_fltr_from||') between '||v_column_fltr_from||' and '||v_column_fltr_to, '');
						else
							v_sql_fltr_t := coalesce(' and coalesce(t.'||v_column_fltr_rdv||','''||v_column_fltr_from||''') between '''||v_column_fltr_from||''' and '''||v_column_fltr_to||'''', '');
							v_sql_fltr_s := coalesce(' and coalesce(s.'||v_column_fltr_stg||','''||v_column_fltr_from||''') between '''||v_column_fltr_from||''' and '''||v_column_fltr_to||'''', '');
						end if;
					if v_column_fltr_to is null or v_column_fltr_to = '' then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '10',
							p_descr := 'Поиск диапазонов загрузки для load_mode = 3',
							p_start_dttm := v_start_dttm,
							p_err := 'Тип column_fltr не поддерживается',
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end if;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '10',
						p_descr := 'Поиск диапазонов загрузки для load_mode = 3',
						p_start_dttm := v_start_dttm,
						p_val := 'v_load_mode='''||v_load_mode::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
					
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '10',
						p_descr := 'Поиск диапазонов загрузки для load_mode = 3',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		    
		    --Обнуляем все переменные hash
			v_start_dttm := clock_timestamp() at time zone 'utc';
		    v_i = 0;
		    v_i_hash := 0;
		    v_i_gk_pk := 0;
		    v_i_gk := 0;
		    v_cnt_gu_gk := 0;
		    v_sql_distributed := null;
		    v_sql_pk_where := null;
		    v_sql_rdv_pk_where := null;
		    v_sql_rdv_pk_s_where := null;
		    v_sql_fld_rdv_gk := null;
		    v_sql_field_stg_s := null;
		    v_sql_field_hash_s := null; 
		    v_sql_field_stg := null; 
		    v_sql_field_hash := null; 
		    v_sql_field_rdv := null; 
			v_sql_field_chg := null;
			
		    v_sql_field_rdv_type := null;
			v_json_rule := null;
			v_rule := '';
			v_is_distinct := null;
			--v_backup_table_name := null;
			v_sql_field_hash_chg := null;
			v_sql_field_gu := '';
			
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '11',
				p_descr := 'Обнуление переменных',
				p_start_dttm := v_start_dttm,
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
			
		    --Открываем цикл по полям
			v_start_dttm := clock_timestamp() at time zone 'utc';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '12',
				p_descr := 'Цикл по полям',
				p_start_dttm := v_start_dttm,
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		    for v_rec_cr in (select schema_stg, table_name_stg, column_name_stg, column_name_rdv, datatype_stg, datatype_rdv, key_type_src, datatype_stg_transform, algorithm,
				coalesce(rts.column_name_rts, coalesce(ad.column_name_add, coalesce(alg.column_name_alg, hsh.column_name_hsh))) column_name_chg,
				case when rts.column_name_rts is not null then true end is_rts,
				case when ad.column_name_add is not null then true end is_add,
				case when alg.column_name_alg is not null then true end is_alg,
				case when hsh.column_name_hsh is not null then true end is_hsh
				
		    from sys_dwh.prm_s2t_rdv r
				left join (select trim(string_agg(column_name, ',')) column_name_add 
						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv') 
							) ad on ad.column_name_add = r.column_name_rdv
				left join (select trim(string_agg(column_name, ',')) column_name_rts 
						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg') 
							) rts on rts.column_name_rts = r.column_name_rdv
				left join (select trim(string_agg(column_name, ',')) column_name_alg 
						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm') 
							) alg on alg.column_name_alg = r.column_name_rdv
				left join (select trim(string_agg(column_name, ',')) column_name_hsh 
						from sys_dwh.get_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff') 
							) hsh on hsh.column_name_hsh = r.column_name_rdv
					where src_stm_id = p_src_stm_id
						and column_name_rdv is not null
						and column_name_rdv <> ''
						and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date
						and table_name_rdv = v_table_name_rdv
						order by column_name_stg, column_name_rdv)
				loop
					begin 
						--Выбираем поля, не являющиеся ключами
						v_start_dttm := clock_timestamp() at time zone 'utc';
						
						if v_rec_cr.key_type_src similar to '(%GU%)' then
							if v_sql_field_gu <> '' then
								v_sql_field_gu := ' and ';
							end if;
							select count (*) 
								into v_cnt_gu_gk
								from information_schema.columns c
										join sys_dwh.prm_s2t_rdv p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
										and c.table_name = p.table_name_stg
										where p.src_stm_id = p_src_stm_id
											   and column_name = 'gk_'||v_rec_cr.column_name_stg;
							if v_cnt_gu_gk <> 0 then
								v_sql_field_gu := v_sql_field_gu||' s.gk_'||v_rec_cr.column_name_stg||'=t.'||v_rec_cr.column_name_rdv;
							else
								v_sql_field_gu := v_sql_field_gu||' s.'||v_rec_cr.column_name_stg||'=t.'||v_rec_cr.column_name_rdv;
							end if;
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '12',
								p_descr := 'Поиск полей, не являющиеся ключами',
								p_start_dttm := v_start_dttm,
								p_val := 'v_sql_field_gu='''||v_sql_field_gu::text||'''',
								p_log_tp := '1',
								p_debug_lvl := '3')
							into v_json_ret;
						end if;
					end;
					if (v_rec_cr.key_type_src not similar to '(%PK%)' or v_rec_cr.key_type_src is null or v_rec_cr.key_type_src = '' or v_is_cnt_pk = 0) then 
						--Если инкремент цикла первый, фиксируем внутрь переменной хэш полей
						v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '13',
								p_descr := 'Поиск хэша полей',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '3')
						into v_json_ret;
						if v_i_hash = 0 then
							    if (trim(v_rec_cr.datatype_stg) like('int%') 
									 or trim(v_rec_cr.datatype_stg) like ('smallint%') 
									 or trim(v_rec_cr.datatype_stg) like('numeric%')
									 or trim(v_rec_cr.datatype_stg) like ('bigint%')
									 or trim(v_rec_cr.datatype_stg) like ('double precision%')
									 or trim(v_rec_cr.datatype_stg) like('decimal%')
									 or trim(v_rec_cr.datatype_stg) like('real%') 
									 or trim(v_rec_cr.datatype_stg) like('float%') 
									 or trim(v_rec_cr.datatype_stg) like('money%')) then
									 v_sql_field_hash := 'md5(coalesce('||v_rec_cr.column_name_stg||',-1)::text';
									 v_sql_field_hash_s := 'md5(coalesce(s.'||v_rec_cr.column_name_stg||',-1)::text';
							    end if;
							    if  (v_rec_cr.datatype_stg like('char%') 
									 or v_rec_cr.datatype_stg like('varchar%') 
									 or v_rec_cr.datatype_stg like('text%')
									 or v_rec_cr.datatype_stg like('uuid%')
									 or v_rec_cr.datatype_stg like('bool%')
									 or v_rec_cr.datatype_stg like('json%')) then
									 if v_rec_cr.column_name_rdv like 'gk_%' then
											v_sql_field_hash := 'md5(gk_'||v_rec_cr.column_name_stg||'::text';
											v_sql_field_hash_s := 'md5(s.gk_'||v_rec_cr.column_name_stg||'::text';
									 else
										if v_rec_cr.algorithm is not null and v_rec_cr.algorithm <> '' then
											begin
												with sel as
													(select num, when_val, val, query_type, grp from json_populate_recordset(null::record, v_rec_cr.algorithm::json)
														as (num int4, when_val text, val text, query_type int4, grp int4)),
													sel_2 as (select 
														(case when num = 1 and query_type = 2
																then 'case '
															  when num = 1 and query_type = 1
																then ' '
														else '' end)||
														(case when query_type = 1 then coalesce(val,'') when query_type = 2 then coalesce(when_val,'') end)||' '||
														(case when query_type = 2 then coalesce(val,'') else '' end)||' '||
														(case when ((query_type = 2) and (lead(num) over(partition by grp order by num) is null)) 
															then 'end'
														else ''
														end) as f
													from sel)
													select ' '||E'\n'||string_agg(f, E'\n')||E'\n' into v_algorithm
													from sel_2;
												v_sql_field_hash := 'md5(coalesce('||v_algorithm||'::text,''-1'')';
												v_sql_field_hash_s := 'md5(coalesce('||v_algorithm||'::text,''-1'')';
											end;
										else
											v_sql_field_hash := 'md5(coalesce('||v_rec_cr.column_name_stg||'::text,''-1'')';
											v_sql_field_hash_s := 'md5(coalesce(s.'||v_rec_cr.column_name_stg||'::text,''-1'')';
										end if;
									 end if;
							    end if;
							    if (v_rec_cr.datatype_stg like('date%')) then
									 v_sql_field_hash := 'md5(coalesce('||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
									 v_sql_field_hash_s := 'md5(coalesce(s.'||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
							    end if;
							    if (v_rec_cr.datatype_stg like('timestamp%')) then
									 v_sql_field_hash := 'md5(coalesce('||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
									 v_sql_field_hash_s := 'md5(coalesce(s.'||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
							    end if;
							    v_i_hash := v_i_hash + 1;
								if v_sql_field_hash is null or v_sql_field_hash_s is null then
									select sys_dwh.get_json4log(p_json_ret := v_json_ret,
										p_step := '13',
										p_descr := 'Поиск хэша полей',
										p_start_dttm := v_start_dttm,
										p_val := 'v_i_hash='''||coalesce(v_i_hash,'')::text||''', v_sql_field_hash='''||coalesce(v_sql_field_hash,'')::text||''', v_sql_field_hash_s='''||coalesce(v_sql_field_hash_s,'')::text||'''',
										p_err := 'Хэш не найден или пустой',
										p_log_tp := '3',
										p_debug_lvl := '1',
										p_cls := ']')
									into v_json_ret;
									raise exception '%', v_json_ret;
								end if;
						else
					 --Если инкремент цикла не первый, добавляем к переменной хэш полей
							if (v_rec_cr.key_type_src not similar to '(%PK%)' or v_rec_cr.key_type_src is null or v_rec_cr.key_type_src = '' or v_is_cnt_pk = 0)
									 and (trim(v_rec_cr.datatype_stg) like('int%') 
									 or trim(v_rec_cr.datatype_stg) like('smallint%') 
									 or trim(v_rec_cr.datatype_stg) like('numeric%')
									 or trim(v_rec_cr.datatype_stg) like ('bigint%')
									 or trim(v_rec_cr.datatype_stg) like ('double precision%')
									 or trim(v_rec_cr.datatype_stg) like('decimal%')
									 or trim(v_rec_cr.datatype_stg) like('real%') 
									 or trim(v_rec_cr.datatype_stg) like('float%') 
									 or trim(v_rec_cr.datatype_stg) like('money%')) then
									 v_sql_field_hash := v_sql_field_hash||'||coalesce('||v_rec_cr.column_name_stg||',-1)::text';
									 v_sql_field_hash_s := v_sql_field_hash_s||'||coalesce(s.'||v_rec_cr.column_name_stg||',-1)::text';
							    end if;
							    if (v_rec_cr.key_type_src similar to '(%PK%)' or v_rec_cr.key_type_src is null or v_rec_cr.key_type_src = '')
									 and (v_rec_cr.datatype_stg like('char%') 
									 or v_rec_cr.datatype_stg like('varchar%') 
									 or v_rec_cr.datatype_stg like('text%')
									 or v_rec_cr.datatype_stg like('uuid%')
									 or v_rec_cr.datatype_stg like('bool%')
									 or v_rec_cr.datatype_stg like('json%')) then
									 if v_rec_cr.column_name_rdv like 'gk_%' then
											v_sql_field_hash := v_sql_field_hash||'||gk_'||v_rec_cr.column_name_stg||'::text';
											v_sql_field_hash_s := v_sql_field_hash_s||'||s.gk_'||v_rec_cr.column_name_stg||'::text';
									 else
										if v_rec_cr.algorithm is not null and v_rec_cr.algorithm <> '' then
											begin
												with sel as
													(select num, when_val, val, query_type, grp from json_populate_recordset(null::record, v_rec_cr.algorithm::json)
														as (num int4, when_val text, val text, query_type int4, grp int4)),
													sel_2 as (select 
														(case when num = 1 and query_type = 2
																then 'case '
															  when num = 1 and query_type = 1
																then ' '
														else '' end)||
														(case when query_type = 1 then coalesce(val,'') when query_type = 2 then coalesce(when_val,'') end)||' '||
														(case when query_type = 2 then coalesce(val,'') else '' end)||' '||
														(case when ((query_type = 2) and (lead(num) over(partition by grp order by num) is null)) 
															then 'end'
														else ''
														end) as f
													from sel)
													select ' '||E'\n'||string_agg(f, E'\n')||E'\n' into v_algorithm
													from sel_2;
												v_sql_field_hash := v_sql_field_hash||'||coalesce('||v_algorithm||'::text,''-1'')';
												v_sql_field_hash_s := v_sql_field_hash_s||'||coalesce('||v_algorithm||'::text,''-1'')';
											end;
										else
											v_sql_field_hash := v_sql_field_hash||'||coalesce('||v_rec_cr.column_name_stg||'::text,''-1'')';
											v_sql_field_hash_s := v_sql_field_hash_s||'||coalesce(s.'||v_rec_cr.column_name_stg||'::text,''-1'')';
										end if;
									 end if;
							    end if;
							    if (v_rec_cr.key_type_src not similar to '(%PK%)' or v_rec_cr.key_type_src is null or v_rec_cr.key_type_src = '')
									 and (v_rec_cr.datatype_stg like('date%') 
									 or v_rec_cr.datatype_stg like('timestamp%')) then
									 v_sql_field_hash := v_sql_field_hash||'||coalesce('||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
									 v_sql_field_hash_s := v_sql_field_hash_s||'||coalesce(s.'||v_rec_cr.column_name_stg||'::text,''1900-01-01'')::text';
							    end if;
							end if;
							if v_sql_field_hash is null or v_sql_field_hash_s is null then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '13',
									p_descr := 'Поиск хэша полей',
									p_start_dttm := v_start_dttm,
									p_val := 'v_i_hash='''||coalesce(v_i_hash,'')::text||''', v_sql_field_hash='''||coalesce(v_sql_field_hash,'')::text||''', v_sql_field_hash_s='''||coalesce(v_sql_field_hash_s,'')::text||'''',
									p_err := 'Хэш не найден или пустой',
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
								into v_json_ret;
								raise exception '%', v_json_ret;
							end if;
						end if;
						
					 --Если инкремент цикла первый, фиксируем внутрь переменной список полей и их типы 
						v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '15',
							p_descr := 'Поиск списока полей и их типы и алгоритмы',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
						begin
							if v_i = 0 then
							    if v_rec_cr.column_name_rdv like 'gk_%' then
									v_sql_field_stg := 'gk_'||v_rec_cr.column_name_stg;
									v_sql_field_stg_s  := 's.gk_'||v_rec_cr.column_name_stg;
							    else
									
									v_sql_field_stg := v_rec_cr.column_name_stg;
									--Фиксируем преобразование типов полей и алгоритм
									if v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then
											v_sql_field_stg_s := '('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg','s.'||v_rec_cr.column_name_stg)||')::'||v_rec_cr.datatype_rdv;
									elsif v_rec_cr.algorithm is not null and v_rec_cr.algorithm <> '' then
										begin
											with sel as
												(select num, when_val, val, query_type, grp from json_populate_recordset(null::record, v_rec_cr.algorithm::json)
													as (num int4, when_val text, val text, query_type int4, grp int4)),
												sel_2 as (select 
													(case when num = 1 and query_type = 2
															then 'case '
														  when num = 1 and query_type = 1
															then ' '
													else '' end)||
													(case when query_type = 1 then coalesce(val,'') when query_type = 2 then coalesce(when_val,'') end)||' '||
												    (case when query_type = 2 then coalesce(val,'') else '' end)||' '||
													(case when ((query_type = 2) and (lead(num) over(partition by grp order by num) is null)) 
														then 'end'
													else ''
													end) as f
												from sel)
												select ' '||E'\n'||string_agg(f, E'\n')||E'\n' into v_algorithm
												from sel_2;
											v_sql_field_stg_s := v_algorithm;
										end;
									else
											v_sql_field_stg_s := '(s.'||v_rec_cr.column_name_stg||')::'||v_rec_cr.datatype_rdv;
									end if;
							    end if;                           
							    v_sql_field_rdv := v_rec_cr.column_name_rdv;
								if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 or v_cnt_hsh <> 0 then
									if v_rec_cr.is_add or v_rec_cr.is_rts or v_rec_cr.is_alg then
										if v_rec_cr.column_name_rdv like('gk_%') then
											if v_rec_cr.is_rts then
												v_sql_field_chg := 't.gk_'||v_rec_cr.column_name_stg;
											elsif v_rec_cr.is_add or v_rec_cr.is_alg then	
												v_sql_field_chg := 's.gk_'||v_rec_cr.column_name_stg;
											else
												v_sql_field_chg := 't.'||v_rec_cr.column_name_rdv;
											end if;
										else
											if v_rec_cr.is_rts then
												v_sql_field_chg := 't.'||v_rec_cr.column_name_stg;
											elsif v_rec_cr.is_add or v_rec_cr.is_alg then
												v_sql_field_chg := 's.'||v_rec_cr.column_name_stg;
											else
												v_sql_field_chg := 't.'||v_rec_cr.column_name_rdv;
											end if;
										end if;
									else
										v_sql_field_chg := 't.'||v_rec_cr.column_name_rdv;
									end if;
								end if;
							    v_sql_field_rdv_type := v_rec_cr.column_name_rdv||' '||v_rec_cr.datatype_rdv;
							    --v_sql_rdv_pk_s_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;----????Должно быть в PK
							  --Фиксируем связки таблиц
							    if v_table_name_rdv like('l_%') then
									 --перенёс
									 
									v_sql_pk_where := '';
									if v_rec_cr.column_name_rdv like('gk_%') then
										v_sql_pk_where := v_sql_pk_where||' s.gk_'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
										v_sql_rdv_pk_s_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
										v_sql_rdv_pk_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
									elsif v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then
									 	v_sql_pk_where := v_sql_pk_where||' coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg', 's.'||v_rec_cr.column_name_stg)||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
										v_sql_rdv_pk_s_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
										v_sql_rdv_pk_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
									else
										if (trim(v_rec_cr.datatype_stg) like('int%') 
											 or trim(v_rec_cr.datatype_stg) like('smallint%') 
											 or trim(v_rec_cr.datatype_stg) like('numeric%')
											 or trim(v_rec_cr.datatype_stg) like ('bigint%')
											 or trim(v_rec_cr.datatype_stg) like ('double precision%')
											 or trim(v_rec_cr.datatype_stg) like('decimal%')
											 or trim(v_rec_cr.datatype_stg) like('real%') 
											 or trim(v_rec_cr.datatype_stg) like('float%') 
											 or trim(v_rec_cr.datatype_stg) like('money%')) then 
											v_sql_pk_where := v_sql_pk_where||' coalesce(s.'||v_rec_cr.column_name_stg||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
											v_sql_rdv_pk_s_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
											v_sql_rdv_pk_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
										elsif (v_rec_cr.datatype_stg like('char%') 
											 or v_rec_cr.datatype_stg like('varchar%') 
											 or v_rec_cr.datatype_stg like('text%')
											 or v_rec_cr.datatype_stg like('uuid%')
											 or v_rec_cr.datatype_stg like('bool%')
											 or v_rec_cr.datatype_stg like('json%')) then
											v_sql_pk_where := v_sql_pk_where||' coalesce(s.'||v_rec_cr.column_name_stg||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
											v_sql_rdv_pk_s_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
											v_sql_rdv_pk_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
										elsif (v_rec_cr.datatype_stg like('date%') 
											 or v_rec_cr.datatype_stg like('timestamp%')) then
											v_sql_pk_where := v_sql_pk_where||' coalesce(s.'||v_rec_cr.column_name_stg||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
											v_sql_rdv_pk_s_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
											v_sql_rdv_pk_where := ' coalesce(s.'||v_rec_cr.column_name_rdv||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
										else v_sql_pk_where := v_sql_pk_where||' s.'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
											v_sql_rdv_pk_s_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
											v_sql_rdv_pk_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
										end if;
									end if;
							    end if;
								if v_sql_field_rdv is null or v_sql_field_rdv_type is null then
									select sys_dwh.get_json4log(p_json_ret := v_json_ret,
										p_step := '15',
										p_descr := 'Поиск списока полей и их типы и алгоритмы',
										p_start_dttm := v_start_dttm,
										p_val := 'v_sql_field_rdv='''||coalesce(v_sql_field_rdv,'')::text||''', v_sql_field_rdv_type='''||coalesce(v_sql_field_rdv_type,'')::text||'''',
										p_err := 'Поле или тип не найден или пустой',
										p_log_tp := '3',
										p_debug_lvl := '1',
										p_cls := ']')
									into v_json_ret;
									raise exception '%', v_json_ret;
								end if;
							else
							--Если инкремент цикла не первый, добавляем к переменной список полей и их типы
							    if v_rec_cr.column_name_rdv like 'gk_%' then
									 v_sql_field_stg := v_sql_field_stg||', '||'gk_'||v_rec_cr.column_name_stg;
									 v_sql_field_stg_s := v_sql_field_stg_s||', '||'s.gk_'||v_rec_cr.column_name_stg;
							    else 
									 v_sql_field_stg := v_sql_field_stg||', '||v_rec_cr.column_name_stg;
								  --Фиксируем преобразование типов полей                                 
								    if       v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then
											v_sql_field_stg_s := v_sql_field_stg_s||', ('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg','s.'||v_rec_cr.column_name_stg)||')::'||v_rec_cr.datatype_rdv;
									elsif v_rec_cr.algorithm is not null and v_rec_cr.algorithm <> '' then
										begin
											with sel as
												(select num, when_val, val, query_type, grp from json_populate_recordset(null::record, v_rec_cr.algorithm::json)
													as (num int4, when_val text, val text, query_type int4, grp int4)),
												sel_2 as (select 
													(case when num = 1 and query_type = 2
															then ' case '
														  when num = 1 and query_type = 1
															then ' '
													else '' end)||
													(case when query_type = 1 then coalesce(val,'') when query_type = 2 then coalesce(when_val,'') end)||' '||
												    (case when query_type = 2 then coalesce(val,'') else '' end)||' '||
													(case when ((query_type = 2) and (lead(num) over(partition by grp order by num) is null)) 
														then 'end'
													else ''
													end) as f
												from sel)
												select ' '||E'\n'||string_agg(f, E'\n')||E'\n' into v_algorithm
												from sel_2;
											v_sql_field_stg_s := v_sql_field_stg_s||', '||v_algorithm;
										end;
									else
											v_sql_field_stg_s := v_sql_field_stg_s||', (s.'||v_rec_cr.column_name_stg||')::'||v_rec_cr.datatype_rdv;
									end if;                                 
							    end if;                           
							    v_sql_field_rdv := v_sql_field_rdv||', '||v_rec_cr.column_name_rdv;
								
								if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 or v_cnt_hsh <> 0 then
									if v_rec_cr.is_add or v_rec_cr.is_rts or v_rec_cr.is_alg then
										if v_rec_cr.column_name_rdv like('gk_%') then
											if v_rec_cr.is_rts then
												v_sql_field_chg :=  v_sql_field_chg||', '||'t.gk_'||v_rec_cr.column_name_stg;
											elsif v_rec_cr.is_add or v_rec_cr.is_alg then
												v_sql_field_chg :=  v_sql_field_chg||', '||'s.gk_'||v_rec_cr.column_name_stg;
											else
												v_sql_field_chg :=  v_sql_field_chg||', '||'t.'||v_rec_cr.column_name_rdv;
											end if;
										else
											if v_rec_cr.is_rts then
												v_sql_field_chg :=  v_sql_field_chg||', '||'t.'||v_rec_cr.column_name_stg;
											elsif v_rec_cr.is_add or v_rec_cr.is_alg then
												v_sql_field_chg :=  v_sql_field_chg||', '||'s.'||v_rec_cr.column_name_stg;
											else
												v_sql_field_chg :=  v_sql_field_chg||', '||'t.'||v_rec_cr.column_name_rdv;
											end if;
										end if;
									else
										v_sql_field_chg :=  v_sql_field_chg||', '||'t.'||v_rec_cr.column_name_rdv;
									end if;
								end if;
								
							    v_sql_field_rdv_type := v_sql_field_rdv_type||', '||v_rec_cr.column_name_rdv||' '||v_rec_cr.datatype_rdv;
							  --Фиксируем связки таблиц
							    if v_table_name_rdv like('l_%') then                           
									if v_rec_cr.column_name_rdv like('gk_%') then
										v_sql_pk_where := v_sql_pk_where||' and s.gk_'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
										v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
										v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
									elsif v_rec_cr.datatype_stg_transform is not null and v_rec_cr.datatype_stg_transform <> ''  then
									 	v_sql_pk_where := v_sql_pk_where||' and coalesce('||replace(v_rec_cr.datatype_stg_transform, 'column_name_stg', 's.'||v_rec_cr.column_name_stg)||'::'||v_rec_cr.datatype_rdv||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
										v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
										v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||'::text, '''') = coalesce(t.'||v_rec_cr.column_name_rdv||'::text, '''')';
									else
										if (trim(v_rec_cr.datatype_stg) like('int%') 
											 or trim(v_rec_cr.datatype_stg) like('smallint%') 
											 or trim(v_rec_cr.datatype_stg) like('numeric%')
											 or trim(v_rec_cr.datatype_stg) like ('bigint%')
											 or trim(v_rec_cr.datatype_stg) like ('double precision%')
											 or trim(v_rec_cr.datatype_stg) like('decimal%')
											 or trim(v_rec_cr.datatype_stg) like('real%') 
											 or trim(v_rec_cr.datatype_stg) like('float%') 
											 or trim(v_rec_cr.datatype_stg) like('money%')) then 
											v_sql_pk_where := v_sql_pk_where||' and coalesce(s.'||v_rec_cr.column_name_stg||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
											v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
											v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||',-1) = coalesce(t.'||v_rec_cr.column_name_rdv||',-1)';
										elsif (v_rec_cr.datatype_stg like('char%') 
											 or v_rec_cr.datatype_stg like('varchar%') 
											 or v_rec_cr.datatype_stg like('text%')
											 or v_rec_cr.datatype_stg like('uuid%')
											 or v_rec_cr.datatype_stg like('bool%')
											 or v_rec_cr.datatype_stg like('json%')) then
											v_sql_pk_where := v_sql_pk_where||' and coalesce(s.'||v_rec_cr.column_name_stg||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
											v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
											v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||','''') = coalesce(t.'||v_rec_cr.column_name_rdv||','''')';
										elsif (v_rec_cr.datatype_stg like('date%') 
											 or v_rec_cr.datatype_stg like('timestamp%')) then
											v_sql_pk_where := v_sql_pk_where||' and coalesce(s.'||v_rec_cr.column_name_stg||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
											v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
											v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and coalesce(s.'||v_rec_cr.column_name_rdv||',''1900-01-01'') = coalesce(t.'||v_rec_cr.column_name_rdv||',''1900-01-01'')';
										else v_sql_pk_where := v_sql_pk_where||' s.'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
											v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
											v_sql_rdv_pk_where := v_sql_rdv_pk_where||' and s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
										end if;
									end if;	 
							    end if;
								if v_sql_field_rdv is null or v_sql_field_rdv_type is null then
									select sys_dwh.get_json4log(p_json_ret := v_json_ret,
										p_step := '15',
										p_descr := 'Поиск списока полей и их типы и алгоритмы',
										p_start_dttm := v_start_dttm,
										p_val := 'v_i='''||coalesce(v_i,'')::text||''', v_sql_field_rdv='''||coalesce(v_sql_field_rdv,'')::text||''', v_sql_field_rdv_type='''||coalesce(v_sql_field_rdv_type,'')::text||'''',
										p_err := 'Поле или тип не найден или пустой',
										p_log_tp := '3',
										p_debug_lvl := '1',
										p_cls := ']')
									into v_json_ret;
									raise exception '%', v_json_ret;
								end if;
							end if;
						exception when others then
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '15',
								p_descr := 'Поиск списока полей и их типы и алгоритмы',
								p_start_dttm := v_start_dttm,
								p_err := SQLERRM,
								p_log_tp := '3',
								p_debug_lvl := '1',
								p_cls := ']')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
						v_i := v_i + 1;
					    --Выбираем поля, являющиеся ключами                   
						if trim(v_rec_cr.key_type_src) similar to '(%PK%)' then
							v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '16',
								p_descr := 'Поиск списока, являющиеся ключами',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '3')
							into v_json_ret;
						--Если инкремент цикла первый, фиксируем ключи для связок внутрь переменной                    
							    if v_i_gk_pk = 0 then
									 v_sql_distributed := v_rec_cr.column_name_rdv;
									 v_sql_pk_where := ' s.gk_'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
									 v_sql_rdv_pk_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
									 v_sql_rdv_pk_s_where := ' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
							    else
						--Если инкремент цикла не первый, добавляем ключи для связок к переменной
									 v_sql_distributed := v_sql_distributed||', '||v_rec_cr.column_name_rdv;
									 v_sql_pk_where := v_sql_pk_where||' s.gk_'||v_rec_cr.column_name_stg||' = t.'||v_rec_cr.column_name_rdv;
									 v_sql_rdv_pk_where := v_sql_rdv_pk_where||' s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
									 v_sql_rdv_pk_s_where := v_sql_rdv_pk_s_where||' and s.'||v_rec_cr.column_name_rdv||' = t.'||v_rec_cr.column_name_rdv;
							    end if;
							if v_sql_distributed is null or v_sql_pk_where is null or v_sql_rdv_pk_where is null or v_sql_rdv_pk_s_where is null then
									select sys_dwh.get_json4log(p_json_ret := v_json_ret,
										p_step := '16',
										p_descr := 'Поиск списока, являющиеся ключами',
										p_start_dttm := v_start_dttm,
										p_val := 'v_i_gk_pk='''||coalesce(v_i_gk_pk,'')::text||''', v_sql_distributed='''||coalesce(v_sql_distributed,'')::text||''', v_sql_pk_where='''||coalesce(v_sql_pk_where,'')::text||''', v_sql_rdv_pk_where='''||coalesce(v_sql_rdv_pk_where,'')::text||''', v_sql_rdv_pk_s_where='''||coalesce(v_sql_rdv_pk_s_where,'')::text||'''',
										p_err := 'Поле или условие не найдено или пустое',
										p_log_tp := '3',
										p_debug_lvl := '1',
										p_cls := ']')
									into v_json_ret;
									raise exception '%', v_json_ret;
							end if;
							    v_i_gk_pk := v_i_gk_pk + 1;
						end if;
					 --Выбираем поля, являющиеся ключами                                    
						if trim(v_rec_cr.key_type_src) similar to '(%PK%|%FK%)'
							    and trim(v_rec_cr.key_type_src) is not null
							    and trim(v_rec_cr.key_type_src) <> '' then
						--Если инкремент цикла первый, фиксируем ключи внутрь переменной
								v_start_dttm := clock_timestamp() at time zone 'utc';
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '16',
									p_descr := 'Поиск списока, являющиеся ключами',
									p_start_dttm := v_start_dttm,
									p_log_tp := '1',
									p_debug_lvl := '3')
								into v_json_ret;
							    if v_i_gk = 0 then                                 
									v_sql_fld_rdv_gk := trim(v_rec_cr.column_name_rdv);
									if trim(v_rec_cr.key_type_src) similar to '(%FK%)' and substring(trim(v_table_name_rdv) from 1 for 2) = 'l_' then
										v_sql_distributed := v_rec_cr.column_name_rdv;
									end if;
						--Если инкремент цикла не первый, добавляем ключи к переменной
							    else
									v_sql_fld_rdv_gk := v_sql_fld_rdv_gk||', '||trim(v_rec_cr.column_name_rdv);
									if trim(v_rec_cr.key_type_src) similar to '(%FK%)' and substring(trim(v_table_name_rdv) from 1 for 2) = 'l_' then
										v_sql_distributed := v_sql_distributed||', '||v_rec_cr.column_name_rdv;
									end if;
							    end if;
								if v_sql_fld_rdv_gk is null then
									select sys_dwh.get_json4log(p_json_ret := v_json_ret,
										p_step := '17',
										p_descr := 'Поиск списка, являющиеся ключами',
										p_start_dttm := v_start_dttm,
										p_val := 'v_i_gk='''||coalesce(v_i_gk,'')::text||''', v_sql_fld_rdv_gk='''||coalesce(v_sql_fld_rdv_gk,'')::text||'''',
										p_err := 'Поле или условие не найдено или пустое',
										p_log_tp := '3',
										p_debug_lvl := '1',
										p_cls := ']')
									into v_json_ret;
									raise exception '%', v_json_ret;
								end if;
							    v_i_gk := v_i_gk + 1;
						end if;
				end loop;
				--Фиксируем json-правила заполнения в переменную
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					select where_json, is_distinct into v_json_rule, v_is_distinct
						from sys_dwh.prm_s2t_rdv_rule
							where src_stm_id = p_src_stm_id
								and table_name_rdv = v_table_name_rdv
								and daterange(eff_dt,end_dt,'[]')@>(now() at time zone 'utc')::date;
					/*if v_json_rule is null then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '17',
							p_descr := 'json-правило пустое',
							p_start_dttm := v_start_dttm,
							p_val := 'v_cnt='''||v_cnt::text||'''', 
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '3',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end if;*/
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '18',
						p_descr := 'Поиск json-правила заполнения',
						p_start_dttm := v_start_dttm,
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '18',
						p_descr := 'Поиск json-правила заполнения',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;
				--Парсим правила в переменную
				if v_json_rule is not null and v_json_rule <> '' then
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						with sel as
						(select num, predicate, fld_nm , equal_type, val, func_nm, where_type, grp from json_populate_recordset(null::record, v_json_rule::json)
									as (num int4, predicate text, fld_nm text, equal_type text, val text, func_nm text, where_type int4, grp int4)),
						sel_2 as (select 
						(case when num = 1 then predicate||' (' else predicate||' ' end)||
						(case when where_type = 1 then 's.'||fld_nm when where_type = 2 then func_nm end)||' '||
						' '||equal_type||' '||val||
						(case when lead(num) over(partition by grp order by num) is null then ')' else '' end) as f
						from sel)
						select ' and 1=1'||E'\n'||string_agg(f, E'\n')||E'\n' into v_rule
						from sel_2;
						
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '19',
							p_descr := 'Парсинг json-правила',
							p_start_dttm := v_start_dttm,
							p_val := 'v_rule='''||v_rule::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '19',
							p_descr := 'Парсинг json-правила',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
				end if;
				v_rule := coalesce(v_rule, '');
		    --Закрываем цикл по полям
				v_sql_field_hash := v_sql_field_hash||')';
				v_sql_field_hash_s := v_sql_field_hash_s||')';
			--Проверяем прохеширована ли таблица по новому
				select count (*) 
				into v_cnt_is_new_hash
				from sys_dwh.prm_task
					where success_fl = true
						and type_of_task = 'change_hash_diff'
						and table_name = v_tbl_nm_rdv;
			--Фиксируем новый hash
			if v_cnt_add <> 0 or v_cnt_rts <> 0 or v_cnt_alg <> 0 or v_cnt_hsh <> 0 or v_cnt_is_new_hash <> 0 then
				select * from sys_dwh.get_hash_diff_rdv(v_tbl_nm_rdv) into v_sql_field_hash, v_sql_field_hash_s, v_sql_field_hash_chg;
			end if;
		    --Проверяем и фиксируем наличие gk_pk
				v_start_dttm := clock_timestamp() at time zone 'utc';
				select count (*) 
				into v_cnt
				from information_schema.columns c
						join sys_dwh.prm_s2t_rdv p on c.table_schema = regexp_replace(schema_stg,'^stg', 'sdv') 
						and c.table_name = p.table_name_stg
						where p.src_stm_id = p_src_stm_id
							   and column_name = 'gk_pk'
							   and substring(trim(v_table_name_rdv) from 1 for 2) <> 'l_';
				if v_cnt <> 0 then
						v_sql_gk_pk := 'gk_pk';
						v_sql_pk_where := 's.gk_pk = t.gk_pk';
						v_sql_rdv_pk_s_where := 's.gk_pk = t.gk_pk';
				end if;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '20',
					p_descr := 'Проверка наличия gk_pk',
					p_start_dttm := v_start_dttm,
					p_val := 'v_cnt='''||v_cnt::text||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
		   --Удаляем временную таблицу с изменениями, если она уже есть на БД
				begin 
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_exec_sql := 'drop table if exists rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '21',
						p_descr := 'Удаление временной таблицы',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
					execute v_exec_sql;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '21',
						p_descr := 'Удаление временной таблицы',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;
				--Создаём временную таблицу с изменениями
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_exec_sql := 'create table rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
					if v_sql_gk_pk is not null then
							v_exec_sql := v_exec_sql||v_sql_gk_pk||' uuid, ';
					end if;
					v_exec_sql := v_exec_sql||v_sql_field_rdv_type||', eff_dt date NULL, end_dt date NULL, src_stm_id int8 NULL, hash_diff text NULL, upd_dttm timestamp NULL';          
					v_exec_sql := v_exec_sql||') with (appendonly=true)  distributed ';
					--получаем ключ дистрибьюции из словаря данных
					select
						replace(pg_get_table_distributedby(c.oid), 'DISTRIBUTED BY ','')
						into v_sql_distributed_t
						from pg_class as c
							inner join pg_namespace as n
							on c.relnamespace = n.oid
							where n.nspname = v_schema_rdv and c.relname = v_table_name_rdv;
					if v_sql_distributed_t is not null then
						v_exec_sql := v_exec_sql||' by '||v_sql_distributed_t;
					elsif (v_sql_gk_pk is not null) or (v_sql_distributed is not null) then
							v_exec_sql := v_exec_sql||' by (';
							if v_sql_gk_pk is not null then
							  v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							end if;
							if v_sql_distributed is not null then
							-----
								--if v_column_fltr is not null then
								--	v_exec_sql := v_exec_sql||v_sql_distributed||', eff_dt, end_dt, '||v_column_fltr||')';
								--else	
									v_exec_sql := v_exec_sql||v_sql_distributed||', eff_dt)';
								--end if;
								--v_exec_sql := v_exec_sql||v_sql_distributed||')';
							end if;
					else
							v_exec_sql := v_exec_sql||' randomly ';
					end if;
					execute v_exec_sql;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '22',
						p_descr := 'Создание временной таблицы',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '22',
						p_descr := 'Создание временной таблицы',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;
				--сбор статистики на приёмнике
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';	
					v_exec_sql := 'analyze '||v_tbl_nm_rdv;
					execute v_exec_sql;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '23',
							p_descr := 'Сбор статистики на приёмнике',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
				end;
				--Блок предобработки приёмника в случае добавления нового поля или изменения ref_to_stg
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					if v_cnt_add > 0 or v_cnt_alg > 0 or v_cnt_hsh > 0 or v_cnt_rts > 0 then
						--вставка неактуальных записей
						v_exec_sql := 'insert into '||v_tbl_nm_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm)';
						v_exec_sql := v_exec_sql||' select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||'t.'||v_sql_gk_pk||', ';
						end if;
						if v_cnt_rts > 0 then
							v_exec_sql := v_exec_sql||v_sql_field_chg;
						else
							v_exec_sql := v_exec_sql||v_sql_field_rdv;
						end if;
						v_exec_sql := v_exec_sql||', t.eff_dt, t.end_dt, t.src_stm_id, hash_diff, t.upd_dttm';
						v_exec_sql := v_exec_sql||' from '||v_backup_table_name||' t';
						v_exec_sql := v_exec_sql||' where t.end_dt <> ''9999-12-31''::date';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '24.1',
							p_descr := 'Применение блока предобработки. Вставка неактуальных записей',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						execute v_exec_sql;
						v_exec_sql := 'analyze '||v_tbl_nm_rdv;
						execute v_exec_sql;
						--вставка актуальных
						v_exec_sql := 'insert into '||v_tbl_nm_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm)';
						v_exec_sql := v_exec_sql||' select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||'t.'||v_sql_gk_pk||', ';
						end if;
						if v_cnt_rts > 0 or v_cnt_alg > 0 or v_cnt_add > 0 or v_cnt_hsh > 0 then
							v_exec_sql := v_exec_sql||v_sql_field_chg;
						end if;
						v_exec_sql := v_exec_sql||', t.eff_dt, t.end_dt, t.src_stm_id, '||v_sql_field_hash_chg||', t.upd_dttm';
						v_exec_sql := v_exec_sql||' from '||v_backup_table_name||' t';
						if (v_cnt_add > 0 or v_cnt_alg > 0) and v_cnt_rts = 0 then
							v_exec_sql := v_exec_sql||' left join '||v_tbl_nm_sdv||' s ';
							v_exec_sql := v_exec_sql||' on s.ssn = '||coalesce(v_ssn::text, '')||' and '||coalesce(v_sql_pk_where::text, '')
							;
						end if;
						v_exec_sql := v_exec_sql||' where t.end_dt = ''9999-12-31''::date';
						--raise notice '%', v_exec_sql;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '24.2',
							p_descr := 'Применение блока предобработки. Вставка актуальных записей',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						execute v_exec_sql;
						v_exec_sql := 'analyze '||v_tbl_nm_rdv;
						--raise notice '%', v_exec_sql;
						execute v_exec_sql;
						if v_cnt_add > 0 then
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'add_column_rdv') ;
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'change_hash_diff') ;
						end if;
						if v_cnt_alg > 0 then
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'change_algorithm', 'change_algorithm');
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'change_hash_diff') ;
						end if;
						if v_cnt_hsh > 0 then
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'change_hash_diff', 'change_hash_diff');
							
						end if;
						if v_cnt_rts > 0 then
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'change_ref_to_stg', 'change_ref_to_stg');
							perform sys_dwh.set_task_prev_rdv(v_tbl_nm_rdv, 'add_column_rdv', 'change_hash_diff') ;
						end if;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '24.3',
							p_descr := 'Применение блока предобработки. Закрытие заданий',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						execute v_exec_sql;
					end if;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '24',
						p_descr := 'Применение блока предобработки',
						p_start_dttm := v_start_dttm,
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;
				--Заполняем временную таблицу для сущностей, загружаемых снепшотом
				if v_load_mode in (1,3) then
						--Наполняем временную таблицу значениями отсутствующими в снепшоте
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							
							v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
							if v_sql_gk_pk is not null then
								   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							end if;      
							v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm)';             
							v_exec_sql := v_exec_sql||' select ';
							if v_sql_gk_pk is not null then
								   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							end if;
							v_exec_sql := v_exec_sql||coalesce(v_sql_field_rdv, '')||', eff_dt, 
								   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '''||v_valid_dt::text||'''::date - 2
								   else end_dt end end_dt, 
								   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '||coalesce(p_src_stm_id::text, '')||'
								   else t.src_stm_id end src_stm_id
								   , hash_diff, 
								   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then (now() at time zone ''utc'')
								   else upd_dttm end upd_dttm ';
							v_exec_sql := v_exec_sql||' from '||coalesce(v_tbl_nm_rdv::text, '')||' t where not exists (select 1 from '||coalesce(v_tbl_nm_sdv::text, '')||' s where s.ssn = '||coalesce(v_ssn::text, '')||' and '||coalesce(v_sql_pk_where::text, '')||coalesce(v_rule::text, '')||coalesce(v_sql_fltr_s::text, '');
							if v_load_mode = 3 then
								v_exec_sql := v_exec_sql||') '||v_sql_fltr_t;
							else
								v_exec_sql := v_exec_sql||')';
							end if;
							execute v_exec_sql;
							get diagnostics v_cnt = row_count;
							v_del := v_cnt;
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '25',
								p_descr := 'Наполняем временную таблицу значениями отсутствующими в снепшоте',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_del_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
						exception when others then
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '25',
								p_descr := 'Наполняем временную таблицу значениями отсутствующими в снепшоте',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_debug_lvl := '1',
								p_cls := ']')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
						--Собираем статистику по временной таблице
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '26',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '3')
								into v_json_ret;
							v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
							execute v_exec_sql;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '26',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
				end if;
		    --Заполняем временную таблицу для сущностей, загружаемых CDC       
				if v_load_mode in (2,5) then
					v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '27',
							p_descr := 'Заполняем временную таблицу для сущностей, загружаемых CDC',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						--Наполняем временную таблицу значениями удаленными CDC
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
							if v_sql_gk_pk is not null then
								   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							end if;      
							v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
							v_exec_sql := v_exec_sql||'select ';
							if v_sql_gk_pk is not null then
								   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							end if;
							
							if v_load_mode = 2 then
								v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, 
								case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '''||v_valid_dt::text||'''::date - 2
									   else end_dt end end_dt, 
									   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '||p_src_stm_id||'
									   else t.src_stm_id end src_stm_id
									   , hash_diff, 
									   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then (now() at time zone ''utc'')
									   else upd_dttm end upd_dttm ';
								v_exec_sql := v_exec_sql||' from '||v_tbl_nm_rdv||' t where exists (select 1 from '||v_tbl_nm_sdv||' s where '||v_sql_pk_where||' 
										and (dml_tp = ''D'') and s.ssn = '||v_ssn::text||v_rule||')
										and daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date';
								--return v_exec_sql;
							elsif v_load_mode = 5 then
								v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, 
								case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '''||v_valid_dt::text||'''::date - 2
									   else end_dt end end_dt, 
									   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '||p_src_stm_id||'
									   else t.src_stm_id end src_stm_id
									   , hash_diff, 
									   case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then (now() at time zone ''utc'')
									   else upd_dttm end upd_dttm ';
								v_exec_sql := v_exec_sql||' from '||v_tbl_nm_rdv||' t where not exists (select 1 from '||v_tbl_nm_sdv||' s where '||v_sql_pk_where||' and s.ssn = '||v_ssn::text||v_rule||') 
										and exists (select 1 from '||v_tbl_nm_sdv||' s where '||v_sql_field_gu||' and s.ssn = '||v_ssn::text||v_rule||')
										and daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date';
								
							end if;
						    --return v_exec_sql;
							execute v_exec_sql;
							get diagnostics v_cnt = row_count;
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '28',
								p_descr := 'Наполняем временную таблицу значениями из rdv удаленных CDC',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_del_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
						exception when others then
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '28',
								p_descr := 'Наполняем временную таблицу значениями из rdv удаленных CDC',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_debug_lvl := '1',
								p_cls := ']')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
						--Собираем статистику по временной таблице
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '29',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '3')
								into v_json_ret;
							v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
							execute v_exec_sql;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '29',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
					    if v_cnt <> 0 then
						--Записываем состояние из sdv
							begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							    v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
							    if v_sql_gk_pk is not null then
									v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							    end if;      
							    v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
							    v_exec_sql := v_exec_sql||'select ';
							    if v_sql_gk_pk is not null then
									v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
							    end if;
							    v_exec_sql := v_exec_sql||v_sql_field_stg||', '''||v_valid_dt::text||'''::date - 1 eff_dt, 
							    case when daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date then '''||v_valid_dt::text||'''::date - 1
									else end_dt end end_dt,
							    '||p_src_stm_id||' src_stm_id
							    , '||v_sql_field_hash||' hash_diff, 
							    (now() at time zone ''utc'') upd_dttm';
							    v_exec_sql := v_exec_sql||' from '||v_tbl_nm_sdv||' s where (dml_tp = ''D'') and s.ssn = '||v_ssn::text||v_rule||' and exists (select 1 from '||v_tbl_nm_rdv||' t where '||v_sql_pk_where||')';
							    --return v_exec_sql;
							    --execute v_exec_sql; !!!Шаг выключен, закрываем состояние из rdv, последнее состояние источника хранить не имеет смысла
							    get diagnostics v_cnt = row_count;
								--v_del := v_cnt;
							    v_del := 0;
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '30',
									p_descr := 'Наполняем временную таблицу значениями удаленными CDC',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
									p_del_qty := v_cnt::text,
									p_log_tp := '1',
									p_debug_lvl := '2')
									into v_json_ret;
							exception when others then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '30',
									p_descr := 'Наполняем временную таблицу значениями удаленными CDC',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
								into v_json_ret;
								raise exception '%', v_json_ret;
							end;
							--Собираем статистику по временной таблице
							begin
								v_start_dttm := clock_timestamp() at time zone 'utc';
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '31',
									p_descr := 'Сбор статистики',
									p_start_dttm := v_start_dttm,
									p_log_tp := '1',
									p_debug_lvl := '3')
									into v_json_ret;
								v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
								execute v_exec_sql;
							exception when others then	
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '31',
									p_descr := 'Сбор статистики',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_cls := ']',
									p_debug_lvl := '1')
								into v_json_ret;
								raise exception '%', v_json_ret;
							end;
						end if;
				end if;
		   --Заполняем временную таблицу для сущностей, загружаемых c закрытием старых записей
				if v_load_mode <> 4 then
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						--Наполняем временную таблицу значениями из rdv, где данные уже были закрыты (!) 
						v_start_dttm := clock_timestamp() at time zone 'utc';
						v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
						v_exec_sql := v_exec_sql||'select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, 
							   end_dt, 
							   src_stm_id, hash_diff, 
							   upd_dttm '; --t.eff_dt <= now()::date and t.end_dt >= now()::date
						v_exec_sql := v_exec_sql||' from '||v_tbl_nm_rdv||' t where not daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date and exists (select 1 from '||v_tbl_nm_sdv||' s where s.ssn = '||v_ssn::text||' and (dml_tp <> ''D'') and '||v_sql_pk_where||v_rule||coalesce(v_sql_fltr_s::text, '')||')';
						if v_load_mode = 3 and v_tbl_nm_rdv = 'rdv_skb_3card.s_ev_entr' then
							v_exec_sql := v_exec_sql||' '||v_sql_fltr_t;
						end if;
						--return v_exec_sql;
						execute v_exec_sql;
						get diagnostics v_cnt = row_count;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '32',
								p_descr := 'Наполняем временную таблицу значениями из rdv, где данные уже были закрыты',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '32',
									p_descr := 'Наполняем временную таблицу значениями из rdv, где данные уже были закрыты',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
							into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Собираем статистику по временной таблице
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '33',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
						execute v_exec_sql;
					exception when others then	
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '33',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Наполняем временную таблицу значениями из rdv, где данные актуальны (хэш sdv = хэш rdv)
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
						
						v_exec_sql := v_exec_sql||' select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;                    
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, 
							   end_dt, 
							   src_stm_id, hash_diff, 
							   upd_dttm ';
						v_exec_sql := v_exec_sql||' from '||v_tbl_nm_rdv||' t where daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date and exists (select 1 from '||v_tbl_nm_sdv||' s where s.ssn = '||v_ssn::text||' and '||v_sql_field_hash||' = t.hash_diff and (dml_tp <> ''D'' ) and '||v_sql_pk_where||v_rule||coalesce(v_sql_fltr_s::text, '')||')';
						if v_load_mode = 3 and v_tbl_nm_rdv = 'rdv_skb_3card.s_ev_entr' then
							v_exec_sql := v_exec_sql||' '||v_sql_fltr_t;
						end if;
						--return v_exec_sql;
						execute v_exec_sql;
						get diagnostics v_cnt = row_count;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '34',
								p_descr := 'Наполняем временную таблицу значениями из rdv, где данные актуальны (хэш sdv = хэш rdv)',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '34',
									p_descr := 'Наполняем временную таблицу значениями из rdv, где данные актуальны (хэш sdv = хэш rdv)',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
							into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Собираем статистику по временной таблице
						begin
							v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '35',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '3')
								into v_json_ret;
							v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
							execute v_exec_sql;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '35',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
							raise exception '%', v_json_ret;
						end;
					 --Наполняем временную таблицу значениями из rdv, где данные не актуальны (хэш sdv <> хэш rdv)
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
						
						v_exec_sql := v_exec_sql||' select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, 
							   '''||v_valid_dt::text||'''::date - 2 end_dt, 
							   src_stm_id, hash_diff, 
							   (now() at time zone ''utc'') upd_dttm';
						v_exec_sql := v_exec_sql||' from '||v_tbl_nm_rdv||' t where daterange(t.eff_dt,t.end_dt,''[]'')@>'''||v_valid_dt::text||'''::date and exists (select 1 from '||v_tbl_nm_sdv||' s where s.ssn = '||v_ssn::text||' and '||v_sql_field_hash||' <> t.hash_diff and (dml_tp <> ''D'') and '||v_sql_pk_where||v_rule||coalesce(v_sql_fltr_s::text, '')||')';
						if v_load_mode = 3 and v_tbl_nm_rdv = 'rdv_skb_3card.s_ev_entr' then
							v_exec_sql := v_exec_sql||' '||v_sql_fltr_t;
						end if;
						--return v_exec_sql;
						execute v_exec_sql;
						get diagnostics v_cnt = row_count;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '36',
								p_descr := 'Наполняем временную таблицу значениями из rdv, где данные не актуальны (хэш sdv <> хэш rdv)',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
								p_upd_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '36',
									p_descr := 'Наполняем временную таблицу значениями из rdv, где данные не актуальны (хэш sdv <> хэш rdv)',
									p_start_dttm := v_start_dttm,
									p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
							into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Собираем статистику по временной таблице
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '37',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
						execute v_exec_sql;
					exception when others then	
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '37',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Наполняем временную таблицу значениями из sdv, где данные актуальны (хэш sdv <> хэш rdv)
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
						end if;      
						v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
						
						v_exec_sql := v_exec_sql||' select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||'s.'||v_sql_gk_pk||', ';
						end if;
						v_exec_sql := v_exec_sql||v_sql_field_stg_s||', '''||v_valid_dt::text||'''::date - 1 eff_dt, 
							   ''9999-12-31''::date end_dt, 
							   '||p_src_stm_id||', '||v_sql_field_hash_s||' hash_diff, 
							   (now() at time zone ''utc'') upd_dttm ';
						v_exec_sql := v_exec_sql||' from '||v_tbl_nm_sdv||' s join (select ';
						if v_sql_gk_pk is not null then
							   v_exec_sql := v_exec_sql||' '||v_sql_gk_pk||', ';
						end if;
							   v_exec_sql := v_exec_sql||v_sql_field_rdv;
						v_exec_sql := v_exec_sql||', hash_diff from '||v_tbl_nm_rdv||' where daterange(eff_dt,end_dt,''[]'')@>'''||v_valid_dt::text||'''::date) t on '||v_sql_pk_where||' where s.ssn = '||v_ssn::text||' and (dml_tp <> ''D'') and '||v_sql_field_hash_s||' <> t.hash_diff'||v_rule||coalesce(v_sql_fltr_s::text, '');
						if v_load_mode = 3 and v_tbl_nm_rdv = 'rdv_skb_3card.s_ev_entr' then
							v_exec_sql := v_exec_sql||' '||v_sql_fltr_t;
						end if;
						--return v_exec_sql;
						execute v_exec_sql;
						get diagnostics v_cnt = row_count;
						v_upd := v_cnt;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '38',
							p_descr := 'Наполняем временную таблицу значениями из sdv, где данные актуальны (хэш sdv <> хэш rdv)',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_upd_qty := v_cnt::text,
							p_log_tp := '1',
							p_debug_lvl := '2')
							into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '38',
							p_descr := 'Наполняем временную таблицу значениями из sdv, где данные актуальны (хэш sdv <> хэш rdv)',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
					--Собираем статистику по временной таблице
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '39',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
						v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
						execute v_exec_sql;
					exception when others then	
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '39',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
				end if;
		    --Заполняем временную таблицу для сущностей, загружаемых без закрытия старых записей
				if v_load_mode = 4 then
					v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '40',
								p_descr := 'Заполняем временную таблицу для сущностей, загружаемых без закрытия старых записей',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
						v_exec_sql := 'insert into '||v_tbl_nm_rdv||'(';
				else 
					v_start_dttm := clock_timestamp() at time zone 'utc';
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '40',
								p_descr := 'Заполняем временную таблицу для сущностей, загружаемых без закрытия старых записей',
								p_start_dttm := v_start_dttm,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
						v_exec_sql := 'insert into rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||'(';
				end if;
		   --Заполняем временную таблицу новыми записями с источника
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				if v_sql_gk_pk is not null then
						v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
				end if;      
				v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
				v_exec_sql := v_exec_sql||'select ';
				if v_load_mode = 4 and v_is_distinct = 1 then
					v_exec_sql := v_exec_sql||'distinct ';
				end if;
				if v_sql_gk_pk is not null then
						v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
				end if;
				--Если первичная загрузка, то eff_dt = '1900-01-01'
				if v_load_mode = 4 then
					v_exec_sql := v_exec_sql||v_sql_field_stg_s||', ''1900-01-01''::date, ''9999-12-31''::date, '||p_src_stm_id||', '||v_sql_field_hash||', (now() at time zone ''utc'')';
				else
					v_exec_sql := v_exec_sql||v_sql_field_stg_s||', '''||v_valid_dt::text||'''::date - 1, ''9999-12-31''::date, '||p_src_stm_id||', '||v_sql_field_hash||', (now() at time zone ''utc'')';
				end if;
				v_exec_sql := v_exec_sql||' from '||v_tbl_nm_sdv||' s where s.ssn = '||v_ssn::text||' and (dml_tp <> ''D'') '||coalesce(v_sql_fltr_s::text, '')||' and not exists (select 1 from '||v_tbl_nm_rdv||' t where (('''||v_valid_dt::text||'''::date between t.eff_dt and t.end_dt) or (t.eff_dt = ''9999-12-31''::date)) and '||v_sql_pk_where;
				/*if v_load_mode = 3 and  then
					v_exec_sql := v_exec_sql||' '||v_sql_fltr_t||')'||v_rule;
				else*/
					v_exec_sql := v_exec_sql||')'||v_rule;
				--end if;
				
				--return v_exec_sql;                    
				execute v_exec_sql; 
				get diagnostics v_cnt = row_count;
				v_ins := v_cnt;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '41',
					p_descr := 'Заполняем временную таблицу новыми записями с источника',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_ins_qty := v_cnt::text,
					p_log_tp := '1',
					p_debug_lvl := '2')
					into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '41',
					p_descr := 'Заполняем временную таблицу новыми записями с источника',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			--Собираем статистику по временной таблице
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '42',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
				v_exec_sql := 'analyze rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
				execute v_exec_sql;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '42',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
		   --Удаляем из rdv-таблицы, значения, хранящиеся во временной таблице
		    begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				v_exec_sql := 'delete from '||v_tbl_nm_rdv||' t where exists (select 1 from rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||' s where '||v_sql_rdv_pk_s_where;
				if v_load_mode = 3 then
					v_exec_sql := v_exec_sql||' and s.eff_dt = t.eff_dt';
				end if;
				v_exec_sql := v_exec_sql||')';
				if v_load_mode = 3 and v_tbl_nm_rdv = 'rdv_skb_3card.s_ev_entr' then
					v_exec_sql := v_exec_sql||' '||v_sql_fltr_t;
				end if;
				
				--return v_exec_sql;
				execute v_exec_sql;
				get diagnostics v_cnt = row_count;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '43',
					p_descr := 'Удаляем из rdv-таблицы, значения, хранящиеся во временной таблице',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_del_qty := v_cnt::text,
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '43',
					p_descr := 'Удаляем из rdv-таблицы, значения, хранящиеся во временной таблице',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
		    --Вставляем данные в rdv-таблице, хранящиеся во временной таблице
			begin   
				v_start_dttm := clock_timestamp() at time zone 'utc';
				v_exec_sql := 'insert into '||v_tbl_nm_rdv||'(';
				if v_sql_gk_pk is not null then
						v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
				end if;      
				v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm) ';
				v_exec_sql := v_exec_sql||'select ';
				if v_is_distinct = 1 then
					v_exec_sql := v_exec_sql||'distinct ';
				end if;
				if v_sql_gk_pk is not null then
						v_exec_sql := v_exec_sql||v_sql_gk_pk||', ';
				end if;
				v_exec_sql := v_exec_sql||v_sql_field_rdv||', eff_dt, end_dt, src_stm_id, hash_diff, upd_dttm';
				v_exec_sql := v_exec_sql||' from rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv||' where eff_dt<=end_dt';             
				--return v_exec_sql;
				execute v_exec_sql;
				get diagnostics v_cnt = row_count;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '44',
					p_descr := 'Вставляем данные в rdv-таблице, хранящиеся во временной таблице',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_ins_qty := v_ins::text,
					p_upd_qty := v_upd::text,
					p_del_qty := v_del::text,
					p_rej_qty := v_cnt::text,
					p_log_tp := '1',
					p_debug_lvl := '1')
					into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '44',
					p_descr := 'Вставляем данные в rdv-таблице, хранящиеся во временной таблице',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
		    --Собираем статистику по обновленной таблице
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				v_exec_sql := 'analyze '||v_tbl_nm_rdv;
				execute v_exec_sql;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '45',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
					into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '45',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			--Удаляем временную таблицу с изменениями, если она уже есть на БД
				begin 
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_exec_sql := 'drop table if exists rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '46',
						p_descr := 'Удаление временной таблицы',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
					execute v_exec_sql;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '46',
						p_descr := 'Удаление временной таблицы',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;	
	end loop;         
--Возвращаем результат выполнения           
	v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		--Удаляем временную таблицу с изменениями, если она уже есть на БД
		if v_table_name_rdv is not null then
				begin 
					v_exec_sql := 'drop table if exists rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
					execute v_exec_sql;
				end;
		end if;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_chain_2_bdv(text, int8, int8, int8);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_chain_2_bdv(p_scnr_name text, p_step_id int8, p_next_step_id int8, p_chain_is_active int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_chain 
(scnr_name, step_id, next_step_id, chain_is_active, apply_f)
values (p_scnr_name, p_step_id, p_next_step_id, p_chain_is_active, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_chain_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_chain_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_step_id text; --Переменная для номера шага

v_scnr_name text; --Переменная для имени сценария

v_next_step_id text; --Переменная для номера следующего шага

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select scnr_name, step_id, next_step_id, chain_is_active,

md5(coalesce(chain_is_active, '-1')::text)::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_chain

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_chain

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_chain s 

where s.scnr_name = v_rec_s2t.scnr_name and s.step_id = v_rec_s2t.step_id and coalesce(s.next_step_id, -1) = coalesce(v_rec_s2t.next_step_id, -1)

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_chain 

if v_cnt > 0 then 

select count(1), scnr_name, step_id, next_step_id,

md5(coalesce(chain_is_active, '-1')::text)::text as hash_diff

into v_cnt, v_scnr_name, v_step_id, v_next_step_id, v_hash_diff

from sys_dwh.prm_s2t_bdv_chain s 

where s.scnr_name = v_rec_s2t.scnr_name and s.step_id = v_rec_s2t.step_id and coalesce(s.next_step_id, -1) = coalesce(v_rec_s2t.next_step_id, -1)

and now() between eff_dt and end_dt

group by scnr_name, step_id, next_step_id,

md5(coalesce(chain_is_active, '-1')::text)::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_chain запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_chain

(scnr_name, step_id, next_step_id, chain_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_name, v_rec_s2t.step_id, v_rec_s2t.next_step_id, v_rec_s2t.chain_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_chain s set apply_f = 1

where s.scnr_name = v_rec_s2t.scnr_name and s.step_id = v_rec_s2t.step_id and coalesce(s.next_step_id, -1) = coalesce(v_rec_s2t.next_step_id, -1);

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_chain, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_chain

update sys_dwh.prm_s2t_bdv_chain s set end_dt = current_date-1 

where s.scnr_name = v_rec_s2t.scnr_name and s.step_id = v_rec_s2t.step_id and coalesce(s.next_step_id, -1) = coalesce(v_rec_s2t.next_step_id, -1)

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_chain

(scnr_name, step_id, next_step_id, chain_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_name, v_rec_s2t.step_id, v_rec_s2t.next_step_id, v_rec_s2t.chain_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_chain s set apply_f = 1

where s.scnr_name = v_rec_s2t.scnr_name and s.step_id = v_rec_s2t.step_id and coalesce(s.next_step_id, -1) = coalesce(v_rec_s2t.next_step_id, -1);

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_scnr_name='||v_rec_s2t.scnr_name||', v_step_id='||v_rec_s2t.step_id,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_scnr_name='||v_rec_s2t.scnr_name||', v_step_id='||v_rec_s2t.step_id,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_chain

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_chain;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_chain

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_chain p set end_dt = now()::date-1

where 1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_chain s

where s.scnr_name = p.scnr_name and s.step_id = p.step_id and coalesce(s.next_step_id, -1) = coalesce(p.next_step_id, -1));

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_chain is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_chain where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_chain',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_chain',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_obj_2_bdv(text, text, int8);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_obj_2_bdv(p_object_name text, p_object_descr text, p_object_is_active int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_obj 
(object_name, object_descr, object_is_active, apply_f)
values (p_object_name, p_object_descr, p_object_is_active, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_obj_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_obj_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_object_name text; --Переменная для имени объекта

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select object_name, object_descr, object_is_active,

md5(coalesce(object_descr, '-1')||

coalesce(object_is_active, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_obj

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_obj

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_obj s 

where s.object_name = v_rec_s2t.object_name

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_obj 

if v_cnt > 0 then 

select count(1), object_name,

md5(coalesce(object_descr, '-1')||

coalesce(object_is_active, '-1'))::text as hash_diff

into v_cnt, v_object_name, v_hash_diff

from sys_dwh.prm_s2t_bdv_obj s 

where s.object_name = v_rec_s2t.object_name

and now() between eff_dt and end_dt

group by object_name,

md5(coalesce(object_descr, '-1')||

coalesce(object_is_active, '-1'))::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_obj запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_obj

(object_name, object_descr, object_is_active, eff_dt, end_dt)

values(v_rec_s2t.object_name, v_rec_s2t.object_descr, v_rec_s2t.object_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_obj s set apply_f = 1

where s.object_name = v_rec_s2t.object_name;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_obj, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_obj

update sys_dwh.prm_s2t_bdv_obj s set end_dt = current_date-1 

where s.object_name = v_rec_s2t.object_name

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_obj

(object_name, object_descr, object_is_active, eff_dt, end_dt)

values(v_rec_s2t.object_name, v_rec_s2t.object_descr, v_rec_s2t.object_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_obj s set apply_f = 1

where s.object_name = v_rec_s2t.object_name;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_object_name='||v_rec_s2t.object_name,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_object_name='||v_rec_s2t.object_name,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_obj

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_obj;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_obj

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_obj p set end_dt = now()::date-1

where 1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_obj s

where s.object_name = p.object_name);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_obj is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_obj where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_obj',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_obj',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_2_bdv(text, text, int8);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_2_bdv(p_scnr_name text, p_scnr_descr text, p_scnr_is_active int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_scnr 
(scnr_name, scnr_descr, scnr_is_active, apply_f)
values (p_scnr_name, p_scnr_descr, p_scnr_is_active, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_scnr_name text; --Переменная для имени сценария

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select scnr_name, scnr_descr, scnr_is_active,

md5(coalesce(scnr_descr, '-1')||

coalesce(scnr_is_active, '-1')::text)::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_scnr

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_scnr

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_scnr s 

where s.scnr_name = v_rec_s2t.scnr_name

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr 

if v_cnt > 0 then 

select count(1), scnr_name,

md5(coalesce(scnr_descr, '-1')||

coalesce(scnr_is_active, '-1')::text)::text as hash_diff

into v_cnt, v_scnr_name, v_hash_diff

from sys_dwh.prm_s2t_bdv_scnr s 

where s.scnr_name = v_rec_s2t.scnr_name

and now() between eff_dt and end_dt

group by scnr_name,

md5(coalesce(scnr_descr, '-1')||

coalesce(scnr_is_active, '-1')::text)::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_scnr запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_scnr

(scnr_name, scnr_descr, scnr_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_name, v_rec_s2t.scnr_descr, v_rec_s2t.scnr_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr s set apply_f = 1

where s.scnr_name = v_rec_s2t.scnr_name;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_scnr

update sys_dwh.prm_s2t_bdv_scnr s set end_dt = current_date-1 

where s.scnr_name = v_rec_s2t.scnr_name

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_scnr

(scnr_name, scnr_descr, scnr_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_name, v_rec_s2t.scnr_descr, v_rec_s2t.scnr_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr s set apply_f = 1

where s.scnr_name = v_rec_s2t.scnr_name;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_scnr_name='||v_rec_s2t.scnr_name,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_scnr_name='||v_rec_s2t.scnr_name,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_scnr

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_scnr;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_scnr

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_scnr p set end_dt = now()::date-1

where 1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_scnr s

where s.scnr_name = p.scnr_name);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_scnr is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_scnr where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_line_2_bdv(text, text, text, int8);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_line_2_bdv(p_scnr_line_name text, p_scnr_line_descr text, p_scnr_line_owner text, p_scnr_line_is_active int8)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_scnr_line 
(scnr_line_name, scnr_line_descr, scnr_line_owner, scnr_line_is_active, apply_f)
values (p_scnr_line_name, p_scnr_line_descr, p_scnr_line_owner, p_scnr_line_is_active, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_line_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_line_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_scnr_line_name text; --Переменная для имени линии сценария

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select scnr_line_name, scnr_line_descr, scnr_line_owner, scnr_line_is_active,

md5(coalesce(scnr_line_descr, '-1')||

coalesce(scnr_line_owner, '-1')||

coalesce(scnr_line_is_active, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_scnr_line

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_scnr_line

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_scnr_line s 

where s.scnr_line_name = v_rec_s2t.scnr_line_name

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr_line 

if v_cnt > 0 then 

select count(1), scnr_line_name,

md5(coalesce(scnr_line_descr, '-1')||

coalesce(scnr_line_owner, '-1')||

coalesce(scnr_line_is_active, '-1'))::text as hash_diff

into v_cnt, v_scnr_line_name, v_hash_diff

from sys_dwh.prm_s2t_bdv_scnr_line s 

where s.scnr_line_name = v_rec_s2t.scnr_line_name

and now() between eff_dt and end_dt

group by scnr_line_name,

md5(coalesce(scnr_line_descr, '-1')||

coalesce(scnr_line_owner, '-1')||

coalesce(scnr_line_is_active, '-1'))::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_scnr_line запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_scnr_line

(scnr_line_name, scnr_line_descr, scnr_line_owner, scnr_line_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_line_name, v_rec_s2t.scnr_line_descr, v_rec_s2t.scnr_line_owner, v_rec_s2t.scnr_line_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr_line s set apply_f = 1

where s.scnr_line_name = v_rec_s2t.scnr_line_name;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr_line, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_scnr_line

update sys_dwh.prm_s2t_bdv_scnr_line s set end_dt = current_date-1 

where s.scnr_line_name = v_rec_s2t.scnr_line_name

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_scnr_line

(scnr_line_name, scnr_line_descr, scnr_line_owner, scnr_line_is_active, eff_dt, end_dt)

values(v_rec_s2t.scnr_line_name, v_rec_s2t.scnr_line_descr, v_rec_s2t.scnr_line_owner, v_rec_s2t.scnr_line_is_active, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr_line s set apply_f = 1

where s.scnr_line_name = v_rec_s2t.scnr_line_name;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_scnr_line_name='||v_rec_s2t.scnr_line_name,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_scnr_line_name='||v_rec_s2t.scnr_line_name,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_scnr_line

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_scnr_line;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_scnr_line

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_scnr_line p set end_dt = now()::date-1

where 1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_scnr_line s

where 1=1

and s.scnr_line_name = p.scnr_line_name);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_scnr_line is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_scnr_line where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr_line',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr_line',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_line_scnr_2_bdv(text, text, date, date);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_line_scnr_2_bdv(p_scnr_line_name text, p_scnr_name text, p_scnr_start_dt date, p_scnr_stop_dt date)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr 
(scnr_line_name, scnr_name, scnr_start_dt, scnr_stop_dt, apply_f)
values (p_scnr_line_name, p_scnr_name, p_scnr_start_dt, p_scnr_stop_dt, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_scnr_line_scnr_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_scnr_line_scnr_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_scnr_line_name text; --Переменная для имени линии сценария

v_scnr_name text; --Переменная для имени сценария

v_scnr_start_dt date; --Переменная для даты начала

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select scnr_line_name, scnr_name, scnr_start_dt, scnr_stop_dt,

md5(coalesce(scnr_stop_dt::text, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_scnr_line_scnr

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_scnr_line_scnr s 

where s.scnr_line_name = v_rec_s2t.scnr_line_name and s.scnr_name = v_rec_s2t.scnr_name and s.scnr_start_dt = v_rec_s2t.scnr_start_dt

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr_line_scnr 

if v_cnt > 0 then 

select count(1), scnr_line_name, scnr_name, scnr_start_dt,

md5(coalesce(scnr_stop_dt::text, '-1'))::text as hash_diff

into v_cnt, v_scnr_line_name, v_scnr_name, v_scnr_start_dt, v_hash_diff

from sys_dwh.prm_s2t_bdv_scnr_line_scnr s 

where s.scnr_line_name = v_rec_s2t.scnr_line_name and s.scnr_name = v_rec_s2t.scnr_name and s.scnr_start_dt = v_rec_s2t.scnr_start_dt

and now() between eff_dt and end_dt

group by scnr_line_name, scnr_name, scnr_start_dt,

md5(coalesce(scnr_stop_dt::text, '-1'))::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_scnr_line_scnr запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_scnr_line_scnr

(scnr_line_name, scnr_name, scnr_start_dt, scnr_stop_dt, eff_dt, end_dt)

values(v_rec_s2t.scnr_line_name, v_rec_s2t.scnr_name, v_rec_s2t.scnr_start_dt, v_rec_s2t.scnr_stop_dt, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr s set apply_f = 1

where s.scnr_line_name = v_rec_s2t.scnr_line_name and s.scnr_name = v_rec_s2t.scnr_name and s.scnr_start_dt = v_rec_s2t.scnr_start_dt;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_scnr_line_scnr, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_scnr_line_scnr

update sys_dwh.prm_s2t_bdv_scnr_line_scnr s set end_dt = current_date-1 

where s.scnr_line_name = v_rec_s2t.scnr_line_name and s.scnr_name = v_rec_s2t.scnr_name and s.scnr_start_dt = v_rec_s2t.scnr_start_dt

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_scnr_line_scnr

(scnr_line_name, scnr_name, scnr_start_dt, scnr_stop_dt, eff_dt, end_dt)

values(v_rec_s2t.scnr_line_name, v_rec_s2t.scnr_name, v_rec_s2t.scnr_start_dt, v_rec_s2t.scnr_stop_dt, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr s set apply_f = 1

where s.scnr_line_name = v_rec_s2t.scnr_line_name and s.scnr_name = v_rec_s2t.scnr_name and s.scnr_start_dt = v_rec_s2t.scnr_start_dt;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_scnr_line_name='||v_rec_s2t.scnr_line_name||', v_scnr_name'||v_rec_s2t.scnr_name,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_scnr_line_name='||v_rec_s2t.scnr_line_name||', v_scnr_name'||v_rec_s2t.scnr_name,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_scnr_line_scnr

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_scnr_line_scnr p set end_dt = now()::date-1

where 1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr s

where s.scnr_line_name = p.scnr_line_name and s.scnr_name = p.scnr_name and s.scnr_start_dt = p.scnr_start_dt);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_scnr_line_scnr is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_scnr_line_scnr where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr_line_scnr',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_scnr_line_scnr',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_step_2_bdv(varchar, varchar, int8, int8, text, text, text, int8, varchar, varchar, text, varchar, text, varchar, int8, varchar, text, text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_step_2_bdv(p_src_nm varchar, p_table_name_bdv varchar, p_step_order_in_object int8, p_step_id int8, p_step_name text, p_step_algorithm text, p_object_name text, p_step_is_active int8, p_schema_tgt varchar, p_table_name_tgt varchar, p_table_comment_tgt text, p_column_name_tgt varchar, p_column_comment_tgt text, p_datatype_tgt varchar, p_distribution_by_tgt int8, p_schema_src varchar, p_column_algorithm_src text, p_main_table_src text, p_table_algorithm_src text, p_where_src text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_start_dttm text;
v_json_ret text := '';
begin
--Вставляем заданные значения
begin
v_start_dttm := timeofday()::timestamp;
insert into stg_sys_dwh.prm_s2t_bdv_step 
(src_nm, table_name_bdv, step_order_in_object, step_id, step_name, step_algorithm, object_name, step_is_active, schema_tgt, table_name_tgt, table_comment_tgt, column_name_tgt, column_comment_tgt, datatype_tgt, distribution_by_tgt, schema_src, column_algorithm_src, main_table_src, table_algorithm_src, where_src, apply_f)
values (p_src_nm, p_table_name_bdv, p_step_order_in_object, p_step_id, p_step_name, p_step_algorithm, p_object_name, p_step_is_active, p_schema_tgt, p_table_name_tgt, p_table_comment_tgt, p_column_name_tgt, p_column_comment_tgt, p_datatype_tgt, p_distribution_by_tgt, p_schema_src, p_column_algorithm_src, p_main_table_src, p_table_algorithm_src, p_where_src, 0);
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Вставка заданных значений',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Оповещаем об окончании транзакции
return(v_json_ret||']');
--Выводим возникшие ошибки
exception
when others then
raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_bdv_step_2_dwh();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_bdv_step_2_dwh()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_src_nm text; --Переменная для записи источника

v_step_id text; --Переменная для номера шага

v_table_name_tgt text; --Переменная для записи имени таблицы

v_column_name_tgt text; --Переменная для записи имени таблицы

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

begin

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select src_nm, table_name_bdv, step_order_in_object, step_id, step_name, step_algorithm, object_name, step_is_active, schema_tgt, table_name_tgt, table_comment_tgt, column_name_tgt, column_comment_tgt, datatype_tgt, distribution_by_tgt, schema_src, column_algorithm_src, main_table_src, table_algorithm_src, where_src,

md5(coalesce(table_name_bdv, '-1')||

coalesce(step_order_in_object, '-1')||

coalesce(step_name, '-1')||

coalesce(step_algorithm, '-1')||

coalesce(object_name, '-1')||

coalesce(step_is_active, '-1')||

coalesce(schema_tgt, '-1')||

coalesce(table_comment_tgt, '-1')||

coalesce(column_comment_tgt, '-1')||

coalesce(datatype_tgt, '-1')||

coalesce(distribution_by_tgt, '-1')||

coalesce(schema_src, '-1')||

coalesce(column_algorithm_src, '-1')||

coalesce(main_table_src, '-1')||

coalesce(table_algorithm_src, '-1')||

coalesce(where_src, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_bdv_step

where apply_f = 0

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_bdv_step

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(1)

into v_cnt

from sys_dwh.prm_s2t_bdv_step s 

where coalesce(s.step_id, '-1') = coalesce(v_rec_s2t.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(v_rec_s2t.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(v_rec_s2t.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(v_rec_s2t.src_nm, '')

and now() between eff_dt and end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_bdv_step 

if v_cnt > 0 then 

select count(1), src_nm, step_id, table_name_tgt, column_name_tgt,

md5(coalesce(table_name_bdv, '-1')||

coalesce(step_order_in_object, '-1')||

coalesce(step_name, '-1')||

coalesce(step_algorithm, '-1')||

coalesce(object_name, '-1')||

coalesce(step_is_active, '-1')||

coalesce(schema_tgt, '-1')||

coalesce(table_comment_tgt, '-1')||

coalesce(column_comment_tgt, '-1')||

coalesce(datatype_tgt, '-1')||

coalesce(distribution_by_tgt, '-1')||

coalesce(schema_src, '-1')||

coalesce(column_algorithm_src, '-1')||

coalesce(main_table_src, '-1')||

coalesce(table_algorithm_src, '-1')||

coalesce(where_src, '-1'))::text as hash_diff

into v_cnt, v_src_nm, v_step_id, v_table_name_tgt, v_column_name_tgt, v_hash_diff

from sys_dwh.prm_s2t_bdv_step s 

where coalesce(s.step_id, '-1') = coalesce(v_rec_s2t.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(v_rec_s2t.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(v_rec_s2t.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(v_rec_s2t.src_nm, '')

and now() between eff_dt and end_dt

group by src_nm, step_id, table_name_tgt, column_name_tgt,

md5(coalesce(table_name_bdv, '-1')||

coalesce(step_order_in_object, '-1')||

coalesce(step_name, '-1')||

coalesce(step_algorithm, '-1')||

coalesce(object_name, '-1')||

coalesce(step_is_active, '-1')||

coalesce(schema_tgt, '-1')||

coalesce(table_comment_tgt, '-1')||

coalesce(column_comment_tgt, '-1')||

coalesce(datatype_tgt, '-1')||

coalesce(distribution_by_tgt, '-1')||

coalesce(schema_src, '-1')||

coalesce(column_algorithm_src, '-1')||

coalesce(main_table_src, '-1')||

coalesce(table_algorithm_src, '-1')||

coalesce(where_src, '-1'))::text;

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_bdv_step запись

if v_cnt = 0 then

insert into sys_dwh.prm_s2t_bdv_step

(src_nm, table_name_bdv, step_order_in_object, step_id, step_name, step_algorithm, object_name, step_is_active, schema_tgt, table_name_tgt, table_comment_tgt, column_name_tgt, column_comment_tgt, datatype_tgt, distribution_by_tgt, schema_src, column_algorithm_src, main_table_src, table_algorithm_src, where_src, eff_dt, end_dt)

values(v_rec_s2t.src_nm, v_rec_s2t.table_name_bdv, v_rec_s2t.step_order_in_object, v_rec_s2t.step_id, v_rec_s2t.step_name, v_rec_s2t.step_algorithm, v_rec_s2t.object_name, v_rec_s2t.step_is_active, v_rec_s2t.schema_tgt, v_rec_s2t.table_name_tgt, v_rec_s2t.table_comment_tgt, v_rec_s2t.column_name_tgt, v_rec_s2t.column_comment_tgt, v_rec_s2t.datatype_tgt, v_rec_s2t.distribution_by_tgt, v_rec_s2t.schema_src, v_rec_s2t.column_algorithm_src, v_rec_s2t.main_table_src, v_rec_s2t.table_algorithm_src, v_rec_s2t.where_src, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_step s set apply_f = 1

where coalesce(s.step_id, '-1') = coalesce(v_rec_s2t.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(v_rec_s2t.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(v_rec_s2t.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(v_rec_s2t.src_nm, '');

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_bdv_step, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_bdv_step

update sys_dwh.prm_s2t_bdv_step s set end_dt = current_date-1 

where coalesce(s.step_id, '-1') = coalesce(v_rec_s2t.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(v_rec_s2t.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(v_rec_s2t.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(v_rec_s2t.src_nm, '')

and now() between eff_dt and end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_bdv_step

(src_nm, table_name_bdv, step_order_in_object, step_id, step_name, step_algorithm, object_name, step_is_active, schema_tgt, table_name_tgt, table_comment_tgt, column_name_tgt, column_comment_tgt, datatype_tgt, distribution_by_tgt, schema_src, column_algorithm_src, main_table_src, table_algorithm_src, where_src, eff_dt, end_dt)

values(v_rec_s2t.src_nm, v_rec_s2t.table_name_bdv, v_rec_s2t.step_order_in_object, v_rec_s2t.step_id, v_rec_s2t.step_name, v_rec_s2t.step_algorithm, v_rec_s2t.object_name, v_rec_s2t.step_is_active, v_rec_s2t.schema_tgt, v_rec_s2t.table_name_tgt, v_rec_s2t.table_comment_tgt, v_rec_s2t.column_name_tgt, v_rec_s2t.column_comment_tgt, v_rec_s2t.datatype_tgt, v_rec_s2t.distribution_by_tgt, v_rec_s2t.schema_src, v_rec_s2t.column_algorithm_src, v_rec_s2t.main_table_src, v_rec_s2t.table_algorithm_src, v_rec_s2t.where_src, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_bdv_step s set apply_f = 1

where coalesce(s.step_id, '-1') = coalesce(v_rec_s2t.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(v_rec_s2t.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(v_rec_s2t.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(v_rec_s2t.src_nm, '');

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_table_name_bdv='||v_rec_s2t.table_name_bdv||' v_column_name_tgt='||v_rec_s2t.column_name_tgt,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_table_name_bdv='||v_rec_s2t.table_name_bdv||' v_column_name_tgt='||v_rec_s2t.column_name_tgt,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_bdv_step

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_bdv_step;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_bdv_step

if v_cnt > 0 then

update sys_dwh.prm_s2t_bdv_step p set end_dt = now()::date-1

where 

1=1

and now() between eff_dt and end_dt

and not exists (select 1 from stg_sys_dwh.prm_s2t_bdv_step s

where 1=1

and coalesce(s.step_id, '-1') = coalesce(p.step_id, '-1') and coalesce(s.table_name_tgt, '') = coalesce(p.table_name_tgt, '') and coalesce(s.column_name_tgt, '') = coalesce(p.column_name_tgt, '') and coalesce(s.src_nm, '') = coalesce(p.src_nm, ''));

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_bdv_step is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_bdv_step where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_step',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_bdv_step',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_rdv_2_dwh(text);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_rdv_2_dwh(p_tbl_stg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_src_stm_id int; --Переменная для записи src_stm_id таблицы

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_schema_stg text; --Переменная для записи схемы таблицы

v_table_name_stg text; --Переменная для записи схемы таблицы

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

v_user text; --Перемнная для фиксирования пользователя

begin

--Проверяем источники в таблице stg_sys_dwh.prm_s2t_rdv

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(*)

into v_cnt

from (select distinct schema_stg

from stg_sys_dwh.prm_s2t_rdv) s;

--Логируем ошибку о большом количестве источников

if v_cnt > 1 then 

raise exception '%', 'Too many sources';

end if;

--Фиксируем источники в таблице stg_sys_dwh.prm_s2t_rdv

select schema_stg

into v_schema_stg

from stg_sys_dwh.prm_s2t_rdv

group by schema_stg;

--Логируем ошибку о пустом поле с источником

if v_schema_stg is null then 

raise exception '%', 'Source is null';

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_rdv',

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_rdv',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select schema_stg, table_name_stg, table_comment_stg, column_name_stg, column_comment_stg, datatype_stg, algorithm, schema_rdv, table_name_rdv, column_name_rdv, column_comment_rdv, datatype_rdv, key_type_src, ref_to_hub, ref_to_stg, datatype_stg_transform, table_comment_rdv, upd_user,

md5(coalesce(schema_rdv,'-1')||

coalesce(table_comment_stg,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(algorithm,'-1')||

coalesce(column_comment_rdv,'-1')||

coalesce(datatype_rdv,'-1')||

coalesce(key_type_src,'-1')||

coalesce(ref_to_hub,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg_transform, '-1')||

coalesce(table_comment_rdv, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_rdv

where apply_f = 0

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), table_name_stg) = table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), schema_stg) = schema_stg

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_stg

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(*)

into v_cnt

from sys_dwh.prm_s2t_rdv s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg and s.table_name_rdv = v_rec_s2t.table_name_rdv and s.column_name_rdv = v_rec_s2t.column_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_rdv 

if v_cnt > 0 then 

select count(*), src_stm_id, 

md5(coalesce(schema_rdv,'-1')||

coalesce(table_comment_stg,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(algorithm,'-1')||

coalesce(column_comment_rdv,'-1')||

coalesce(datatype_rdv,'-1')||

coalesce(key_type_src,'-1')||

coalesce(ref_to_hub,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg_transform, '-1')||

coalesce(table_comment_rdv, '-1'))::text as hash_diff

into v_cnt, v_src_stm_id, v_hash_diff 

from sys_dwh.prm_s2t_rdv s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg and s.table_name_rdv = v_rec_s2t.table_name_rdv and s.column_name_rdv = v_rec_s2t.column_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

group by src_stm_id, md5(coalesce(schema_rdv,'-1')||

coalesce(table_comment_stg,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(algorithm,'-1')||

coalesce(column_comment_rdv,'-1')||

coalesce(datatype_rdv,'-1')||

coalesce(key_type_src,'-1')||

coalesce(ref_to_hub,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg_transform, '-1')||

coalesce(table_comment_rdv, '-1'))::text;

end if;

--Фиксируем src_stm_id для записи, отсутствующей в sys_dwh.prm_s2t_rdv

if v_cnt = 0 then

select s.src_stm_id into v_src_stm_id from sys_dwh.prm_src_stm s where s.nm = v_rec_s2t.table_name_stg and v_rec_s2t.schema_stg = (select 'stg_'||p.nm from sys_dwh.prm_src_stm p where s.prn_src_stm_id = p.src_stm_id);

--Уведомляем, если sys_dwh.prm_src_stm не заполнена для записи, отсутствующей в sys_dwh.prm_s2t_rdv

if v_src_stm_id is null then

raise exception '%', 'The src_stm_id in sys_dwh.prm_src_stm is empty';

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_rdv запись

insert into sys_dwh.prm_s2t_rdv

(schema_stg, table_name_stg, table_comment_stg, column_name_stg, column_comment_stg, datatype_stg, algorithm, schema_rdv, table_name_rdv, column_name_rdv, column_comment_rdv, datatype_rdv, key_type_src, ref_to_hub, ref_to_stg, src_stm_id, eff_dt, end_dt, datatype_stg_transform, table_comment_rdv, upd_user)

values(v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.table_comment_stg, v_rec_s2t.column_name_stg, v_rec_s2t.column_comment_stg, v_rec_s2t.datatype_stg, v_rec_s2t.algorithm, v_rec_s2t.schema_rdv, v_rec_s2t.table_name_rdv, v_rec_s2t.column_name_rdv, v_rec_s2t.column_comment_rdv, v_rec_s2t.datatype_rdv, v_rec_s2t.key_type_src, v_rec_s2t.ref_to_hub, v_rec_s2t.ref_to_stg, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt, v_rec_s2t.datatype_stg_transform, v_rec_s2t.table_comment_rdv, v_rec_s2t.upd_user);

update stg_sys_dwh.prm_s2t_rdv s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg and s.table_name_rdv = v_rec_s2t.table_name_rdv and s.column_name_rdv = v_rec_s2t.column_name_rdv;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_rdv, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_rdv

update sys_dwh.prm_s2t_rdv s set end_dt = current_date-1 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg and s.table_name_rdv = v_rec_s2t.table_name_rdv and s.column_name_rdv = v_rec_s2t.column_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_rdv

(schema_stg, table_name_stg, table_comment_stg, column_name_stg, column_comment_stg, datatype_stg, algorithm, schema_rdv, table_name_rdv, column_name_rdv, column_comment_rdv, datatype_rdv, key_type_src, ref_to_hub, ref_to_stg, src_stm_id, eff_dt, end_dt, datatype_stg_transform, table_comment_rdv, upd_user)

values(v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.table_comment_stg, v_rec_s2t.column_name_stg, v_rec_s2t.column_comment_stg, v_rec_s2t.datatype_stg, v_rec_s2t.algorithm, v_rec_s2t.schema_rdv, v_rec_s2t.table_name_rdv, v_rec_s2t.column_name_rdv, v_rec_s2t.column_comment_rdv, v_rec_s2t.datatype_rdv, v_rec_s2t.key_type_src, v_rec_s2t.ref_to_hub, v_rec_s2t.ref_to_stg, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt, v_rec_s2t.datatype_stg_transform, v_rec_s2t.table_comment_rdv, v_rec_s2t.upd_user);

update stg_sys_dwh.prm_s2t_rdv s set apply_f = 1

where s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg and s.table_name_rdv = v_rec_s2t.table_name_rdv and s.column_name_rdv = v_rec_s2t.column_name_rdv;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_ssn='||v_src_stm_id||' v_column='||v_rec_s2t.column_name_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_ssn='||v_src_stm_id||' v_column='||v_rec_s2t.column_name_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_stg

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_rdv;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_rdv

if v_cnt > 0 then

select max(upd_user) into v_user from stg_sys_dwh.prm_s2t_rdv;

update sys_dwh.prm_s2t_rdv p set end_dt = now()::date-1

where 

p.schema_stg = v_schema_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), p.table_name_stg) = p.table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), p.schema_stg) = p.schema_stg

and not exists (select 1 from stg_sys_dwh.prm_s2t_rdv s

where s.schema_stg = v_schema_stg

and s.schema_stg = p.schema_stg and s.table_name_stg = p.table_name_stg and s.column_name_stg =p.column_name_stg and s.table_name_rdv = p.table_name_rdv  and s.column_name_rdv = p.column_name_rdv);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_rdv is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_rdv where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_rdv',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_val := 'v_schema_stg='||v_schema_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_rdv',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_schema_stg='||v_schema_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_rdv_2_rdv(varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_rdv_2_rdv(p_schema_stg varchar, p_table_name_stg varchar, p_table_comment_stg varchar, p_column_name_stg varchar, p_column_comment_stg varchar, p_datatype_stg varchar, p_algorithm varchar, p_schema_rdv varchar, p_table_name_rdv varchar, p_column_name_rdv varchar, p_column_comment_rdv varchar, p_datatype_rdv varchar, p_key_type_src varchar, p_ref_to_hub varchar, p_ref_to_stg varchar, p_datatype_stg_transform varchar DEFAULT NULL::character varying)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
		 	 	 	 	   

declare

v_start_dttm text;

v_json_ret text := '';

begin

--Вставляем заданные значения

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';	

INSERT INTO stg_sys_dwh.prm_s2t_rdv

(schema_stg, table_name_stg, table_comment_stg, column_name_stg, column_comment_stg, datatype_stg, algorithm, schema_rdv, table_name_rdv, column_name_rdv, column_comment_rdv, datatype_rdv, key_type_src, ref_to_hub, ref_to_stg, datatype_stg_transform, apply_f)

VALUES(p_schema_stg, p_table_name_stg, p_table_comment_stg, p_column_name_stg, p_column_comment_stg, p_datatype_stg, p_algorithm, p_schema_rdv, p_table_name_rdv, p_column_name_rdv, p_column_comment_rdv, p_datatype_rdv, p_key_type_src, p_ref_to_hub, p_ref_to_stg, p_datatype_stg_transform, 0);

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Оповещаем об окончании транзакции

return(v_json_ret||']');

--Выводим возникшие ошибки

exception

when others then

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_rdv_rule_2_dwh(text);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_rdv_rule_2_dwh(p_tbl_stg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_src_stm_id int; --Переменная для записи src_stm_id таблицы

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_schema_stg text; --Переменная для записи схемы таблицы

v_table_name_stg text; --Переменная для записи схемы таблицы

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

v_user text; --Перемнная для фиксирования пользователя

begin

--Проверяем источники в таблице stg_sys_dwh.prm_s2t_rdv_rule

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(*)

into v_cnt

from (select distinct schema_stg

from stg_sys_dwh.prm_s2t_rdv_rule) s;

--Логируем ошибку о большом количестве источников

if v_cnt > 1 then 

raise exception '%', 'Too many sources';

end if;

--Фиксируем источники в таблице stg_sys_dwh.prm_s2t_rdv_rule

select schema_stg

into v_schema_stg

from stg_sys_dwh.prm_s2t_rdv_rule

group by schema_stg;

--Логируем ошибку о пустом поле с источником

if v_schema_stg is null then 

raise exception '%', 'Source is null';

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (select schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, is_distinct, upd_user, stm_ext_upload,

md5(coalesce(where_json,'-1')||

coalesce(is_distinct, '-1')||

coalesce(stm_ext_upload, '-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_rdv_rule

where apply_f = 0

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), table_name_stg) = table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), schema_stg) = schema_stg

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_rdv_rule

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(*)

into v_cnt

from sys_dwh.prm_s2t_rdv_rule s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_rdv = v_rec_s2t.table_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_rdv_rule

if v_cnt > 0 then 

select count(*), src_stm_id, 

md5(coalesce(where_json,'-1')||

coalesce(is_distinct, '-1')||

coalesce(stm_ext_upload, '-1'))::text as hash_diff

into v_cnt, v_src_stm_id, v_hash_diff 

from sys_dwh.prm_s2t_rdv_rule s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_rdv = v_rec_s2t.table_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

group by src_stm_id, md5(coalesce(where_json,'-1')||

coalesce(is_distinct, '-1')||

coalesce(stm_ext_upload, '-1'))::text;

end if;

--Фиксируем src_stm_id для записи, отсутствующей в sys_dwh.prm_s2t_rdv_rule

if v_cnt = 0 then

select s.src_stm_id into v_src_stm_id from sys_dwh.prm_src_stm s where s.nm = v_rec_s2t.table_name_stg and v_rec_s2t.schema_stg = (select 'stg_'||p.nm from sys_dwh.prm_src_stm p where s.prn_src_stm_id = p.src_stm_id);

--Уведомляем, если sys_dwh.prm_src_stm не заполнена для записи, отсутствующей в sys_dwh.prm_s2t_rdv

if v_src_stm_id is null then

raise exception '%', 'The src_stm_id in sys_dwh.prm_src_stm is empty';

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_rdv_rule запись

insert into sys_dwh.prm_s2t_rdv_rule

(schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, is_distinct, src_stm_id, eff_dt, end_dt, upd_user, stm_ext_upload)

values (v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.schema_rdv, v_rec_s2t.table_name_rdv, v_rec_s2t.where_json, v_rec_s2t.is_distinct, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt, v_rec_s2t.upd_user, v_rec_s2t.stm_ext_upload);

update stg_sys_dwh.prm_s2t_rdv_rule s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_rdv = v_rec_s2t.table_name_rdv;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_rdv_rule, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_rdv_rule

update sys_dwh.prm_s2t_rdv_rule s set end_dt = current_date-1 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_rdv = v_rec_s2t.table_name_rdv

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_rdv_rule

(schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, is_distinct, src_stm_id, eff_dt, end_dt, upd_user, stm_ext_upload)

values (v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.schema_rdv, v_rec_s2t.table_name_rdv, v_rec_s2t.where_json, v_rec_s2t.is_distinct, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt, v_rec_s2t.upd_user, v_rec_s2t.stm_ext_upload);

update stg_sys_dwh.prm_s2t_rdv_rule s set apply_f = 1

where s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_stg = v_rec_s2t.table_name_stg and s.schema_rdv = v_rec_s2t.schema_rdv and s.table_name_rdv = v_rec_s2t.table_name_rdv;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_ssn='||v_src_stm_id||' v_table_name_stg='||v_rec_s2t.table_name_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_ssn='||v_src_stm_id||' v_table_name_stg='||v_rec_s2t.table_name_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_rdv_rule

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_rdv_rule;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_rdv_rule

if v_cnt > 0 then

select max(upd_user) into v_user from stg_sys_dwh.prm_s2t_rdv_rule;

update sys_dwh.prm_s2t_rdv_rule p set end_dt = now()::date-1, upd_user = v_user

where 

p.schema_stg = v_schema_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), p.table_name_stg) = p.table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), p.schema_stg) = p.schema_stg

and not exists (select 1 from stg_sys_dwh.prm_s2t_rdv_rule s

where s.schema_stg = v_schema_stg

and s.schema_stg = p.schema_stg and s.table_name_stg = p.table_name_stg and s.schema_rdv = p.schema_rdv and s.table_name_rdv = p.table_name_rdv);

get diagnostics v_cnt_del = row_count;

else raise exception '%', 'The table stg_sys_dwh.prm_s2t_rdv_rule is empty';

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_rdv_rule where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_val := 'v_schema_stg='||v_schema_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_schema_stg='||v_schema_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_rdv_rule_2_rdv(varchar, varchar, varchar, varchar, varchar, int4);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_rdv_rule_2_rdv(p_schema_stg varchar, p_table_name_stg varchar, p_schema_rdv varchar, p_table_name_rdv varchar, p_where_json varchar, p_is_distinct int4 DEFAULT 0)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

begin

--Вставляем заданные значения

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

INSERT INTO stg_sys_dwh.prm_s2t_rdv_rule

(schema_stg, table_name_stg, schema_rdv, table_name_rdv, where_json, apply_f, is_distinct)

VALUES(p_schema_stg, p_table_name_stg, p_schema_rdv, p_table_name_rdv, p_where_json, 0, p_is_distinct);

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Оповещаем об окончании транзакции

return(v_json_ret||']');

--Выводим возникшие ошибки

exception

when others then

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_stg_2_dwh(text);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_stg_2_dwh(p_tbl_stg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_rec_s2t record; --Переменная для работы с циклом 

v_cnt int = 0; --Переменная для записи количества

v_cnt_ins int = 0; --Счетчик вставленных записей

v_cnt_upd int = 0; --Счетчик обновленных записей

v_cnt_del int = 0; --Счетчик удаленных записей

v_src_stm_id int; --Переменная для записи src_stm_id таблицы

v_hash_diff text; --Переменная для записи md5-атрибутов 

v_schema_stg text; --Переменная для записи схемы таблицы

v_table_name_stg text; --Переменная для записи схемы таблицы

v_start_dttm text; --Переменная для работы с логированием

v_json_ret text := ''; --Переменная для работы с логированием

v_user text; --Перемнная для фиксирования пользователя

begin

--Проверяем источники в таблице stg_sys_dwh.prm_s2t_stg

begin

v_start_dttm := clock_timestamp() at time zone 'utc';

select count(*)

into v_cnt

from (select distinct schema_stg

from stg_sys_dwh.prm_s2t_stg) s;

if v_cnt > 1 then 

raise exception '%', 'Too many sources';

end if;

select schema_stg

into v_schema_stg

from stg_sys_dwh.prm_s2t_stg

group by schema_stg;

if v_schema_stg is null then 

raise exception '%', 'Source is null';

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '1',

	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Открываем цикл с запросом, содержащим новые записи

for v_rec_s2t in (SELECT schema_src, table_name_src, table_comment_src, column_name_src, column_comment_src, datatype_src, key_type_src, schema_stg, table_name_stg, column_name_stg, column_comment_stg, ref_to_stg, datatype_stg, connection_name_src, column_order_src, upd_user,

md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(table_comment_src,'-1')||

coalesce(column_name_src,'-1')||

coalesce(column_comment_src,'-1')||

coalesce(datatype_src,'-1')||

coalesce(key_type_src,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(connection_name_src,'-1')||

coalesce(column_order_src,'-1'))::text as hash_diff,

now()::date as eff_dt, '9999-12-31'::date as end_dt

FROM stg_sys_dwh.prm_s2t_stg

where apply_f = 0

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), table_name_stg) = table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), schema_stg) = schema_stg

) loop

--Фиксируем наличие или отсутствие записи в sys_dwh.prm_s2t_stg

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select count(*)

into v_cnt

from sys_dwh.prm_s2t_stg s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Получаем информацию у записи, присутствующей в sys_dwh.prm_s2t_stg

if v_cnt > 0 then 

select count(*), src_stm_id, 

md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(table_comment_src,'-1')||

coalesce(column_name_src,'-1')||

coalesce(column_comment_src,'-1')||

coalesce(datatype_src,'-1')||

coalesce(key_type_src,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(connection_name_src,'-1')||

coalesce(column_order_src,'-1'))::text as hash_diff

into v_cnt, v_src_stm_id, v_hash_diff 

from sys_dwh.prm_s2t_stg s 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

group by src_stm_id, md5(coalesce(schema_src,'-1')||

coalesce(table_name_src,'-1')||

coalesce(table_comment_src,'-1')||

coalesce(column_name_src,'-1')||

coalesce(column_comment_src,'-1')||

coalesce(datatype_src,'-1')||

coalesce(key_type_src,'-1')||

coalesce(column_comment_stg,'-1')||

coalesce(ref_to_stg,'-1')||

coalesce(datatype_stg,'-1')||

coalesce(connection_name_src,'-1')||

coalesce(column_order_src,'-1'))::text;

end if;

--Фиксируем src_stm_id для записи, отсутствующей в sys_dwh.prm_s2t_stg

if v_cnt = 0 then

select s.src_stm_id into v_src_stm_id from sys_dwh.prm_src_stm s where s.nm = v_rec_s2t.table_name_stg and v_rec_s2t.schema_stg = (select 'stg_'||p.nm from sys_dwh.prm_src_stm p where s.prn_src_stm_id = p.src_stm_id);

if v_src_stm_id is null then

raise exception '%', 'The src_stm_id in sys_dwh.prm_src_stm is empty';

end if;

--Вставляем новую, отсутствующую в sys_dwh.prm_s2t_rdv запись

insert into sys_dwh.prm_s2t_stg

(schema_src, table_name_src, table_comment_src, column_name_src, column_comment_src, datatype_src, key_type_src, schema_stg, table_name_stg, column_name_stg, column_comment_stg, ref_to_stg, datatype_stg, connection_name_src, column_order_src, upd_user, src_stm_id, eff_dt, end_dt)

values(v_rec_s2t.schema_src, v_rec_s2t.table_name_src, v_rec_s2t.table_comment_src, v_rec_s2t.column_name_src, v_rec_s2t.column_comment_src, v_rec_s2t.datatype_src, v_rec_s2t.key_type_src, v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.column_name_stg, v_rec_s2t.column_comment_stg, v_rec_s2t.ref_to_stg, v_rec_s2t.datatype_stg, v_rec_s2t.connection_name_src, v_rec_s2t.column_order_src, v_rec_s2t.upd_user, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_stg s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg;

v_cnt_ins := v_cnt_ins + 1;

end if;

--Для записи, присутствующей в sys_dwh.prm_s2t_rdv, сверяем атрибуты

if v_cnt > 0 then

if v_hash_diff <> v_rec_s2t.hash_diff then

--Если атрибуты различаются, то закрываем предыдущую версию записи в sys_dwh.prm_s2t_rdv

update sys_dwh.prm_s2t_stg s set end_dt = current_date-1 

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;

--Запись с новыми атрибутами вставляем с актуальными датами

insert into sys_dwh.prm_s2t_stg

(schema_src, table_name_src, table_comment_src, column_name_src, column_comment_src, datatype_src, key_type_src, schema_stg, table_name_stg, column_name_stg, column_comment_stg, ref_to_stg, datatype_stg, connection_name_src, column_order_src, upd_user, src_stm_id, eff_dt, end_dt)

values(v_rec_s2t.schema_src, v_rec_s2t.table_name_src, v_rec_s2t.table_comment_src, v_rec_s2t.column_name_src, v_rec_s2t.column_comment_src, v_rec_s2t.datatype_src, v_rec_s2t.key_type_src, v_rec_s2t.schema_stg, v_rec_s2t.table_name_stg, v_rec_s2t.column_name_stg, v_rec_s2t.column_comment_stg, v_rec_s2t.ref_to_stg, v_rec_s2t.datatype_stg, v_rec_s2t.connection_name_src, v_rec_s2t.column_order_src, v_rec_s2t.upd_user, v_src_stm_id, v_rec_s2t.eff_dt, v_rec_s2t.end_dt);

update stg_sys_dwh.prm_s2t_stg s set apply_f = 1

where s.schema_stg = v_rec_s2t.schema_stg and s.table_name_stg = v_rec_s2t.table_name_stg and s.column_name_stg = v_rec_s2t.column_name_stg;

v_cnt_upd := v_cnt_upd + 1;

end if;

end if;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_val := 'v_ssn='||v_src_stm_id||' v_column='||v_rec_s2t.column_name_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '2',

	p_descr := 'Загрузка данных',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_ssn='||v_src_stm_id||' v_column='||v_rec_s2t.column_name_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Проверяем наличие записей в stg_sys_dwh.prm_s2t_stg

begin

v_start_dttm := clock_timestamp() at time zone 'utc';	

select coalesce(count(1), 0) 

into v_cnt 

from stg_sys_dwh.prm_s2t_stg;

--Закрываем все неактуальные записи в sys_dwh.prm_s2t_stg

if v_cnt > 0 then

select max(upd_user) into v_user from stg_sys_dwh.prm_s2t_stg;

update sys_dwh.prm_s2t_stg p set end_dt = now()::date-1, upd_user = v_user

where p.schema_stg = v_schema_stg

and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

and coalesce(substring(p_tbl_stg from '%.#"%#"' for '#'), p.table_name_stg) = p.table_name_stg

and coalesce(substring(p_tbl_stg from '#"%#".%' for '#'), p.schema_stg) = p.schema_stg

and not exists (select 1 from stg_sys_dwh.prm_s2t_stg s

where s.schema_stg = v_schema_stg

and s.schema_stg = p.schema_stg and s.table_name_stg = p.table_name_stg and s.column_name_stg =p.column_name_stg);

get diagnostics v_cnt_del = ROW_COUNT;

end if;

--Удаление записей с неверным диапазоном

delete from sys_dwh.prm_s2t_stg where eff_dt > end_dt;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_ins_qty := v_cnt_ins::text,

	p_upd_qty := v_cnt_upd::text,

	p_del_qty := v_cnt_del::text,

	p_val := 'v_schema_stg='||v_schema_stg,

	p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

	p_step := '3',

	p_descr := 'Закрытие неактуальных записей в таблице sys_dwh.prm_s2t_stg',

	p_start_dttm := v_start_dttm,

	p_err := SQLERRM,

	p_log_tp := '3',

	p_val := 'v_schema_stg='||v_schema_stg,

	p_cls := ']',

	p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

return(v_json_ret||']');   

--Обрабатываем ошибки

exception

when others then 

raise exception '%', v_json_ret;   

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_stg_2_prm_src_stm();

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_stg_2_prm_src_stm()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
v_rec_s2t record;
v_rec_s2t_ref record;
v_rec_s2t_json record;
v_cnt int = 0;
v_cnt_ins int = 0;
v_cnt_json int = 0;
v_src_stm_id int;
v_schema_stg text;
v_table_name_stg text;
v_json_schema_stg text;
v_json_table_name_stg text;
v_cnt_json_sql text;
v_start_dttm text; --Переменная для работы с логированием
v_json_ret text := ''; --Переменная для работы с логированием
v_out text := '';
begin
--Проверяем количество новых исчтоников
begin
v_start_dttm := clock_timestamp() at time zone 'utc';
select count(*) 
into v_cnt 
from
(select s.schema_stg, s.table_name_stg, now()::date as eff_dt, '9999-12-31'::date as end_dt
from
(select schema_stg, table_name_stg
 from (select schema_stg, table_name_stg from stg_sys_dwh.prm_s2t_stg  
	   union
	   select split_part(ref_to_stg, '.',1) schema_stg, split_part(ref_to_stg, '.',2) table_name_stg
	   from stg_sys_dwh.prm_s2t_stg 
	   where trim(ref_to_stg) <> '' and ref_to_stg is not null and substring(trim(ref_to_stg) from 1 for 3) = 'stg') t) s
 left join sys_dwh.prm_src_stm p 
 on s.table_name_stg = p.nm 
 and p.prn_src_stm_id  = (select src_stm_id from sys_dwh.prm_src_stm where 'stg_'||nm = s.schema_stg)
 where p.src_stm_id is null) t;
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '1',
	p_descr := 'Проверка количества новых исчтоников',
	p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '1',
	p_descr := 'Проверка количества новых исчтоников',
	p_start_dttm := v_start_dttm,
	p_err := SQLERRM,
	p_log_tp := '3',
	p_cls := ']',
	p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Открываем цикл по новым источникам, если они есть
begin
v_start_dttm := clock_timestamp() at time zone 'utc';
if v_cnt > 0 then
for v_rec_s2t in (select s.schema_stg, s.table_name_stg, now()::date as eff_dt, '9999-12-31'::date as end_dt from
   (select schema_stg, table_name_stg
	 from (select schema_stg, table_name_stg
	 	   from stg_sys_dwh.prm_s2t_stg  
		   union
		   select split_part(ref_to_stg, '.',1) schema_stg, split_part(ref_to_stg, '.',2) table_name_stg
		   from stg_sys_dwh.prm_s2t_stg 
		   where trim(ref_to_stg) <> '' and ref_to_stg is not null and substring(trim(ref_to_stg) from 1 for 3) = 'stg') t) s
	left join sys_dwh.prm_src_stm p 
	on s.table_name_stg = p.nm 
	and p.prn_src_stm_id  = (select src_stm_id from sys_dwh.prm_src_stm where 'stg_'||nm = s.schema_stg)
	where p.src_stm_id is null)
loop
--Фиксируем информацию о новом источнике
if v_cnt_ins = 0 then
v_out := 'Missing src_stm_id for tables '||v_rec_s2t.schema_stg||'.'||v_rec_s2t.table_name_stg;
else
v_out := v_out||', '||v_rec_s2t.schema_stg||'.'||v_rec_s2t.table_name_stg;
end if;
v_cnt_ins := v_cnt_ins + 1;
end loop;
raise exception '%', v_out;
end if;
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '2',
	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',
	p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '2',
	p_descr := 'Проверка источников в таблице stg_sys_dwh.prm_s2t_stg',
	p_start_dttm := v_start_dttm,
	p_err := SQLERRM,
	p_log_tp := '3',
	p_cls := ']',
	p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Обнуляем счетчик
v_cnt_ins := 0;
--Проверяем количество исчтоников в поле ref_to_stg в формате json
begin
v_start_dttm := clock_timestamp() at time zone 'utc';
select count(*) 
into v_cnt 
from
(select ref_to_stg from stg_sys_dwh.prm_s2t_stg 
 where trim(ref_to_stg) <> '' and ref_to_stg is not null and substring(trim(ref_to_stg) from 1 for 1) = '[' ) t;
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '3',
	p_descr := 'Проверка количества исчтоников в поле ref_to_stg в формате json',
	p_val := v_cnt::text,
	p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '3',
	p_descr := 'Проверка количества исчтоников в поле ref_to_stg в формате json',
	p_start_dttm := v_start_dttm,
	p_err := SQLERRM,
	p_log_tp := '3',
	p_cls := ']',
	p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
--Открываем цикл по полям json
begin
v_start_dttm := clock_timestamp() at time zone 'utc';
if v_cnt <> 0 then
for v_rec_s2t_ref in (select ref_to_stg from stg_sys_dwh.prm_s2t_stg 
	where trim(ref_to_stg) <> '' and ref_to_stg is not null and substring(trim(ref_to_stg) from 1 for 1) = '[')
loop
--Открываем цикл по источникам внутри json
for v_rec_s2t_json in (select split_part(tbl_nm, '.',1) schema_stg, split_part(tbl_nm, '.',2) table_name_stg 
	from (select tbl_nm from json_populate_recordset(null::record, v_rec_s2t_ref.ref_to_stg::json)
	as (tbl_nm text, fld_nm text, val text)) j)
loop
--Проверяем наличие источника в sys_dwh.prm_src_stm
v_json_schema_stg := v_rec_s2t_json.schema_stg;
v_json_table_name_stg := v_rec_s2t_json.table_name_stg;
v_cnt_json_sql := 'select count(*) 
from     
(select s.schema_stg, s.table_name_stg
,now()::date as eff_dt, ''9999-12-31''::date as end_dt
from
(
select '''||v_json_schema_stg||''' schema_stg, '''||v_json_table_name_stg||''' table_name_stg
) s
left join sys_dwh.prm_src_stm p 
on s.table_name_stg = p.nm and 
p.prn_src_stm_id  = (select src_stm_id from sys_dwh.prm_src_stm where ''stg_''||nm = s.schema_stg )
where p.src_stm_id is null ) t';
execute v_cnt_json_sql into v_cnt_json;
--Фиксируем номер нового источника
if v_cnt_json > 0 then
--Вставляем в таблицу sys_dwh.prm_src_stm информацию о новом источнике
if v_cnt_ins = 0 then
v_out := 'Missing src_stm_id for tables (json) '||v_rec_s2t_json.schema_stg||'.'||v_rec_s2t_json.table_name_stg;
else
v_out := v_out||', '||v_rec_s2t_json.schema_stg||'.'||v_rec_s2t_json.table_name_stg;
end if;
v_cnt_ins := v_cnt_ins + 1;
end if;
end loop;
end loop;
end if;
if v_cnt_ins > 0
then raise exception '%', v_out;
end if;
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '4',
	p_descr := 'Обработка и вставка новых исчтоников из поля ref_to_stg в формате json',
	p_val := v_cnt_ins::text,
	p_start_dttm := v_start_dttm)
into v_json_ret;
exception when others then
select sys_dwh.get_json4log(p_json_ret := v_json_ret,
	p_step := '4',
	p_descr := 'Обработка и вставка новых исчтоников из поля ref_to_stg в формате json',
	p_start_dttm := v_start_dttm,
	p_val := v_cnt_ins::text,
	p_err := SQLERRM,
	p_log_tp := '3',
	p_cls := ']',
	p_debug_lvl := '1')
into v_json_ret;
raise exception '%', v_json_ret;
end;
return(v_json_ret||']');   
--Обрабатываем ошибки
exception
when others then 
raise exception '%', v_json_ret;   
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_s2t_stg_2_stg(varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION sys_dwh.load_s2t_stg_2_stg(p_schema_src varchar, p_table_name_src varchar, p_table_comment_src varchar, p_column_name_src varchar, p_column_comment_src varchar, p_datatype_src varchar, p_key_type_src varchar, p_schema_stg varchar, p_table_name_stg varchar, p_column_name_stg varchar, p_column_comment_stg varchar, p_ref_to_stg varchar, p_datatype_stg varchar)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

begin

--Вставляем заданные значения

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

INSERT INTO stg_sys_dwh.prm_s2t_stg

(schema_src, table_name_src, table_comment_src, column_name_src, column_comment_src, datatype_src, key_type_src, schema_stg, table_name_stg, column_name_stg, column_comment_stg, ref_to_stg, datatype_stg, apply_f)

VALUES(p_schema_src, p_table_name_src, p_table_comment_src, p_column_name_src, p_column_comment_src, p_datatype_src, p_key_type_src, p_schema_stg, p_table_name_stg, p_column_name_stg, p_column_comment_stg, p_ref_to_stg, p_datatype_stg, 0);

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Оповещаем об окончании транзакции

return(v_json_ret||']');

--Выводим возникшие ошибки

exception

when others then

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_satellete(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.load_satellete(p_src_stm_id int4, p_json text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

	v_rec_json_ssn record;

	v_json_ret text = '';

	v_ret text = '';

 	v_src_stm_id int4;

    v_start_dttm text;

	

	err_code text; -- код ошибки

    msg_text text; -- текст ошибки

    exc_context text; -- контекст исключения

    msg_detail text; -- подробный текст ошибки

    exc_hint text; -- текст подсказки к исключению

	begin

		v_src_stm_id := p_src_stm_id;

			

		for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,p_json::json)

			as (ssn int))

			loop

				begin

					v_start_dttm := clock_timestamp() at time zone 'utc';

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '1',

						p_descr := 'Запуск load_ref_satellete',

						p_start_dttm := v_start_dttm,

						p_val := 'v_src_stm_id='''||v_src_stm_id::text||''', ssn='''||v_rec_json_ssn.ssn::text||'''',

						p_log_tp := '1',

						p_debug_lvl := '3')

				    into v_json_ret;

					select sys_dwh.load_ref_satellete(p_src_stm_id:=v_src_stm_id,p_ssn:=v_rec_json_ssn.ssn::text) into v_ret::text;

					

					v_json_ret := v_json_ret || ',' || replace(replace(v_ret::text,']',''),'[','');

				end;

			end loop;

		v_json_ret := v_json_ret||']';

		return(v_json_ret);

    exception

        when others then

			v_json_ret := v_json_ret || ',' || replace(replace(v_ret::text,']',''),'[','');

			GET STACKED DIAGNOSTICS

				err_code = RETURNED_SQLSTATE, -- код ошибки

				msg_text = MESSAGE_TEXT, -- текст ошибки

				exc_context = PG_CONTEXT, -- контекст исключения

				msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки

				exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению

			if v_json_ret is null then

				v_json_ret := '';

			end if;

			select sys_dwh.get_json4log(p_json_ret := v_json_ret,

				p_step := '0',

				p_descr := 'Фатальная ошибка',

				p_start_dttm := v_start_dttm,

						

				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',

				p_log_tp := '3',

				p_cls := ']',

				p_debug_lvl := '1')

				into v_json_ret;

			

		  if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

			v_json_ret := v_json_ret||']';

		  end if;

		  raise exception '%', v_json_ret;   

    end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_set_hub(text);

CREATE OR REPLACE FUNCTION sys_dwh.load_set_hub(p_json_param text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	declare
		v_tbl_nm_sdv text;
		--p_src_stm_id int4,
		v_is_load bool;
		v_src_stm_id_ref text; 
		v_ref_to_hub text; 
		v_column_name_stg text;
		v_ssn text; 
		v_json_fld_nm text; 
		v_json_val text;
		v_rule text;
		v_gk_pk_str_when text;
		v_src_stm_id bigint;
		
		v_partitiontablename text;
		v_cnt bigint;
		v_cnt_block bigint;
		v_res int = -1;
		v_max_ret int = 1200;
		v_ret int = v_max_ret;
		v_json_ret text = '';
		
		
		v_exec_part_sql text;
		v_exec_sql text;
		v_set_sql text;
		v_exec_block_sql text;

		v_start_dttm text;

		err_code text; -- код ошибки
		msg_text text; -- текст ошибки
		exc_context text; -- контекст исключения
		msg_detail text; -- подробный текст ошибки
		exc_hint text; -- текст подсказки к исключению
	begin
		--Парсим json
		--Проверяем наличие партиции в целевой таблице
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
		
		select distinct j_tbl_nm_sdv::text j_tbl_nm_sdv,
				j_src_stm_id_ref::text j_src_stm_id_ref, 
				j_ref_to_hub::text j_ref_to_hub, 
				j_column_name_stg::text j_column_name_stg,
				j_ssn::text j_ssn,
				j_json_fld_nm::text j_json_fld_nm, 
				j_json_val::text j_json_val,
				j_rule::text j_rule,
				j_gk_pk_str_when::text j_gk_pk_str_when,
				j_is_load::bool j_is_load,
				j_src_stm_id::bigint j_src_stm_id from
			(select t.value ->> 'j_tbl_nm_sdv' as j_tbl_nm_sdv, 
				t.value ->> 'j_src_stm_id_ref' as j_src_stm_id_ref,
				t.value ->> 'j_ref_to_hub' as j_ref_to_hub,
				t.value ->> 'j_column_name_stg' as j_column_name_stg,
				t.value ->> 'j_ssn' as j_ssn,
				t.value ->> 'j_json_fld_nm' as j_json_fld_nm,
				t.value ->> 'j_json_val' as j_json_val,
				t.value ->> 'j_rule' as j_rule,
				t.value ->> 'j_gk_pk_str_when' as j_gk_pk_str_when,
				t.value ->> 'j_is_load' as j_is_load,
				t.value ->> 'j_src_stm_id' as j_src_stm_id
			from json_array_elements(p_json_param::json) t(value)) r
			 into
				v_tbl_nm_sdv,
				v_src_stm_id_ref, 
				v_ref_to_hub, 
				v_column_name_stg,
				v_ssn,
				v_json_fld_nm, 
				v_json_val,
				v_rule,
				v_gk_pk_str_when,
				v_is_load,
				v_src_stm_id;
			if v_is_load then
				v_ssn := '{{ var_ssn }}';
			end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'v_tbl_nm_sdv='''||coalesce(v_tbl_nm_sdv::text,'')||''',
					v_src_stm_id_ref='''||coalesce(v_src_stm_id_ref::text,'')||''',
					v_ref_to_hub='''||coalesce(v_ref_to_hub::text,'')||''',
					v_column_name_stg='''||coalesce(v_column_name_stg::text,'')||''',
					v_ssn='''||coalesce(v_ssn::text,'')||''',
					v_json_fld_nm='''||coalesce(v_json_fld_nm::text,'')||''',
					v_json_val='''||coalesce(v_json_val::text,'')||''',
					v_rule='''||coalesce(v_rule::text,'')||''',
					v_gk_pk_str_when='''||coalesce(v_gk_pk_str_when::text,'')||'''
					v_src_stm_id'''||coalesce(v_src_stm_id::text,'')||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		
		--Проверяем наличие партиции в целевой таблице
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
		  select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 
			into v_cnt
			from pg_catalog.pg_partitions pp
				where lower(pp.schemaname) = 'rdv'
					and lower(pp.tablename) = regexp_replace(v_ref_to_hub,'^rdv.', '')
					and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref::bigint;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '4',
				p_descr := 'Поиск партиции в целевой таблице',
				p_start_dttm := v_start_dttm,
				p_val := 'cnt='''||v_cnt::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '4',
				p_descr := 'Поиск партиции в целевой таблице',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;  
		if v_cnt = 0 and v_is_load = false then
			--проверяем права на создание партиции
			--Проверяем owners у таблиц
			begin	
				v_start_dttm := clock_timestamp() at time zone 'utc';
				/*WITH RECURSIVE cte AS (
					SELECT pg_roles.oid,
						pg_roles.rolname
					   FROM pg_roles
					  WHERE pg_roles.rolname = CURRENT_USER
					UNION
					 SELECT m.roleid, pgr.rolname
						FROM cte cte_1
							JOIN pg_auth_members m ON m.member = cte_1.oid
							JOIN pg_roles pgr ON pgr.oid = m.roleid
					),
					ow as (select tableowner 
						from pg_catalog.pg_tables pt 
							where lower(schemaname) = 'rdv'
							and lower(tablename) =  regexp_replace(v_ref_to_hub,'^rdv.', ''))
					select coalesce(count(1), 0) into v_cnt from (			
				SELECT cte.rolname
					FROM cte
						where cte.rolname in (select tableowner from ow)) as t;
				if v_cnt < 1 then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '5',
						p_descr := 'Проверка прав на создание партиции',
						p_start_dttm := v_start_dttm,
						p_val := 'v_ref_to_hub='''||v_ref_to_hub::text||'''',
						p_err := 'Нет прав на создание партиции',
						p_log_tp := '3',
						p_cls := ']',
						p_debug_lvl := '1')
					into v_json_ret;

					raise exception '%', v_json_ret;
				end if;*/
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '5',
					p_descr := 'Проверка прав на создание партиции',
					p_start_dttm := v_start_dttm,
					--p_val := 'cnt='''||v_cnt::text||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '5',
					p_descr := 'Проверка прав на создание партиции',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			begin
				v_start_dttm := clock_timestamp() at time zone 'utc';
				--Создаем новую партицию
				v_exec_part_sql := 'alter table '||v_ref_to_hub||' add partition start ('||
				v_src_stm_id_ref::bigint||') inclusive end ('||(v_src_stm_id_ref::bigint+1)::text||') exclusive with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=5)';
				execute v_exec_part_sql;
				execute 'analyze ' || v_ref_to_hub;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '6',
					p_descr := 'Создание новой партиции',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_part_sql='''||v_exec_part_sql||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '6',
					p_descr := 'Создание новой партиции',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					--p_cls := ']',
					p_debug_lvl := '1')
					into v_json_ret;
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					PERFORM pg_sleep(240);
					select count(replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint) 
					into v_cnt
					from pg_catalog.pg_partitions pp
						where lower(pp.schemaname) = 'rdv'
							and lower(pp.tablename) = regexp_replace(v_ref_to_hub,'^rdv.', '')
							and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref::bigint;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '7',
									p_descr := 'Повторный поиск партиции в целевой таблице',
									p_start_dttm := v_start_dttm,
									p_val := 'cnt='''||v_cnt::text||'''',
									p_log_tp := '1',
									p_debug_lvl := '2')
								into v_json_ret;
				exception when others then
								select sys_dwh.get_json4log(p_json_ret := v_json_ret,
									p_step := '7',
									p_descr := 'Повторный поиск партиции в целевой таблице',
									p_start_dttm := v_start_dttm,
									p_err := SQLERRM,
									p_log_tp := '3',
									p_debug_lvl := '1',
									p_cls := ']')
								into v_json_ret;
				end;
				if v_cnt = 0 then
					raise exception '%', v_json_ret;
				end if;
			end;
		end if; 
		--вносим в hub  
		begin
			v_start_dttm := clock_timestamp() at time zone 'utc';
			--ищем партицию в хабе
			select partitiontablename 
				into v_partitiontablename
				from pg_catalog.pg_partitions pp
					where lower(pp.schemaname) = 'rdv'
						and lower(pp.tablename) = regexp_replace(v_ref_to_hub,'^rdv.', '')
						and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_src_stm_id_ref::bigint;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '8',
					p_descr := 'Поиск партиции в хабе',
					p_start_dttm := v_start_dttm,
					p_val := 'v_partitiontablename='''||v_partitiontablename||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
		exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '8',
					p_descr := 'Поиск партиции в хабе',
					p_start_dttm := v_start_dttm,
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
					raise exception '%', v_json_ret;
				 
		end;
				
		--проверяем на блокировку и блокируем
		v_exec_block_sql := '(select count(*) AS tablename FROM pg_locks l1 JOIN pg_stat_user_tables t ON l1.relation = t.relid
				where t.schemaname || ''.'' || t.relname = ''rdv.'||v_partitiontablename||''' and mode = ''AccessExclusiveLock'' 
					and mppsessionid in (select sess_id from pg_stat_activity where pid!=pg_backend_pid() 
												and waiting=''t'') and mppsessionid != 0)';	  
		v_res = -1;
		v_max_ret = 1200;
		v_ret = v_max_ret;
		WHILE v_ret > 0 AND v_res < 0
			loop						
				v_ret :=  v_ret - 1;
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					execute v_exec_block_sql into v_cnt_block;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '9',
						p_descr := 'Проверка блокировки хаба',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
						p_ins_qty := v_cnt_block::text,
						p_log_tp := '1',
						p_debug_lvl := '3')
					into v_json_ret;
				exception when others then	
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '9',
						p_descr := 'Проверка блокировки хаба',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_cls := ']',
						p_debug_lvl := '1')
					into v_json_ret;
						raise exception '%', v_json_ret;
				end;
				-- block function
				if v_cnt_block = 0 then
					v_exec_block_sql := 'lock table rdv.'||v_partitiontablename||';';
					--v_exec_block_sql := ' ';
					v_start_dttm := clock_timestamp() at time zone 'utc';
					begin
						if v_is_load then
							v_set_sql := v_exec_block_sql;
						else
							execute v_exec_block_sql;
						end if;
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '10',
							p_descr := 'Блокировка хаба',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
							p_ins_qty := v_cnt_block::text,
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
					exception when others then	
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '10',
							p_descr := 'Блокировка хаба',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
						into v_json_ret;
							raise exception '%', v_json_ret;
					end;
					v_start_dttm := clock_timestamp() at time zone 'utc';  	
					  
						begin
							v_exec_sql := 'drop table if exists tmp'||replace(v_tbl_nm_sdv,'.','_')||'_'||regexp_replace(v_ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||';';
							if v_is_load then
								v_set_sql := v_set_sql||v_exec_sql;
							else
								execute v_exec_sql;
							end if;
							v_exec_sql := 'create temporary table tmp'||replace(v_tbl_nm_sdv,'.','_')||'_'||regexp_replace(v_ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||'
								(bk text, gk uuid) with (appendonly=true) on commit drop 
								distributed by (bk, gk);';
							if v_is_load then
								v_set_sql := v_set_sql||v_exec_sql;
							else
								execute v_exec_sql;
							end if;	
							
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '11',
								p_descr := 'Создание временной таблицы',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								--p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '1')
							into v_json_ret;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '11',
								p_descr := 'Создание временной таблицы',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
								raise exception '%', v_json_ret;
						end;
						v_start_dttm := clock_timestamp() at time zone 'utc';
						begin
							v_exec_sql := 'insert into tmp'||replace(v_tbl_nm_sdv,'.','_')||'_'||regexp_replace(v_ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||' (bk, gk)														
												select distinct';
							if v_gk_pk_str_when is null then
								v_exec_sql := v_exec_sql ||' coalesce('||v_column_name_stg||'::text,''-1'')';
								v_exec_sql := v_exec_sql ||', gk_'||v_column_name_stg;
							else 
								v_exec_sql := v_exec_sql ||' case when '||v_gk_pk_str_when||' then ''-1''::text else '||v_column_name_stg||'::text end, gk_pk ';
							end if;
							v_exec_sql := v_exec_sql ||' from '||v_tbl_nm_sdv||' p where ssn in( '||v_ssn||')';
							if v_json_fld_nm is not null
								then
									v_exec_sql := v_exec_sql||' and '||v_json_fld_nm||' in( '||replace(replace(v_json_val,'[',''),']','')||')';
							elsif v_rule is not null
								then
									v_exec_sql := v_exec_sql||' '||v_rule;
							
							end if;
							v_exec_sql := v_exec_sql ||';';
							if v_is_load then
								v_set_sql := v_set_sql||v_exec_sql;
							else
								execute v_exec_sql;
							end if;
							v_exec_sql := 'analyze tmp'||replace(v_tbl_nm_sdv,'.','_')||'_'||regexp_replace(v_ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||';';
							if v_is_load then
								v_set_sql := v_set_sql||v_exec_sql;
							else
								execute v_exec_sql;
							end if;
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '12',
								p_descr := 'Наполнение временной таблицы',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								--p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '1')
							into v_json_ret;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '12',
								p_descr := 'Наполнение временной таблицы',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
								raise exception '%', v_json_ret;
						end;
						v_start_dttm := clock_timestamp() at time zone 'utc';
						begin
							v_exec_sql := 'insert into rdv.'||v_partitiontablename||' (bk, gk, src_stm_id, upd_dttm)
								select p.bk, 
									p.gk,
									'||v_src_stm_id_ref||',
									now() at time zone ''utc''
									from tmp'||replace(v_tbl_nm_sdv,'.','_')||'_'||regexp_replace(v_ref_to_hub,'^rdv.', '')||'_'||v_src_stm_id_ref||' p 
										left join rdv.'||v_partitiontablename||' h on h.gk = p.gk and h.bk = p.bk and h.src_stm_id = '||v_src_stm_id_ref||'
											where h.bk is null;';
							if v_is_load then
								--v_set_sql := v_set_sql||' lock table rdv.'||v_partitiontablename||';';
								v_set_sql := v_set_sql||v_exec_sql;
							else
								execute v_exec_sql;
								get diagnostics v_cnt = row_count;
							end if;
							
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '13',
								p_descr := 'Заполнение хаба',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '1')
							into v_json_ret;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '13',
								p_descr := 'Заполнение хаба',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
								raise exception '%', v_json_ret;
						end;    
						-- Собираем статистику
						v_start_dttm := clock_timestamp() at time zone 'utc';  	
						begin	
							v_exec_block_sql := 'analyze rdv.'||v_partitiontablename||';';
							if v_is_load then
								v_set_sql := v_set_sql;--||v_exec_block_sql;
							else
								execute v_exec_block_sql;
							end if;
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '14',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
								p_ins_qty := v_cnt_block::text,
								p_log_tp := '1',
								p_debug_lvl := '3')
							into v_json_ret;
						exception when others then	
							select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '14',
								p_descr := 'Сбор статистики',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
								p_err := SQLERRM,
								p_log_tp := '3',
								p_cls := ']',
								p_debug_lvl := '1')
							into v_json_ret;
								raise exception '%', v_json_ret;
						end;
					--PERFORM pg_sleep(1);
					v_res := 0;
					exit;
				end if;
				if v_cnt_block <> 0 then
				   PERFORM pg_sleep(1);
				   if v_ret = 0 then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '9',
							p_descr := 'Проверка блокировки хаба',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_block_sql='''||v_exec_block_sql||'''',
							p_err := 'Хаб был заблокирован дольше '||v_max_ret||' секунд' ,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
						into v_json_ret;
							raise exception '%', v_json_ret;
				   end if;
				   --v_ret := v_ret - 1;
				   --RAISE exception '%', 'Error: Despite having made '||v_max_ret||' retries. Lock on '||p_src_stm_id||' failed';--v_msg;
				end if;
		end loop;
		--запись в таблицу
		if v_is_load then
			/*v_start_dttm := clock_timestamp() at time zone 'utc';  	
			begin
				v_exec_sql := 'delete from sys_dwh.reg_hub_sql 
				where src_stm_id = '||v_src_stm_id||'
				and hub_nm = '''||v_ref_to_hub||'''
				and column_name_stg = '''||v_column_name_stg||'''
				and src_stm_id_ref = '||v_src_stm_id_ref||'';
				execute v_exec_sql;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '15',
					p_descr := 'Удаление записи в reg_hub_sql',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_ins_qty := v_cnt_block::text,
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '15',
					p_descr := 'Удаление записи в reg_hub_sql',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
					raise exception '%', v_json_ret;
			end;*/
			v_start_dttm := clock_timestamp() at time zone 'utc';  	
			begin
				v_set_sql := 'do \\$\\$ begin '||v_set_sql||' end;\\$\\$;';
				v_exec_sql := 'insert into sys_dwh.reg_hub_sql (src_stm_id, hub_nm, tbl_nm_sdv, column_name_stg, src_stm_id_ref, set_sql) values ('||v_src_stm_id||','''||v_ref_to_hub||''','''||v_tbl_nm_sdv||''', '''||v_column_name_stg||''', '||v_src_stm_id_ref||', '''||replace(v_set_sql,'''','''''')||''')';
				execute v_exec_sql;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '16',
					p_descr := 'Вставка записи в reg_hub_sql',
					p_start_dttm := v_start_dttm,
					--p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_ins_qty := v_cnt_block::text,
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then	
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '16',
					p_descr := 'Вставка записи в reg_hub_sql',
					p_start_dttm := v_start_dttm,
					--p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
					raise exception '%', v_json_ret;
			end;
		end if;
		v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;
 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_st2_stg_src(varchar, varchar, varchar, varchar, varchar, varchar, int4, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION sys_dwh.load_st2_stg_src(p_schema_src varchar, p_table_name_src varchar, p_schema_stg varchar, p_table_name_stg varchar, p_pxf_name_src varchar, p_ext_pb varchar, p_ext_pb_itrv int4, p_act_dt_column varchar, p_pk_src varchar, p_ext_format varchar DEFAULT NULL::character varying, p_ext_encoding varchar DEFAULT NULL::character varying, p_column_fltr varchar DEFAULT NULL::character varying, p_column_fltr_from varchar DEFAULT NULL::character varying, p_column_fltr_to varchar DEFAULT NULL::character varying, p_ext_prd_pb varchar DEFAULT NULL::character varying, p_ext_prd_pb_itrv varchar DEFAULT NULL::character varying, p_ext_prd_pb_from varchar DEFAULT NULL::character varying)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

begin

--Вставляем заданные значения

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

INSERT INTO stg_sys_dwh.prm_s2t_stg_src

(schema_src, table_name_src, schema_stg, table_name_stg, pxf_name_src, ext_pb, ext_pb_itrv, act_dt_column, pk_src, ext_format, ext_encoding, column_fltr, column_fltr_from, column_fltr_to, ext_prd_pb, ext_prd_pb_itrv, ext_prd_pb_from, apply_f)

VALUES(p_schema_src, p_table_name_src, p_schema_stg, p_table_name_stg, p_pxf_name_src, p_ext_pb, p_ext_pb_itrv, p_act_dt_column, p_pk_src, p_ext_format, p_ext_encoding, p_column_fltr, p_column_fltr_from, p_column_fltr_to, p_ext_prd_pb, p_ext_prd_pb_itrv, p_ext_prd_pb_from, 0);

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Оповещаем об окончании транзакции

return(v_json_ret||']');

--Выводим возникшие ошибки

exception

when others then

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.load_st2_stg_src(varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION sys_dwh.load_st2_stg_src(p_schema_src varchar, p_table_name_src varchar, p_schema_stg varchar, p_table_name_stg varchar, p_pxf_name_src varchar, p_ext_pb varchar, p_ext_pb_itrv varchar, p_act_dt_column varchar, p_pk_src varchar, p_ext_format varchar DEFAULT NULL::character varying, p_ext_encoding varchar DEFAULT NULL::character varying, p_column_fltr varchar DEFAULT NULL::character varying, p_column_fltr_from varchar DEFAULT NULL::character varying, p_column_fltr_to varchar DEFAULT NULL::character varying, p_ext_prd_pb varchar DEFAULT NULL::character varying, p_ext_prd_pb_itrv varchar DEFAULT NULL::character varying, p_ext_prd_pb_from varchar DEFAULT NULL::character varying)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_start_dttm text;

v_json_ret text := '';

begin

--Вставляем заданные значения

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

INSERT INTO stg_sys_dwh.prm_s2t_stg_src

(schema_src, table_name_src, schema_stg, table_name_stg, pxf_name_src, ext_pb, ext_pb_itrv, act_dt_column, pk_src, ext_format, ext_encoding, column_fltr, column_fltr_from, column_fltr_to, ext_prd_pb, ext_prd_pb_itrv, ext_prd_pb_from, apply_f)

VALUES(p_schema_src, p_table_name_src, p_schema_stg, p_table_name_stg, p_pxf_name_src, p_ext_pb, p_ext_pb_itrv, p_act_dt_column, p_pk_src, p_ext_format, p_ext_encoding, p_column_fltr, p_column_fltr_from, p_column_fltr_to, p_ext_prd_pb, p_ext_prd_pb_itrv, p_ext_prd_pb_from, 0);

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm)

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Вставка заданных значений',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

into v_json_ret;

raise exception '%', v_json_ret;

end;

--Оповещаем об окончании транзакции

return(v_json_ret||']');

--Выводим возникшие ошибки

exception

when others then

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.merge_prttn(text, text, date, date, text);

CREATE OR REPLACE FUNCTION sys_dwh.merge_prttn(p_tbl_nm text, p_col text, p_dt_from date, p_dt_to date, p_interval text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
v_tbl_nm text; -- имя таблицы (схема.имя)
v_col text; -- поле (по нему определяется суб партиция или партиция)
v_sql text; -- текст запроса
v_sql_l text; -- текст запроса в цикле
v_src_stm_id int;
v_cnt int;
v_rslt text;
v_interval_in text; -- изначальный интервал
v_year text;
v_mon text;
v_all text = 'all'||replace((now()::date)::text,'-',''); -- для негранулярных партиций
v_prt_lvl int; -- партиция (0) или субпартиция (1)
v_rec_prttn text; -- партиции
v_rec_sprttn text; -- субпартиция
v_strt_prttn text;
v_end_prttn text;
v_with_prttn text;
v_col_prttn text; -- поле партицирования (для схлапывания субпартиций)

begin
	v_tbl_nm := p_tbl_nm;

	-- проверяем есть ли у таблицы партиции/субпартиции с датой
	v_sql := 'select count(*) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart like ''%::date%'';';

	execute v_sql  into v_cnt;
	if v_cnt > 0 
	then 
	-- узнаем фактическую гранулярность
	v_sql := 'select partitioneveryclause from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart like ''%::date%'' and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date < '''||p_dt_to||''') group by partitioneveryclause;';

	execute v_sql  into v_interval_in;

	v_interval_in := replace(v_interval_in,'''','');

v_interval_in := replace(v_interval_in,'::interval','');

	-- партиция (0) или субпартиция (1)
	v_sql := 'select partitionlevel from pg_catalog.pg_partition_columns where schemaname||''.''||tablename = '''||v_tbl_nm||''' and columnname = '''||p_col||''';';

	execute v_sql into v_prt_lvl;

-- схлапывание в негранулярный период
	if p_interval = 'all'
	then 

-- уровень схлапываемой партиции = 0
		if v_prt_lvl = 0 -- подмена партиций
		then
	-- создаем времянку под целевую партицию
					v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_all||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

					execute v_sql;
	-- заполняем времянку под целевую партицию
					v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_all||' select * from '||v_tbl_nm||' where '||p_col||' >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''';';

					execute v_sql;
	-- удаляем партиции		
					v_sql_l := 'select partitionrangestart from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');';
							
						execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;
					for v_rec_prttn in execute v_sql_l 
					loop 
						execute 'alter table '||v_tbl_nm||' drop partition for ('||v_rec_prttn||');';

					end loop;
	-- создаем партицию за год
						execute 'alter table '||v_tbl_nm||'  add partition   START ('''||p_dt_from||''') END ('''||p_dt_to||''') WITH ('||v_with_prttn||');';	

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_all||';';
						
	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_all||';';

				v_rslt := 'сделана подмена партиций за указаный период';			
		end if;
-- уровень схлапываемой партиции = 1	
		if v_prt_lvl = 1 -- подмена субпартиций
		then -- собираем список партиций
				v_sql := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;';
	
					execute 'select columnname from  pg_catalog.pg_partition_columns  where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;'
					into v_col_prttn;		

					for v_rec_prttn in execute v_sql 
					loop 

	-- создаем времянку под целевую партицию
						v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_all||'_'||v_rec_prttn||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

						execute v_sql;
	-- заполняем времянку под целевую партицию
						v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_all||'_'||v_rec_prttn||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''' and '||v_col_prttn||' = '||v_rec_prttn||';';
						execute v_sql;		

						-- собираем список субпартиций 
						v_sql_l := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');';

						 execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;

	-- удаляем субпартиции					
						for v_rec_sprttn in execute v_sql_l
						loop

							execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||') drop partition for ('||v_rec_sprttn||');';

						end loop;
	-- создаем субпартицию за год	

						execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||')  add partition   START ('''||p_dt_from||''') END ('''||p_dt_to||''') WITH ('||v_with_prttn||');';

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_all||'_'||v_rec_prttn||';';

	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_all||'_'||v_rec_prttn||';';

					end loop;
				v_rslt := 'сделана подмена субпартиций за указанный период';

		end if; 
	end if;

	-- проверяем фактическую гранулярность
	-- гранулярность по месяцам - в год
		if v_interval_in = '1 mon'
		then 
			if p_interval = 'year'
			then
	-- создать таблицу бэкапа 
			
				if v_prt_lvl = 0 -- подмена партиций
				then
	-- создаем времянку под целевую партицию
					v_year := SUBSTRING(p_dt_from::text, 1, 4);
					v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_year||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

					execute v_sql;
	-- заполняем времянку под целевую партицию
					v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_year||' select * from '||v_tbl_nm||' where '||p_col||' >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''';';

					execute v_sql;
	-- удаляем партиции		
					v_sql_l := 'select partitionrangestart from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');';

					execute 'select min(partitionrangestart) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');'
							into v_strt_prttn;

						execute'select max(partitionrangeend) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');' 
							into v_end_prttn;

						execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;
					for v_rec_prttn in execute v_sql_l 
					loop 
						execute 'alter table '||v_tbl_nm||' drop partition for ('||v_rec_prttn||');';

					end loop;
	-- создаем партицию за год
						execute 'alter table '||v_tbl_nm||'  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';	

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_year||';';
						
	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_year||';';

				v_rslt := 'сделана подмена партиций мес в год';						
				end if;
				
				if v_prt_lvl = 1 -- подмена субпартиций
				then -- собираем список партиций 
					v_sql := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;';
			
					execute 'select columnname from  pg_catalog.pg_partition_columns  where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;'
					into v_col_prttn;		

					for v_rec_prttn in execute v_sql 
					loop 

	-- создаем времянку под целевую партицию
						v_year := SUBSTRING(p_dt_from::text, 1, 4); 

						v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

						execute v_sql;
	-- заполняем времянку под целевую партицию
						v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''' and '||v_col_prttn||' = '||v_rec_prttn||';';
						execute v_sql;				
						-- собираем список субпартиций 
						v_sql_l := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');';

						execute 'select min(partitionrangestart) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_strt_prttn;

						execute 'select max(partitionrangeend) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_end_prttn;

						 execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'

							into v_with_prttn;

	-- удаляем субпартиции					
						for v_rec_sprttn in execute v_sql_l
						loop
							execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||') drop partition for ('||v_rec_sprttn||');';

						end loop;
	-- создаем субпартицию за год					
						execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||')  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';
	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||';';
	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||';';
					end loop;
				v_rslt := 'сделана подмена субпартиций мес в год';
				end if;	
			else v_rslt := 'Неправильно задана целевая гранулярность партиции.';
			end if;
		end if;
	-- гранулярность по дням - в месяц/год	
		if v_interval_in = '1 day'
		then 
			if p_interval = 'year' 
			then
-- создать таблицу бэкапа 
				v_sql := 'create table '||v_tbl_nm||'_hist_'||replace((now()::date)::text,'-','')||' (like '||v_tbl_nm||' including all);';
				
				if v_prt_lvl = 0
				then
-- создаем времянку под целевую партицию
					v_year := SUBSTRING(p_dt_from::text, 1, 4);
					v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_year||' (like '||v_tbl_nm||' including all EXCLUDING storage);';
				
					execute v_sql;
	-- заполняем времянку под целевую партицию
					v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_year||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''';';
						
					execute v_sql;
	-- удаляем партиции		
					v_sql_l := 'select partitionrangestart from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');';

					execute 'select min(partitionrangestart) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');'
							into v_strt_prttn;		

						execute'select max(partitionrangeend) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');' 
							into v_end_prttn;

						execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;

					for v_rec_prttn in execute v_sql_l
					loop 
						execute 'alter table '||v_tbl_nm||' drop partition for ('||v_rec_prttn||');';

					end loop;
	-- создаем партицию за год
						execute 'alter table '||v_tbl_nm||'  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';	

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_year||';';

	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_year||';';

				v_rslt := 'сделана подмена партиций мес в год';							
				end if;
				if v_prt_lvl = 1
				then
-- собираем список партиций 
					v_sql := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;';
		
					execute 'select columnname from  pg_catalog.pg_partition_columns  where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;'
					into v_col_prttn;		

					for v_rec_prttn in execute v_sql 
					loop 

	-- создаем времянку под целевую партицию
						v_year := SUBSTRING(p_dt_from::text, 1, 4); 

						v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||' (like '||v_tbl_nm||' including all EXCLUDING storage);';


						execute v_sql;
	-- заполняем времянку под целевую партицию
						v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''' and '||v_col_prttn||' = '||v_rec_prttn||';';

						execute v_sql;				
						-- собираем список субпартиций 
						v_sql_l := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');';

						execute 'select min(partitionrangestart) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_strt_prttn;

						execute 'select max(partitionrangeend) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_end_prttn;

						 execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'

							into v_with_prttn;

	-- удаляем субпартиции					
						for v_rec_sprttn in execute v_sql_l
						loop
							execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||') drop partition for ('||v_rec_sprttn||');';
					
						end loop;
	-- создаем субпартицию за год					
						execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||')  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||';';

	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_year||'_'||v_rec_prttn||';';

					end loop;
				v_rslt := 'сделана подмена субпартиций день в год';
				end if;
			end if;
		
			if  p_interval = 'mon'
			then 

-- создать таблицу бэкапа 
				v_sql := 'create table '||v_tbl_nm||'_hist_'||replace((now()::date)::text,'-','')||' (like '||v_tbl_nm||' including all);';
			
				if v_prt_lvl = 0
				then

-- создаем времянку под целевую партицию
					v_mon := replace(SUBSTRING(p_dt_from::text, 1, 7),'-',''); -- select replace(SUBSTRING('2024-06-05', 1, 7),'-','');
					v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_mon||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

					execute v_sql;
	-- заполняем времянку под целевую партицию
					v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_mon||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''';';

					execute v_sql;
	-- удаляем партиции		
					v_sql_l := 'select partitionrangestart from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');';

					execute 'select min(partitionrangestart) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');'
							into v_strt_prttn;		

						execute'select max(partitionrangeend) from (select SUBSTRING(partitionrangestart,2,10) dt,*  
								from pg_catalog.pg_partitions where  schemaname||''.''||tablename ='''||v_tbl_nm||''' and partitionlevel = 0 ) t
								where (dt >= '''||p_dt_from||''' and dt < '''||p_dt_to||''');' 
							into v_end_prttn;

						execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;

					for v_rec_prttn in execute v_sql_l -- v_rec_prttn = ('2023-01-01'::date')::text
					loop 
						execute 'alter table '||v_tbl_nm||' drop partition for ('||v_rec_prttn||');';
	
					end loop;
	-- создаем партицию за год
						execute 'alter table '||v_tbl_nm||'  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';	

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_mon||';';

	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_mon||';';

				v_rslt := 'сделана подмена партиций день в мес';	
				end if;
				if v_prt_lvl = 1
				then

-- собираем список партиций 
					v_sql := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;';

					execute 'select columnname from  pg_catalog.pg_partition_columns  where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 0;'
					into v_col_prttn;		

					for v_rec_prttn in execute v_sql -- v_rec_prttn = '10002'
					loop 

	-- создаем времянку под целевую партицию
						v_mon := replace(SUBSTRING(p_dt_from::text, 1, 7),'-','');

						v_sql := 'create table '||v_tbl_nm||'_prttn_'||v_mon||'_'||v_rec_prttn||' (like '||v_tbl_nm||' including all EXCLUDING storage);';

						execute v_sql;
	-- заполняем времянку под целевую партицию
						v_sql := 'insert into '||v_tbl_nm||'_prttn_'||v_mon||'_'||v_rec_prttn||' select * from '||v_tbl_nm||' where '||p_col||'  >= '''||p_dt_from||''' and '||p_col||'  < '''||p_dt_to||''' and '||v_col_prttn||' = '||v_rec_prttn||';';

						execute v_sql;				
						-- собираем список субпартиций 
						v_sql_l := 'select partitionrangestart from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');';

						execute 'select min(partitionrangestart) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_strt_prttn;

						execute 'select max(partitionrangeend) from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionlevel = 1 
										and (SUBSTRING(partitionrangestart,2,10)::date >= '''||p_dt_from||''' and SUBSTRING(partitionrangeend,2,10)::date <= '''||p_dt_to||''')
										and  parentpartitiontablename =  (select partitiontablename from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_tbl_nm||''' and partitionrangestart = '''||v_rec_prttn||''');'
							into v_end_prttn;

						 execute 'select replace(replace(c.reloptions::text,''{'',''''),''}'','''') FROM pg_class c
									JOIN pg_attribute a ON a.attrelid = c.oid
									JOIN pg_namespace n ON n.oid = c.relnamespace
									where n.nspname||''.''||c.relname = '''||v_tbl_nm||''' and a.attname = '''||p_col||''';'
							into v_with_prttn;

	-- удаляем субпартиции					
						for v_rec_sprttn in execute v_sql_l
						loop

							execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||') drop partition for ('||v_rec_sprttn||');';

						end loop;
	-- создаем субпартицию за год			

						execute 'alter table '||v_tbl_nm||' alter partition for ('||v_rec_prttn||')  add partition   START ('||v_strt_prttn||') END ('||v_end_prttn||') WITH ('||v_with_prttn||');';

	-- заполняем партицию
						execute 'insert into '||v_tbl_nm||' select * from '||v_tbl_nm||'_prttn_'||v_mon||'_'||v_rec_prttn||';';

	-- удаляем времянку				
						execute 'drop table '||v_tbl_nm||'_prttn_'||v_mon||'_'||v_rec_prttn||';';

					end loop;
				v_rslt := 'сделана подмена субпартиций день в мес';
				end if;
			end if;
		end if;
		
		if v_interval_in = '1 year' 
		then v_rslt := 'Максимальная гранулярность партиции - партицирование по годам.';
		end if;
		
	else v_rslt := 'В таблице нет партиций по датам.';
	end if;
raise notice '%', 'END '||v_rslt;	

return v_rslt; 

end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.mrg_bdv_prttn(text, text, _text);

CREATE OR REPLACE FUNCTION sys_dwh.mrg_bdv_prttn(p_src_table text, p_trgt_table text, prttns_list _text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_prttn text;
v_src_schema text;
v_src_table text;
v_trgt_schema text;
v_trgt_table text;
v_tmp_table text;
v_tmp_schema text;
v_cnt int8 := 0;
v_prttn_level int8;
v_output_text text;
v_ddl text;
v_tmp_tbl text;
v_start_dttm text;
v_json_ret text := '';
v_start_prttn text;
v_start_parent_prttn text;
v_name_parent_prttn text;
v_prttn_name_trgt text;
v_owner text;
v_subprttn text;
v_max_levelpart int8;
v_cnt_prttns int8;
v_cnt_prnt_parttns int8;
 begin
	--Проверяем наличие тестовой таблицы в БД	
	begin	
	select split_part(p_src_table,'.', 1) into v_src_schema;
	select split_part(p_src_table,'.', 2) into v_src_table;
	select split_part(p_trgt_table,'.', 1) into v_trgt_schema;
	select split_part(p_trgt_table,'.', 2) into v_trgt_table;
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select count(1) into v_cnt from pg_catalog.pg_tables pt 
    where lower(pt.schemaname) = lower(v_src_schema)
    and lower(pt.tablename) = lower(v_src_table);
    if v_cnt = 0 then
    v_output_text := 'The table' || p_src_table ||' is missing on DB';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверяем наличие тестовой таблицы в БД	',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверяем наличие тестовой таблицы в БД	',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем наличие целевой таблицы в БД
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select count(1) into v_cnt from pg_catalog.pg_tables pt 
    where lower(pt.schemaname) = lower(v_trgt_schema)
    and lower(pt.tablename) = lower(v_trgt_table);
    if v_cnt = 0 then
    v_output_text := 'The table' || p_trgt_table || 'is missing on DB';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Проверяем наличие целевой таблицы в БД',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Проверяем наличие целевой таблицы в БД',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем owners у таблиц
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select coalesce(count(1), 1) into v_cnt from
    (select tableowner from pg_catalog.pg_tables pt 
     where lower(schemaname) = v_trgt_schema
     and lower(tablename) = v_trgt_table
	 except
	 select tableowner from pg_catalog.pg_tables pt 
	 where lower(schemaname) = v_src_schema
	 and lower(tablename) = v_src_table) as t;
    if v_cnt > 0 then
    v_output_text := 'The owners of the source table and the target table are different';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Проверка владельцев таблиц',
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Проверка владельцев таблиц',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Фиксируем owner таблицы
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select pt.tableowner into v_owner from pg_catalog.pg_tables pt 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table);  
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Фиксация владельца',
		p_val := 'v_owner='||v_owner,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'Фиксация владельца',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_owner='||v_owner,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--собираем макс.уровень партиций
	begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select max(partitionlevel) into v_max_levelpart from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table);
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5.1',
		p_descr := 'проверяем макс.уровень партиций',
		p_val := 'v_max_levelpart='||v_max_levelpart,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5.1',
		p_descr := 'проверяем макс.уровень партиций',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_max_levelpart='||v_max_levelpart,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Открываем цикл по list_prttn
    for v_prttn in select unnest(prttns_list) loop
	--проверяем уровень партиции
	begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select partitionlevel into v_prttn_level from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_prttn);
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5.2',
		p_descr := 'проверка уровеня партиции',
		p_val := 'v_prttn='||v_prttn||',v_prttn_level='||v_prttn_level,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5.2',
		p_descr := 'проверка уровеня партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_prttn='||v_prttn||',v_prttn_level='||v_prttn_level,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Открываем блоки условий по уровню партиции(партиции с субпартициями)
	if v_prttn_level = 0 then 
	if v_max_levelpart = 1 then
	--Проверяем правильность партицирования таблицы
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select count(distinct parentpartitiontablename) into v_cnt_prnt_parttns from pg_catalog.pg_partitions where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table) and partitionlevel = 1;
	select count(distinct partitiontablename) into v_cnt_prttns from pg_catalog.pg_partitions where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table) and partitionlevel = 0;
    if v_cnt_prnt_parttns <> v_cnt_prttns then
    v_output_text := 'У каждой таблицы должна быть субпартиция, в таблице' || p_src_table || 'количество партиций - '||v_cnt_prnt_parttns||', а партиций с субпартициями - '||v_cnt_prttns||'.';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Проверяем правильность партицированияя таблицы',
		p_val := 'v_cnt_prnt_parttns='||v_cnt_prnt_parttns,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Проверяем правильность партицированияя таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--открываем цикл субпартиций, если нужно передать всю партицию
	for v_subprttn in select partitiontablename from pg_catalog.pg_partitions
	where lower(parentpartitiontablename) = lower(v_prttn) loop
	--Собираем границы партиции из таблицы источника
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select partitionrangestart into v_start_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_subprttn); 
	select parentpartitiontablename into v_name_parent_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_subprttn); 
	select case when partitiontype = 'range' then partitionrangestart when partitiontype = 'list' then partitionlistvalues end into v_start_parent_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
    and lower(partitiontablename) = lower(v_name_parent_prttn); 
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_val := 'v_subprttn='||v_subprttn||',v_start_prttn='||v_start_prttn||',v_start_parent_prttn='||v_start_parent_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_subprttn='||v_subprttn||',v_start_prttn='||v_start_prttn||',v_start_parent_prttn='||v_start_parent_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Собираем имя партиции в целевой таблице
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select pp1.partitiontablename into v_prttn_name_trgt 
    from pg_catalog.pg_partitions pp1
    join pg_catalog.pg_partitions pp2 on pp2.partitiontablename = pp1.parentpartitiontablename
    where lower(pp1.partitionrangestart) = lower(v_start_prttn)
    and lower(pp1.schemaname) = lower(v_trgt_schema)
    and lower(pp1.tablename) = lower(v_trgt_table)
    and case 
            when pp2.partitiontype = 'range' then pp2.partitionrangestart 
            when pp2.partitiontype = 'list' then pp2.partitionlistvalues 
        end = v_start_parent_prttn; 	
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_val := 'v_subprttn='||v_subprttn || ',v_prttn_name_trgt='||v_prttn_name_trgt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_subprttn='||v_subprttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем наличие партиции в целевой таблице
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select count(1)
    into v_cnt
    from pg_catalog.pg_partitions pp1
    join pg_catalog.pg_partitions pp2 on pp2.partitiontablename = pp1.parentpartitiontablename
    where lower(pp1.partitiontablename) = lower(v_prttn_name_trgt)
    and lower(pp1.schemaname) = lower(v_trgt_schema)
    and lower(pp1.tablename) = lower(v_trgt_table)
    and case 
            when pp2.partitiontype = 'range' then pp2.partitionrangestart 
            when pp2.partitiontype = 'list' then pp2.partitionlistvalues 
        end = v_start_parent_prttn;
    if v_cnt = 0 then
    v_output_text := 'В целевой таблице отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_val := 'v_subprttn='||v_subprttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_subprttn='||v_subprttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
     --Проверяем отсутствие  партиции в  таблице источнике
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select count(1)  
    into v_cnt
    from pg_catalog.pg_partitions pp
    where lower(pp.schemaname) = lower(v_src_schema)
    and lower(pp.tablename) = lower(v_src_table)
    and lower(pp.partitiontablename) = lower(v_subprttn);
    if v_cnt = 0 then
    v_output_text := 'В таблице источнике отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_val := 'v_subprttn='||v_subprttn||',v_cnt='||v_cnt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_subprttn='||v_subprttn||',v_cnt='||v_cnt,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Фиксируем имя временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	    
    select v_src_schema ||'.'||v_src_table||to_char(now(),'ddHH24miSS') into v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_val := 'v_subprttn='||v_subprttn||',v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_subprttn='||v_subprttn||',v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
   --Создаем временную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	 
    v_ddl := 'create table '||v_tmp_tbl||' (like '||v_src_schema||'.'||v_subprttn||' including all EXCLUDING storage)';
    execute v_ddl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_val := 'v_subprttn='||v_subprttn||',v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_subprttn='||v_subprttn||',v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем владельца временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||v_tmp_tbl||' owner to '||v_owner;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию таблицы-источника с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_src_table||' alter partition for ('|| v_start_parent_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;  
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_val := 'p_src_table='||p_src_table||',v_start_parent_prttn='||v_start_parent_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_src_table='||p_src_table||',v_start_parent_prttn='||v_start_parent_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Удаляем ограничения временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	select split_part(v_tmp_tbl,'.', 1) into v_tmp_schema;
	select split_part(v_tmp_tbl,'.', 2) into v_tmp_table;
    for v_ddl in (select 'alter table '||v_tmp_tbl||' drop constraint '||c.conname as ddl_drop
    from pg_constraint c
    join pg_class t on c.conrelid = t.oid
    join pg_namespace n on t.relnamespace = n.oid
    where t.relname = v_tmp_table
    and n.nspname = v_tmp_schema) loop
	execute v_ddl;
	end loop;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию целевой таблицы с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_trgt_table||' alter partition for ('|| v_start_parent_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_parent_prttn='||v_start_parent_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_parent_prttn='||v_start_parent_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Удаляем врменную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	execute 'drop table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	end loop;
	ELSE--Блок с только партициями, условие от if v_max_levelpart = 1 then
	--Собираем границы партиции из таблицы источника
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select partitionrangestart into v_start_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_prttn); 
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_val := 'v_prttn='||v_prttn||',v_start_prttn='||v_start_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_prttn='||v_prttn||',v_start_prttn='||v_start_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Собираем имя партиции в целевой таблице
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	select partitiontablename into v_prttn_name_trgt 
	from pg_catalog.pg_partitions 
    where lower(partitionrangestart) = lower(v_start_prttn)
	and lower(schemaname) = lower(v_trgt_schema)
	and lower(tablename) = lower(v_trgt_table);	
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_val := 'v_prttn='||v_prttn ||', v_prttn_name_trgt =' || v_prttn_name_trgt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_prttn='||v_prttn ||', v_prttn_name_trgt =' || v_prttn_name_trgt,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем наличие партиции в целевой таблице
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select count(1)
    into v_cnt
    from pg_catalog.pg_partitions pp1
    where lower(pp1.partitiontablename) = lower(v_prttn_name_trgt)
	and lower(pp1.schemaname) = lower(v_trgt_schema)
	and lower(pp1.tablename) = lower(v_trgt_table);
    if v_cnt = 0 then
    v_output_text := 'В целевой таблице отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_val := 'v_cnt='||v_cnt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_cnt='||v_cnt,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
     --Проверяем отсутствие  партиции в  таблице источнике
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select count(1)  
    into v_cnt
    from pg_catalog.pg_partitions pp
    where lower(pp.schemaname) = lower(v_src_schema)
    and lower(pp.tablename) = lower(v_src_table)
    and lower(pp.partitiontablename) = lower(v_prttn);
    if v_cnt = 0 then
    v_output_text := 'В таблице источнике отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_val := 'v_cnt='||v_cnt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_cnt='||v_cnt,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Фиксируем имя временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	    
    select v_src_schema ||'.'||v_src_table||to_char(now(),'ddHH24miSS') into v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
   --Создаем временную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	 
    v_ddl := 'create table '||v_tmp_tbl||' (like '||v_src_schema||'.'||v_prttn||' including all EXCLUDING storage)';
    execute v_ddl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema||',v_prttn='||v_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema||',v_prttn='||v_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем владельца временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||v_tmp_tbl||' owner to '||v_owner;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию таблицы-источника с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_src_table||' alter partition for ('|| v_start_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;  
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_val := 'p_src_table='||p_src_table||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_src_table='||p_src_table||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Удаляем ограничения временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	select split_part(v_tmp_tbl,'.', 1) into v_tmp_schema;
	select split_part(v_tmp_tbl,'.', 2) into v_tmp_table;
    for v_ddl in (select 'alter table '||v_tmp_tbl||' drop constraint '||c.conname as ddl_drop
    from pg_constraint c
    join pg_class t on c.conrelid = t.oid
    join pg_namespace n on t.relnamespace = n.oid
    where t.relname = v_tmp_table
    and n.nspname = v_tmp_schema) loop
	execute v_ddl;
	end loop;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию целевой таблицы с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_trgt_table||' alter partition for ('|| v_start_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Удаляем врменную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	execute 'drop table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	end if;
	--для субпартиций
	ELSE -- условие от if v_prttn_level = 0 then 
	--Собираем границы партиции из таблицы источника
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    select partitionrangestart into v_start_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_prttn); 
	select parentpartitiontablename into v_name_parent_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_prttn); 
	select case when partitiontype = 'range' then partitionrangestart when partitiontype = 'list' then partitionlistvalues end into v_start_parent_prttn from pg_catalog.pg_partitions 
    where lower(schemaname) = lower(v_src_schema) and lower(tablename) = lower(v_src_table)
	and lower(partitiontablename) = lower(v_name_parent_prttn); 
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_val := 'v_start_prttn='||v_start_prttn||',v_name_parent_prttn='||v_name_parent_prttn||',v_start_parent_prttn='||v_start_parent_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Сбор границ партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_start_prttn='||v_start_prttn||',v_name_parent_prttn='||v_name_parent_prttn||',v_start_parent_prttn='||v_start_parent_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Собираем имя партиции в целевой таблице
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	select pp1.partitiontablename into v_prttn_name_trgt 
	from pg_catalog.pg_partitions pp1
	join pg_catalog.pg_partitions pp2 on pp2.partitiontablename = pp1.parentpartitiontablename
    where lower(pp1.partitionrangestart) = lower(v_start_prttn)
	and lower(pp1.schemaname) = lower(v_trgt_schema)
	and lower(pp1.tablename) = lower(v_trgt_table)
	and case 
            when pp2.partitiontype = 'range' then pp2.partitionrangestart 
            when pp2.partitiontype = 'list' then pp2.partitionlistvalues 
        end = v_start_parent_prttn;		
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_val := 'v_prttn='||v_prttn ||', v_prttn_name_trgt =' || v_prttn_name_trgt ,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '7',
		p_descr := 'Собираем имя партиции в целевой таблице',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_prttn='||v_prttn ||', v_prttn_name_trgt =' || v_prttn_name_trgt ,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Проверяем наличие партиции в целевой таблице
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	select count(1)
    into v_cnt
    from pg_catalog.pg_partitions pp1
	join pg_catalog.pg_partitions pp2 on pp2.partitiontablename = pp1.parentpartitiontablename
    where lower(pp1.partitiontablename) = lower(v_prttn_name_trgt)
	and lower(pp1.schemaname) = lower(v_trgt_schema)
	and lower(pp1.tablename) = lower(v_trgt_table)
	and case 
            when pp2.partitiontype = 'range' then pp2.partitionrangestart 
            when pp2.partitiontype = 'list' then pp2.partitionlistvalues 
        end = v_start_parent_prttn;
    if v_cnt = 0 then
    v_output_text := 'В целевой таблице отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_val := 'v_prttn='||v_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '8',
		p_descr := 'Проверка наличия партиции',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_prttn='||v_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
     --Проверяем отсутствие  партиции в  таблице источнике
	begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
    select count(1)  
    into v_cnt
    from pg_catalog.pg_partitions pp
    where lower(pp.schemaname) = lower(v_src_schema)
    and lower(pp.tablename) = lower(v_src_table)
    and lower(pp.partitiontablename) = lower(v_prttn);
    if v_cnt = 0 then
    v_output_text := 'В таблице источнике отсутствует партиция, используйте функцию добавления партиций sys_dwh.bdv_add_prttn';
    raise exception '%', v_output_text;
    end if;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_val := 'v_cnt='||v_cnt,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '9',
		p_descr := 'Проверка наличия партиции в таблице источнике',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_cnt='||v_cnt,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Фиксируем имя временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	    
    select v_src_schema ||'.'||v_src_table||to_char(now(),'ddHH24miSS') into v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '10',
		p_descr := 'Фиксация имени временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
   --Создаем временную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	 
    v_ddl := 'create table '||v_tmp_tbl||' (like '||v_src_schema||'.'||v_prttn||' including all EXCLUDING storage)';
    execute v_ddl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema||',v_prttn='||v_prttn,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '11',
		p_descr := 'Создание временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_src_schema='||v_src_schema||',v_prttn='||v_prttn,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем владельца временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||v_tmp_tbl||' owner to '||v_owner;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '12',
		p_descr := 'Смена владельца временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_cls := ']',
		p_val := 'v_tmp_tbl='||v_tmp_tbl||',v_owner='||v_owner,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию таблицы-источника с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_src_table||' alter partition for ('|| v_start_parent_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;  
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_val := 'p_src_table='||p_src_table||',v_start_parent_prttn='||v_start_parent_prttn||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '13',
		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_src_table='||p_src_table||',v_start_parent_prttn='||v_start_parent_prttn||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	--Удаляем ограничения временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	select split_part(v_tmp_tbl,'.', 1) into v_tmp_schema;
	select split_part(v_tmp_tbl,'.', 2) into v_tmp_table;
    for v_ddl in (select 'alter table '||v_tmp_tbl||' drop constraint '||c.conname as ddl_drop
    from pg_constraint c
    join pg_class t on c.conrelid = t.oid
    join pg_namespace n on t.relnamespace = n.oid
    where t.relname = v_tmp_table
    and n.nspname = v_tmp_schema) loop
	execute v_ddl;
	end loop;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '14',
		p_descr := 'Удаляем ограничения временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Меняем партицию целевой таблицы с временной таблицы
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
    execute 'alter table '||p_trgt_table||' alter partition for ('|| v_start_parent_prttn ||') 
    exchange partition for ('||v_start_prttn||')
    with table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_parent_prttn='||v_start_parent_prttn||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '15',
		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'p_trgt_table='||p_trgt_table||',v_start_parent_prttn='||v_start_parent_prttn||',v_start_prttn='||v_start_prttn||',v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
    --Удаляем врменную таблицу
    begin
	v_start_dttm := clock_timestamp() at time zone 'utc';	
	execute 'drop table '||v_tmp_tbl;
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_start_dttm := v_start_dttm)
    into v_json_ret;
    exception when others then
    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '16',
		p_descr := 'Удаление временной таблицы',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_cls := ']',
		p_log_tp := '3',
		p_val := 'v_tmp_tbl='||v_tmp_tbl,
		p_debug_lvl := '1')
    into v_json_ret;
    raise exception '%', v_json_ret;
    end;
	end if;
    end loop;
    --Оповещаем об окончании
    return(v_json_ret||']');   
    --Регистрируем ошибки
    exception
    when others then 
    raise exception '%', v_json_ret;   
    end 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.mrg_stg_sdv_prttn(int4, json);

CREATE OR REPLACE FUNCTION sys_dwh.mrg_stg_sdv_prttn(p_src_stm_id int4, p_json json)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	

	

declare

v_ssn_rec record;

v_trg_table_name text;

v_trg_scheme_name text;

v_trg_table text;

v_src_table_name text;

v_src_scheme_name text;

v_src_table text;

v_cnt int8 := 0;

v_output_text text;

v_cursor refcursor;

v_ddl text;

v_answr text;

v_owner text;

v_tmp_tbl text;

v_start_dttm text;

v_json_ret text := '';

v_ssn int8;

 begin

	--Проверяем наличие sys_dwh.prm_src_stm в БД	

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select count(1) into v_cnt from pg_catalog.pg_tables pt 

    where lower(pt.schemaname) = 'sys_dwh'

    and lower(pt.tablename) = 'prm_src_stm';

    if v_cnt = 0 then

    v_output_text := 'The table sys_dwh.prm_src_stm is missing on DB';

    raise exception '%', v_output_text;

    end if;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Проверка наличия sys_dwh.prm_src_stm в БД',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '1',

		p_descr := 'Проверка наличия sys_dwh.prm_src_stm в БД',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Проверяем наличие таблиц в sys_dwh.prm_src_stm

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select sys_dwh.prv_tbl_id(p_src_stm_id) into v_answr;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '2',

		p_descr := 'Проверка наличия таблиц в sys_dwh.prm_src_stm',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '2',

		p_descr := 'Проверка наличия таблиц в sys_dwh.prm_src_stm',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Фиксируем имя и схему целевой таблицы

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select 

    'sdv_'||lower(pr.nm) scheme_name,

     lower(ch.nm) table_name

    into v_trg_scheme_name, v_trg_table_name

    from sys_dwh.prm_src_stm as ch

    join sys_dwh.prm_src_stm as pr

    on ch.prn_src_stm_id = pr.src_stm_id 

    where ch.src_stm_id = p_src_stm_id;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

		p_descr := 'Фиксация имени и схемы целевой таблицы',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '3',

		p_descr := 'Фиксация имени и схемы целевой таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

	end;

    --Фиксируем имя и схему таблицы-источника

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select 

    'stg_'||lower(pr.nm) scheme_name,

     lower(ch.nm) table_name

    into v_src_scheme_name, v_src_table_name

    from sys_dwh.prm_src_stm as ch

    join sys_dwh.prm_src_stm as pr

    on ch.prn_src_stm_id = pr.src_stm_id 

    where ch.src_stm_id = p_src_stm_id;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '4',

		p_descr := 'Фиксация имени и схемы таблицы-источника',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '4',

		p_descr := 'Фиксация имени и схемы таблицы-источника',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Фиксируем полные наименования таблиц

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    v_trg_table := v_trg_scheme_name||'.'||v_trg_table_name;   

    v_src_table := v_src_scheme_name||'.'||v_src_table_name;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '5',

		p_descr := 'Фиксация полных наименований',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '5',

		p_descr := 'Фиксация полных наименований',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   --Проверяем наличие целевой таблицы на БД

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select count(1) into v_cnt 

	from  pg_catalog.pg_tables  

    where 1=1

    and lower(tablename) = v_trg_table_name

    and lower(schemaname) = v_trg_scheme_name;

    if v_cnt = 0 then

    v_output_text := 'The target table '||v_trg_table||' is missing on DB';

    raise exception '%', v_output_text;

    end if;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '6',

		p_descr := 'Проверка наличия целевой таблицы на БД',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '6',

		p_descr := 'Проверка наличия целевой таблицы на БД',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   --Проверяем наличие таблицы-источника на БД  

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

	select count(1) into v_cnt 

	from  pg_catalog.pg_tables  

    where 1=1

    and lower(tablename) = v_src_table_name

    and lower(schemaname) = v_src_scheme_name;

    if v_cnt = 0 then

    v_output_text := 'The source table '||v_src_table||' is missing on DB';

    raise exception '%', v_output_text;

    end if;   

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '7',

		p_descr := 'Проверка наличия таблицы-источника на БД',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '7',

		p_descr := 'Проверка наличия таблицы-источника на БД',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   --Проверяем структуры таблиц

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    select coalesce(count(1), 1) into v_cnt

    from  

   (select i.column_name, i.ordinal_position  

 	from information_schema.columns i 

 	where i.table_schema = v_src_scheme_name and i.table_name = v_src_table_name) t1

	full outer join 

	(select i.column_name, i.ordinal_position  

 	from information_schema.columns i 

 	where i.table_schema = v_trg_scheme_name and i.table_name = v_trg_table_name) t2

  	 on t1.column_name = t2.column_name

 	and t1.ordinal_position = t2.ordinal_position

 	where t1.column_name is null 

       or t2.column_name is null;

    if v_cnt > 0 then

    v_output_text := 'The table structures vary';

    raise exception '%', v_output_text;

    end if;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '8',

		p_descr := 'Проверка структуры таблиц',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '8',

		p_descr := 'Проверка структуры таблиц',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Проверяем owners у таблиц

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    select coalesce(count(1), 1) into v_cnt from

    (select tableowner from pg_catalog.pg_tables pt 

     where lower(schemaname) = v_trg_scheme_name

     and lower(tablename) = v_trg_table_name

	 except

	 select tableowner from pg_catalog.pg_tables pt 

	 where lower(schemaname) = v_src_scheme_name

	 and lower(tablename) = v_src_table_name) as t;

    if v_cnt > 0 then

    v_output_text := 'The owners of the source table and the target table are different';

    raise exception '%', v_output_text;

    end if;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '9',

		p_descr := 'Проверка владельцев таблиц',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '9',

		p_descr := 'Проверка владельцев таблиц',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Фиксируем owner таблицы

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    select pt.tableowner into v_owner from pg_catalog.pg_tables pt 

    where lower(schemaname) = v_src_scheme_name and lower(tablename) = v_src_table_name;  

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '10',

		p_descr := 'Фиксация владельца',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '10',

		p_descr := 'Фиксация владельца',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   --Проверяем структуру json

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';   

    select ssn into v_ssn from json_populate_recordset(null::record, p_json::json) as (ssn int8);

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '11',

		p_descr := 'Проверка структуры json',

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '11',

		p_descr := 'Проверка структуры json',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Открываем цикл по ssn

    for v_ssn_rec in (select ssn from json_populate_recordset(null::record,	p_json::json) as (ssn int8)) loop

    --Проверяем наличие партиции в целевой таблице

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    select replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint 

    into v_cnt

    from pg_catalog.pg_partitions pp

    where lower(pp.schemaname) = v_trg_scheme_name

    and lower(pp.tablename) = v_trg_table_name

    and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_ssn_rec.ssn;

    if v_cnt > 0 then

    v_output_text := 'The partition has already been created in the target table';

    raise exception '%', v_output_text;

    end if;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '12',

		p_descr := 'Проверка наличия партиции',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '12',

		p_descr := 'Проверка наличия партиции',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

     --Проверяем отсутствие  партиции в  таблице источнике

	begin	

	v_start_dttm := clock_timestamp() at time zone 'utc';

    select count(1)  

    into v_cnt

    from pg_catalog.pg_partitions pp

    where lower(pp.schemaname) = v_src_scheme_name

    and lower(pp.tablename) = v_src_table_name

    and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )::bigint = v_ssn_rec.ssn;

    if v_cnt = 0 then

    v_output_text := 'Warning: Партиция '||v_ssn_rec.ssn||' отсутствует!';

   select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '13',

		p_descr := 'Проверка наличия партиции',

		p_val :=v_output_text,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    else 

    --Фиксируем имя временной таблицы

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	    

    select v_src_scheme_name||'.'||v_src_table_name||to_char(now(),'ddHH24miSS') into v_tmp_tbl;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '14',

		p_descr := 'Фиксация имени временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '14',

		p_descr := 'Фиксация имени временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   --Создаем временную таблицу

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	 

    v_ddl := 'create table '||v_tmp_tbl||' (like '||v_src_table||')';

    execute v_ddl;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '15',

		p_descr := 'Создание временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '15',

		p_descr := 'Создание временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Меняем владельца временной таблицы

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

    execute 'alter table '||v_tmp_tbl||' owner to '||v_owner;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '16',

		p_descr := 'Смена владельца временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '16',

		p_descr := 'Смена владельца временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Меняем партицию таблицы-источника с временной таблицы

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

    execute 'alter table '||v_src_table||' alter partition for ('||v_ssn_rec.ssn::text||') 

    exchange partition for ('||v_ssn_rec.ssn::text||')

    with table '||v_tmp_tbl;  

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '17',

		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '17',

		p_descr := 'Обмен данными партиции таблицы-источника и временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Создаем новую партицию в trg

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

    execute 'alter table '||v_trg_table||' add partition start ('||

     v_ssn_rec.ssn::text||') inclusive end ('||(v_ssn_rec.ssn+1)::text||') exclusive with (appendonly=''true'')';

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '18',

		p_descr := 'Создание новой партиции в целевой таблице',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '18',

		p_descr := 'Создание новой партиции в целевой таблице',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Меняем партицию целевой таблицы с временной таблицы

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

    execute 'alter table '||v_trg_table||' alter partition for ('||v_ssn_rec.ssn::text||') 

    exchange partition for ('||v_ssn_rec.ssn::text||')

    with table '||v_tmp_tbl;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '19',

		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '19',

		p_descr := 'Обмен данными партиции целевой таблицы и временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Удаляем врменную таблицу

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

	execute 'drop table '||v_tmp_tbl;

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '20',

		p_descr := 'Удаление временной таблицы',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '20',

		p_descr := 'Удаление временной таблицы',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    --Удаляем пустую партицию в таблице-источнике    

    begin

	v_start_dttm := clock_timestamp() at time zone 'utc';	

	execute 'select count(1) from '||v_src_table||' where ssn = '||v_ssn_rec.ssn::text 

    into v_cnt;

    if v_cnt > 0 then

    v_output_text := 'The data started to be loaded into the '||v_src_table||' for ssn = '||v_ssn_rec.ssn::text;

    raise exception '%', v_output_text;

    end if;

    execute 'alter table '||v_src_table||' drop partition for ('||v_ssn_rec.ssn||')';

        select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '21',

		p_descr := 'Удаление пустой партиции в таблице-источнике',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_start_dttm := v_start_dttm)

    into v_json_ret;

    exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '21',

		p_descr := 'Удаление пустой партиции в таблице-источнике',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_cls := ']',

		p_log_tp := '3',

		p_val := 'v_ssn='||v_ssn_rec.ssn,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

   end if;

   exception when others then

    select sys_dwh.get_json4log(p_json_ret := v_json_ret,

		p_step := '13',

		p_descr := 'Проверка наличия партиции',

		p_start_dttm := v_start_dttm,

		p_err := SQLERRM,

		p_log_tp := '3',

		p_cls := ']',

		p_val := 'v_ssn='||v_ssn_rec.ssn||' v_cnt='||v_cnt::text,

		p_debug_lvl := '1')

    into v_json_ret;

    raise exception '%', v_json_ret;

    end;

    end loop;

	--Собираем статистику

	execute 'analyze '||v_trg_table;

    --Оповещаем об окончании

    return(v_json_ret||']');   

    --Регистрируем ошибки

    exception

    when others then 

    raise exception '%', v_json_ret;   

    end 




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.prv_tbl_id(int4);

CREATE OR REPLACE FUNCTION sys_dwh.prv_tbl_id(p_src_stm_id int4)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

v_output_text text;

v_cnt int4;

 begin

	--Фиксируем количество таблиц в переменную

	select count(1) into v_cnt 

	from sys_dwh.prm_src_stm as ch

	join sys_dwh.prm_src_stm as pr

	on ch.prn_src_stm_id = pr.src_stm_id 

	where ch.src_stm_id = p_src_stm_id;

	--Фиксируем наличие или отсуствие таблицы в справочнике

	if v_cnt = 0 then

	v_output_text := 'The p_src_stm_id is missing as table in sys_dwh.prm_src_stm';

	raise exception '%', v_output_text;

	else

	v_output_text := 'Success';

	end if;

	--Возвращаем ответ

	return(v_output_text);   

 end


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.put_bk_from_hub(json);

CREATE OR REPLACE FUNCTION sys_dwh.put_bk_from_hub(p_json_param json)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	declare
		v_rec_cr record;
		h_list record;
		v_exec_sql text=''; --переменная для фиксации sql-скрипта
		v_rec_cr_sql text='';
		j_tbl_nm text;
		j_src_stm_id bigint;
		
		v_json_ret text = ''; 
		v_start_dttm text;

		err_code text; -- код ошибки
		msg_text text; -- текст ошибки
		exc_context text; -- контекст исключения
		msg_detail text; -- подробный текст ошибки
		exc_hint text; -- текст подсказки к исключению
		
	begin
		--Парсим json
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			select v_tbl_nm,
				v_src_stm_id
				from json_populate_recordset(null::record,
			p_json_param::json)
			as (v_tbl_nm text,
				v_src_stm_id bigint) into
				j_tbl_nm,
				j_src_stm_id;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'j_tbl_nm='''||j_tbl_nm::text||''', j_src_stm_id='''||j_src_stm_id::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		--получение запроса по полям
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
		v_rec_cr_sql := 
			'select distinct r.column_name_stg::text, r.column_name_rdv::text, c.data_type::text datatype_stg
			from sys_dwh.prm_s2t_rdv r
			join information_schema.columns c on 
				c.table_schema||''.''||c.table_name = r.schema_stg||''.''||r.table_name_stg
							and c.column_name = r.column_name_stg
							and c.table_name not like ''%_prt_%'' 
			where r.src_stm_id = '||j_src_stm_id::text||'
					and r.column_name_rdv like ''gk_%''
					and (now() at time zone ''utc'')::date between r.eff_dt and r.end_dt
					and exists (SELECT 1
						FROM information_schema.columns i
						WHERE i.table_schema||''.''||i.table_name = '''||j_tbl_nm||'''
							and i.column_name = r.column_name_stg
							and i.table_name not like ''%_prt_%'')';
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение запроса по полям',
				p_start_dttm := v_start_dttm,
				p_val := 'v_rec_cr_sql='''||v_rec_cr_sql::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Получение запроса по полям',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		-- основной цикл по всем gk входной таблицы
	--execute v_rec_cr_sql;
		for v_rec_cr in execute v_rec_cr_sql
		loop
			
			-- Цикл по всем хабам
			for h_list in (
				SELECT 'rdv.' || table_name::text tbl_hub
				FROM information_schema.tables
				WHERE table_schema = 'rdv'
					and table_name not like '%_prt_%'
					and table_name like '%h_%'
					--and table_name <> 'h_tst'
			)
			loop
			--Обновление полей bk
			v_start_dttm := clock_timestamp() at time zone 'utc';
			
			begin
				v_exec_sql := 'update '||coalesce(j_tbl_nm::text,'')||' set '||coalesce(v_rec_cr.column_name_stg::text,'')||'= 
								case when h.gk = ''6bb61e3b7bce0931da574d19d1d82c88'' then null else
								h.bk::'||coalesce(v_rec_cr.datatype_stg::text,'')||'  end 
								from '||coalesce(h_list.tbl_hub::text,'')||' h
								where h.gk = '||coalesce(v_rec_cr.column_name_rdv::text,'');
				execute v_exec_sql;
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '3',
					p_descr := 'Обновление полей bk',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '3',
					p_descr := 'Обновление полей bk',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;
			end loop;
		end loop;
		v_json_ret := v_json_ret||']';
		return(v_json_ret);
	
	--Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.put_gen_bkgk(int4, text, json);

CREATE OR REPLACE FUNCTION sys_dwh.put_gen_bkgk(p_src_stm_id int4, p_json text DEFAULT NULL::text, p_json_param json DEFAULT NULL::json)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
  v_rec_cr record;
  v_rec_ssn record;
  v_rec_json record;
  v_rec_gk_pk record;
  v_src_stm_id_ref text;
  v_src_stm_id_ref_json text;
  v_tbl_nm_sdv text;
  v_cnt int8 = 0;
  v_ssn text;
  v_exec_sql text;
  v_gk_pk_str text;
  v_gk_pk_str_when text;
  v_exec_sql_json text;
  v_output_text text;
  v_i int8 = 0;
  v_i_json int8 = 0;
  v_i_gk_pk int8 = 0;
  v_i_ssn int8 = 0;
  v_tbl_nm text;
  v_partitiontablename text;
  v_rec_json_ssn record;
  v_json_ret text = ''; 
  v_start_dttm text;
  v_rec_cr_sql text;
 
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
	--Парсим json
	if p_json_param is not null then
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			select j_tbl_nm
				from json_populate_recordset(null::record,
			p_json_param::json)
			as (j_tbl_nm text) into
				v_tbl_nm;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'p_json_param='''||p_json_param::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Получение параметров',
				p_start_dttm := v_start_dttm,
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
	end if;
	--собираем sql для не json
	--таблица источник
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select distinct regexp_replace(schema_stg,'^stg', 'sdv')||'.'||table_name_stg 
			into v_tbl_nm_sdv--, v_table_name_rdv
			from sys_dwh.prm_s2t_stg
				where src_stm_id = p_src_stm_id 
				and ((trim(ref_to_stg) <> '' 
				and ref_to_stg is not null)
				or (select count (*) 
				from information_schema.columns c
					join sys_dwh.prm_s2t_stg p on regexp_replace(c.table_schema, '^sdv', 'stg') = p.schema_stg
					and c.table_name = p.table_name_stg
					where src_stm_id = p_src_stm_id
						and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt
						and column_name = 'gk_pk') > 0
				)
				and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск таблицы источника',
			p_start_dttm := v_start_dttm,
			p_val := 'v_tbl_nm_sdv='''||v_tbl_nm_sdv||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск таблицы источника',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;		
	--Фиксируем номер сессии загрузки в таблице-источнике
	v_start_dttm := clock_timestamp() at time zone 'utc';
	if p_json is null then 
		execute 'select max(ssn)::text from '||v_tbl_nm_sdv into v_ssn;
	else
		for v_rec_json_ssn in (select ssn from json_populate_recordset(null::record,p_json::json)
			as (ssn int))
			loop
				if v_i_ssn = 0 then
					v_ssn := v_rec_json_ssn.ssn::text;
				else
					v_ssn := v_ssn||','||v_rec_json_ssn.ssn::text;
				end if;
				v_i_ssn = v_i_ssn + 1;
			end loop;
	end if;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Поиск ssn',
		p_start_dttm := v_start_dttm,
		p_val := 'ssn='''||v_ssn||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
	if v_ssn is null then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Поиск ssn',
			p_start_dttm := v_start_dttm,
			p_val := 'Warning: ssn не найден',
			p_log_tp := '2',
			p_debug_lvl := '1',
			p_cls := ']')
	    into v_json_ret;
		return(v_json_ret);
	end if;
	--получение запроса по полям
	v_start_dttm := clock_timestamp() at time zone 'utc';
	begin
		if v_tbl_nm is not null then
			v_tbl_nm_sdv := v_tbl_nm;
		end if;
		v_exec_sql := 'update '||v_tbl_nm_sdv||' set ';
		--return v_exec_sql;
		--цикл по gk полям
		v_rec_cr_sql := 
			'select schema_stg, table_name_stg, column_name_stg, ref_to_stg
			from sys_dwh.prm_s2t_stg s
			where src_stm_id = '||p_src_stm_id::text||'
					and trim(ref_to_stg) <> '''' 
					and ref_to_stg is not null
					and (now() at time zone ''utc'')::date between eff_dt and end_dt';
		if v_tbl_nm is not null then
			v_rec_cr_sql := v_rec_cr_sql||' and exists (SELECT 1
						FROM information_schema.columns
						WHERE table_schema||''.''||table_name = '''||v_tbl_nm||'''
							and column_name = s.column_name_stg
							and table_name not like ''%_prt_%'')';
		end if;
		v_rec_cr_sql := v_rec_cr_sql||' order by schema_stg, table_name_stg, column_name_stg';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '3',
				p_descr := 'Получение запроса по полям',
				p_start_dttm := v_start_dttm,
				p_val := 'v_rec_cr_sql='''||v_rec_cr_sql::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Получение запроса по полям',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;
	for v_rec_cr in execute v_rec_cr_sql
		loop
			--если не json
			if substring(trim(v_rec_cr.ref_to_stg) from 1 for 3) = 'stg' then
				--находим src_stm_id 
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					select src_stm_id into v_src_stm_id_ref 
				    		from sys_dwh.prm_src_stm 
				    			where nm = split_part(v_rec_cr.ref_to_stg, '.',2)
				    				and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
				    										where nm = regexp_replace(split_part(v_rec_cr.ref_to_stg, '.',1),'^stg_', ''));

				    if (v_i <> 0) or (v_i_json <> 0) then 
						v_exec_sql := v_exec_sql||', ';
				    end if;
					v_exec_sql := v_exec_sql||' gk_'||v_rec_cr.column_name_stg||' = md5(coalesce('''||v_src_stm_id_ref||'::text||~''||'||v_rec_cr.column_name_stg||'::text,''-1''))::uuid ';
					v_i := v_i + 1;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '4',
						p_descr := 'Поиск src_stm_id',
						p_start_dttm := v_start_dttm,
						p_val := 'src_stm_id='''||v_src_stm_id_ref||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
					into v_json_ret;						
			    exception when others then
					if v_ssn is null then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '4',
							p_descr := 'Поиск src_stm_id',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
					end if;
					raise exception '%', v_json_ret;
			    end;
			elseif
				--json
				substring(trim(v_rec_cr.ref_to_stg) from 1 for 1) = '[' then
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					v_i_json := 0;
					for v_rec_json in (select tbl_nm, fld_nm, val 
						from json_populate_recordset(null::record,
							v_rec_cr.ref_to_stg::json)
							as (tbl_nm text, fld_nm text, val text))
						loop
				    	
					    --находим src_stm_id
					    select src_stm_id into v_src_stm_id_ref_json 
				    		from sys_dwh.prm_src_stm 
				    			where nm = split_part(v_rec_json.tbl_nm, '.',2)
				    				and prn_src_stm_id = (select src_stm_id from sys_dwh.prm_src_stm
				    										where nm = regexp_replace(split_part(v_rec_json.tbl_nm, '.',1),'^stg_', ''));
				    	if v_i <> 0 and v_i_json = 0 then 
						    v_exec_sql := v_exec_sql||', ';
						end if;
				    	if v_i_json = 0 then 
						    v_exec_sql := v_exec_sql||' gk_'||v_rec_cr.column_name_stg||' = case ';
						end if; 								
				    		
				    	if left(replace(replace(v_rec_json.val,'[',''),']',''),4) = 'like' then
					    	v_exec_sql := v_exec_sql||' when '||v_rec_json.fld_nm||' ';
					    	v_exec_sql := v_exec_sql||replace(replace(v_rec_json.val,'[',''),']','')||' then md5(coalesce('''||v_src_stm_id_ref_json||'::text||~''||'||v_rec_cr.column_name_stg||'::text,''-1''))::uuid ';
				    	else
				    		v_exec_sql := v_exec_sql||' when '||v_rec_json.fld_nm||' in( ';
				    		v_exec_sql := v_exec_sql||replace(replace(v_rec_json.val,'[',''),']','')||') then md5(coalesce('''||v_src_stm_id_ref_json||'::text||~''||'||v_rec_cr.column_name_stg||'::text,''-1''))::uuid ';
						end if;
				    	v_i_json := v_i_json + 1; 
				    end loop;
				    v_exec_sql := v_exec_sql||' else  md5(''-1'')::uuid end ';
				    v_i := v_i + 1;
				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '5',
						p_descr := 'Поиск src_stm_id в json',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
				    into v_json_ret;
				exception when others then
				    select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '5',
						p_descr := 'Поиск src_stm_id в json',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end if;
		end loop;
    --return v_exec_sql;
	--проверяем на наличие gk_pk
	if v_tbl_nm is null then
		select count (*) 
		into v_cnt
		from information_schema.columns c
			join sys_dwh.prm_s2t_stg p on regexp_replace(c.table_schema,'^sdv', 'stg') = p.schema_stg
			and c.table_name = p.table_name_stg
			where p.src_stm_id = p_src_stm_id
				and column_name = 'gk_pk'
				and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt;
		if v_cnt <> 0 then
			v_start_dttm := clock_timestamp() at time zone 'utc';
			begin
				for v_rec_gk_pk in (select column_name_stg  
					from sys_dwh.prm_s2t_stg
						where src_stm_id = p_src_stm_id 
						and trim(key_type_src) like '%PK%'
						and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt
					order by column_name_stg)
					loop
						if v_i_gk_pk = 0 then 
							v_gk_pk_str := 'coalesce('||v_rec_gk_pk.column_name_stg||'::text ,''-1'')';
							v_gk_pk_str_when := v_rec_gk_pk.column_name_stg||' is null ';
						else
							v_gk_pk_str := v_gk_pk_str||'||''~''||coalesce('||v_rec_gk_pk.column_name_stg||'::text ,''-1'')';
							v_gk_pk_str_when := v_gk_pk_str_when||' and '||v_rec_gk_pk.column_name_stg||' is null ';
						end if;
						v_i_gk_pk := v_i_gk_pk + 1;
					end loop;
					if(v_i <> 0) or (v_i_json <> 0) then 
						v_exec_sql := v_exec_sql||', ';
					end if;
					v_exec_sql := v_exec_sql||' gk_pk = case when '||v_gk_pk_str_when||' 
						then md5(''-1'')::uuid
						else md5(coalesce('''||p_src_stm_id||'::text||~''||'||v_gk_pk_str||'::text ,''-1''))::uuid end ';
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '6',
							p_descr := 'Поиск gk_pk',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
			exception when others then
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '6',
					p_descr := 'Поиск gk_pk',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_debug_lvl := '1',
					p_cls := ']')
				into v_json_ret;
				raise exception '%', v_json_ret;
			end;

		end if;
	end if;
	--смотрим есть ли gk
	select count(*)  
	into v_cnt
	from sys_dwh.prm_s2t_stg 
		where src_stm_id = p_src_stm_id 
			and ((trim(ref_to_stg) <> '' 
			and ref_to_stg is not null)
			or (select count (*) 
			from information_schema.columns c
				join sys_dwh.prm_s2t_stg p on regexp_replace(c.table_schema,'^sdv', 'stg') = p.schema_stg
				and c.table_name = p.table_name_stg
			  	where src_stm_id = p_src_stm_id
			  		and now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt
			  		and column_name = 'gk_pk') > 0
			)
			and now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt;
	if v_cnt > 0 then
		if v_tbl_nm is null then
			v_exec_sql := v_exec_sql||' where ssn in ('||v_ssn||')';
		end if;
	  	--return (v_exec_sql);
		v_start_dttm := clock_timestamp() at time zone 'utc';
		begin
			execute v_exec_sql;
			get diagnostics v_cnt = row_count;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '7',
						p_descr := 'Процесс ключевания',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql||'''',
						p_upd_qty := v_cnt::text,
						p_log_tp := '1',
						p_debug_lvl := '3')
				    into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '7',
				p_descr := 'Процесс ключевания',
				p_start_dttm := v_start_dttm,
				p_val := 'v_exec_sql='''||v_exec_sql||'''',
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
		if v_tbl_nm is not null then 
			--ищем партицию в sdv
			for v_rec_ssn in
				(select partitiontablename 
					--into v_partitiontablename
					from pg_catalog.pg_partitions pp
						where lower(pp.schemaname) = split_part(v_tbl_nm_sdv, '.',1)
							and lower(pp.tablename) = split_part(v_tbl_nm_sdv, '.',2)
							and replace(replace(partitionrangestart, '::bigint', ''), '''', '' )--::bigint 
							in (v_ssn)) loop
				
				--собираем статистику
				v_exec_sql := 'analyze '||v_tbl_nm_sdv;
				v_start_dttm := clock_timestamp() at time zone 'utc';  	
				begin	
					execute v_exec_sql;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '8',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
						into v_json_ret;
				exception when others then	
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '8',
							p_descr := 'Сбор статистики',
							p_start_dttm := v_start_dttm,
							p_val := 'v_exec_sql='''||v_exec_sql||'''',
							p_err := SQLERRM,
							p_log_tp := '3',
							p_cls := ']',
							p_debug_lvl := '1')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
			end loop;
		end if; 
	end if;	
	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
    --return (v_exec_sql); 
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		--raise exception '%', sqlerrm;
		raise exception '%', v_json_ret; 
    end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.put_gen_gk_bdv(text);

CREATE OR REPLACE FUNCTION sys_dwh.put_gen_gk_bdv(p_tbl_nm text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
  v_tbl_nm text;
  v_exec_sql text;
  v_where_sql text = '';
  v_start_dttm text;
  v_cnt int8 = 0;
  v_json_param text;
  v_json_where text;

  v_rec_json_param record;
  v_json_ret text = ''; 
 
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		v_tbl_nm := p_tbl_nm;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Проверка наличия таблицы',
			p_start_dttm := v_start_dttm,
			p_val := 'v_tbl_nm='''||v_tbl_nm::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		--Проверяем наличие таблицы на БД
		v_exec_sql := 'select count(1) 
			from  pg_catalog.pg_tables  
				where 1=1
					and lower(schemaname)||''.''||lower(tablename) = '''||v_tbl_nm||'''';
		execute v_exec_sql into v_cnt;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Проверка наличия таблицы',
				p_start_dttm := v_start_dttm,
				p_val := 'v_tbl_nm='''||v_tbl_nm::text||'''',
				p_err := 'Таблица не найдена',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	end;
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Проверка наличия параметров',
			p_start_dttm := v_start_dttm,
			p_val := 'v_tbl_nm='''||v_tbl_nm::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
		--Проверяем наличие параметров на БД
		v_exec_sql := 'select count(1) 
			from sys_dwh.prm_gen_gk_bdv  
				where 1=1
					and tbl_nm = '''||v_tbl_nm||''' and end_dt = ''9999-12-31''::date';
		execute v_exec_sql into v_cnt;
		if v_cnt = 0 or v_cnt > 1 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '2',
				p_descr := 'Проверка наличия параметров',
				p_start_dttm := v_start_dttm,
				p_val := 'v_tbl_nm='''||v_tbl_nm::text||''', v_cnt='''||v_cnt::text||'''',
				p_err := 'Параметры не найдены',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
	end;
	--Парсим json
	begin
		v_start_dttm := clock_timestamp() at time zone 'utc';
		v_exec_sql := 'select json_param 
			from sys_dwh.prm_gen_gk_bdv  
				where tbl_nm = '''||v_tbl_nm::text||''' and end_dt = ''9999-12-31''::date';
      	execute v_exec_sql into v_json_param;
      	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Поиск параметров json',
			p_start_dttm := v_start_dttm,
			p_val := 'v_json_param='''||v_json_param::text||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
      	for v_rec_json_param in (select tbl_nm_ref, col_nm, col_list, json_where 
      	from json_populate_recordset(null::record, v_json_param::json)
			as (tbl_nm_ref text, col_nm text, col_list text, json_where json
			))
			loop
				v_start_dttm := clock_timestamp() at time zone 'utc';
				v_where_sql := '';
				v_json_where := '['||v_rec_json_param.json_where::text||']';
				select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '4',
							p_descr := 'Парсинг json_where-правила',
							p_start_dttm := v_start_dttm,
							p_val := 'json_where='''||v_json_where::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
				v_exec_sql := 'update '||v_tbl_nm||' set '||v_rec_json_param.col_nm||' = ';
				
				v_exec_sql := v_exec_sql||'md5('''||v_rec_json_param.tbl_nm_ref||'~''||coalesce('||replace(v_rec_json_param.col_list,',','::text,'''')|| coalesce(')||'::text,''''))::uuid';
				--if v_json_where is not null then
					--Парсим правила в переменную
					begin
						v_start_dttm := clock_timestamp() at time zone 'utc';
						with sel as
						(select num, predicate, fld_nm , equal_type, val, func_nm, where_type, grp from json_populate_recordset(null::record, v_json_where::json)
									as (num int4, predicate text, fld_nm text, equal_type text, val text, func_nm text, where_type int4, grp int4)),
						sel_2 as (select 
						(case when num = 1 then predicate||' (' else predicate||' ' end)||
						(case when where_type = 1 then ' '||fld_nm when where_type = 2 then func_nm end)||' '||
						' '||equal_type||' '||val||
						(case when lead(num) over(partition by grp order by num) is null then ')' else '' end) as f
						from sel)
						select ' and 1=1'||E' '||string_agg(f, E' ')||E' ' into v_where_sql
						from sel_2;
						
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '5',
							p_descr := 'Парсинг where-правила',
							p_start_dttm := v_start_dttm,
							p_val := 'v_where_sql='''||v_where_sql::text||'''',
							p_log_tp := '1',
							p_debug_lvl := '3')
							into v_json_ret;
					exception when others then
						select sys_dwh.get_json4log(p_json_ret := v_json_ret,
							p_step := '5',
							p_descr := 'Парсинг where-правила',
							p_start_dttm := v_start_dttm,
							p_err := SQLERRM,
							p_log_tp := '3',
							p_debug_lvl := '1',
							p_cls := ']')
						into v_json_ret;
						raise exception '%', v_json_ret;
					end;
				--end if;
				v_where_sql := coalesce(v_where_sql,'');
				v_exec_sql := v_exec_sql||' where 1=1 '||v_where_sql;
				begin
					v_start_dttm := clock_timestamp() at time zone 'utc';
					execute v_exec_sql;
					get diagnostics v_cnt = row_count;
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '6',
								p_descr := 'Процесс ключевания',
								p_start_dttm := v_start_dttm,
								p_val := 'v_exec_sql='''||v_exec_sql||''', v_cnt='''||v_cnt::text||'''',
								p_upd_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '3')
						    into v_json_ret;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '6',
						p_descr := 'Процесс ключевания',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
					into v_json_ret;
					raise exception '%', v_json_ret;
				end;
				
				
			end loop;
		--собираем статистику
		v_exec_sql := 'analyze '||v_tbl_nm;
		v_start_dttm := clock_timestamp() at time zone 'utc';  	
		begin	
			execute v_exec_sql;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '7',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_log_tp := '1',
					p_debug_lvl := '3')
				into v_json_ret;
		exception when others then	
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '7',
					p_descr := 'Сбор статистики',
					p_start_dttm := v_start_dttm,
					p_val := 'v_exec_sql='''||v_exec_sql||'''',
					p_err := SQLERRM,
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
			
	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
    --return (v_exec_sql); 
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		--raise exception '%', sqlerrm;
		raise exception '%', v_json_ret; 
    end;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.put_prm_src_stm_s2t_stg(bool);

CREATE OR REPLACE FUNCTION sys_dwh.put_prm_src_stm_s2t_stg(debug bool DEFAULT false)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	



DECLARE

	v_rec_s2t record;

	v_cnt int8 := 0;

	v_cnt_upd int8 := 0;

	v_load_mode int4 := 0;

	v_day_del int2 := 0;

/*	v_pxf_string varchar(255);

hf:core19_2*/

	v_prn_src_stm text; /*hf:core19_2*/

	v_db_src_stm text := ''; /*hf:core19_2*/

	v_ext_pb varchar(50);

	v_ext_pb_itrv text;

	v_act_dt_column varchar(50);

	v_ext_location text;

	v_ext_format text;

	v_ext_encoding text;



BEGIN

--Проверки:



--получаем записи необходимых полей источника

FOR v_rec_s2t IN(

	SELECT

		src_stm_id,

		schema_src,

		table_name_src,

		schema_stg,

		table_name_stg,

/*		pxf_name_src,

hf:core19_2*/

		ext_pb,

		ext_pb_itrv,

		act_dt_column,

		ext_format,

		ext_encoding

	FROM sys_dwh.prm_s2t_stg_src

	WHERE now() at time zone 'utc' >= eff_dt and now() at time zone 'utc' <= end_dt

)LOOP

	--проверяем соответствие src_stm_id,schema_stg,table_name_stg

	SELECT COUNT(*)

	INTO v_cnt

	FROM sys_dwh.prm_src_stm p

	 JOIN sys_dwh.prm_src_stm pp ON p.prn_src_stm_id = pp.src_stm_id AND now() BETWEEN pp.eff_dt AND pp.end_dt

	WHERE v_rec_s2t.src_stm_id = p.src_stm_id

	 AND now() BETWEEN p.eff_dt AND p.end_dt

	 AND v_rec_s2t.schema_stg='stg_'||pp.nm

	 AND v_rec_s2t.table_name_stg=p.nm;

	IF v_cnt<>1 THEN

	  BEGIN

	--	RETURN('Error: src_stm_id does not satisfy to schema_stg.table_name_stg');

		RAISE EXCEPTION '%', 'src_stm_id = '||v_rec_s2t.src_stm_id||' does not satisfy to schema_stg.table_name_stg';

	  END; 

	END IF;



/*	--проверяем pxf_name_src

	IF (NULLIF(v_rec_s2t.pxf_name_src,'') IS NULL

		AND LEFT(v_rec_s2t.schema_src,7)<>'file://'

		AND v_rec_s2t.schema_src<>v_rec_s2t.schema_stg

	   )THEN

	  BEGIN

	--	RETURN('Error: pxf_name_src is empty');

		RAISE EXCEPTION '%', 'Error: pxf_name_src is empty';

	  END; 

	END IF;

hf:core19_2*/



	--получаем имя источника /*hf:core19_2*/

	select pp.nm

	into v_prn_src_stm

	from sys_dwh.prm_src_stm p

	 join sys_dwh.prm_src_stm pp on p.prn_src_stm_id = pp.src_stm_id and now() at time zone 'utc' >= pp.eff_dt and now() at time zone 'utc' <= pp.end_dt

	where now() at time zone 'utc' >= p.eff_dt and now() at time zone 'utc' <= p.end_dt

	 and p.src_stm_id = v_rec_s2t.src_stm_id;



	--получаем имя бд источника для pxf /*hf:core19_2*/

	IF v_rec_s2t.schema_src<>v_rec_s2t.schema_stg THEN

		BEGIN

			select sys_service.get_cctn(v_prn_src_stm)

			into v_db_src_stm;

			exception when others then

			raise exception '%', v_prn_src_stm;

		END;

	END IF;



	--собираем значения в параметры

	SELECT

		--Меняем режим загрузки с 0 на 1 для таблиц, по которым описаны поля в S2T

		COALESCE((

			SELECT CASE WHEN p.load_mode=0 THEN 1 ELSE p.load_mode END

			FROM sys_dwh.prm_src_stm p

			WHERE p.src_stm_id = v_rec_s2t.src_stm_id

			 AND now() BETWEEN p.eff_dt AND p.end_dt

		),0),

		--Меняем глубину хранения с 0 на 3 для таблиц, по которым описаны поля в S2T

		COALESCE((

			SELECT CASE WHEN p.day_del=0 THEN 3 ELSE p.day_del END

			FROM sys_dwh.prm_src_stm p

			WHERE p.src_stm_id = v_rec_s2t.src_stm_id

			 AND now() BETWEEN p.eff_dt AND p.end_dt

		),0),

/*		--Убрать это поле в ближайшем релизе!!!

		'pxf://'||v_rec_s2t.schema_src||'.'||v_rec_s2t.table_name_src||'?PROFILE=JDBC&SERVER='||v_rec_s2t.pxf_name_src,	--pxf://dbo.tobject?PROFILE=JDBC&SERVER=skb_daily

hf:core19_2*/

		(	SELECT	v_rec_s2t.ext_pb||

				CASE

					WHEN lower(datatype_src) LIKE'num%' OR lower(datatype_src) LIKE'%int%' THEN ':int'

					WHEN lower(datatype_src) LIKE'%date%' OR lower(datatype_src) LIKE'%time%' THEN ':date'

				END

			FROM sys_dwh.prm_s2t_stg s

			WHERE s.src_stm_id = v_rec_s2t.src_stm_id

			 AND s.column_name_src = v_rec_s2t.ext_pb

			 AND now() BETWEEN s.eff_dt AND s.end_dt),	--ObjectID:int

		NULLIF(v_rec_s2t.ext_pb_itrv,''),

		NULLIF(v_rec_s2t.act_dt_column,''),

		CASE

			WHEN v_rec_s2t.schema_src=v_rec_s2t.schema_stg

				THEN NULL

			WHEN LEFT(v_rec_s2t.schema_src,7)='file://'

				THEN '('''||v_rec_s2t.schema_src||v_rec_s2t.table_name_src||''') ON ALL'	--('file://ad-data01-t/mnt/ptk/PTK_101/*.TEL') ON all

		    WHEN lower(v_rec_s2t.schema_src)='pxf'

		    	then '(''pxf://query:'||v_rec_s2t.schema_src||'_ini_'||v_db_src_stm||'_'||replace(v_rec_s2t.table_name_src, '.', '_')||'?PROFILE=JDBC&SERVER='||v_db_src_stm/*hf:core19_2*/||''') ON ALL'

			ELSE '(''pxf://'||v_rec_s2t.schema_src||'.'||v_rec_s2t.table_name_src||'?PROFILE=JDBC&SERVER='||v_db_src_stm/*hf:core19_2*/||''') ON ALL'	--('pxf://dbo.tcurrency?PROFILE=JDBC&SERVER=skb_daily') ON ALL

		END,

		CASE

			WHEN v_rec_s2t.schema_src=v_rec_s2t.schema_stg

				THEN NULL

			ELSE COALESCE(NULLIF(v_rec_s2t.ext_format,''),'''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )')	--'CUSTOM' ( FORMATTER='pxfwritable_import' )

		END,

		CASE

			WHEN v_rec_s2t.schema_src=v_rec_s2t.schema_stg

				THEN NULL

			ELSE COALESCE(NULLIF(v_rec_s2t.ext_encoding,''),'''UTF8''')	--'UTF8'

		END

	INTO

		v_load_mode,

		v_day_del,

/*		v_pxf_string,

hf:core19_2*/

		v_ext_pb,

		v_ext_pb_itrv,

		v_act_dt_column,

		v_ext_location,

		v_ext_format,

		v_ext_encoding;

	

	--фиксируем в случае различия

	IF debug = true THEN --режим отладки

		SELECT COUNT(*)

		INTO v_cnt

		FROM sys_dwh.prm_src_stm p

		WHERE p.src_stm_id = v_rec_s2t.src_stm_id

		 AND now() BETWEEN p.eff_dt AND p.end_dt

		 AND(

			NOT(COALESCE(load_mode,-1)=v_load_mode) OR

			NOT(COALESCE(day_del,-1)=v_day_del) OR

/*			NOT(COALESCE(pxf_string,'')=v_pxf_string) OR

hf:core19_2*/

			NOT(COALESCE(ext_pb,'')=COALESCE(v_ext_pb,'')) OR

			NOT(COALESCE(ext_pb_itrv,'')=COALESCE(v_ext_pb_itrv,'')) OR

			NOT(COALESCE(act_dt_column,'')=COALESCE(v_act_dt_column,'')) OR

			NOT(COALESCE(ext_location,'')=COALESCE(v_ext_location,'')) OR

			NOT(COALESCE(ext_format,'')=COALESCE(v_ext_format,'')) OR

			NOT(COALESCE(ext_encoding,'')=COALESCE(v_ext_encoding,'')) );

		v_cnt_upd := v_cnt_upd + v_cnt;

	ELSE

		UPDATE sys_dwh.prm_src_stm p

		SET

			upd_dttm = now(),

			load_mode = v_load_mode,

			day_del = v_day_del,

/*			pxf_string = v_pxf_string,

hf:core19_2*/

			ext_pb = v_ext_pb,

			ext_pb_itrv = v_ext_pb_itrv,

			act_dt_column = v_act_dt_column,

			ext_location = v_ext_location,

			ext_format = v_ext_format,

			ext_encoding = v_ext_encoding

		WHERE p.src_stm_id = v_rec_s2t.src_stm_id

		 AND now() BETWEEN p.eff_dt AND p.end_dt

		 AND(

			NOT(COALESCE(load_mode,-1)=v_load_mode) OR

			NOT(COALESCE(day_del,-1)=v_day_del) OR

/*			NOT(COALESCE(pxf_string,'')=v_pxf_string) OR

hf:core19_2*/

			NOT(COALESCE(ext_pb,'')=COALESCE(v_ext_pb,'')) OR

			NOT(COALESCE(ext_pb_itrv,'')=COALESCE(v_ext_pb_itrv,'')) OR

			NOT(COALESCE(act_dt_column,'')=COALESCE(v_act_dt_column,'')) OR

			NOT(COALESCE(ext_location,'')=COALESCE(v_ext_location,'')) OR

			NOT(COALESCE(ext_format,'')=COALESCE(v_ext_format,'')) OR

			NOT(COALESCE(ext_encoding,'')=COALESCE(v_ext_encoding,'')) );

		GET DIAGNOSTICS v_cnt = ROW_COUNT;

		v_cnt_upd := v_cnt_upd + v_cnt;

	END IF;



END LOOP;





	RETURN('cnt_upd='||v_cnt_upd);



	--Регистрируем ошибки

	EXCEPTION

	WHEN OTHERS THEN 

	--RETURN('Error: ' || SQLERRM);

	RAISE EXCEPTION '%', 'Error: ' || SQLERRM;   



END;




$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.put_undbls(text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.put_undbls(p_tbl_nm text, p_col_pk text, p_col_unreg text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
 
declare
	v_cnt bigint = 0;
	v_start_dttm text;

	v_rec_col record;

	v_col text = '';
	v_col_sql text = '';
	v_col_ins_sql text = '';
	v_row_hash text = '';
	v_tmp_tbl text = '';

	v_sql text = '';
	v_exec_sql text = '';
	v_sql_back text = '';
  
  	v_json_ret text = '';
	err_code text; -- код ошибки
  	msg_text text; -- текст ошибки
  	exc_context text; -- контекст исключения
  	msg_detail text; -- подробный текст ошибки
  	exc_hint text; -- текст подсказки к исключению
begin
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--проверка, является ли таблица исторической
	select count(attname)
		into v_cnt
		from   pg_attribute a
			where  attrelid = p_tbl_nm::regclass
			--where  attrelid = 'rdv_skb_3card.s_ao_acc'::regclass
				and    not attisdropped
				and    attnum > 0
				and    attname in ('eff_dt', 'end_dt');
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверка на историчность',
		p_start_dttm := v_start_dttm,
		p_val := 'v_cnt='''||v_cnt||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
	if v_cnt = 0 then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Проверка на историчность',
			p_start_dttm := v_start_dttm,
			p_val := 'Warning: Таблица не историчная',
			p_log_tp := '2',
			p_debug_lvl := '1',
			p_cls := ']')
	    into v_json_ret;
		return(v_json_ret);
	end if;
  exception when others then
      select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверка на историчность',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
      into v_json_ret;
	  raise exception '%', v_json_ret;
  end;
  
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--собираем метаданные
	--Открываем цикл по полям
	for v_rec_col in ( 
		select attname, format_type(atttypid, atttypmod) as ctype
			from   pg_attribute a
				where  attrelid = p_tbl_nm::regclass
				--where  attrelid = 'rdv_skb_3card.s_ao_acc'::regclass
					and    not attisdropped
					and    attnum > 0
					and attname not in ('eff_dt', 'end_dt', 'upd_dttm')
				order by atttypid)
	loop 
		if v_rec_col.ctype = 'json' then
			v_col := v_col||v_rec_col.attname||', ';
			v_col_sql := v_col_sql||v_rec_col.attname||'::text, ';
			v_col_ins_sql := v_col_ins_sql||v_rec_col.attname||'::json, ';
		else
			v_col := v_col||v_rec_col.attname||', ';
			v_col_sql := v_col_sql||v_rec_col.attname||', ';
			v_col_ins_sql := v_col_ins_sql||v_rec_col.attname||', ';
		end if;
		if v_rec_col.attname in (coalesce(p_col_unreg,'')) then
			v_row_hash := v_row_hash||' lower(coalesce('||v_rec_col.attname||'::text, '''')) || ''!~@#'' ||';
		else
			v_row_hash := v_row_hash||' coalesce('||v_rec_col.attname||'::text, '''') || ''!~@#'' ||';
		end if;
	end loop;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Сборка метаданных',
		p_start_dttm := v_start_dttm,
		p_val := 'v_col='''||v_col||''', v_row_hash='''||v_row_hash||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
      select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Сборка метаданных',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
      into v_json_ret;
	  raise exception '%', v_json_ret;
  end;
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--Создаём таблицу, переносим туда данные
	--execute 'select '''||p_tbl_nm||'_tmp_undbls_hist_''||to_char(CURRENT_DATE, ''YYYYMMDD'')::text' into v_tmp_tbl;
	execute 'select '''||p_tbl_nm||'_tmp_undbls_hist_''||to_char(now()  at time zone ''utc'', ''YYYYMMDDHH24MISS'')::text' into v_tmp_tbl;
	
	v_exec_sql := 'drop table if exists '||v_tmp_tbl;
	execute v_exec_sql;
	v_exec_sql := 'create table '||v_tmp_tbl||' (like '||p_tbl_nm||' including all)';
	execute v_exec_sql;
	
	v_exec_sql := 'insert into '||v_tmp_tbl||' ('||v_col||' eff_dt, end_dt, upd_dttm) 
		select '||v_col||' eff_dt, end_dt, upd_dttm
			from '||p_tbl_nm;
	execute v_exec_sql;
	get diagnostics v_cnt = row_count;
	v_exec_sql := 'truncate table '||p_tbl_nm;
	execute v_exec_sql;
	v_exec_sql := 'analyze '||p_tbl_nm;
	execute v_exec_sql;
	v_exec_sql := 'analyze '||v_tmp_tbl;
	execute v_exec_sql;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Создание новой таблицы, перенос данных',
		p_start_dttm := v_start_dttm,
		p_val := 'v_cnt='''||v_cnt::text||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
      select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Создание новой таблицы, перенос данных',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
      into v_json_ret;
	  raise exception '%', v_json_ret;
  end;
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--план отката
	v_sql_back := 'do
	$do$
	DECLARE 
	 v_cnt int8; 
	begin
		select count(1) from '||v_tmp_tbl||' into v_cnt;
		if v_cnt > 0 then
			truncate table '||p_tbl_nm||';
			insert into '||p_tbl_nm||' ('||v_col||' eff_dt, end_dt, upd_dttm) 
		select '||v_col||' eff_dt, end_dt, upd_dttm
			from '||v_tmp_tbl||';
		end if;
	end;
	$do$;';
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'План отката',
		p_start_dttm := v_start_dttm,
		p_val := 'v_sql_back='''||v_sql_back||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  end;	
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	--сборка sql запроса
	
	v_sql := 'insert into '||p_tbl_nm||' ('||v_col||' eff_dt, end_dt, upd_dttm) ';
	v_sql := v_sql||'select '||v_col_ins_sql||' 
	min(foo3.eff_dt) as eff_dt,
	max(foo3.end_dt) as end_dt,
	now() at time zone ''utc'' as upd_dttm
	from ( 
		select '||v_col||' eff_dt, end_dt, upd_dttm, row_hash, row_hash_prev,
			max(foo2.eff_dt)
			filter(where foo2.row_hash != foo2.row_hash_prev)
			over(partition by foo2.row_hash order by foo2.eff_dt rows between unbounded preceding and current row) eff_dt_min_in_group
		from (
			select '||v_col||' eff_dt, end_dt, upd_dttm, row_hash,
				case
                	when lag(foo.end_dt+1) over(partition by '||p_col_pk||' order by foo.eff_dt) = foo.eff_dt
                    	then lag(foo.row_hash, 1, md5(''!@#'')::uuid) over(partition by '||p_col_pk||' order by foo.eff_dt)
                    else md5(''!@#'')::uuid
                end as row_hash_prev
			from (
				select '||v_col_sql||' eff_dt, end_dt, upd_dttm,  md5('||v_row_hash||'1)::uuid row_hash
				from '||v_tmp_tbl||'
					) foo
			) foo2
		) foo3
	group by 
		'||v_col||' eff_dt_min_in_group';
  select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'Сборка sql запроса',
		p_start_dttm := v_start_dttm,
		p_val := 'v_sql='''||v_sql||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
    into v_json_ret;
  exception when others then
      select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'Сборка sql запроса',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
      into v_json_ret;
	  raise exception '%', v_json_ret;
  end;
  begin	
	v_start_dttm := clock_timestamp() at time zone 'utc';
	
	execute v_sql;
	get diagnostics v_cnt = row_count;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
								p_step := '6',
								p_descr := 'Дедупликация',
								p_start_dttm := v_start_dttm,
								p_val := 'v_sql='''||v_sql::text||'''',
								p_ins_qty := v_cnt::text,
								p_log_tp := '1',
								p_debug_lvl := '2')
								into v_json_ret;
  exception when others then
      select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '6',
		p_descr := 'Дедупликация',
		p_start_dttm := v_start_dttm,
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
      into v_json_ret;
	  raise exception '%', v_json_ret;
  end;
  
 --Возвращаем результат выполнения           
	v_json_ret := v_json_ret||']';
    return(v_json_ret);   
    --Регистрируем ошибки
    exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
					p_step := '0',
					p_descr := 'Фатальная ошибка',
					p_start_dttm := v_start_dttm,
					
					p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
					p_log_tp := '3',
					p_cls := ']',
					p_debug_lvl := '1')
				into v_json_ret;
		
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;   
    end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.set_log_dwh(text, text, text, text, text, text, text, timestamp, timestamp, int8, int8, int8, int8, text, int2, int2, json, varchar, text);

CREATE OR REPLACE FUNCTION sys_dwh.set_log_dwh(p_job_id text, p_job_nm text, p_job_prm text, p_step_id text, p_step_nm text, p_step_prm text, p_descr text, p_start_dttm timestamp, p_end_dttm timestamp, p_ins_qty int8, p_upd_qty int8, p_del_qty int8, p_rej_qty int8, p_err text, p_log_tp int2, p_debug_lvl int2, p_ret_json json, p_env_nm varchar, p_env_runtime text DEFAULT 'airflow'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

	   

declare

      begin

INSERT INTO sys_dwh.log_dwh

(job_id, job_nm, job_prm, step_id, step_nm, step_prm, descr, start_dttm, end_dttm, ins_qty, upd_qty, del_qty, rej_qty, err, log_tp, debug_lvl, ret_json, env_nm, env_runtime)

VALUES(p_job_id::text, p_job_nm::text, p_job_prm::text, p_step_id::text, p_step_nm::text, p_step_prm::text, p_descr::text, p_start_dttm::timestamp, p_end_dttm::timestamp, p_ins_qty::int8, p_upd_qty::int8, p_del_qty::int8, p_rej_qty::int8, p_err::text, p_log_tp::int2, p_debug_lvl::int2, p_ret_json::json, p_env_nm::varchar(20), p_env_runtime::text);



      --commit;

    return('Success: '||p_job_id);   

    --Регистрируем ошибки

    exception

    when others then 

    return('Error: '||sqlerrm);

    raise exception '%', 'Error: '||sqlerrm;   

    end;

 


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.set_task_prev_rdv(text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.set_task_prev_rdv(p_rdv_table_name text, p_type_of_changes text, p_type_of_task text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
	v_exec_sql text;
	--v_rec record;
	v_json_ret text = ''; --переменная для return

  v_start_dttm text; --время старта шага
  v_cnt bigint;
 
  err_code text; -- код ошибки
  msg_text text; -- текст ошибки
  exc_context text; -- контекст исключения
  msg_detail text; -- подробный текст ошибки
  exc_hint text; -- текст подсказки к исключению
begin
	begin   
		v_start_dttm := clock_timestamp() at time zone 'utc';
		v_exec_sql := 'update sys_dwh.prm_task s 
				set success_fl = true, dttm_end = now() at time zone ''utc'' where 
	 		active_fl = true and success_fl = false and table_name = '''||p_rdv_table_name||'''
			and type_of_changes = '''||p_type_of_changes||''' and type_of_task = '''||p_type_of_task||'''
			and (select success_fl from sys_dwh.prm_task t where t.id_task=s.id_task and t.type_of_task = ''master_master'')';
		
		execute v_exec_sql;
		get diagnostics v_cnt = row_count;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '1',
						p_descr := 'Закрытие заданий',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||''', v_cnt='''||v_cnt::text||'''',
						p_log_tp := '1',
						p_debug_lvl := '3')
						into v_json_ret;
					execute v_exec_sql;
				exception when others then
					select sys_dwh.get_json4log(p_json_ret := v_json_ret,
						p_step := '1',
						p_descr := 'Закрытие заданий',
						p_start_dttm := v_start_dttm,
						p_val := 'v_exec_sql='''||v_exec_sql::text||'''',
						p_err := SQLERRM,
						p_log_tp := '3',
						p_debug_lvl := '1',
						p_cls := ']')
				    into v_json_ret;
					raise exception '%', v_json_ret;
				end;	
	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
  --Регистрируем ошибки
  exception
      when others then
		GET STACKED DIAGNOSTICS
			err_code = RETURNED_SQLSTATE, -- код ошибки
			msg_text = MESSAGE_TEXT, -- текст ошибки
			exc_context = PG_CONTEXT, -- контекст исключения
			msg_detail = PG_EXCEPTION_DETAIL, -- подробный текст ошибки
			exc_hint = PG_EXCEPTION_HINT; -- текст подсказки к исключению
		if v_json_ret is null then
			v_json_ret := '';
		end if;
		v_json_ret := regexp_replace(v_json_ret, ']$', '');
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '0',
				p_descr := 'Фатальная ошибка',
				p_start_dttm := v_start_dttm,
				
				p_err := 'ERROR CODE: '||coalesce(err_code,'')||' MESSAGE TEXT: '||coalesce(msg_text,'')||' CONTEXT: '||coalesce(exc_context,'')||' DETAIL: '||coalesce(msg_detail,'')||' HINT: '||coalesce(exc_hint,'')||'',
				p_log_tp := '3',
				p_cls := ']',
				p_debug_lvl := '1')
			into v_json_ret;
		--Удаляем временную таблицу с изменениями, если она уже есть на БД
		if v_table_name_rdv is not null then
				begin 
					v_exec_sql := 'drop table if exists rdv_tmp.tmp_'||replace(v_tbl_nm_sdv,'.','_')||'_'||v_table_name_rdv;
					execute v_exec_sql;
				end;
		end if;
		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		end if;
		raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.splt_dflt_prttn(text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.splt_dflt_prttn(p_scheme_name text, p_table_name text, p_column_name text DEFAULT 'created_at'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare 
	v_table text;
	v_cnt int8 := 0;
	v_dml text;
	v_ddl text;
	v_rec_p record;
	v_json_ret text = '';
	v_start_dttm text;

begin
	begin
		v_start_dttm := timeofday()::timestamp;
		--Фиксируем полное наименование таблицы
		v_table := p_scheme_name||'.'||p_table_name;

		--Проверяем наличие таблицы на БД
		select count(1) into v_cnt 
			from  pg_catalog.pg_tables  
				where 1=1
					and lower(tablename) = p_table_name
					and lower(schemaname) = p_scheme_name;
		if v_cnt = 0 then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '1',
				p_descr := 'Поиск метаданных',
				p_start_dttm := v_start_dttm,
				p_val := 'v_cnt='''||v_cnt||'''',
				p_err := 'The target table '||v_table||' is missing on DB',
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск метаданных',
			p_start_dttm := v_start_dttm,
			p_val := 'v_table='''||v_table||'''',
			p_log_tp := '1',
			p_debug_lvl := '3')
		into v_json_ret;
	exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Поиск метаданных',
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_debug_lvl := '1',
			p_cls := ']')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;

	--Создаем временную таблицу с датами из дефолтной партиции
	
	begin
		v_start_dttm := timeofday()::timestamp;
	v_dml := '
	create temp table tmp_tbl_1_prt_outlying_dates on commit drop as
	select
	 to_char(' || p_column_name || ',''YYYY-MM-DD'') start_p,
	 to_char(' || p_column_name || '::date+1,''YYYY-MM-DD'') end_p,
	 to_char(' || p_column_name || ',''YYYYMMDD'') name_p,
	 count(*) cnt_p
	from '||v_table||'_1_prt_outlying_dates
	group by
	 to_char(' || p_column_name || ',''YYYY-MM-DD''),
	 to_char(' || p_column_name || '::date+1,''YYYY-MM-DD''),
	 to_char(' || p_column_name || ',''YYYYMMDD'');';
		execute v_dml;
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Создаем временную таблицу с датами из дефолтной партиции',
		p_start_dttm := v_start_dttm,
		p_val := 'v_dml='''||v_dml::text||'''',
		p_log_tp := '1',
		p_debug_lvl := '3')
	into v_json_ret;
	exception when others then
	select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Создаем временную таблицу с датами из дефолтной партиции',
		p_start_dttm := v_start_dttm,
		p_val := 'v_dml='''||v_dml::text||'''',
		p_err := SQLERRM,
		p_log_tp := '3',
		p_debug_lvl := '1',
		p_cls := ']')
	into v_json_ret;
	raise exception '%', v_json_ret;
	end;

	if exists(select * from tmp_tbl_1_prt_outlying_dates)then
	--Открываем цикл сплитования новых партиций
	for v_rec_p in
		(	select
			 start_p,
			 end_p,
			 name_p,
			 cnt_p
			from tmp_tbl_1_prt_outlying_dates
			order by start_p)

	--Динамически сплитуем партиции
	loop
		begin
			v_start_dttm := timeofday()::timestamp;
			if v_rec_p.cnt_p > 0 then
			v_ddl := '
	ALTER TABLE '||v_table||' SPLIT DEFAULT PARTITION 
	START ('''||v_rec_p.start_p||''') INCLUSIVE 
	END ('''||v_rec_p.end_p||''') EXCLUSIVE 
	INTO (PARTITION "'||v_rec_p.name_p||'", DEFAULT PARTITION);';
			execute v_ddl;
			end if;
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '3',
				p_descr := 'Динамически сплитуем партиции',
				p_start_dttm := v_start_dttm,
				p_ins_qty := v_rec_p.cnt_p::text,
				p_val := 'v_ddl='''||v_ddl::text||'''',
				p_log_tp := '1',
				p_debug_lvl := '3')
			into v_json_ret;
		exception when others then
			select sys_dwh.get_json4log(p_json_ret := v_json_ret,
				p_step := '3',
				p_descr := 'Динамически сплитуем партиции',
				p_start_dttm := v_start_dttm,
				p_val := 'v_ddl='''||v_ddl::text||'''',
				p_err := SQLERRM,
				p_log_tp := '3',
				p_debug_lvl := '1',
				p_cls := ']')
			into v_json_ret;
			raise exception '%', v_json_ret;
		end;
	end loop;
	end if;

	--Собираем статистику
	begin
		v_start_dttm := timeofday()::timestamp;
		execute 'analyze '||v_table;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Сбор статистики',
			p_start_dttm := v_start_dttm,
			p_val := 'analyze '||v_table,
			p_log_tp := '1',
			p_debug_lvl := '3')
				    into v_json_ret;
	exception when others then	
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'Сбор статистики',
			p_start_dttm := v_start_dttm,
			p_val := 'analyze '||v_table,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
	end;


	v_json_ret := v_json_ret||']';
    return(v_json_ret); 
    --return (v_exec_sql); 
    --Регистрируем ошибки
    exception
      when others then
		  if right(v_json_ret, 1) <> ']' and v_json_ret is not null then
			v_json_ret := v_json_ret||']';
		  end if;
      raise exception '%', v_json_ret;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.srch_bdv_prttn(_text, text, text, text, bool, text);

CREATE OR REPLACE FUNCTION sys_dwh.srch_bdv_prttn(p_src_table _text, p_trgt_table text, p_start_range text, p_end_range text, p_ignore_empty_prttn bool DEFAULT true, p_call_func text DEFAULT 'merge'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_rslt text = 'функция не была запущена';
v_prttns_list text;
v_prttns_list_all_tables text;
v_prttns_list_full text = '';
v_prttns_list_try_empty text;
v_src_tablename text;
v_prttn text;
v_src_schema text;
v_src_table text;
v_cnt int8 := 0;
v_prttn_level int8;
v_output_text text;
v_start_dttm text;
v_json_ret text := '';
v_start_arr_len int8;
v_end_arr_len int8;
v_prttn_rec record;
v_start_range text[] := '{'||p_start_range||'}';
v_end_range text[] := '{'||p_end_range||'}';
 begin
	--Проверяем, что массивы начала и конца партиций имеют один размер
	begin
		raise notice '%', p_start_range;
		execute 'select array_length(array['||p_start_range||'], 1)' into v_start_arr_len;
		execute 'select array_length(array['||p_end_range||'], 1)' into v_end_arr_len;
		if v_start_arr_len <> v_end_arr_len then
			v_output_text := 'Размер p_start_range='||v_start_arr_len||' не равен размеру P_end_range='||v_end_arr_len;
			raise exception '%', v_output_text;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Проверяем, что массивы начала и конца партиций имеют один размер',
		p_val := 'v_prttn_level='||v_prttn_level|| ' v_prttns_list = '||v_prttns_list,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Проверяем, что массивы начала и конца партиций имеют один размер',
			p_val := 'v_prttn_level='||v_prttn_level || ' v_prttns_list = '||v_prttns_list,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	--открываю цикл по таблицам источникам
	for v_src_table in select unnest(p_src_table)
    loop
	 FOR v_prttn_rec IN SELECT unnest(v_start_range) AS num1, unnest(v_end_range) AS num2 
	 LOOP
	--Поиск уровня партиций
	begin
		select split_part(v_src_table,'.', 1) into v_src_schema;
		select split_part(v_src_table,'.', 2) into v_src_tablename;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		if v_prttn_rec.num1 like '%::date%' and v_prttn_rec.num2 like '%::date%' then
			v_prttn_level := 1;
		else
			v_prttn_level := 0;
		end if;
		if v_prttn_level = 0 then
			v_prttn_rec.num1:=v_prttn_rec.num1::int;
			v_prttn_rec.num2:= v_prttn_rec.num2::int;
			
    end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Поиск уровня партиций',
		p_val := 'v_prttn_level='||v_prttn_level,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Поиск уровня партиций	',
			p_val := 'v_prttn_level='||v_prttn_level,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	--Собираем партиции
	begin	
		if v_prttn_level = 1 then
		execute  'select string_agg(partitiontablename, '''''','''''') 
		from pg_catalog.pg_partitions where  partitionlevel = 1  and schemaname||''.''||tablename = '''||v_src_table||''' 
		and (substring(partitionrangestart from 1 for 11)::date >= '||v_prttn_rec.num1||'::DATE and substring(partitionrangeend from 1 for 11)::date <= '||v_prttn_rec.num2||'::date)'
		into v_prttns_list;
		elsif v_prttn_level = 0 then
		select string_agg(partitiontablename, ''',''')  into v_prttns_list from pg_catalog.pg_partitions
		where  schemaname||'.'||tablename = v_src_table
		and case when partitionrangestart~'^[0-9]+$' then cast(partitionrangestart as integer) else null end >= v_prttn_rec.num1::int8
		and case when partitionrangeend~'^[0-9]+$' then cast(partitionrangeend as integer) else null end <= v_prttn_rec.num2::int8;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Собираем партиции',
		p_val := 'v_prttn_level='||v_prttn_level|| ' v_prttns_list = '||v_prttns_list,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Собираем партиции	',
			p_val := 'v_prttn_level='||v_prttn_level || ' v_prttns_list = '||v_prttns_list,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	raise notice '%', v_prttns_list;
	--проверка на пустые партиции
	begin	
		v_prttns_list_try_empty = replace(v_prttns_list, '''','');
		v_prttns_list := v_prttns_list ||''',''';
		v_prttns_list_all_tables := '';
		if p_ignore_empty_prttn = true then
		for v_prttn in select unnest(string_to_array(v_prttns_list_try_empty,','))
		loop
			execute 'select count(*) from '||v_src_schema || '.' || v_prttn ||';' into v_cnt;
			if v_cnt > 0 then
				v_prttns_list_all_tables := v_prttns_list_all_tables || v_prttn||''',''';
				raise notice 'v_prttns_list = %', v_prttns_list_all_tables;
			end if;
		end loop;
		v_prttns_list := v_prttns_list_all_tables;
		end if;
		if p_call_func = 'truncate' then
			v_prttns_list_try_empty = replace(v_prttns_list, '''','');
			v_prttns_list_try_empty := RTRIM(v_prttns_list_try_empty,',');
			raise notice 'v_prttns_list_try_empty = %', v_prttns_list_try_empty;
			v_prttns_list_all_tables := '';
			for v_prttn in select unnest(string_to_array(v_prttns_list_try_empty,',')) loop
				v_prttns_list_all_tables := v_prttns_list_all_tables || v_src_schema || '.' || v_prttn || ''',''';
				raise notice 'v_prttns_list_all_tables = %', v_prttns_list_all_tables;
				raise notice 'v_prttn = %', v_prttn;
			end loop;
		v_prttns_list := v_prttns_list_all_tables;
		end if;
		v_prttns_list_full := v_prttns_list_full || v_prttns_list;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'проверка на пустые партиции',
		p_val := 'v_prttn_level='||v_prttn_level,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'проверка на пустые партиции',
			p_val := 'v_prttn_level='||v_prttn_level,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	end loop;
	end loop;
	--запуск функции
	begin
		v_prttns_list_full := RTRIM(v_prttns_list_full,''',''');
		raise notice 'v_prttns_list_full=%', v_prttns_list_full;
		if v_prttns_list_full <> '' then 
		if p_call_func = 'merge' then
		v_output_text = 'select sys_dwh.mrg_bdv_prttn('''||v_src_table||''', '''||p_trgt_table||''',array ['''||v_prttns_list_full||'''])';
		raise notice '%', v_output_text;
		execute 'select sys_dwh.mrg_bdv_prttn('''||v_src_table||''', '''||p_trgt_table||''',array ['''||v_prttns_list_full||'''])'
        into v_rslt;
		elsif p_call_func = 'truncate' then
		v_output_text = 'select sys_dwh.bdv_truncate_prttn(array ['''||v_prttns_list_full||'''])';
		raise notice '%', v_output_text;
		execute 'select sys_dwh.bdv_truncate_prttn(array ['''||v_prttns_list_full||'''])'
        into v_rslt;
		end if;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'запуск функции',
		p_val := 'v_prttn_level='||v_prttn_level || ' v_rslt = '|| v_rslt,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'запуск функции',
			p_val := 'v_prttn_level='||v_prttn_level || ' v_rslt = '|| v_rslt,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	return(v_json_ret);
	
 end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.srch_bdv_prttn_v2(_text, text, _text, text, bool, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.srch_bdv_prttn_v2(p_src_table _text, p_trgt_table text, p_range_prttns _text, p_call_func text, p_ignore_empty_prttn bool DEFAULT true, p_load_bdv_tbl text DEFAULT NULL::text, p_load_bdv_dag text DEFAULT NULL::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_rslt text = 'функция не была запущена';
v_prttns_list text;
v_prttns_list_all_tables text;
v_prttns_list_full text = '';
v_prttns_list_try_empty text;
v_src_tablename text;
v_prttn text;
v_src_schema text;
v_src_table text;
v_cnt int8 := 0;
v_prttn_level int8;
v_call_func_ddl text;
v_start_dttm text;
v_json_ret text := '';
v_start_arr_len int8;
v_end_arr_len int8;
v_prttn_rec record;

v_stm_prttn_1 text;
v_stm_prttn_2 text;
v_ddl text;
v_strt_dt text;
v_stop_dt text;


 begin
	--открываю цикл по таблицам источникам
	for v_src_table in select unnest(p_src_table)
    loop
	 FOR v_prttn_rec IN SELECT unnest(p_range_prttns) AS prttn
	 LOOP
	--Раскладываю таблицу на схему и имя
	begin
		select split_part(v_src_table,'.', 1) into v_src_schema;
		select split_part(v_src_table,'.', 2) into v_src_tablename;
		select split_part(v_prttn_rec.prttn,'-', 1) into v_stm_prttn_1;
		select split_part(v_prttn_rec.prttn,'-', 2) into v_stm_prttn_2;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Разбитие таблицы на имя и схему',
		p_val := 'v_src_schema='||v_src_schema||', v_src_tablename='||v_src_tablename||', v_stm_prttn_1='||v_stm_prttn_1||', v_stm_prttn_2='||v_stm_prttn_2,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Разбитие таблицы на имя и схему',
			p_val := 'v_src_schema='||v_src_schema||', v_src_tablename='||v_src_tablename||', v_stm_prttn_1='||v_stm_prttn_1||', v_stm_prttn_2='||v_stm_prttn_2,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	--Собираем партиции
	begin
		v_ddl = 'select string_agg(partitiontablename, '''''','''''')   from pg_catalog.pg_partitions
		where  schemaname||''.''||tablename = '''||v_src_table||'''
		and case when partitionrangestart~''^[0-9]+$'' then cast(partitionrangestart as integer) else null end '||case 
	when v_stm_prttn_2 = '' then '= '||v_stm_prttn_1::int8||'' else '>= '||v_stm_prttn_1::int8 end||'
		'
	||case 
	when v_stm_prttn_2 <> '' then ' and case when partitionrangestart~''^[0-9]+$'' then  cast(partitionrangestart as integer) else null end <= '||v_stm_prttn_2::int8||'' 
	else ''  end ||';';

		execute v_ddl into v_prttns_list;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Собираем партиции',
		p_val := 'v_ddl='||v_ddl|| ' v_prttns_list = '||v_prttns_list,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Собираем партиции	',
			p_val := 'v_ddl='||v_ddl|| ' v_prttns_list = '||v_prttns_list,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;

	--Проверяем есть ли данные в p_load_bdv_tbl и собираем их, если есть
	begin
		if p_load_bdv_tbl is not null then
		select strt_dt, stop_dt into v_strt_dt, v_stop_dt from sys_dwh.prm_load_bdv where tbl_name = p_load_bdv_tbl and coalesce(dag_id,'') = coalesce(p_load_bdv_dag,'');
		v_strt_dt := coalesce(v_strt_dt, '9999-12-31');
		v_ddl = 'select string_agg(partitiontablename, '''''','''''')  from pg_catalog.pg_partitions where schemaname||''.''||tablename = '''||v_src_table||'''
and parentpartitiontablename in ('''||v_prttns_list||''') and substring(partitionrangestart from 1 for 11)::date >= '''||v_strt_dt||'''::date 
'|| case when v_stop_dt is not null then ' and substring(partitionrangeend from 1 for 11)::date >= '''||v_stop_dt||'''::date' else '' end||';';

		execute v_ddl into v_prttns_list;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '3',
		p_descr := 'Проверяем данные в p_load_bdv_tbl',
		p_val := 'v_ddl='||v_ddl|| ' v_prttns_list = '||v_prttns_list,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '3',
			p_descr := 'Собираем партиции	',
			p_val := 'v_ddl='||v_ddl|| ' v_prttns_list = '||v_prttns_list,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;

	--проверка на пустые партиции
	begin	
		v_prttns_list_try_empty = replace(v_prttns_list, '''','');
		v_prttns_list := v_prttns_list ||''',''';
		v_prttns_list_all_tables := '';
		if p_ignore_empty_prttn = true then
		for v_prttn in select unnest(string_to_array(v_prttns_list_try_empty,','))
		loop
			execute 'select count(*) from '||v_src_schema || '.' || v_prttn ||';' into v_cnt;
			if v_cnt > 0 then
				v_prttns_list_all_tables := v_prttns_list_all_tables || v_prttn||''',''';
			end if;
		end loop;
		v_prttns_list := v_prttns_list_all_tables;
		end if;
		if p_call_func = 'truncate' then
			v_prttns_list_try_empty = replace(v_prttns_list, '''','');
			v_prttns_list_try_empty := RTRIM(v_prttns_list_try_empty,',');
			v_prttns_list_all_tables := '';
			for v_prttn in select unnest(string_to_array(v_prttns_list_try_empty,',')) loop
				v_prttns_list_all_tables := v_prttns_list_all_tables || v_src_schema || '.' || v_prttn || ''',''';

			end loop;
		v_prttns_list := v_prttns_list_all_tables;
		end if;
		v_prttns_list_full := v_prttns_list_full || v_prttns_list;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '4',
		p_descr := 'проверка на пустые партиции',
		p_val := 'v_prttns_list_try_empty='||v_prttns_list_try_empty||',v_prttns_list_full='||v_prttns_list_full||',v_prttns_list='||v_prttns_list,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '4',
			p_descr := 'проверка на пустые партиции',
			p_val := 'v_prttns_list_try_empty='||v_prttns_list_try_empty||',v_prttns_list_full='||v_prttns_list_full||',v_prttns_list='||v_prttns_list,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	end loop;
	end loop;
	--запуск функции
	begin
		v_prttns_list_full := RTRIM(v_prttns_list_full,''',''');
		if v_prttns_list_full <> '' then 
		if p_call_func = 'merge' then
		v_call_func_ddl = 'select sys_dwh.mrg_bdv_prttn('''||v_src_table||''', '''||p_trgt_table||''',array ['''||v_prttns_list_full||'''])';
		execute 'select sys_dwh.mrg_bdv_prttn('''||v_src_table||''', '''||p_trgt_table||''',array ['''||v_prttns_list_full||'''])'
        into v_rslt;
		elsif p_call_func = 'truncate' then
		v_call_func_ddl = 'select sys_dwh.bdv_truncate_prttn(array ['''||v_prttns_list_full||'''])';
		execute 'select sys_dwh.bdv_truncate_prttn(array ['''||v_prttns_list_full||'''])'
        into v_rslt;
		end if;
		end if;
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '5',
		p_descr := 'запуск функции',
		p_val := 'v_prttns_list_full = '|| v_prttns_list_full||', v_call_func_ddl(call_func)='||v_call_func_ddl||', v_rslt='||v_rslt,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '5',
			p_descr := 'запуск функции',
			p_val := 'v_prttns_list_full = '|| v_prttns_list_full||', v_call_func_ddl(call_func)='||v_call_func_ddl||', v_rslt='||v_rslt,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	v_json_ret := v_json_ret||']';
	return(v_json_ret);
	
 end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.strt_put_undbls(text);

CREATE OR REPLACE FUNCTION sys_dwh.strt_put_undbls(p_table_name_rdv text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare
v_table_name_rdv text; 
v_cnt_act_in bigint; --кол-во актуальных запией до дедупликации 
v_cnt_in  bigint; --кол-во всех запией до дедупликации
v_pk text; 
v_cnt_act_out bigint; --кол-во актуальных запией после дедупликации
v_cnt_out  bigint; --кол-во всех запией после дедупликации
v_cnt int;
v_json json;
v_rslt text; -- результат повторной проверки
v_sql_back text; --план отката
BEGIN
	v_table_name_rdv := p_table_name_rdv;
-- подсчет актуальных записей до дедупликации
	execute 'select count(*) from '||v_table_name_rdv||' where end_dt = ''9999-12-31'';' 
	into v_cnt_act_in;
	execute 'select count(*) from '||v_table_name_rdv||';' 
	into v_cnt_in;
-- запуск функции дедупликации
	execute  'select string_agg(column_name_rdv,'','') from sys_dwh.prm_s2t_rdv  where key_type_src like ''PK%'' and schema_rdv||''.''||table_name_rdv = '''||v_table_name_rdv||''';'  --group by src_stm_id 
	into v_pk;
	execute 'select sys_dwh.put_undbls('''||v_table_name_rdv||''','''||v_pk||''');'
	into v_json;
	v_json := replace(v_json::text, '''', '''''');
-- выделение и сохранение в переменную плана отката
	execute  'select t2.value ->> ''val''::text as val_jsn 
				from (
					select t1.value 
					from (
						SELECT * from  json_array_elements('''||v_json||''')
						)t1
					where t1.value::text like ''{"step":"4"%'' 
					)t2 ;'		
	into v_sql_back;
	v_sql_back:=replace(replace(v_sql_back,'v_sql_back=''',''),'$do$;''','$do$;');
-- подсчет актуальных записей после дедупликации
	execute 'select count(*) from '||v_table_name_rdv||' where end_dt = ''9999-12-31'';'
	into v_cnt_act_out;
	execute 'select count(*) from '||v_table_name_rdv||';' 
	into v_cnt_out;
-- проверка изменилась ли таблица 
	if  v_cnt_in = v_cnt_out then 
	RAISE EXCEPTION 'ERROR: %', 'После запуска sys_dwh.put_undbls('''||v_table_name_rdv||''','''||v_pk||''') количество записей не изменилось.';
	else v_cnt:= v_cnt_in - v_cnt_out;
-- проверка излишненарезанной истории на конкретной таблице после исправления
	execute 'update sys_tst_dwh.d_chk_dwh set jsn_chk = ''[{"tbl_nm":"'||v_table_name_rdv||'"}]'', activ = 1, email = ''-'' where chk_num = 1;';
	select sys_dwh.chk_strtr(1) into v_rslt;
	update sys_tst_dwh.d_chk_dwh set activ = 0 where chk_num = 1;
-- обработка результат повторной проверки 
	if v_rslt like '%есть чрезмерно нарезанная история%' then
	RAISE EXCEPTION 'ERROR: %', v_rslt;
	else 	
-- проверка количеста записей из п.1 и п.3 - должны совпадать
	if v_cnt_act_in = v_cnt_act_out 
	then 
	RETURN 'В таблице '||v_table_name_rdv||' удалено '||v_cnt||' строк излишне нарезаной истории. По необходимости запустите план отката: '||v_sql_back;
	else RAISE EXCEPTION 'ERROR: %', 'Количество активных записей изменилось после схлапывания истории. Необходимо применить план отката: '||v_sql_back;
	end if;
	end if;
	end if;
EXCEPTION WHEN OTHERS  
THEN
    RAISE EXCEPTION 'ERROR CODE: %. MESSAGE TEXT: %', SQLSTATE, SQLERRM;
end;

$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.transfer_s2t_prm(int4);

CREATE OR REPLACE FUNCTION sys_dwh.transfer_s2t_prm(p_src_stm_id int4 DEFAULT NULL::integer)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare 

v_output text;

v_sql text;

v_cnt int4;

v_tbl text;

v_ssn text;

v_start_dttm text;

v_json_ret text := '';

begin

--Проверяем аргумент функции

if p_src_stm_id is null

--Загружаем все s2t-карты целиком

then

--Открываем цикл по таблицам s2t

for v_tbl in (select tbl_prm from ext_sys_dwh.prm_rule where tbl_grp in ('s2t stg', 's2t rdv')) loop 

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

	v_sql := 'truncate table sys_dwh.'||v_tbl||';';

	v_sql := v_sql||E'\n'||'insert into sys_dwh.'||v_tbl||' select * from ext_sys_dwh.'||v_tbl||';';

	execute v_sql;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Загружаем справочник '||v_tbl,

p_start_dttm := v_start_dttm,

p_val := v_sql,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Загружаем справочник '||v_tbl,

p_val := v_sql,

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

--Загружаем s2t-карты по заданному src_stm_id

else

--Открываем цикл по таблицам s2t

for v_tbl in (select tbl_prm from ext_sys_dwh.prm_rule where tbl_grp in ('s2t stg', 's2t rdv')) loop 

begin	

v_start_dttm := clock_timestamp() at time zone 'utc';

	v_sql := 'delete from sys_dwh.'||v_tbl||' where src_stm_id = '||p_src_stm_id||';';

	v_sql := v_sql||E'\n'||'insert into sys_dwh.'||v_tbl||' select * from ext_sys_dwh.'||v_tbl

	||' where src_stm_id = '||p_src_stm_id||';';

	execute v_sql;

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Загружаем справочник '||v_tbl||' для src_stm_id='||p_src_stm_id,

p_start_dttm := v_start_dttm,

p_val := v_sql,

p_log_tp := '1',

p_debug_lvl := '3')

into v_json_ret;

exception when others then

select sys_dwh.get_json4log(p_json_ret := v_json_ret,

p_step := '1',

p_descr := 'Загружаем справочник '||v_tbl||' для src_stm_id='||p_src_stm_id,

p_val := v_sql,

p_start_dttm := v_start_dttm,

p_err := SQLERRM,

p_log_tp := '3',

p_debug_lvl := '1',

p_cls := ']')

into v_json_ret;

raise exception '%', v_json_ret;

end;

end loop;

end if;

--Возвращаем результат

return(v_json_ret||']'); 

--Регистрируем ошибки

exception

when others then

if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

v_json_ret := v_json_ret||']';

end if;

raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.update_lm_reload_cdc();

CREATE OR REPLACE FUNCTION sys_dwh.update_lm_reload_cdc()
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	



DECLARE 



weekday_target int4 := 6; --суббота;

weekday_current int4; --текущий день недели

msg text :='не требуется.'; --сообщения для лога

v_json_ret text := ''; --возвращаемый json

v_start_dttm text; --время запуска

BEGIN

	begin

		

v_start_dttm := clock_timestamp() at time zone 'utc';



select extract(isodow from date (now() at TIME zone 'utc')::DATE )

into weekday_current;

if weekday_current =  weekday_target then

msg:='будет выполнено.';



	select STRING_AGG (s.nm || '.' || t.nm || ' ('||cast(t.src_stm_id as text)||') текущий load_mode=' || t.load_mode || ' будет обновлен по load_mode_reload_cdc='||t.load_mode_reload_cdc, '; ') as msg

	into msg

	from sys_dwh.prm_src_stm t

	join sys_dwh.prm_src_stm s on s.src_stm_id = t.prn_src_stm_id 

	where t.load_mode_reload_cdc is not null and t.load_mode <> 1;



	if msg is null then

		msg:='нет таблиц для обновления';

	end if;



	update sys_dwh.prm_src_stm 

	set load_mode = load_mode_reload_cdc

	where load_mode_reload_cdc  is not null and load_mode <> 1;



end if;

		



	SELECT sys_dwh.get_json4log(p_json_ret := v_json_ret, p_step := '1', p_descr := 'Обновление lm по load_mode_reload_cdc: ' || msg , p_start_dttm := v_start_dttm, p_val := '', p_log_tp := '1', p_debug_lvl := '3')

	INTO v_json_ret;



	exception when others then



	SELECT sys_dwh.get_json4log(p_json_ret := v_json_ret, p_step := '1', p_descr := 'Обновление lm по load_mode_reload_cdc: ' || msg , p_val := '', p_start_dttm := v_start_dttm, p_err := SQLERRM, p_log_tp := '3', p_debug_lvl := '1', p_cls := ']')

	INTO v_json_ret;



	raise exception '%' ,v_json_ret;

END;



--Возвращаем результат

RETURN (v_json_ret || ']');



--Регистрируем ошибки

exception when others then



IF right(v_json_ret, 1) <> ']'

	AND v_json_ret IS NOT NULL then v_json_ret := v_json_ret || ']';END

	IF ;

		raise exception '%'

			,v_json_ret;

	END;
$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.updt_column_fltr(int4, text);

CREATE OR REPLACE FUNCTION sys_dwh.updt_column_fltr(p_src_stm_id int4, p_db text DEFAULT 'sys_dwh'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
v_json_ret text := '';
v_cndtn_updt_fltr text;
v_cndtn_updt_pb text;
v_start_dttm text;
v_update_sql text;
 begin
	--Поиск условия, по которому обновлять
	begin
		execute 'select cndtn_updt_fltr_from, cndtn_updt_pb_from from ext_sys_dwh.prm_updt_fltr_from where src_stm_id = '||p_src_stm_id||'' into v_cndtn_updt_fltr, v_cndtn_updt_pb;
		if v_cndtn_updt_fltr is NULL then
			return('Пропуск, не найдены условия обновления в таблице prm_updt_fltr_from');
		end if;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'Собираем условие, по-которому обновлять',
		p_val := 'v_cndtn_updt_fltr='||v_cndtn_updt_fltr ||', v_cndtn_updt_pb='||v_cndtn_updt_pb,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'Разбитие таблицы на имя и схему',
			p_val := 'v_cndtn_updt_fltr='||v_cndtn_updt_fltr||', v_cndtn_updt_pb='||v_cndtn_updt_pb,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	--Обновляем границу
	begin
		v_update_sql := 'update '||p_db||'.prm_s2t_stg_src set column_fltr_from = '||v_cndtn_updt_fltr
		||case when (v_cndtn_updt_pb is null or v_cndtn_updt_pb = '') then ' ' else ',ext_prd_pb_from = '||v_cndtn_updt_pb end ||' where src_stm_id = '||p_src_stm_id||' and eff_dt <= (now() at time zone ''utc'')::date and end_dt >= (now() at time zone ''utc'')::date';
		execute v_update_sql;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'Обновляем границу',
		p_val := 'v_update_sql='||v_update_sql,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'Обновляем границу',
			p_val := 'v_update_sql='||v_update_sql,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	return(v_json_ret||']');
	
 end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.updt_lm(int4);

CREATE OR REPLACE FUNCTION sys_dwh.updt_lm(p_src_stm_id int4 DEFAULT NULL::integer)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	   

declare

v_ddl text;

v_start_dttm text;

v_json_ret text := '';

v_cnt_upd int8;

v_min_dml_dttm timestamp;

v_max_dml_dttm timestamp :='1900-01-01'::timestamp;

v_cdc_tp text;

v_load_mode int4;

v_tgt_load_mode int4;

v_int_table varchar(50);

v_sql text;

v_mv_populated int4;

v_src_jetlag text; --переменная смещения времени на источнике

v_interval interval;

begin

	--Запускаем обновление текущего load mode

	begin	

		v_start_dttm := clock_timestamp() at time zone 'utc';

		if p_src_stm_id is null

		then 

			v_ddl := 'update sys_dwh.prm_src_stm set load_mode = load_mode_trgt';

		else 

			--получение текущего lm

			select load_mode into v_load_mode from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

			--целевой lm

			select load_mode_trgt into v_tgt_load_mode from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id and end_dt = '9999-12-31';

			--получаем текущий cdc_tp

			select cdc_tp into v_cdc_tp from sys_dwh.prm_s2t_stg_src pstss where pstss.src_stm_id = p_src_stm_id and end_dt = '9999-12-31';	

			 

			--если переключение будет идти на lm2 dbz и текущий режим не lm2

			if v_tgt_load_mode = 2 and v_cdc_tp = 'dbz' and v_load_mode <> v_tgt_load_mode then		

			    

				/*получение имени mv*/

				SELECT replace(schema_stg,'stg_','src_')||'.'||'vm_dbz_'||table_name_stg

				INTO v_int_table

				FROM sys_dwh.prm_s2t_stg_src

				WHERE src_stm_id = p_src_stm_id AND now() at time zone 'utc' BETWEEN eff_dt AND end_dt;	

			

				/*определение, обновлено ли MV dbz*/

				SELECT count(*)

				into v_mv_populated

				FROM pg_catalog.pg_class AS c

				JOIN pg_catalog.pg_namespace AS ns

				  ON c.relnamespace = ns.oid

				WHERE relkind = 'm' and relispopulated = true and nspname||'.'||relname = v_int_table;

			

				if 	v_mv_populated > 0 then /*если MV существует и обновлено*/				

					/*получение текущих границ, за которые получены последний раз данные dbz*/

					v_sql:='select min(dml_dttm) as min_dml_dttm, max(dml_dttm) as max_dml_dttm from '||v_int_table;

					execute v_sql into v_min_dml_dttm, v_max_dml_dttm;		   

				end if;

				/*получение временного смещения источника*/

				select src_jetlag::text into v_src_jetlag from sys_dwh.prm_src_stm where src_stm_id = p_src_stm_id;

				

				/*если данные в dbz НЕ содержат данные за сегодня, то обновление lm не будет*/

				v_interval:=(v_src_jetlag::text||' hour')::interval;

				if v_mv_populated = 0 or ((v_max_dml_dttm + v_interval)::date < (now() at time zone 'utc')::date) then				

					select sys_dwh.get_json4log(p_json_ret := v_json_ret,

						p_step := '2',

						p_descr := 'Представление '||v_int_table||' не содержало данные за текущий день (((v_max_dml_dttm + v_interval)::date < (now() at time zone ''utc'')::date)) либо mv не обновлено (v_mv_populated = 0). Переключение load_mode не будет выполнено.',

						p_start_dttm := v_start_dttm,

						p_val := 'v_mv_populated='||v_mv_populated::text||', v_max_dml_dttm='||v_max_dml_dttm::text||', v_src_jetlag='||v_src_jetlag::text||', v_interval='||v_interval::text||' (v_max_dml_dttm + v_interval)='||(v_max_dml_dttm + v_interval)::text,

						p_upd_qty := v_cnt_upd::text,

						p_log_tp := '1',

						p_debug_lvl := '3')

						into v_json_ret;

						v_json_ret := v_json_ret||']';

					return(v_json_ret); /*выход*/		    

				end if;

			end if;

			if v_load_mode <> v_tgt_load_mode then/*обновление lm по lm_target*/					

				v_ddl := 'update sys_dwh.prm_src_stm set load_mode = load_mode_trgt where src_stm_id = '||p_src_stm_id::text || ' /*load_mode_trgt=' || v_tgt_load_mode::text||', load_mode was ' || v_load_mode::text || '*/';

			else

				v_ddl := 'select 1; /*Обновление не требуется load_mode_trgt=' || v_tgt_load_mode::text||', load_mode was '||v_load_mode::text||'*/';

			end if;	

		end if;

		execute v_ddl;

	

		get diagnostics v_cnt_upd = row_count;

		select sys_dwh.get_json4log(p_json_ret := v_json_ret, p_step := '1', p_descr := 'Обновляем load_mode в таблице sys_dwh.prm_src_stm', p_start_dttm := v_start_dttm, p_val := v_ddl, p_upd_qty := v_cnt_upd::text, p_log_tp := '1', p_debug_lvl := '3') into v_json_ret;

		exception when others then

		select sys_dwh.get_json4log(p_json_ret := v_json_ret, p_step := '1', p_descr := 'Обновляем load_mode в таблице sys_dwh.prm_src_stm', p_val := v_ddl, p_start_dttm := v_start_dttm, p_err := SQLERRM, p_log_tp := '3', p_debug_lvl := '1', p_cls := ']') into v_json_ret;

		raise exception '%', v_json_ret;

	end;

		v_json_ret := v_json_ret||']';

	return(v_json_ret); 

	--Регистрируем ошибки

	exception

	when others then

		if right(v_json_ret, 1) <> ']' and v_json_ret is not null then

			v_json_ret := v_json_ret||']';

		end if;

	raise exception '%', v_json_ret;

end;


$$
EXECUTE ON ANY;

-- DROP FUNCTION sys_dwh.upload_data(text, text, text, text, text);

CREATE OR REPLACE FUNCTION sys_dwh.upload_data(p_src_table text, p_trgt_table text, p_field_list text, p_column_fltr text DEFAULT 'None'::text, p_column_fltr_from text DEFAULT 'None'::text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
sql_insert text;
v_start_dttm text;
v_json_ret text := '';


 begin
	--формирование sql запроса
	begin
		sql_insert:= 'INSERT INTO ' || p_trgt_table || (case when p_field_list = '*' then '' else '(' || p_field_list || ')' end) || ' SELECT '|| p_field_list ||' FROM ' || p_src_table ;
		if p_column_fltr <> 'None' then 
		sql_insert:= sql_insert || ' where ' || p_column_fltr || p_column_fltr_from;
		end if;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '1',
		p_descr := 'формирование sql запроса',
		p_val := 'sql_insert='||sql_insert,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '1',
			p_descr := 'формирование sql запроса',
			p_val := 'sql_insert='||sql_insert,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	
	--выполнение запроса
	begin
		EXECUTE sql_insert;
		v_start_dttm := clock_timestamp() at time zone 'utc';
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
		p_step := '2',
		p_descr := 'выполнение запроса',
		p_val := 'sql_insert='||sql_insert,
		p_start_dttm := v_start_dttm)
		into v_json_ret;
		exception when others then
		select sys_dwh.get_json4log(p_json_ret := v_json_ret,
			p_step := '2',
			p_descr := 'выполнение запроса',
			p_val := 'sql_insert='||sql_insert,
			p_start_dttm := v_start_dttm,
			p_err := SQLERRM,
			p_log_tp := '3',
			p_cls := ']',
			p_debug_lvl := '1')
		into v_json_ret;
		raise exception '%', v_json_ret;
    end;
	
	return(v_json_ret);
	
 end;


$$
EXECUTE ON ANY;