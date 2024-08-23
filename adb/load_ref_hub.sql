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
