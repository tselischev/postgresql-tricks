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
