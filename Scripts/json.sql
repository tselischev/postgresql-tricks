--Разбор вложенного JSON - значение ключа message


select request_history_gid, object_ref_id, properties, jkey, jvalue, 
(json_array_elements(jvalue::json))->>'key' msg_key, (json_array_elements(jvalue::json))->>'value' msg_value from (
select 
request_history_gid, object_ref_id, 
properties, 
(json_array_elements((properties->'valueList')::json))->>'key' jkey,
(json_array_elements((properties->'valueList')::json))->>'value' jvalue
from ad_sandbox.fct_kvp_request_history
) t
where jkey = 'message'


--Разбор JSON  в поле properties


select

request_history_gid, object_ref_id,

properties,

(json_array_elements((properties->'valueList')::json))->>'key' jkey,

(json_array_elements((properties->'valueList')::json))->>'value' jvalue

from ad_sandbox.fct_kvp_request_history


--Разбор вложенных xml:


select request_history_gid, object_ref_id, properties, jkey, jvalue, length(jvalue) from (

select

request_history_gid, object_ref_id,

properties,

(json_array_elements((properties->'valueList')::json))->>'key' jkey,

(json_array_elements((properties->'valueList')::json))->>'value' jvalue

from ad_sandbox.fct_kvp_request_history

) t

where jkey::text like '%ANSWER%'