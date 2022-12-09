
-- pgsql
CREATE OR REPLACE FUNCTION prc_tst_bulk()
RETURNS TABLE (name text, rowcount integer) AS 
$$
BEGIN
    RETURN QUERY
    WITH v_tb_person AS (SELECT * FROM tb_person)
    select name, count(*) from v_tb_person where age > 50
    union 
    select name, count(*) from v_tb_person where gender = 1;
END;
$$ LANGUAGE plpgsql;
This particular case can be further simplified into a plain SQL function:

-- sql
CREATE OR REPLACE FUNCTION prc_tst_bulk()
RETURNS TABLE (name text, rowcount integer) AS 
$$
    WITH v_tb_person AS (SELECT * FROM tb_person)
    select name, count(*) from v_tb_person where age > 50
    union 
    select name, count(*) from v_tb_person where gender = 1;
$$ LANGUAGE sql;