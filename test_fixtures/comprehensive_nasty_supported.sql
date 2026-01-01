{# This is the nastiest SQL file ever written - comprehensive Snowflake + Jinja test #}
{% set my_database = 'PROD_DB' %}{% set schema_name = "analytics" %}{%set table_list=['users','orders','products']%}
{% macro generate_column_list(cols) %}{% for col in cols %}{{col}}{% if not loop.last %},{% endif %}{% endfor %}{% endmacro %}
{%- macro safe_divide(numerator, denominator) -%}CASE WHEN {{denominator}} = 0 THEN NULL ELSE {{numerator}} / {{denominator}} END{%- endmacro -%}
-- Crazy CTE with everything
WITH RECURSIVE base_cte AS(SELECT 1 as n UNION ALL SELECT n+1 FROM base_cte WHERE n<10),
{% for tbl in table_list %}{{tbl}}_cte AS (
SELECT/*+PARALLEL(4)*/ a.id,a.name,b."WeirdColumn",c.'even_weirder',
CAST(d.value AS VARCHAR(100))as casted_val,TRY_CAST(e.data AS NUMBER(38,2))as try_casted,
d.variant_col:nested.path::STRING as json_extract,
d.variant_col['array'][0]:field AS bracket_notation,
ARRAY_AGG(DISTINCT f.item)WITHIN GROUP(ORDER BY f.item DESC)AS agg_array,
OBJECT_AGG(g.key,g.value)AS obj_agg,
LISTAGG(h.name,', ')WITHIN GROUP(ORDER BY h.name)AS name_list,
SUM(i.amount)OVER(PARTITION BY i.category ORDER BY i.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)as running_total,
LAG(j.value,1,0)OVER(ORDER BY j.ts)as prev_val,LEAD(j.value)OVER(ORDER BY j.ts)as next_val,
ROW_NUMBER()OVER(ORDER BY l.id)as rn,RANK()OVER(ORDER BY l.score DESC)as rnk,DENSE_RANK()OVER(ORDER BY l.score DESC)as dense_rnk,
COUNT(*)as cnt_all,COUNT(DISTINCT dq.id)as cnt_distinct,
SUM(ds.amount)as sum_val,AVG(dt.value)as avg_val,MIN(du.val)as min_val,MAX(du.val)as max_val,
ABS(bt.num)as abs_val,CEIL(bt.num)as ceil_val,FLOOR(bt.num)as floor_val,ROUND(bt.num,2)as round_val,
LENGTH(cj.str)as str_len,UPPER(ck.str)as upper_str,LOWER(ck.str)as lower_str,
TRIM(cl.str)as trimmed,LEFT(cn.str,5)as left_str,RIGHT(cn.str,5)as right_str,
CONCAT(cq.a,cq.b,cq.c)as concat_val,cq.a||cq.b||cq.c as concat_op,
COALESCE(v.a,v.b,v.c,v.d)as coalesce_result,
CASE x.status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END as simple_case,
CASE WHEN y.value>100 THEN 'high' WHEN y.value>50 THEN 'medium' ELSE 'low' END as searched_case,
CURRENT_TIMESTAMP()as curr_ts,CURRENT_DATE()as curr_dt,
DATEADD(day,7,aw.dt)as week_later,DATEDIFF(day,ax.start_dt,ax.end_dt)as day_diff
FROM {{my_database}}.{{schema_name}}.{{tbl}} a
LEFT OUTER JOIN schema2.table_b b ON a.id=b.a_id AND b.active=TRUE
RIGHT JOIN schema3.table_c c ON b.id=c.b_id
FULL OUTER JOIN schema4.table_d d ON c.id=d.c_id OR d.alt_id=c.alt_id
INNER JOIN schema5.table_e e ON d.id=e.d_id
CROSS JOIN schema6.table_f f
WHERE a.created_at >= DATEADD(day,-30,CURRENT_DATE())
AND a.status IN ('active','pending','review')
AND a.status NOT IN ('deleted','archived')
AND EXISTS(SELECT 1 FROM related_table r WHERE r.parent_id=a.id AND r.active=TRUE)
AND NOT EXISTS(SELECT 1 FROM blacklist bl WHERE bl.id=a.id)
AND a.name LIKE '%pattern%' ESCAPE '\\'
AND a.code ILIKE 'prefix%'
AND a.value BETWEEN 10 AND 100
AND a.other_col IS NOT NULL
AND a.nullable_col IS NULL
AND a.bool_col IS TRUE
AND a.id = {{ some_id | default(0) }}
{% if filter_by_date %}AND a.event_date = '{{ filter_date }}'{% endif %}
GROUP BY 1,2,3,a.id,a.name
HAVING COUNT(*)>1 AND SUM(a.amount)>1000
QUALIFY ROW_NUMBER()OVER(PARTITION BY a.group_key ORDER BY a.rank DESC)=1),
{% endfor %}
-- Time travel queries
time_travel_cte AS(
SELECT * FROM historical_table AT(TIMESTAMP=>'2024-01-01 00:00:00'::TIMESTAMP_LTZ)
UNION ALL SELECT * FROM historical_table AT(OFFSET=>-3600)
),
-- PIVOT example
pivot_cte AS(
SELECT * FROM(SELECT region,quarter,sales FROM sales_data)
PIVOT(SUM(sales) FOR quarter IN ('Q1','Q2','Q3','Q4'))AS p(region,q1_sales,q2_sales,q3_sales,q4_sales)
),
-- UNPIVOT example
unpivot_cte AS(
SELECT * FROM pivot_cte UNPIVOT(sales FOR quarter IN(q1_sales,q2_sales,q3_sales,q4_sales))
),
-- Sampling
sample_cte AS(
SELECT * FROM large_table SAMPLE(10)
UNION ALL SELECT * FROM large_table TABLESAMPLE(100 ROWS)
)
-- Main query combining everything
SELECT{% raw %} {{ literal_jinja }} {% endraw %} AS raw_jinja,
u.*,o.order_total,p.product_count,
t.historical_data,
pv.q1_sales+pv.q2_sales+pv.q3_sales+pv.q4_sales AS total_pivoted,
upv.quarter,upv.sales AS unpivoted_sales,
s.*,
(SELECT MAX(x.val) FROM inline_table x WHERE x.id=u.id) AS scalar_subquery,
{{ generate_column_list(['a','b','c']) }} AS generated_cols,
{{ safe_divide('revenue','cost') }} AS margin
FROM users_cte u
LEFT JOIN orders_cte o ON u.id=o.user_id
LEFT JOIN products_cte p ON o.product_id=p.id
LEFT JOIN time_travel_cte t ON u.id=t.user_id
LEFT JOIN pivot_cte pv ON u.region=pv.region
LEFT JOIN unpivot_cte upv ON u.id=upv.id
LEFT JOIN sample_cte s ON u.id=s.id
UNION SELECT * FROM backup_users WHERE backup_date>CURRENT_DATE()-7
UNION ALL SELECT * FROM pending_users
EXCEPT SELECT * FROM deleted_users
ORDER BY u.created_at DESC NULLS LAST,u.name ASC NULLS FIRST,3,4 DESC
LIMIT {{ page_size | default(100) }} OFFSET {{ (page - 1) * page_size if page is defined else 0 }}
;

-- VALUES clause standalone
SELECT column1,column2 FROM VALUES(10,'x'),(20,'y'),(30,'z');

-- INSERT statements
INSERT INTO target_table(col1,col2,col3)SELECT a,b,c FROM source_table WHERE condition=TRUE;
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table(col1,col2)VALUES(1,'a'),(2,'b'),(3,'c');

-- UPDATE statements
UPDATE target_table SET col1='new_value',col2=col2+1,col3=NULL WHERE id=123;
UPDATE target_table t SET t.col1=s.val1,t.col2=s.val2 FROM source_table s WHERE t.id=s.id AND s.active=TRUE;

-- DELETE statements
DELETE FROM target_table WHERE created_at<DATEADD(year,-1,CURRENT_DATE());
DELETE FROM target_table t USING source_table s WHERE t.id=s.id AND s.delete_flag=TRUE;

-- MERGE statement
MERGE INTO target_table t
USING source_table s ON t.id=s.id
WHEN MATCHED AND s.delete_flag=TRUE THEN DELETE
WHEN MATCHED AND s.update_flag=TRUE THEN UPDATE SET t.col1=s.val1,t.col2=s.val2,t.updated_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.insert_flag=TRUE THEN INSERT(id,col1,col2,created_at)VALUES(s.id,s.val1,s.val2,CURRENT_TIMESTAMP())
;

-- CREATE TABLE variations
CREATE TABLE simple_table(id INT,name VARCHAR,amount NUMBER);
CREATE OR REPLACE TABLE replaced_table AS SELECT * FROM source_table WHERE 1=0;
CREATE TABLE IF NOT EXISTS conditional_table AS SELECT * FROM template_table;

-- CREATE VIEW variations
CREATE VIEW simple_view AS SELECT id,name FROM base_table;
CREATE OR REPLACE VIEW replace_view AS SELECT * FROM sensitive_table WHERE user_id=CURRENT_USER();

-- DROP statements
DROP TABLE IF EXISTS old_table;
DROP VIEW old_view;
