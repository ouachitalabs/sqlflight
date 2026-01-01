{# This is the nastiest SQL file ever written - comprehensive Snowflake + Jinja test #}
{% set my_database = 'PROD_DB' %}{% set schema_name = "analytics" %}{%set table_list=['users','orders','products']%}
{% macro generate_column_list(cols) %}{% for col in cols %}{{col}}{% if not loop.last %},{% endif %}{% endfor %}{% endmacro %}
{%- macro safe_divide(numerator, denominator) -%}CASE WHEN {{denominator}} = 0 THEN NULL ELSE {{numerator}} / {{denominator}} END{%- endmacro -%}
-- Start with some variable declarations
SET my_var = 'hello';SET another_var=42;set (a,b,c)=(1,2,3);
UNSET my_var;UNSET (a,b);
-- USE statements
USE DATABASE {{my_database}};USE SCHEMA {{ schema_name }};USE ROLE analyst_role;USE WAREHOUSE compute_wh;
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
FIRST_VALUE(k.score)OVER(PARTITION BY k.group ORDER BY k.rank ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)as first_score,
NTH_VALUE(k.score,2)OVER(PARTITION BY k.group ORDER BY k.rank)as second_score,
ROW_NUMBER()OVER(ORDER BY l.id)as rn,RANK()OVER(ORDER BY l.score DESC)as rnk,DENSE_RANK()OVER(ORDER BY l.score DESC)as dense_rnk,
NTILE(4)OVER(ORDER BY m.value)as quartile,
PERCENT_RANK()OVER(ORDER BY m.value)as pct_rank,CUME_DIST()OVER(ORDER BY m.value)as cum_dist,
RATIO_TO_REPORT(n.amount)OVER(PARTITION BY n.region)as ratio_rpt,
CONDITIONAL_TRUE_EVENT(o.flag='Y')OVER(ORDER BY o.seq)as cond_event,
CONDITIONAL_CHANGE_EVENT(p.status)OVER(ORDER BY p.ts)as change_event,
HASH(q.*)as row_hash,HASH_AGG(*)as hash_agg_val,
IFF(r.type='A',r.val_a,r.val_b)as iff_result,
NULLIF(s.x,s.y)as nullif_result,NVL(t.a,t.b)as nvl_result,NVL2(u.x,u.y,u.z)as nvl2_result,
COALESCE(v.a,v.b,v.c,v.d)as coalesce_result,ZEROIFNULL(w.nullable_num)as zero_if_null,
DECODE(x.code,'A',1,'B',2,'C',3,0)as decoded,
CASE x.status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END as simple_case,
CASE WHEN y.value>100 THEN 'high' WHEN y.value>50 THEN 'medium' WHEN y.value>0 THEN 'low' ELSE 'none' END as searched_case,
REGEXP_SUBSTR(z.text,'\\d+',1,1,'e')as regex_match,REGEXP_REPLACE(z.text,'[^a-zA-Z]','')as regex_replace,
REGEXP_COUNT(z.text,'pattern')as regex_count,REGEXP_INSTR(z.text,'find')as regex_instr,
SPLIT_PART(aa.delimited,',',2)as split_part_result,STRTOK(aa.str,' ',1)as strtok_result,
PARSE_JSON('{"key":"value"}')as parsed_json,PARSE_XML('<root><child/></root>')as parsed_xml,
TRY_PARSE_JSON(ab.maybe_json)as try_parsed,TO_JSON(OBJECT_CONSTRUCT('a',1,'b',2))as to_json_result,
OBJECT_CONSTRUCT('key1',ac.val1,'key2',ac.val2)as obj_construct,
OBJECT_CONSTRUCT_KEEP_NULL('key1',NULL,'key2',ac.val2)as obj_keep_null,
ARRAY_CONSTRUCT(1,2,3,4,5)as arr_construct,ARRAY_COMPACT(ad.sparse_array)as arr_compact,
ARRAY_FLATTEN(ae.nested_array)as arr_flatten_fn,ARRAY_SLICE(af.arr,0,5)as arr_slice,
ARRAYS_OVERLAP(ag.arr1,ag.arr2)as arr_overlap,ARRAY_INTERSECTION(ag.arr1,ag.arr2)as arr_intersect,
ARRAY_CAT(ah.arr1,ah.arr2)as arr_cat,ARRAY_APPEND(ai.arr,'new_element')as arr_append,
ARRAY_PREPEND(ai.arr,'first_element')as arr_prepend,ARRAY_INSERT(aj.arr,2,'inserted')as arr_insert,
ARRAY_POSITION('find'::VARIANT,ak.arr)as arr_position,ARRAY_CONTAINS('elem'::VARIANT,ak.arr)as arr_contains,
ARRAY_SIZE(al.arr)as arr_size,ARRAY_DISTINCT(am.arr)as arr_distinct,ARRAY_SORT(an.arr,TRUE,TRUE)as arr_sorted,
GET(ao.variant_data,'key')as get_result,GET_PATH(ao.variant_data,'a.b.c')as get_path_result,
FLATTEN(INPUT=>ap.variant_col,PATH=>'items',OUTER=>TRUE,RECURSIVE=>FALSE,MODE=>'ARRAY')as flattened,
TYPEOF(aq.variant_val)as type_of,AS_INTEGER(aq.variant_val)as as_int,AS_DOUBLE(aq.variant_val)as as_dbl,
OBJECT_KEYS(ar.obj)as obj_keys,OBJECT_DELETE(ar.obj,'unwanted_key')as obj_deleted,
OBJECT_INSERT(as_tbl.obj,'new_key','new_value')as obj_inserted,OBJECT_PICK(at.obj,'key1','key2')as obj_picked,
CURRENT_TIMESTAMP()as curr_ts,CURRENT_DATE()as curr_dt,CURRENT_TIME()as curr_tm,
SYSDATE()as sys_dt,GETDATE()as get_dt,LOCALTIMESTAMP()as local_ts,
DATE_PART(epoch_second,au.ts)as epoch_sec,DATE_TRUNC('month',av.dt)as trunc_month,
DATEADD(day,7,aw.dt)as week_later,DATEDIFF(day,ax.start_dt,ax.end_dt)as day_diff,
TIMESTAMPADD(hour,2,ay.ts)as two_hours_later,TIMESTAMPDIFF(minute,az.ts1,az.ts2)as min_diff,
ADD_MONTHS(ba.dt,3)as three_months_later,MONTHS_BETWEEN(bb.dt1,bb.dt2)as months_btwn,
LAST_DAY(bc.dt)as last_day_of_month,NEXT_DAY(bc.dt,'monday')as next_monday,PREVIOUS_DAY(bc.dt,'friday')as prev_friday,
DAYNAME(bd.dt)as day_name,MONTHNAME(bd.dt)as month_name,QUARTER(bd.dt)as qtr,
YEAR(be.dt)as yr,MONTH(be.dt)as mo,DAY(be.dt)as dy,HOUR(be.ts)as hr,MINUTE(be.ts)as mi,SECOND(be.ts)as sc,
TO_DATE(bf.str,'YYYY-MM-DD')as to_dt,TO_TIMESTAMP(bg.str,'YYYY-MM-DD HH24:MI:SS')as to_ts,
TO_TIMESTAMP_NTZ(bh.str)as to_ts_ntz,TO_TIMESTAMP_LTZ(bi.str)as to_ts_ltz,TO_TIMESTAMP_TZ(bj.str)as to_ts_tz,
TRY_TO_DATE(bk.maybe_dt)as try_dt,TRY_TO_TIMESTAMP(bl.maybe_ts)as try_ts,
TO_CHAR(bm.dt,'YYYY-MM-DD')as dt_str,TO_VARCHAR(bn.num)as num_str,
TO_NUMBER(bo.str)as to_num,TO_DECIMAL(bp.str,10,2)as to_dec,TO_DOUBLE(bq.str)as to_dbl,
TRY_TO_NUMBER(br.maybe_num)as try_num,TRY_TO_DECIMAL(bs.maybe_dec,10,2)as try_dec,
ABS(bt.num)as abs_val,CEIL(bt.num)as ceil_val,FLOOR(bt.num)as floor_val,ROUND(bt.num,2)as round_val,TRUNC(bt.num,2)as trunc_val,
MOD(bu.num,7)as mod_val,POWER(bv.base,bv.exp)as power_val,SQRT(bw.num)as sqrt_val,CBRT(bw.num)as cbrt_val,
EXP(bx.num)as exp_val,LN(bx.num)as ln_val,LOG(10,by.num)as log_val,
SIN(bz.rad)as sin_val,COS(bz.rad)as cos_val,TAN(bz.rad)as tan_val,
ASIN(ca.val)as asin_val,ACOS(ca.val)as acos_val,ATAN(ca.val)as atan_val,ATAN2(cb.y,cb.x)as atan2_val,
DEGREES(cc.rad)as degrees_val,RADIANS(cd.deg)as radians_val,PI()as pi_val,
SIGN(ce.num)as sign_val,FACTORIAL(5)as factorial_val,
GREATEST(cf.a,cf.b,cf.c)as greatest_val,LEAST(cf.a,cf.b,cf.c)as least_val,
WIDTH_BUCKET(cg.val,0,100,10)as bucket,
BITAND(ch.a,ch.b)as bitand_val,BITOR(ch.a,ch.b)as bitor_val,BITXOR(ch.a,ch.b)as bitxor_val,BITNOT(ch.a)as bitnot_val,
BITSHIFTLEFT(ci.num,2)as shifted_left,BITSHIFTRIGHT(ci.num,2)as shifted_right,
LENGTH(cj.str)as str_len,LEN(cj.str)as str_len2,CHAR_LENGTH(cj.str)as char_len,OCTET_LENGTH(cj.str)as octet_len,
UPPER(ck.str)as upper_str,LOWER(ck.str)as lower_str,INITCAP(ck.str)as initcap_str,
TRIM(cl.str)as trimmed,LTRIM(cl.str)as ltrimmed,RTRIM(cl.str)as rtrimmed,
LPAD(cm.str,10,'0')as lpadded,RPAD(cm.str,10,'0')as rpadded,
LEFT(cn.str,5)as left_str,RIGHT(cn.str,5)as right_str,SUBSTR(cn.str,2,3)as substr_str,SUBSTRING(cn.str,2,3)as substring_str,
POSITION('find' IN co.str)as pos_val,CHARINDEX('find',co.str)as charindex_val,
INSTR(cp.str,'find')as instr_val,LOCATE('find',cp.str)as locate_val,
CONCAT(cq.a,cq.b,cq.c)as concat_val,cq.a||cq.b||cq.c as concat_op,
CONCAT_WS(',',cr.a,cr.b,cr.c)as concat_ws_val,
REPLACE(cs.str,'old','new')as replaced,TRANSLATE(ct.str,'abc','xyz')as translated,
REVERSE(cu.str)as reversed,REPEAT(cv.str,3)as repeated,SPACE(10)as spaces,
ASCII(cw.char)as ascii_val,CHR(65)as chr_val,UNICODE(cx.char)as unicode_val,
COMPRESS(cy.data,'gzip')as compressed,DECOMPRESS_STRING(cz.compressed,'gzip')as decompressed,
MD5(da.str)as md5_hash,MD5_HEX(da.str)as md5_hex,MD5_BINARY(da.str)as md5_bin,
SHA1(db.str)as sha1_hash,SHA1_HEX(db.str)as sha1_hex,SHA1_BINARY(db.str)as sha1_bin,
SHA2(dc.str,256)as sha256_hash,SHA2_HEX(dc.str,256)as sha256_hex,SHA2_BINARY(dc.str,256)as sha256_bin,
BASE64_ENCODE(dd.data)as base64_enc,BASE64_DECODE_STRING(de.b64)as base64_dec,
HEX_ENCODE(df.data)as hex_enc,HEX_DECODE_STRING(dg.hex)as hex_dec,
TRY_BASE64_DECODE_STRING(dh.maybe_b64)as try_b64,TRY_HEX_DECODE_STRING(di.maybe_hex)as try_hex,
UUID_STRING()as uuid_val,
RANDOM()as random_val,UNIFORM(1,100,RANDOM())as uniform_val,NORMAL(0,1,RANDOM())as normal_val,
SEQ1()as seq1_val,SEQ2()as seq2_val,SEQ4()as seq4_val,SEQ8()as seq8_val,
SYSTEM$STREAM_GET_TABLE_TIMESTAMP('my_stream')as stream_ts,
SYSTEM$STREAM_HAS_DATA('my_stream')as stream_has_data,
IDENTIFIER({{ 'col_' ~ tbl }})as dynamic_col,
$1 as positional_col,$2::NUMBER as typed_positional,
{% if loop.first %}CURRENT_ROLE()as curr_role,CURRENT_USER()as curr_user,CURRENT_ACCOUNT()as curr_account,CURRENT_REGION()as curr_region,
CURRENT_DATABASE()as curr_db,CURRENT_SCHEMA()as curr_schema,CURRENT_WAREHOUSE()as curr_wh,CURRENT_SESSION()as curr_session,{% endif %}
EQUAL_NULL(dj.a,dj.b)as equal_null_check,IS_NULL_VALUE(dk.variant_field)as is_null_val,
IS_ARRAY(dl.val)as is_arr,IS_OBJECT(dl.val)as is_obj,IS_BOOLEAN(dl.val)as is_bool,
IS_CHAR(dm.val)as is_chr,IS_VARCHAR(dm.val)as is_vc,IS_BINARY(dm.val)as is_bin,
IS_DATE(dn.val)as is_dt,IS_TIME(dn.val)as is_tm,IS_TIMESTAMP_NTZ(dn.val)as is_ts_ntz,
IS_INTEGER(do.val)as is_int,IS_REAL(do.val)as is_real,IS_DECIMAL(do.val)as is_dec,
BOOLAND_AGG(dp.flag)as bool_and,BOOLOR_AGG(dp.flag)as bool_or,
COUNT(*)as cnt_all,COUNT(DISTINCT dq.id)as cnt_distinct,COUNT_IF(dr.active)as cnt_if,
SUM(ds.amount)as sum_val,SUM(DISTINCT ds.amount)as sum_distinct,
AVG(dt.value)as avg_val,AVG(DISTINCT dt.value)as avg_distinct,
MIN(du.val)as min_val,MAX(du.val)as max_val,
STDDEV(dv.val)as stddev_val,STDDEV_POP(dv.val)as stddev_pop,STDDEV_SAMP(dv.val)as stddev_samp,
VARIANCE(dw.val)as var_val,VAR_POP(dw.val)as var_pop,VAR_SAMP(dw.val)as var_samp,
MEDIAN(dx.val)as median_val,MODE(dy.val)as mode_val,
PERCENTILE_CONT(0.5)WITHIN GROUP(ORDER BY dz.val)as pct_cont,
PERCENTILE_DISC(0.5)WITHIN GROUP(ORDER BY dz.val)as pct_disc,
APPROX_COUNT_DISTINCT(ea.id)as approx_cnt,HLL(ea.id)as hll_val,
APPROX_TOP_K(eb.item,10)as approx_top,APPROX_PERCENTILE(ec.val,0.95)as approx_pct,
CORR(ed.x,ed.y)as corr_val,COVAR_POP(ed.x,ed.y)as covar_pop,COVAR_SAMP(ed.x,ed.y)as covar_samp,
REGR_SLOPE(ee.y,ee.x)as regr_slope,REGR_INTERCEPT(ee.y,ee.x)as regr_intercept,REGR_R2(ee.y,ee.x)as regr_r2,
MINHASH(100,ef.val)as minhash_val,MINHASH_COMBINE(eg.mh1,eg.mh2)as minhash_combined,
BITAND_AGG(eh.flags)as bitand_agg,BITOR_AGG(eh.flags)as bitor_agg,BITXOR_AGG(eh.flags)as bitxor_agg,
ANY_VALUE(ei.col)as any_val,
OBJECT_AGG(ej.key,ej.val)as obj_agg_result,
GROUPING(ek.dim1)as grp1,GROUPING(ek.dim2)as grp2,GROUPING_ID(ek.dim1,ek.dim2)as grp_id
FROM {{my_database}}.{{schema_name}}.{{tbl}} a
LEFT OUTER JOIN schema2.table_b b ON a.id=b.a_id AND b.active=TRUE
RIGHT JOIN schema3.table_c c ON b.id=c.b_id
FULL OUTER JOIN schema4.table_d d ON c.id=d.c_id OR d.alt_id=c.alt_id
INNER JOIN schema5.table_e e ON d.id=e.d_id
CROSS JOIN schema6.table_f f
NATURAL JOIN schema7.table_g g
LEFT SEMI JOIN schema8.table_h h ON g.id=h.g_id
LEFT ANTI JOIN schema9.table_i i ON h.id=i.h_id
JOIN LATERAL(SELECT * FROM schema10.table_j WHERE table_j.parent_id=a.id LIMIT 5)j ON TRUE
,TABLE(FLATTEN(INPUT=>a.array_col,OUTER=>TRUE))flat_alias
,TABLE(GENERATOR(ROWCOUNT=>1000))gen
,TABLE(RESULT_SCAN(LAST_QUERY_ID()))rs
WHERE a.created_at >= DATEADD(day,-30,CURRENT_DATE())
AND a.status IN ('active','pending','review')
AND a.status NOT IN ('deleted','archived')
AND a.type = ANY(SELECT DISTINCT type FROM ref_types WHERE category='main')
AND a.score > ALL(SELECT min_score FROM thresholds)
AND a.score >= SOME(SELECT target_score FROM goals)
AND EXISTS(SELECT 1 FROM related_table r WHERE r.parent_id=a.id AND r.active=TRUE)
AND NOT EXISTS(SELECT 1 FROM blacklist bl WHERE bl.id=a.id)
AND a.name LIKE '%pattern%' ESCAPE '\\'
AND a.code ILIKE 'prefix%'
AND a.description RLIKE '.*regex.*'
AND a.tags LIKE ANY('%tag1%','%tag2%')
AND a.category ILIKE ALL('cat%','%gory')
AND a.value BETWEEN 10 AND 100
AND a.value NOT BETWEEN 200 AND 300
AND a.other_col IS NOT NULL
AND a.nullable_col IS NULL
AND a.bool_col IS TRUE
AND a.other_bool IS NOT FALSE
AND a.variant_col IS NOT DISTINCT FROM b.variant_col
AND a.id = {{ some_id | default(0) }}
AND a.region IN ({% for r in regions %}'{{r}}'{% if not loop.last %},{% endif %}{% endfor %})
{% if filter_by_date %}AND a.event_date = '{{ filter_date }}'{% endif %}
{% if exclude_test %}AND a.environment != 'test'{% endif %}
{%- if custom_filter is defined -%}AND {{ custom_filter }}{%- endif -%}
GROUP BY 1,2,3,a.id,a.name,ROLLUP(b.category,c.subcategory),CUBE(d.dim1,d.dim2),GROUPING SETS((e.g1),(e.g2),(e.g1,e.g2),())
HAVING COUNT(*)>1 AND SUM(a.amount)>1000 AND {{ 'MAX(a.score)' }} > 50
QUALIFY ROW_NUMBER()OVER(PARTITION BY a.group_key ORDER BY a.rank DESC)=1
AND DENSE_RANK()OVER(ORDER BY a.score DESC)<=10),
{% endfor %}
-- Time travel queries
time_travel_cte AS(
SELECT * FROM historical_table AT(TIMESTAMP=>'2024-01-01 00:00:00'::TIMESTAMP_LTZ)
UNION ALL SELECT * FROM historical_table AT(OFFSET=>-3600)
UNION ALL SELECT * FROM historical_table AT(STATEMENT=>'8e5d0ca9-005e-44e6-b858-a8f5b37c5726')
UNION ALL SELECT * FROM historical_table BEFORE(TIMESTAMP=>'2024-01-02 00:00:00'::TIMESTAMP_LTZ)
UNION ALL SELECT * FROM historical_table BEFORE(STATEMENT=>'8e5d0ca9-005e-44e6-b858-a8f5b37c5727')
),
-- Changes clause
changes_cte AS(
SELECT * FROM my_table CHANGES(INFORMATION=>DEFAULT) AT(TIMESTAMP=>'2024-01-01'::TIMESTAMP) END(TIMESTAMP=>'2024-01-02'::TIMESTAMP)
UNION ALL SELECT * FROM my_table CHANGES(INFORMATION=>APPEND_ONLY) AT(OFFSET=>-86400) END(OFFSET=>0)
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
-- MATCH_RECOGNIZE
match_recognize_cte AS(
SELECT * FROM stock_data
MATCH_RECOGNIZE(
PARTITION BY symbol
ORDER BY trade_date
MEASURES
STRT.trade_date AS start_date,
LAST(UP.trade_date) AS end_date,
MATCH_NUMBER() AS match_num,
CLASSIFIER() AS pattern_class,
COUNT(*) AS cnt,
COUNT(UP.*) AS up_cnt,
FINAL LAST(price) AS final_price,
RUNNING AVG(price) AS running_avg
ONE ROW PER MATCH
AFTER MATCH SKIP TO LAST UP
PATTERN(STRT UP+ DOWN+)
DEFINE
UP AS UP.price > PREV(UP.price),
DOWN AS DOWN.price < PREV(DOWN.price)
)
),
-- CONNECT BY (hierarchical query)
connect_by_cte AS(
SELECT id,parent_id,name,SYS_CONNECT_BY_PATH(name,'/')AS path,LEVEL AS lvl,CONNECT_BY_ROOT name AS root_name,CONNECT_BY_ISLEAF AS is_leaf
FROM org_chart
START WITH parent_id IS NULL
CONNECT BY PRIOR id=parent_id AND LEVEL<=10
ORDER SIBLINGS BY name
),
-- Sampling
sample_cte AS(
SELECT * FROM large_table SAMPLE(10)
UNION ALL SELECT * FROM large_table SAMPLE BERNOULLI(5 ROWS)
UNION ALL SELECT * FROM large_table SAMPLE SYSTEM(10)REPEATABLE(42)
UNION ALL SELECT * FROM large_table SAMPLE BLOCK(20)SEED(123)
UNION ALL SELECT * FROM large_table TABLESAMPLE(100 ROWS)
)
-- Main query combining everything
SELECT{% raw %} {{ literal_jinja }} {% endraw %} AS raw_jinja,
u.*,o.order_total,p.product_count,
t.historical_data,c.change_record,
pv.q1_sales+pv.q2_sales+pv.q3_sales+pv.q4_sales AS total_pivoted,
upv.quarter,upv.sales AS unpivoted_sales,
mr.pattern_class,mr.match_num,
cb.path AS org_path,cb.lvl AS org_level,
s.*,
(SELECT MAX(x.val) FROM inline_table x WHERE x.id=u.id) AS scalar_subquery,
ARRAY(SELECT y.item FROM array_table y WHERE y.parent=u.id ORDER BY y.seq) AS array_subquery,
{{ generate_column_list(['a','b','c']) }} AS generated_cols,
{{ safe_divide('revenue','cost') }} AS margin,
{# This is a jinja comment that should be preserved or stripped depending on settings #}
{% for i in range(3) %}
col_{{i}} AS alias_{{i}}{% if not loop.last %},{% endif %}
{% endfor %}
FROM users_cte u
LEFT JOIN orders_cte o ON u.id=o.user_id
LEFT JOIN products_cte p ON o.product_id=p.id
LEFT JOIN time_travel_cte t ON u.id=t.user_id
LEFT JOIN changes_cte c ON u.id=c.user_id
LEFT JOIN pivot_cte pv ON u.region=pv.region
LEFT JOIN unpivot_cte upv ON u.id=upv.id
LEFT JOIN match_recognize_cte mr ON u.symbol=mr.symbol
LEFT JOIN connect_by_cte cb ON u.org_id=cb.id
LEFT JOIN sample_cte s ON u.id=s.id
UNION SELECT * FROM backup_users WHERE backup_date>CURRENT_DATE()-7
UNION ALL SELECT * FROM pending_users
UNION ALL(SELECT * FROM archived_users INTERSECT SELECT * FROM recoverable_users)
EXCEPT SELECT * FROM deleted_users
MINUS SELECT * FROM banned_users
ORDER BY u.created_at DESC NULLS LAST,u.name ASC NULLS FIRST,3,4 DESC
LIMIT {{ page_size | default(100) }} OFFSET {{ (page - 1) * page_size if page is defined else 0 }}
;

-- VALUES clause standalone
SELECT * FROM(VALUES(1,'a',TRUE),(2,'b',FALSE),(3,'c',NULL))AS t(id,letter,flag);
SELECT column1,column2 FROM VALUES(10,'x'),(20,'y'),(30,'z');

-- INSERT statements
INSERT INTO target_table(col1,col2,col3)SELECT a,b,c FROM source_table WHERE condition=TRUE;
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table(col1,col2)VALUES(1,'a'),(2,'b'),(3,'c');
INSERT OVERWRITE INTO target_table SELECT * FROM fresh_data;
INSERT ALL
WHEN type='A' THEN INTO table_a(col1,col2)VALUES(v1,v2)
WHEN type='B' THEN INTO table_b(col1,col2)VALUES(v1,v2)
WHEN type='C' THEN INTO table_c(col1,col2,col3)VALUES(v1,v2,v3)
ELSE INTO table_other(col1)VALUES(v1)
SELECT type,v1,v2,v3 FROM source_multi;

-- UPDATE statements
UPDATE target_table SET col1='new_value',col2=col2+1,col3=NULL WHERE id=123;
UPDATE target_table t SET t.col1=s.val1,t.col2=s.val2 FROM source_table s WHERE t.id=s.id AND s.active=TRUE;
UPDATE schema.table SET (a,b,c)=(SELECT x,y,z FROM other WHERE other.id=table.id) WHERE condition;

-- DELETE statements
DELETE FROM target_table WHERE created_at<DATEADD(year,-1,CURRENT_DATE());
DELETE FROM target_table t USING source_table s WHERE t.id=s.id AND s.delete_flag=TRUE;

-- MERGE statement
MERGE INTO target_table t
USING source_table s ON t.id=s.id
WHEN MATCHED AND s.delete_flag=TRUE THEN DELETE
WHEN MATCHED AND s.update_flag=TRUE THEN UPDATE SET t.col1=s.val1,t.col2=s.val2,t.updated_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.insert_flag=TRUE THEN INSERT(id,col1,col2,created_at)VALUES(s.id,s.val1,s.val2,CURRENT_TIMESTAMP())
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.orphaned=TRUE
;

-- COPY INTO statements
COPY INTO target_table FROM @stage_name/path/to/files/ FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('','NULL','null') EMPTY_FIELD_AS_NULL=TRUE TRIM_SPACE=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE) ON_ERROR='CONTINUE' PURGE=TRUE FORCE=FALSE PATTERN='.*\\.csv\\.gz';
COPY INTO target_table FROM 's3://bucket/path/' CREDENTIALS=(AWS_KEY_ID='{{aws_key}}' AWS_SECRET_KEY='{{aws_secret}}') ENCRYPTION=(TYPE='AWS_SSE_S3');
COPY INTO target_table(col1,col2,col3)FROM(SELECT $1,$2::NUMBER,$3::TIMESTAMP FROM @stage/files/) FILE_FORMAT=(TYPE='PARQUET');
COPY INTO @export_stage/output/ FROM target_table FILE_FORMAT=(TYPE='JSON') OVERWRITE=TRUE SINGLE=FALSE MAX_FILE_SIZE=100000000 INCLUDE_QUERY_ID=TRUE HEADER=TRUE;
COPY INTO 'azure://account.blob.core.windows.net/container/path' FROM target_table CREDENTIALS=(AZURE_SAS_TOKEN='{{sas_token}}') FILE_FORMAT=(TYPE='PARQUET' COMPRESSION='SNAPPY') PARTITION BY (YEAR(date_col),MONTH(date_col));

-- CREATE TABLE variations
CREATE TABLE new_table(id NUMBER(38,0) NOT NULL PRIMARY KEY,name VARCHAR(255) NOT NULL,email VARCHAR(255) UNIQUE,created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),status VARCHAR(20) DEFAULT 'active' CHECK(status IN('active','inactive','pending')),parent_id NUMBER REFERENCES parent_table(id),CONSTRAINT fk_parent FOREIGN KEY(parent_id)REFERENCES parent_table(id)ON DELETE CASCADE ON UPDATE NO ACTION);
CREATE OR REPLACE TABLE replaced_table AS SELECT * FROM source_table WHERE 1=0;
CREATE TABLE IF NOT EXISTS conditional_table LIKE template_table;
CREATE TABLE cloned_table CLONE source_table AT(TIMESTAMP=>'2024-01-01'::TIMESTAMP);
CREATE TRANSIENT TABLE transient_table(id INT,data VARIANT);
CREATE TEMPORARY TABLE temp_table AS SELECT * FROM source WHERE date_col=CURRENT_DATE();
CREATE TABLE table_with_cluster(id INT,category VARCHAR,amount NUMBER)CLUSTER BY(category,TRUNC(amount,-3));
CREATE TABLE copy_grants_table COPY GRANTS CLONE original_table;
CREATE TABLE table_with_tags(col1 INT)WITH TAG(sensitivity='pii',department='finance');
CREATE TABLE partitioned CLUSTER BY LINEAR(region,date_trunc('month',event_date))AS SELECT * FROM events;
{% for tbl in table_list %}
CREATE TABLE IF NOT EXISTS {{my_database}}.{{schema_name}}.{{ tbl }}_backup CLONE {{my_database}}.{{schema_name}}.{{ tbl }};
{% endfor %}

-- CREATE VIEW variations
CREATE VIEW simple_view AS SELECT id,name FROM base_table;
CREATE OR REPLACE SECURE VIEW secure_view AS SELECT * FROM sensitive_table WHERE user_id=CURRENT_USER();
CREATE VIEW IF NOT EXISTS conditional_view COPY GRANTS AS SELECT * FROM source;
CREATE RECURSIVE VIEW recursive_view(id,parent_id,name,level)AS SELECT id,parent_id,name,1 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id,t.parent_id,t.name,r.level+1 FROM tree t JOIN recursive_view r ON t.parent_id=r.id;
CREATE MATERIALIZED VIEW mat_view CLUSTER BY(region)AS SELECT region,SUM(amount)AS total FROM sales GROUP BY region;
CREATE VIEW view_with_policy WITH ROW ACCESS POLICY rap_policy ON(user_id)AS SELECT * FROM data;

-- CREATE FUNCTION
CREATE OR REPLACE FUNCTION calculate_tax(amount FLOAT,rate FLOAT DEFAULT 0.1)RETURNS FLOAT LANGUAGE SQL AS 'amount * rate';
CREATE FUNCTION complex_function(input VARIANT)RETURNS TABLE(id INT,name VARCHAR,value FLOAT)LANGUAGE SQL AS 'SELECT f.value:id::INT,f.value:name::VARCHAR,f.value:value::FLOAT FROM TABLE(FLATTEN(INPUT=>input))f';
CREATE SECURE FUNCTION secure_fn(x INT)RETURNS INT LANGUAGE JAVASCRIPT STRICT IMMUTABLE AS 'return X * 2;';
CREATE FUNCTION python_fn(data ARRAY)RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION='3.8' PACKAGES=('pandas','numpy') HANDLER='process_data' AS $$
import pandas as pd
import numpy as np
def process_data(data):
    df = pd.DataFrame(data)
    return df.to_dict()
$$;
CREATE FUNCTION java_fn(input STRING)RETURNS STRING LANGUAGE JAVA RUNTIME_VERSION='11' PACKAGES=('com.snowflake:snowpark:latest') HANDLER='Handler.process' AS 'public class Handler { public static String process(String input) { return input.toUpperCase(); } }';

-- CREATE PROCEDURE
CREATE OR REPLACE PROCEDURE sp_process_data(table_name VARCHAR,batch_size INT DEFAULT 1000)RETURNS VARCHAR LANGUAGE SQL AS
BEGIN
LET counter INT := 0;
LET batch_num INT := 1;
FOR rec IN (SELECT * FROM IDENTIFIER(:table_name) ORDER BY id) DO
counter := counter + 1;
IF (counter >= :batch_size) THEN
batch_num := batch_num + 1;
counter := 0;
END IF;
INSERT INTO processed_data(id,batch_id,data)VALUES(rec.id,:batch_num,rec.data);
END FOR;
RETURN 'Processed ' || :batch_num || ' batches';
END;
CREATE PROCEDURE javascript_proc(input STRING)RETURNS STRING LANGUAGE JAVASCRIPT EXECUTE AS CALLER AS $$
var result = INPUT.split(',').map(x => x.trim().toUpperCase()).join('; ');
return result;
$$;
CREATE PROCEDURE python_proc(config OBJECT)RETURNS TABLE(status VARCHAR,count INT)LANGUAGE PYTHON RUNTIME_VERSION='3.10' PACKAGES=('snowflake-snowpark-python') HANDLER='run' AS $$
from snowflake.snowpark import Session
def run(session: Session, config: dict):
    return session.sql(f"SELECT 'success' AS status, COUNT(*) AS count FROM {config['table']}").collect()
$$;

-- CREATE STAGE
CREATE STAGE my_stage URL='s3://mybucket/path/' CREDENTIALS=(AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx') ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID='xxx') FILE_FORMAT=(TYPE='CSV' FIELD_DELIMITER='|' SKIP_HEADER=1);
CREATE STAGE azure_stage URL='azure://account.blob.core.windows.net/container' CREDENTIALS=(AZURE_SAS_TOKEN='xxx');
CREATE STAGE gcs_stage URL='gcs://mybucket/path' STORAGE_INTEGRATION=gcs_int;
CREATE STAGE internal_stage ENCRYPTION=(TYPE='SNOWFLAKE_SSE') DIRECTORY=(ENABLE=TRUE AUTO_REFRESH=TRUE);

-- CREATE STREAM
CREATE STREAM my_stream ON TABLE source_table APPEND_ONLY=TRUE SHOW_INITIAL_ROWS=TRUE;
CREATE STREAM change_stream ON TABLE source BEFORE(TIMESTAMP=>'2024-01-01'::TIMESTAMP);
CREATE STREAM view_stream ON VIEW my_view;
CREATE STREAM external_stream ON EXTERNAL TABLE ext_table INSERT_ONLY=TRUE;
CREATE OR REPLACE STREAM stage_stream ON STAGE @my_stage;
CREATE STREAM directory_stream ON DIRECTORY(@my_stage);

-- CREATE TASK
CREATE TASK my_task WAREHOUSE=compute_wh SCHEDULE='5 MINUTES' AS INSERT INTO target SELECT * FROM source WHERE processed=FALSE;
CREATE TASK cron_task WAREHOUSE=compute_wh SCHEDULE='USING CRON 0 9 * * MON-FRI America/Los_Angeles' ALLOW_OVERLAPPING_EXECUTION=FALSE AS CALL daily_etl_proc();
CREATE TASK child_task AFTER parent_task WAREHOUSE=compute_wh AS UPDATE processed_flag SET done=TRUE;
CREATE TASK conditional_task WAREHOUSE=compute_wh SCHEDULE='1 HOUR' WHEN SYSTEM$STREAM_HAS_DATA('my_stream') AS MERGE INTO target t USING (SELECT * FROM my_stream) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET t.data=s.data WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.data);
CREATE TASK serverless_task USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL' SCHEDULE='10 MINUTES' AS SELECT 1;
CREATE TASK dag_root SCHEDULE='1 HOUR' AS SELECT 'root';
CREATE TASK dag_child1 AFTER dag_root AS SELECT 'child1';
CREATE TASK dag_child2 AFTER dag_root AS SELECT 'child2';
CREATE TASK dag_finalizer AFTER dag_child1,dag_child2 FINALIZE=dag_root AS SELECT 'finalizer';

-- ALTER statements
ALTER TABLE my_table ADD COLUMN new_col VARCHAR(100);
ALTER TABLE my_table DROP COLUMN old_col;
ALTER TABLE my_table RENAME COLUMN old_name TO new_name;
ALTER TABLE my_table ALTER COLUMN col1 SET DATA TYPE NUMBER(20,5);
ALTER TABLE my_table ALTER COLUMN col2 SET NOT NULL;
ALTER TABLE my_table ALTER COLUMN col3 DROP NOT NULL;
ALTER TABLE my_table ALTER COLUMN col4 SET DEFAULT 0;
ALTER TABLE my_table CLUSTER BY(category,region);
ALTER TABLE my_table DROP CLUSTERING KEY;
ALTER TABLE my_table SET DATA_RETENTION_TIME_IN_DAYS=90;
ALTER TABLE my_table SWAP WITH other_table;
ALTER TABLE my_table ADD ROW ACCESS POLICY rap ON(user_id);
ALTER TABLE my_table DROP ROW ACCESS POLICY rap;
ALTER TABLE my_table SET TAGS sensitivity='high';
ALTER STREAM my_stream SET APPEND_ONLY=FALSE;
ALTER TASK my_task RESUME;
ALTER TASK my_task SUSPEND;
ALTER TASK my_task SET WAREHOUSE=bigger_wh;
ALTER TASK my_task MODIFY WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE()!='SKIP';
ALTER VIEW my_view SET SECURE;
ALTER VIEW my_view UNSET SECURE;
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE='LARGE' MIN_CLUSTER_COUNT=2 MAX_CLUSTER_COUNT=10 SCALING_POLICY='ECONOMY';
ALTER SESSION SET QUERY_TAG='my_query';
ALTER ACCOUNT SET NETWORK_POLICY=my_policy;

-- DROP statements
DROP TABLE IF EXISTS old_table CASCADE;
DROP VIEW old_view;
DROP FUNCTION my_function(INT,VARCHAR);
DROP PROCEDURE my_proc(VARCHAR);
DROP STREAM my_stream;
DROP TASK my_task;
DROP STAGE my_stage;
DROP SCHEMA IF EXISTS old_schema RESTRICT;
DROP DATABASE old_database;

-- GRANT/REVOKE statements
GRANT SELECT ON TABLE my_table TO ROLE analyst;
GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA my_schema TO ROLE data_engineer;
GRANT USAGE ON DATABASE my_db TO ROLE reader;
GRANT USAGE ON SCHEMA my_db.my_schema TO ROLE reader;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE developer;
GRANT CREATE TABLE ON SCHEMA my_schema TO ROLE developer;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE task_admin;
GRANT ROLE child_role TO ROLE parent_role;
GRANT ROLE admin TO USER admin_user;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA my_schema TO ROLE admin;
REVOKE SELECT ON TABLE my_table FROM ROLE analyst;
REVOKE ALL PRIVILEGES ON DATABASE my_db FROM ROLE old_role;

-- SHOW and DESCRIBE statements
SHOW TABLES IN SCHEMA my_schema;
SHOW TABLES LIKE '%user%' IN DATABASE my_db;
SHOW COLUMNS IN TABLE my_table;
SHOW VIEWS IN SCHEMA my_schema;
SHOW FUNCTIONS LIKE '%calc%';
SHOW PROCEDURES;
SHOW STREAMS IN SCHEMA my_schema;
SHOW TASKS;
SHOW GRANTS TO ROLE analyst;
SHOW GRANTS ON TABLE my_table;
SHOW GRANTS OF ROLE admin;
SHOW PARAMETERS IN ACCOUNT;
SHOW PARAMETERS IN SESSION;
SHOW WAREHOUSES;
SHOW DATABASES;
SHOW SCHEMAS;
SHOW STAGES;
SHOW FILE FORMATS;
SHOW PIPES;
SHOW SHARES;
SHOW REGIONS;
SHOW REPLICATION DATABASES;
SHOW USERS LIKE '%admin%';
SHOW ROLES;
SHOW MANAGED ACCOUNTS;
DESCRIBE TABLE my_table;
DESCRIBE VIEW my_view;
DESC FUNCTION my_function(INT);
DESCRIBE PROCEDURE my_proc(VARCHAR);
DESCRIBE STREAM my_stream;
DESCRIBE TASK my_task;
DESCRIBE STAGE my_stage;

-- CALL procedure
CALL sp_process_data('target_table',500);
CALL my_proc(:bind_var,SYSTEM$TYPEOF(:another_var));
CALL dynamic_proc(PARSE_JSON('{"key":"value"}'));

-- EXECUTE statements
EXECUTE TASK my_task;
EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || table_name;
EXECUTE IMMEDIATE $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO :row_count FROM my_table;
    RETURN row_count;
END;
$$;

-- Scripting blocks
DECLARE
    my_var VARCHAR := 'hello';
    counter INT DEFAULT 0;
    result_set RESULTSET;
BEGIN
    result_set := (SELECT * FROM my_table WHERE status=:my_var);
    FOR record IN result_set DO
        counter := counter + 1;
        LET row_id INT := record.id;
        IF (record.type = 'A') THEN
            INSERT INTO type_a_table VALUES(:row_id, record.data);
        ELSEIF (record.type = 'B') THEN
            INSERT INTO type_b_table VALUES(:row_id, record.data);
        ELSE
            INSERT INTO other_table VALUES(:row_id, record.data);
        END IF;
    END FOR;
    CASE counter
        WHEN 0 THEN RETURN 'No records';
        WHEN 1 THEN RETURN 'One record';
        ELSE RETURN counter || ' records';
    END CASE;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        RETURN SQLERRM;
    WHEN OTHER THEN
        RAISE;
END;

-- Transactions
BEGIN TRANSACTION NAME my_txn;
INSERT INTO table1 VALUES(1,2,3);
SAVEPOINT sp1;
INSERT INTO table2 VALUES(4,5,6);
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

BEGIN WORK;
UPDATE accounts SET balance=balance-100 WHERE id=1;
UPDATE accounts SET balance=balance+100 WHERE id=2;
COMMIT WORK;

-- PUT/GET/REMOVE statements
PUT file:///tmp/data.csv @my_stage/path/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=10 SOURCE_COMPRESSION=GZIP;
GET @my_stage/path/file.csv file:///tmp/output/ PARALLEL=4;
REMOVE @my_stage/path/old_files/ PATTERN='.*\\.csv';
LIST @my_stage/path/ PATTERN='.*\\.parquet';
LS @internal_stage/;

-- Jinja control structures
{% if environment == 'production' %}
-- Production specific settings
SET query_tag = 'prod_query';
{% elif environment == 'staging' %}
-- Staging settings
SET query_tag = 'staging_query';
{% else %}
-- Default dev settings
SET query_tag = 'dev_query';
{% endif %}

{% for table in ['users', 'orders', 'products', 'inventory'] %}
ANALYZE TABLE {{ table }};
{% endfor %}

{% set columns = ['id', 'name', 'email', 'created_at'] %}
SELECT {{ columns | join(', ') }} FROM users;

{% macro create_index(table, column) %}
CREATE INDEX IF NOT EXISTS idx_{{ table }}_{{ column }} ON {{ table }}({{ column }});
{% endmacro %}
{{ create_index('users', 'email') }}
{{ create_index('orders', 'user_id') }}
{{ create_index('products', 'category') }}

{%- call(item) list_items(['a','b','c']) -%}
  Processing: {{ item }}
{%- endcall -%}

-- Final complex query using everything
WITH RECURSIVE org_hierarchy AS(
SELECT id,name,manager_id,1 AS level,ARRAY_CONSTRUCT(id)AS path
FROM employees WHERE manager_id IS NULL
UNION ALL
SELECT e.id,e.name,e.manager_id,h.level+1,ARRAY_APPEND(h.path,e.id)
FROM employees e JOIN org_hierarchy h ON e.manager_id=h.id
WHERE h.level<10
)
SELECT/*+ PARALLEL(8) */
o.id,o.name,o.level,o.path,
ARRAY_TO_STRING(o.path,'>')AS path_str,
m.name AS manager_name,
COUNT(*)OVER()AS total_employees,
SUM(CASE WHEN o.level=1 THEN 1 ELSE 0 END)OVER()AS executives,
LISTAGG(o.name,', ')WITHIN GROUP(ORDER BY o.name)OVER(PARTITION BY o.level)AS peers,
{% for metric in ['salary','bonus','stock'] %}
SUM(e.{{ metric }})OVER(PARTITION BY o.manager_id)AS team_{{ metric }}_total{% if not loop.last %},{% endif %}
{% endfor %}
FROM org_hierarchy o
LEFT JOIN employees m ON o.manager_id=m.id
LEFT JOIN employee_compensation e ON o.id=e.employee_id
WHERE o.level <= {{ max_level | default(5) }}
{% if department_filter is defined %}AND o.department_id IN ({{ department_filter | join(',') }}){% endif %}
ORDER BY o.path
LIMIT 1000
;
