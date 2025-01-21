*********13. Week num with the Monday is first day of week

WITH DATA AS (SELECT (ROWNUM  -1 )* 7 AS rn FROM dual CONNECT BY LEVEL < 55)

select to_char(TRUNC(SYSDATE, 'YYYY') + rn, 'iw') AS week
                , TRUNC(TRUNC(SYSDATE, 'YYYY') + rn, 'iw') AS iso_week_start_date
                , TRUNC(TRUNC(SYSDATE, 'YYYY') + rn, 'iw') + 7 - (1/86400) AS iso_week_end_date
from DATA;

*********14. Week num in 1 month with the Monday is first day of week

  with dates (dt, b) as (
                 select  trunc(sysdate, 'month') + level - 1, level -1
                 from    dual
                 connect by level <= 31
                )
select dt, (next_day(dt, 'Mon') - next_day(trunc(dt, 'mm'), 'Mon'))/7 + 1 as week_no_in_month
                    , to_char(dt, 'iw') AS iso_week_no
from   dates
order  by dt
;