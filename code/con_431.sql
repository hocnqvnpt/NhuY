select * from manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2;
with cte as (
        SELECT SUBSCRIBER_ID, PBHKV_T11 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T11 is not null
        UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T10 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T10 is not null
        UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T09 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T09 is not null
        UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T08 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T08 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T07 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T07 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T06 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T06 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T05 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T05 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T04 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T04 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T03 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T03 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T02 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T02 is not null
         UNION ALL
        SELECT SUBSCRIBER_ID, PBHKV_T01 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2 where PBHKV_T01 is not null
--         UNION ALL
--        SELECT SUBSCRIBER_ID, col4 AS value FROM manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2
    ),
    rnk as (
    SELECT 
        SUBSCRIBER_ID,
        value MOST_FREQUENT_VALUE,
        COUNT(*) AS frequency,
        RANK() OVER (PARTITION BY SUBSCRIBER_ID ORDER BY COUNT(*) DESC) AS rank
    FROM cte
    GROUP BY SUBSCRIBER_ID, value
)
, fin as (SELECT 
    SUBSCRIBER_ID, 
    most_frequent_value,
    frequency
FROM rnk
WHERE rank = 1) select* from fin;
select MOST_FREQUENT_VALUE, count(SUBSCRIBER_ID) as sltb
from fin 
group by MOST_FREQUENT_VALUE ;

with cte as(
select a.*,  count(VAL) over(partition by SUBSCRIBER_ID, VAL order by SUBSCRIBER_ID desc) ra
from (
select SUBSCRIBER_ID, COL, VAL
from manpn.KHKT_TAPTHUEBAO_2024_TH@ttkddbbk2
unpivot ( val for col in ( PBHKV_T11, PBHKV_T10, PBHKV_T09, PBHKV_T08, PBHKV_T07, PBHKV_T06
                                                , PBHKV_T05, PBHKV_T04, PBHKV_T03, PBHKV_T02, PBHKV_T01 )) 
    ) a
)
select * 
from (
    select a.*, row_number() over(partition by SUBSCRIBER_ID order by RA desc, COL desc) rank
    from cte a
    )
where rank = 1;