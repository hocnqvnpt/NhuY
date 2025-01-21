, xi2 as (select a.thuebao_id, case when vattu_id in  (892, 891, 893) then 'I-240W'
                            when vattu_id in  (890, 889,  896, 884,  895, 883) then 'HG8xx'
                            when vattu_id in (888, 14914, 14123, 14890) then 'GWxx0-HiB'
                            when vattu_id in (894, 886, 885, 11713, 887, 14699, 13781, 14125, 14098, 14124) then 'GWxx0'
                            when vattu_id in (16456) then 'F671Y-HiB'
                end ONT_TYPE
            from hocnq_ttkd.dulieu_kn_ont a where rnk = 1
),
case when xi2.ont_type like '%HiB' then 1 when xi2.ont_type is null then 1 else 0 end ONT_HiB