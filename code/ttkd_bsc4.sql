update nhanvien set nhomld_id=
    (case  when substr(upper(loai_ld),1,3) like 'CH_' then 2
                when substr(upper(loai_ld),1,3)='CTV' then 3                
                end)
where thang='202404' and donvi='VTTP';
SELECT* FROM nhanvien where thang='202404' and donvi='VTTP';
COMMIT;