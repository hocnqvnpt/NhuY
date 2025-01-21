
    select distinct heso_Thulao, loai_diem_ban from ds_diemban;
    create table TMP_thulao_HHBG as select* from hocnq_ttkd.TMP_thulao_HHBG where 1 =22;
-- updatee danh sach diem  ban ;
update ds_diemban set thang = 2024031 where thang is null;
-- 
update ds_Diemban set heso_Thulao = 2 where loai_diem_Ban in ( 'Pháp nhân TCT' ,'Pháp nhân c?p TTKD') and thang = 2024031  ;
update ds_Diemban set heso_Thulao = 2 where loai_diem_Ban in ( '?i?m bán l?')  and msthue is not null and thang = 2024031;
update ds_Diemban set heso_Thulao = 0 where loai_diem_Ban in ( '?i?m bán l?')  and msthue is null and thang = 2024031;
update ds_Diemban set heso_Thulao = 1 where heso_thulao is null and thang = 2024031;

rollback;

commit;


insert into ttkd_bsc.ct_bsc_thulao_hhbg
select 202403, 1, a.*, 0
from TMP_thulao_HHBG a;

update ttkd_bsc.ct_bsc_thulao_hhbg a
set mucchi_dot_2_bs =
 mucchi_dot_2*
    (select heso_thulao from 
    (select b.*,  row_number() over(partition by so_eload order by loai_diem_ban desc) rn from ds_diemban b where thang = 2024031)
    where thang = 2024031 and rn = 1 and so_eload =a.so_eload) -- a.so_eload)
--from ttkd_bsc.ct_bsc_thulao_hhbg a
where thang = 202403 and lan_import = 1;

select* from ds_diemban where thang = 2024031 and so_eload = '84816793539'
select* from ttkd_bsc.ct_bsc_thulao_hhbg 

update ttkd_bsc.ct_bsc_thulao_hhbg
set tong_thulao_hhbg = mucchi_dot_1 + mucchi_dot_2_bs
where thang = 202403 and lan_import = 1;
-----
commit;
select so_eload, mucchi_dot_2_bs, mucchi_dot_2
from ttkd_bsc.ct_bsc_thulao_hhbg  where thang = 202403 and lan_import = 1 and mucchi_dot_2_bs != mucchi_dot_2;
commit;
select distinct loai_diem_ban from ds_diemban where  thang = 2024031 and loai_diem_ban  in ('Pháp nhân cha c?p huy?n','Pháp nhân c?p huy?n') order by loai_diem_ban desc -- so_eload in (
select so_eload  from ds_diemban where  thang = 2024031 group by so_eload having count (so_eload) > 1)
rollback;