select* from ttkd_Bsc.nhanvien_vttp_potmasco where thang is null;
update  ttkd_Bsc.nhanvien_vttp_potmasco set thang = 202408 where thang is null and loai_ld is null;
select* from  ttkd_Bsc.nhanvien_vttp_potmasco where thang = 202407;
commit;
delete from  ttkd_Bsc.nhanvien_vttp_potmasco  where ma_nv is null;
desc dhsx.v_ghtt_ipcc@dataguard;
select* from  dhsx.v_ghtt_ipcc@dataguard where td_Th is not null;
select* from admin_hcm.nhanvien where nhanvien_id in (13948,	451205);
select* from MYTV_HH;
select *
from gv$sql
where lower(sql_fulltext) like '%create table mytv_hh%';

select* from ttkd_Bsc.nhanvien where thang = 202409 and donvi = 'TTKD';