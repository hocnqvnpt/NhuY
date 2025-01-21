update ttkd_Bsc.nhanvien a
set tinh_bsc = 0 ;
select* from ttkd_Bsc.nhanvien a
--    join temp_nv on a.ma_nv = mã_nv_hrm
where thang = 202407 and donvi = 'TTKD' and exists (select 1 from temp_nv where a.ma_nv = mã_nv_hrm and NGÀY_THÁNG_VÀO_BSC > sysdate) ;
commit ;
insert into ttkd_bsc.blkpi_danhmuc_sms (ma_pb, ma_nv, sdt)
select a.ma_pb, 'CTV085863' , '0853050901'
from ttkd_bsc.dm_phongban a--, (select 'bnp' from dual) b
where active = 1 and nhom_pb in ('BHKV', 'KHDN')
;
rollback ;
select* from ttkd_bsc.blkpi_danhmuc_sms ;
update ttkd_bsc.blkpi_danhmuc_sms set  ma_pb = SUBSTR(ma_pb, 0, LENGTH(ma_pb) - 1) ;
update ttkd_bsc.blkpi_danhmuc_sms set  ma_pb = ma_pb || '0' ;
delete from ttkd_bsc.blkpi_danhmuc_sms  where ma_Nv = 'CTV083743' ;
commit ;
select* from ttkd_Bsc.blkpi_danhmuc_kpi;

update ttkd_Bsc.blkpi_danhmuc_kpi set thuchien = 'Ý' , manv_lead = 'CTV083743' where ma_kpi = 'HCM_SL_HOTRO_006';


