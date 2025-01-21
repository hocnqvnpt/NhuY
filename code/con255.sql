select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202409;
commit;
merge into ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a
using (
  select distinct khachhang_id, phieutt_id, ma_GD, HDKH_ID, MA_ND_OB,NHANVIEN_ID_OB,MANV_OB, THANG, NVTUVAN_ID
  from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt
  WHERE THANG = 202409 AND OB_ID IS NOT NULL
) b
on (a.THANG = b.THANG AND A.KHACHHANG_ID = B.KHACHHANG_ID AND A.PHIEUTT_ID = B.PHIEUTT_ID AND A.MA_GD = B.MA_GD AND A.NVTUVAN_ID = B.NVTUVAN_ID AND A.HDKH_ID = B.HDKH_ID)
when matched then
  update set a.MA_nD_OB = B.MA_ND_OB , A.NHANVIEN_ID_OB = B.NHANVIEN_ID_OB, A.MANV_OB = B.MANV_OB
WHERE A.OB_ID IS NULL
;
select ma_gd from (
  select khachhang_id, phieutt_id, ma_GD, HDKH_ID, MA_ND_OB,NHANVIEN_ID_OB,MANV_OB, THANG, NVTUVAN_ID
  from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt
  WHERE THANG = 202406 AND OB_ID IS NOT NULL
) group by ma_Gd having count(ma_gD)>1;
commit;
update ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt set ipcc =1 where thang = 202406 and manv_ob is not null;
select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt  where thang = 202406 and ipcc = 1;
update ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt set ipcc =0 where thang = 202406 and ipcc is null;
select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a where thang = 202405 and nvl(ipcc,0)=0 and exists
    (select 1 from  ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt 
    where thang = 202405 and ipcc = 1 and a.ma_gd = ma_gd and a.manv_thuyetphuc = manv_thuyetphuc );
alter table tmp_update add  (thang number);
update tmp_update set thang  = 202405 ;
create table tmp_update as ;
insert into tmp_update
with ma_Tb as 
(select  ma_tb, thuebao_id, hdkh_id,to_number(to_char(ngay_yc,'yyyymmdd')) ngay_yc from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang =202406 and manv_thuyetphuc = 'MEDIAPAY')
, ds as (select b.thuebao_id, b.ma_tb, max(a.hdkh_id) hdkh_id, max(a.ngay_yc) ngay_yc--, a.ma_gd
from css_hcm.hd_khachhang a 
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
    join ma_tb c on b.thuebao_id = c.thuebao_id
where a.hdkh_id < c.hdkh_id and b.ngay_tt is not null and to_number(to_char(a.ngay_yc+45,'yyyymmdd'))  >= c.ngay_yc 
group by b.thuebao_id, b.ma_Tb)
--select * from ds where ma_Tb ='tminhson105';
select ds.*, c.ma_to, c.ma_pb, c.ma_nv, 202406 thang
from ds
    left join css_Hcm.hd_khachhang a on ds.hdkh_id = a.hdkh_id
    left join admin_hcm.nhanvien_onebss b on a.ctv_id = b.nhanvien_id
    left join ttkd_Bsc.nhanvien_202406 c on b.ma_nv = c.ma_nv
;
commit;
select* from tmp_update;
update ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a
set manv_thuyetphuc = (select ma_nv from tmp_update where thang = a.thang and thuebao_id = a.thuebao_id),
ma_to = (select ma_to from tmp_update where thang = a.thang and thuebao_id = a.thuebao_id),
ma_pb =  (select ma_pb from tmp_update where thang = a.thang and thuebao_id = a.thuebao_id)
where thang = 202406 and manv_Thuyetphuc like 'MEDIA%' and thuebao_id in (select thuebao_id from tmp_update where thang = 202406);
rollback;