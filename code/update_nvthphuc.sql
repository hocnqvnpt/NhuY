select ma_nv from ADMIN_HCM.NHANVIEN where nhanvien_id_goc = 2267
select thuebao_id from  hocnq_ttkd.tmp3_30ngay 
select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310 and manv_thphuc = 'khongco'
-- 30 day: 21679 khongco
-- update: 673 
-- 60 day: 12942 khongco
-- update: 659
commit;
create table khongco as select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202310 where manv_thphuc = 'khongco'
select* from hocnq_ttkd.tmp3_60ngay
select mapb_thphuc from ttkd_bsc.ct_bsc_tratruoc_moi where manv_thphuc = 'khongco' and thang = 202310
select* from tmp3_60ngay

update ttkd_bsc.ct_bsc_tratruoc_moi c
set manv_thphuc = (
select b.ma_nv
from tmp3_60ngay a
join admin_hcm.nhanvien_onebss b on a.nvtuvan_id = b.nhanvien_id
where  c.thang = 202311 and c.phieutt_id = a.phieutt_id and c.thuebao_id = a.thuebao_id 
)
where c.thang = 202311 and c.manv_thphuc = 'khongco' and c.thuebao_id = (select a.thuebao_id
from tmp3_60ngay a where a.thuebao_id = c.thuebao_id and a.phieutt_id = c.phieutt_id) 
commit;
select* from ct_bsc_tratruoc_moi_30day where thang = 202311
select distinct nvtuvan_id  from tmp3_30ngay
where nvtuvan_id in (select nhanvien_id from admin_hcm.nhanvien_onebss ) and nvtuvan_id not in (select nhanvien_id from admin_hcm.nhanvien)
89112
93683
91687
94315
13987
93071
94444
90925
477648
451222
8916
93852
93640
8461
9118
90450
9059
14049
94691
select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where manv_thphuc = 'khongco' and thang = 202310
select 1 from admin_hcm.nhanvien_onebss where nhanvien_id not in (select nhanvien_id from admin_hcm.nhanvien )
select* from 
select thuebao_id, phieutt_id, tien_khop from ttkd_bsc.ct_bsc_Tratruoc_Moi_30day where rkm_id is not null and manv_thphuc is null and thang = 202310
select thuebao_id, phieutt_id from ttkd_bsc.ct_bsc_tratruoc_moi where manv_thphuc != 'khongco' and thang = 202310
select* from admin_hcm.nhanvien_onebss where nhanvien_id =  -1

select* from ttkd_bsc.ct_bsc_Tratruoc_moi where thang = 202310
update 
select ma_Nv from admin_hcm.nhanvien_onebss where nhanvien_id = 91687
select phieutt_id from hocnq_ttkd.tmp3_30ngay where rownum = 1
select phieutt_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202310 and manv_thphuc = 'khongco'
select manv_Thphuc from  khongco where thang = 202310 and thuebao_id = 9326899


update ttkd_bsc.ct_bsc_tratruoc_moi b
set mapb_Thphuc = (select a.ma_pb from 
ttkd_Bsc.nhanvien_202310 a where a.ma_nv = b.manv_thphuc and b.thang = 202310)
where thang = 202310 and mapb_thphuc is null and manv_thphuc != 'khongco'
commit;
create table dongia as 
select * from ttkd_bsc.bangluong_dongia_qldb where thang = 202310 

-- 53
SELECT * FROM ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang_kt is null

SELECT * FROM ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang_kt is null