select* from ttkd_Bsc.CT_DONGIA_TRATRUOC where thang = 202404 and loai_tinh in ('DONGIA_VTTP','DONGIATRA_OB','DONGIA_OB_VTTP','DONGIA_TS_TP_TT') ;
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202404 and rkm_id is not null;
-- ma_nv_cn: nhanvien xuat hoa don
-- HESO_GIAO: ngay_yeu cau
SELECT* FROM css_hcm.hd_khachhang where hdkh_id = 22125044;

select* from cs
select* from admin_
select a.*,11
from ttkd_bsc.ct_Dongia_tratruoc a
    left join ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt b on a.thuebao_id = b.thuebao_id and a.thang = b.thang and a.ma_Nv = b.manv_thuyetphuc and a.ngay_tt = b.ngay_tt
where a.thang = 202404 and a.loai_tinh in ('DONGIA_VTTP','DONGIATRA_OB','DONGIA_OB_VTTP','DONGIA_TS_TP_TT') ;
ROLLBACK;
SELECT * FROM  ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt B where B.RKM_ID IS NOT NULL AND B.thang = 202404 GROUP BY THUEBAO_ID, manv_thuyetphuc, ngay_tt HAVING COUNT (manv_thungan) > 1;

up