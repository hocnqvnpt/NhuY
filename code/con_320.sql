create table bang_gom as
select THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, NGAY_BDDC_CU, NGAY_KTDC_CU, CUOC_DC_CU, SO_THANGDC_CU, MA_GD, RKM_ID, NGAY_BD_MOI, NGAY_KT_MOI, TIEN_HOPDONG, TIEN_THANHTOAN, 
VAT, NGAY_TT, NGAY_HD, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TD_TH, MA_ND_OB, NHANVIEN_OB_ID, MANV_OB, MA_TO, MA_PB, THANG_KT, NHOMTB_ID_CU, HDTB_ID,
HDKH_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_YC, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID,
NHOMTB_ID, KHACHHANG_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, LOAITB_ID, THANHTOAN_ID, NHANVIEN_XUATHD, TIEN_KHOP, MA_CHUNGTU 
from chot_final a
where not exists (select 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202404 and rkm_id = a.rkm_id)
union all
select THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, NGAY_BDDC_CU, NGAY_KTDC_CU, CUOC_DC_CU, SO_THANGDC_CU, a.MA_GD, RKM_ID, NGAY_BD, NGAY_KT, a.TIEN_HOPDONG, a.TIEN_THANHTOAN 
,a.VAT_thanhtoan, a.NGAY_TT, a.NGAY_HD, a.NGAY_NGANHANG, a.SOSERI, a.SERI, a.KENHTHU, a.TEN_NGANHANG, a.TEN_HT_TRA, a.TD_TH, a.MA_ND_OB, a.NHANVIEN_ID_ob, a.MANV_OB, a.MA_TO, a.MA_PB,
to_Number(to_char(ngay_Ktdc_cu,'yyyymm')) thang_kt
, goi_old_id, HDTB_ID
,a.HDKH_ID, a.NVTUVAN_ID, NVTHU_ID, a.THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_YC, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID
,NHOMTB_ID, a.KHACHHANG_ID, a.PHIEUTT_ID, a.HT_TRA_ID, a.KENHTHU_ID, a.LOAITB_ID, THANHTOAN_ID, c.ma_nv NHANVIEN_XUATHD, TIEN_KHOP, MA_CHUNGTU
from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a
    left join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
    left join css_hcm.hd_khachhang hd on b.hdkh_id = hd.hdkh_id
    left join admin_hcm.nhanvien_onebss c on b.thungan_hd_id = c.nhanvien_id
where thang = 202404 and rkm_id is not null;
drop table bang_gom;
rollback;
select* from ds_giahan_tratruoc where ma_tb = 'hcmvinh2907app'