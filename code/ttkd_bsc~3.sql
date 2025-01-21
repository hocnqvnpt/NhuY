select hcm_tb_giaha_026, ma_Vtcv, ten_vtcv, ten_nv, ma_Nv from ttkd_bsc.bangluong_kpi_202403 where ma_vtcv in 
(select ma_Vtcv from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_026' and thang_kt is null);
SE
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202403 and manv_giao in (select ma_Nv from ttkd_bsc.bangluong_kpi_202403 where ma_vtcv in 
(select ma_Vtcv from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_026' and thang_kt is null) and HCM_TB_GIAHA_026 is null)
and loaitb_id  in (147,148) and thang_ktdc_cu = 202403;-- and ma_kpi ='HCM_TB_GIAHA_024'

update ttkd_bsc.bangluong_kpi_202403 set HCM_TB_GIAHA_024 = 2 where ma_nv = 'VNP016983'
rollback;

select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU' AND MA_NV IS NOT NULL AND TIEN_KHOP = 0 AND  MA_TB IN ('hcm_ivan_00025257','hcm_ivan_00024990')

select* from css_hcm.ct_tienhd where phieutt_id in (6454198,6475673) and KHOANMUCTT_ID ;

update ttkd_bsc.ct_dongia_tratruoc 
SET TIEN_DC_CU = 690000
where thang = 202403 and loai_tinh  = 'DONGIATRU' AND TIEN_DC_CU = 0;
COMMIT;
update ttkd_bsc.ct_dongia_tratruoc 
SET DONGIA = 0.11*TIEN_DC_CU
WHERE  thang = 202403 and loai_tinh  = 'DONGIATRU' AND TIEN_KHOP = 0 AND MA_TB IN ('hcm_ivan_00025257','hcm_ivan_00024990')

DELETE FROM TTKD_bSC.TL_GIAHAN_TRATRUOC WHERE thang = 202403 and loai_tinh  = 'DONGIATRU' 
SELECT DISTINCT LOAI_TINH , MA_KPI FROM TTKD_bSC.TL_GIAHAN_TRATRUOC WHERE thang = 202403 --and loai_tinh  = 'DONGIATRU' 

select * from (
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU, to_char(NGAY_TT,'dd/mm/yyyy'), HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO, TIEN_KHOP, GHICHU
from ttkd_bsc.ct_dongia_tratruoc A
where loai_tinh in ('DONGIATRU')
) where THANG = 202403