select * from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202404 and ma_to is null; where ma_nv in ('VNP016864','VNP017087','CTV079721','CTV079721');

select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202404 and loai_tinh = 'KPI_TO' and ma_to is null;

UPDATE ct_bsc_Tratruoc_moi_tr30day a set ma_to = (select ma_to from ttkd_Bsc.nhanvien_202404 where a.manv_Giao = ma_nv)
where thang = 202404 and ma_to is null;
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU, to_char(NGAY_TT,'dd/mm/yyyy') ngay_tt, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID,  TIEN_KHOP, GHICHU
from ttkd_bsc.ct_dongia_tratruoc A
where loai_tinh in ('DONGIATRU_CA')
commit;

select* from ct_Bsc_tratruoc_moi_tr30day;

insert into ttkd_bsc.ct_Bsc_tratruoc_moi_tr30day(THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO, 
PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, KQ_POPUP, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, 
TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, 
KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, MAPB_THPHUC, HESO_GIAO, HT_TRA_ID, KENHTHU_ID, KHDN, MANV_GT, MANV_THUNGAN, NGAY_NGANHANG)
select thang, gh_id, pbh_ql_id, pbh_giao_id, tbh_Giao_id, pbh_Th_id, pbh_cn_id, ma_tb, manv_cs, phong_cs, ma_to, ma_pb, manv_giao,phong_giao, manv_th, phong_th, manv_cn, manv_thphuc,
null, null ,phong_cn, thang_ktdc_cu, tien_Dc_cu, ma_tt, ma_gd, rkm_id,thang_bd_moi, so_thangdc, avg_thang,tien_thanhtoan, vat, ngay_tt, null, soseri, seri, kenhthu, ten_nganhang, ten_ht_Tra,
trangthai_Tb, thuebao_id, loaitb_id, null, pbh_oa_id, manv_oa, nhomtb_id, khachhang_id, goi_old_id, phieutt_id, tien_khop, ma_chungtu, mapb_thphuc, heso_giao, ht_Tra_id, kenhthu_id,
khdn, manv_Gt,manv_thungan, ngay_nganhang
from ct_Bsc_tratruoc_moi_tr30day;