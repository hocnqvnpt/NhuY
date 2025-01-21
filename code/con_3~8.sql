select * from (
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB,  THUEBAO_ID, MA_TB,   MANV_THUYETPHUC, SOTHANG_DC, KHUVUC, DONGIA, DTHU, 
to_char(NGAY_TT,'dd/mm/yyyy') ngay_tt,  NHOMTB_ID, KHACHHANG_ID, HESO_CHUKY,hESO_DICHVU, TIEN_KHOP, GHICHU, TYLE_THANHCONG, NHANVIEN_XUATHD,
TIEN_XUATHD, TIEN_THUYETPHUC,ipcc, round(tien_thuyetphuc*heso_chuky*heso_dichvu/0.8) doanhthu_021 ,round(tien_thuyetphuc*heso_chuky*heso_dichvu) tien,
'tien = tien_thuyetphuc*heso_chuky*heso_dichvu' cach_Tinh
from ttkd_bsc.ct_dongia_tratruoc
 WHERE LOAI_TINH IN ('DONGIATRA_OB')
 union all
 select THANG, LOAI_TINH, MA_KPI,  MA_NV, MA_TO, MA_PB,  THUEBAO_ID, MA_TB,   MANV_THUYETPHUC, SOTHANG_DC, KHUVUC, DONGIA, DTHU, 
to_char(NGAY_TT,'dd/mm/yyyy') ngay_tt,  NHOMTB_ID, KHACHHANG_ID, HESO_CHUKY,hESO_DICHVU, TIEN_KHOP, GHICHU, TYLE_THANHCONG, NHANVIEN_XUATHD,
TIEN_XUATHD, TIEN_THUYETPHUC,ipcc, round(dongia*heso_chuky*heso_dichvu/0.8) doanhthu_021 ,round(DONGIA*heso_chuky*heso_dichvu) tien,
'tien = DONGIA*heso_chuky*heso_dichvu' cach_Tinh
from ttkd_bsc.ct_dongia_tratruoc
 WHERE LOAI_TINH IN ('DONGIA_TS_TP_TT') 
union all 
select a.THANG, LOAI_TINH, MA_KPI,  b.MA_NV,b.MA_TO, b.MA_PB,  THUEBAO_ID, MA_TB,   MANV_THUYETPHUC, SOTHANG_DC, KHUVUC, DONGIA, DTHU, 
to_char(NGAY_TT,'dd/mm/yyyy') ngay_tt,  NHOMTB_ID, KHACHHANG_ID, HESO_CHUKY,hESO_DICHVU, TIEN_KHOP, GHICHU, TYLE_THANHCONG, NHANVIEN_XUATHD,
TIEN_XUATHD, TIEN_THUYETPHUC,ipcc, round(tien_xuathd*heso_chuky*heso_dichvu/0.8) doanhthu_021 ,round(tien_xuathd*heso_chuky*heso_dichvu) tien,
'tien = tien_xuathd*heso_chuky*heso_dichvu' cach_Tinh
from ttkd_Bsc.ct_dongia_tratruoc a
    join ttkd_Bsc.nhanvien b on a.nhanvien_xuathd = b.ma_Nv and a.thang = b.thang
where a.loai_tinh = 'DONGIATRA_OB' AND A.TIEN_XUATHD > 0 
);
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where ma_Tb = 'hcm_ca_ivan_00019252';
select * from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202410 and ma_Tb in ('hcm_ca_ivan_00019724','hcm_ca_ivan_00019731','hcm_ca_ivan_00019252');
select * from ttkd_bct.bangiao_chungtu_tinhbsc;
select * from ttkdhcm_ktnv.ds_chungtu_nganhang_pqt_sudung;
select ma_Gd,ma_Chungtu from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410 and tien_khop = 0;
commit;
select ten_pb from ttkd_Bsc.nhanvien where thang = 202410 and ma_nv='CTV041776';
update ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt set tien_khop = 1
where thang = 202410 and tien_khop = 0 and ma_gd in
    (select ma_Gd from ttkdhcm_ktnv.ds_chungtu_nganhang_pqt_sudung a join ttkd_bct.bangiao_chungtu_tinhbsc b on a.ma_Ct = b.ma_ct )