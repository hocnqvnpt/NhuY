insert into ttkd_Bsc.dongia_vttp 
select distinct 202404 THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU,
NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON
from ttkd_Bsc.dongia_vttp a where thang = 20240401 and loai_tinh = 'DONGIA_TS_TP_TT';
update ttkd_Bsc.dongia_vttp a set thang = 20240401 where thang = 202404 and loai_tinh = 'DONGIA_TS_TP_TT';
commit;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.dongia_vttp  where thang = 202405;
select* from  ttkd_Bsc.dongia_vttp  where thang = 202405 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_TS_TP_TT';
delete from
--select* from 
ttkd_Bsc.dongia_vttp a
where thang = 202405 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_POTMASCO' and
exists (select 1 from ttkd_Bsc.dongia_vttp  where thang = 202406 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRA_OB' and ma_nv= a.ma_Nv and ma_Tb=a.ma_Tb) ;