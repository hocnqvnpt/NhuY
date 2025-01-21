select distinct ht_Tra_id, kenhthu_id from vttp where rkm_id is not null and tien_khop is null;
update vttp set tien_khop = 2 where tien_khop is null and ma_gd not in (select ma_gd from qrcode) and ht_tra_id = 216-- and ma_Tb not in (select ma_Tb from ttn_t4);
commit;
create table dongia_moi_tmp as 
select THANG, cast(LOAI_TINH as varchar(20)) loai_tinh,cast( MA_KPI as varchar(50)) ma_kpi, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, MANV_OB, TIEN_DC_CU, MA_TO, MA_PB
, cast(MA_TB as varchar(100)) ma_Tb, MA_NV, NHOMTB_CU_ID, NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC,
HT_TRA_ONLINE, KENHTHU_TAINHA, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, NHOMTB_ID, TIEN_KHOP, RNK, DONGIA 
from dongia_moi;
drop table  dongia_moi purge;
rename  dongia_moi_tmp to  dongia_moi;
select* from dongia_moi;
update dongia_moi set ma_kpi = trim(ma_kpi) ;
commit;
select* from ttkd_Bsc.nhanvien_202404 where ma_nv = 'VNP016923';

delete from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202404 and loai_Tinh ='KPI_NV';
COMMIT
