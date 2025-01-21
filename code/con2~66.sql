select * from ttkd_bsc.nhanvien_202411_dieuchuyen;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_nv in (
select ma_nv from ttkd_bsc.nhanvien_202411_moi);
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and loai_tinh = 'DONGIATRA_OB'