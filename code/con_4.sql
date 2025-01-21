    select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_023' and thang_kt is null;

select 
from ttkd_Bsc.nhanvien_202401
where a.ma_nv = ''  
select manv, HCM_LUONG_QLDB_008 from ttkd_Bsc.bangluong_dongia_qldb where thang = 202402;
select distinct loai_tinh, ma_kpi from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202401
;
select MANV, TEN_VTCV, HCM_LUONG_QLDB_053,HCM_LUONG_QLDB_054,HCM_LUONG_QLDB_055,HCM_LUONG_QLDB_056,HCM_LUONG_QLDB_057,HCM_LUONG_QLDB_058,HCM_LUONG_QLDB_059
,HCM_LUONG_QLDB_060,HCM_LUONG_QLDB_061,HCM_LUONG_QLDB_062,HCM_LUONG_QLDB_008 from ttkd_Bsc.bangluong_dongia_qldb where thang = 202402;

select sum(hcm_luong_qldb_024) from ttkd_Bsc.bangluong_dongia_qldb where thang = 20240

select sum(dongia) from ttkd_bsc.ct_dongia_tratruoc where thang = 202401 and loai_Tinh = 'DONGIATRA30D';

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv  where thang_kt is null;

SELECT * 
FROM TTKD_BSC.nhanvien_202402 ;
JOIN TTKD_BSC.blkpi_danhmuc_kpi_vtcv 
ON TTKD_BSC.nhanvien_202402.ma_vtcv = TTKD_BSC.blkpi_danhmuc_kpi_vtcv.ma_vtcv
WHERE TTKD_BSC.nhanvien_202402.ma_nv = 'CTV079490';

select count(b.ma_kpi) sl_makpi
from TTKD_BSC.nhanvien_202402 a
    join TTKD_BSC.blkpi_danhmuc_kpi_vtcv b on a.ma_Vtcv=  b.ma_vtcv 
where  thang_kt is null and a.ma_nv = 'CTV079490';
