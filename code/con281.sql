select* from ttkd_Bsc.nhanvien where thang = 202408 and donvi = 'TTKD' and  MA_VTCV = 'VNP-HNHCM_KDOL_15' ;
UPDATE  ttkd_Bsc.nhanvien
SET TEN_VTCV = 'Nhân Viên Outbound Gia H?n Tr? Tr??c Phòng Bán Hàng Online' , MA_VTCV = 'VNP-HNHCM_KDOL_15'
where thang = 202408 and donvi = 'TTKD' AND MA_nV IN (SELECT MA_nV FROM OBGHTT) AND MA_VTCV != 'VNP-HNHCM_KDOL_15' ;
COMMIT;
select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_Kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023');
insert into ttkd_Bsc.bangluong_kpi (THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG);
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202408 and ma_Kpi in ('HCM_CL_TNGOI_004','HCM_SL_ORDER_001');
select thang , 'HCM_CL_TNGOI_004' ma_kpi,'T? l? th?i gian Talktime ti?p nh?n cu?c g?i_OB GHTT' ten_kpi, ma_Nv, ten_nv, ma_vtcv, ten_vtcv, ma_to, ten_to, ma_pb, ten_pb, 22 ngaycong
from ttkd_Bsc.nhanvien
where thang = 202408 and donvi = 'TTKD' AND MA_nV IN (SELECT MA_nV FROM OBGHTT) and ma_Nv not in ('CTV021801','CTV075484');
update ttkd;
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202408 and ma_nv = 'VNP017528';
select distinct ma_Nv, ten_Vtcv from ttkd_bsc.bangluong_kpi_dot1_20240925 where ma_nv in (select ma_nv from obghtt ) and thang = 202408 and ma_kpi not in ('HCM_CL_TNGOI_004','HCM_SL_ORDER_001');
insert into ttkd_Bsc.bangluong_kpi (THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG)
select thang , 'HCM_SL_ORDER_001' ma_kpi,'S? l??ng ch?t ??n hàng GHTT thành công' ten_kpi, ma_Nv, ten_nv, ma_vtcv, ten_vtcv, ma_to, ten_to, ma_pb, ten_pb, 22 ngaycong
from ttkd_Bsc.nhanvien
where thang = 202408 and donvi = 'TTKD' AND MA_nV IN (SELECT MA_nV FROM OBGHTT) and ma_Nv not in ('CTV021801','CTV075484');
SELECT* FROM ;
rollback;
COMMIT;
UPDATE ttkd_bsc.bangluong_kpi SET mucdo_hoanthanh = null , GIAO = NULL, THUCHIEN = NULL, TYLE_THUCHIEN = NULL;
SELECT* FROM  ttkd_bsc.bangluong_kpi 
where thang = 202408 and ma_kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023') AND MA_nV = 'VNP017528';
SELECT* FROM  ttkd_bsc.bangluong_kpi  where thang = 202408 and ma_nv = 'VNP017528';MA_vTCV = 'VNP-HNHCM_BHKV_2' AND MA_PB = 'VNP0701300'  
and ma_kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023','HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202408 and DICHVU = 'CSKH';MA_nV  = 'VNP017585';

AND MA_nV = 'VNP017528';
COMMIT;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202408 ;
SELECT* FROM TTKD_bSC.NHANVIEN WHERE TEN_nV LIKE '%Tiên Long';