delete from tl_giahan_Tratruoc where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_022';
rollback;
select* from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202408 and ma_Tb in ('hcm_ca_ivan_00018004','hcm_ca_ivan_00018054','hcm_ca_ivan_00018096','hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098','hcm_ivan_00016978','hcm_ca_ivan_00017310','hcm_ca_00037631');
select * from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_024' AND MA_nV IN ('VNP020225','VNP020228','VNP030420');
select* from ttkd_Bsc.bangluong_kpi a where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' and tyle_Thuchien is not null;
select* from rmp_bsc_phong;
commit;
update ttkd_Bsc.bangluong_kpi a
set tyle_thuchien = (select tyle from rmp_bsc_phong where thang = 202408 and ten_kpi = 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T+1 ( D?ch v? Fiber, MyTV, Mesh)' 
    and ma_pb = a.ma_pb)
where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' and tyle_Thuchien is not null;
select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where manv_giao = 'VNP020225' and thang = 202408; ma_Tb in ('hcm_ca_ivan_00017310','hcm_ca_00037631')
;
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202408     
;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003' ;
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 120 where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_027' and thuchien is null;
commit;
update ttkd_Bsc.bangluong_kpi A set mucdo_hoanthanh  = (select mdht from rmp_bsc_phong where thang = 202408 and 
    ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' 
    and ma_pb = a.ma_pb)
where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003' and MUCDO_HOANTHANH is not null;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_Tratruoc where thang =202408 ;
SELECT * ;
update  ttkd_Bsc.tl_giahan_Tratruoc 
set ma_kpi = 'DONGIA'
where thang =202408 and MA_KPI = 'DONGIA_CHUNG_TU';
rollback;
select* from ttkd_bsc.tonghop_giao_372;
select* from  ttkd_Bsc.bangluong_kpi a where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001';
    join ttkd_bsc.tonghop_giao_372 b on a.ma_to = b.ma_to and a.thang =b.thang 
    where a.thang = 202408 and  a.ma_kpi ='HCM_SL_COMBO_006'  and b.ten_kpi = '1.CT PTM thuê bao gói Home Combo, Home Sành/Home ch?t'
    and a.giao = b.sogiao and a.ten_Vtcv like 'C?a%' and b.ten_to like 'TT_%';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408 and  manv_giao  ='VNP030420' ;
select a.*, round(da_giahan_dung_Hen*100/tong,2) from ttkd_Bsc.tl_giahan_Tratruoc a where thang = 202408   and    MA_nV  ='VNP030420' ;
create table bk_up_202408 as select* from ttkd_Bsc.bangluong_kpi where thang = 202408 ;
select A.TEN_NV, A.TEN_PB, A.TYLE_tHUCHIEN SAU, B.TYLE_THUCHIEN TRUOC, A.DIEM_CONG SAU, A.DIEM_TRU AU, B.DIEM_CONG TRUOC, B.DIEM_tRU RUOC 
from ttkd_Bsc.bangluong_kpi a
    join bk_up_202408 b on a.ma_nv = b.ma_nv and a.thang = b.thang and a.ma_kpi = b.ma_kpi
    where a.thang = 202408 and a.ma_kpi = 'HCM_TB_GIAHA_024' AND  A.TYLE_tHUCHIEN > B.TYLE_THUCHIEN;
    
    SELECT* FROM TTKD_bSC.BANGLUONG_KPI WHERE THANG = 202408 AND ma_vtcv ='VNP-HNHCM_KHDN_3.1';
    SELECT* FROM TTKD_BSC.BLKPI_DANHMUC_kPI_VTCV where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_028';
    select DISTINCT MA_vTCV from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_028'  AND THUCHIEN is not null;