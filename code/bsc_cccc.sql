-- bang luong
select distinct ma_Vtcv, ten_vtcv from TTKD_BSC.bangluong_kpi_202407 where HCM_CL_TONDV_001 is not null;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_HOTRO_006 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_HOTRO_006' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_HOTRO_006') and thang=202407 and MA_VTCV = a.MA_VTCV) ;and 'HCM_SL_HOTRO_006' = ma_kpi;
AND MA_NV = 'VNP027256';
;
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202407 and loai_tinh = 'DONGIA_CHUNGTU';
select* from ct_chungtu_quydoi;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, MA_KPI, LOAI_TINH, MA_NV, MA_TO, MA_PB, DA_GIAHAN_DUNG_HEN)
select thang,'HCM_SL_HOTRO_006' ma_kpi, 'KPI_NV'LOAI_TINH, MA_NV, MA_TO, MA_PB, SUM(HESO) DA_GIAHAN_DUNG_HEN
FROM ct_chungtu_quydoi
GROUP BY THANG,  MA_NV, MA_TO, MA_PB;
commit;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_CL_TONDV_001 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and ma_kpi = 'HCM_CL_TONDV_001' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_CL_TONDV_001') and thang=202407 AND TO_TRUONG_PHO = 1 and MA_VTCV = a.MA_VTCV) ;AND MA_NV = 'VNP027256';
;

update ;

update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_CL_TONDV_001 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_CL_TONDV_001' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_CL_TONDV_001') and thang=202407 and GIAMDOC_PHOGIAMDOC = 1 AND MA_VTCV = a.MA_VTCV) ;AND MA_NV = 'VNP027256';
;
COMMIT;
-- CA 
--  bhkv: diem cong tru
UPDATE TTKD_bSC.BANGLUONG_KPI_202407 a
SET HCM_tB_GIAHA_024 = (SELECT NVL(DIEM_CONG,-DIEM_TRU) from ttkd_bsc.bangluong_kpi where thang = 202407 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv=  a.ma_Nv)
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang=202407 and  MA_VTCV = a.MA_VTCV)
and ma_donvi not in ('VNP0702400','VNP0702300','VNP0702500');
commit;
-- nhan vien khdn : diem cong tru
UPDATE TTKD_bSC.BANGLUONG_KPI_202407 a
SET HCM_tB_GIAHA_024 = (SELECT NVL(DIEM_CONG,-DIEM_TRU) from ttkd_bsc.bangluong_kpi where thang = 202407 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv=  a.ma_Nv)
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang=202407 and  MA_VTCV = a.MA_VTCV and to_truong_pho is null 
and giamdoc_phogiamdoc is null)
and ma_donvi  in ('VNP0702400','VNP0702300','VNP0702500') and HCM_tB_GIAHA_024 is null;
-- tl/ pgd khdn
UPDATE TTKD_bSC.BANGLUONG_KPI_202407 a
SET HCM_tB_GIAHA_024 = (SELECT thuchien from ttkd_bsc.bangluong_kpi where thang = 202407 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv=  a.ma_Nv)
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang=202407 and  MA_VTCV = a.MA_VTCV and ( to_truong_pho =1 or
 giamdoc_phogiamdoc =1 ))
and ma_donvi  in ('VNP0702400','VNP0702300','VNP0702500') ;and HCM_tB_GIAHA_024 is null;
commit;
select hcm_Tb_giaha_024,TEN_VTCV, TEN_TO from TTKD_bSC.BANGLUONG_KPI_202407 where ma_Vtcv = 'VNP-HNHCM_KHDN_4';
select distinct ma_Vtcv, ten_Vtcv from ttkd_Bsc.BANGLUONG_KPI_202407 where HCM_CL_TONDV_001 is not null;
;

select a.*, b.ten_nv, b.ten_pb, b.ten_vtcv  from ttkd_Bsc.ct_Dongia_tratruoc a 
    left join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_nv and b.thang = 202407 and b.donvi ='TTKD'
where A.thang = 202407 and loai_tinh = 'DONGIATRA_OB' and ipcc = 0 and a.ma_pb in (select distinct ma_pb  from ttkd_Bsc.nhanvien where thang =202407
and ten_pb like '%Khu V?c%') and b.ten_Vtcv != 'Nhân Viên Nghi?p V? Ch?m Sóc Khách Hàng Phòng Bán Hàng Khu V?c';
--- ten mien: diem cong tru
UPDATE TTKD_bSC.BANGLUONG_KPI_202407 a
SET HCM_tB_GIAHA_026 = (SELECT NVL(DIEM_CONG,-DIEM_TRU) from ttkd_bsc.bangluong_kpi where thang = 202407 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_nv=  a.ma_Nv)
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang=202407 and  MA_VTCV = a.MA_VTCV);
and ma_donvi  in ('VNP0702400','VNP0702300','VNP0702500');
commit;
select* from   ttkd_Bsc.NHUY_CT_BSC_IPCC_OBGHTT a where thang =  202407  and ipcc = 0; 
UPDATE  ttkd_Bsc.ct_Dongia_tratruoc SET IPCC = 0 WHERE IPCC IS NULL AND THANG = 202407 ;
ROLLBACK;
--- ORDER
update TTKD_BSC.bangluong_kpi a set 
        HCM_SL_ORDER_001 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_ORDER_001' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang=202407 and MA_VTCV = a.MA_VTCV) ;AND MA_NV = 'VNP027256';
;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_ORDER_001 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and MA_TO = a.MA_TO and ma_kpi = 'HCM_SL_ORDER_001' AND LOAI_TINH = 'KPI_TO') 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang=202407 and MA_VTCV = a.MA_VTCV AND TO_TRUONG_PHO = 1) ;AND MA_NV = 'VNP027256';
;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202409 and ma_kpi in ('HCM_SL_ORDER_001') ;
ROLLBACK;
COMMIT;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_ORDER_001 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_ORDER_001' AND LOAI_TINH = 'KPI_PB' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang=202407 and MA_VTCV = a.MA_VTCV AND GIAMDOC_PHOGIAMDOC = 1) ;AND MA_NV = 'VNP027256';
;

--- COMBO 
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_COMBO_006 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_COMBO_006' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang=202407 and MA_VTCV = a.MA_VTCV) ;AND MA_NV = 'VNP027256';
;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_COMBO_006 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and MA_TO = a.MA_TO and ma_kpi = 'HCM_SL_COMBO_006' AND LOAI_TINH = 'KPI_TO') 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang=202407 and MA_VTCV = a.MA_VTCV AND TO_TRUONG_PHO = 1) ;AND MA_NV = 'VNP027256';
;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_ORDER_001 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and MA_TO = a.MA_TO and ma_kpi = 'HCM_SL_ORDER_001' AND LOAI_TINH = 'KPI_TO') 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang=202407 and MA_VTCV = a.MA_VTCV AND TO_TRUONG_PHO = 1) ;AND MA_NV = 'VNP027256';
;
select
ROLLBACK;
COMMIT;
update TTKD_BSC.bangluong_kpi_202407 a set 
        HCM_SL_COMBO_006 = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_SL_COMBO_006' AND LOAI_TINH = 'KPI_PB' ) 
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang=202407 and MA_VTCV = a.MA_VTCV AND GIAMDOC_PHOGIAMDOC = 1) ;AND MA_NV = 'VNP027256';
;

select a.ma_nv, a.ten_Nv, a.ten_Vtcv, a.HCM_SL_ORDER_001, b.HCM_SL_ORDER_001
from  TTKD_BSC.bangluong_kpi_202407 a
    join  TTKD_BSC.bangluong_kpi_202407_dot2 b on a.ma_Nv = b.ma_Nv
where nvl(a.HCM_SL_ORDER_001,0) <> nvl(b.HCM_SL_ORDER_001,0);
commit;
select hcm_Tb_giaha_023, a.*
from  TTKD_BSC.bangluong_kpi_202407 a where hcm_Tb_giaha_023 is not null;
select a.TEN_VTCV, A.TEN_NV, A.THUCHIEN,A.DIEM_TRU,A.DIEM_CONG, B.HCM_SL_HOTRO_006
from ttkd_Bsc.bangluong_kpi a
    join ttkd_bsc.bangluong_kpi_202407 b on a.ma_Nv = b.ma_Nv
where a.thang = 202407 and ma_kpi = 'HCM_SL_HOTRO_006'; AND nvl(thuchien,tyle_Thuchien) <> nvl(b.HCM_CL_TONDV_001,0);
select ten_nv, hcm_Tb_Giaha_024 from ttkd_Bsc.bangluong_kpi_202407 where ten_Nv like '%Vân';
update ttkd_Bsc.bangluong_kpi a
set thuchien = (select HCM_CL_TONDV_001 from ttkd_Bsc.bangluong_Kpi_202407 where a.ma_nv = ma_nv)
where thang = 202407  and ma_kpi = 'HCM_CL_TONDV_001';
select* from ttkd_Bsc.nhanvien_202406 where ten_nv like '%Thùy D??ng';
update ttkd_Bsc.bangluong_kpi set diem_tru = null 
--select* from ttkd_Bsc.bangluong_kpi 
where thang = 202407 and ma_kpi = 'HCM_TB_GIAHA_024' and ten_Nv = 'Ngô Th? Vân';
commit;