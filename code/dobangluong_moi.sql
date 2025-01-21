update ttkd_bsc.bangluong_kpi_202311
set HCM_TB_GIAHA_023 = null
commit;
drop table abc;
create table abc as select* from ttkd_Bsc.bangluong_kpi where thang = 202410;
select distinct loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202410 and ma_kpi ='DONGIA';
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and ma_Tb in ('mesh0135488','thuykhoinguyen');
select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202410 and ma_Tb in ('mesh0135488','thuykhoinguyen');
select* from ttkd_Bsc.nhanvien where 
select distinct ma_vtcv, ten_Vtcv, MA_PB , TEN_TO from ttkd_Bsc.nhanvien where thang = 202410 and ma_pb = 'VNP0701100' AND TEN_TO = 'T? Bán Hàng Online';
select ten_Vtcv, ten_to ,count(thuebao_id) from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a
    join ttkd_bsc.nhanvien b on a.thang = b.thang and a.manv_Thuyetphuc = b.ma_Nv
where a.thang = 202410 and a.ma_pb not in ('VNP0702300','VNP0702400','VNP0702500','VNP0703000') 
group by ten_Vtcv, ten_to;
rollback;
-- BSC 60 ngay
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi like 'HCM_TB_GIAHA_023';
select distinct ma_nv from ttkd_Bsc.bangluong_kpi_202403 ; -- where thang = 202403
update TTKD_BSC.bangluong_kpi a set 
        tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023')
        ,giao = (select tong from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023')
        ,thuchien = (select da_giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_023') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_023'
;

rollback;
select* from ttkd_Bsc.nhanvien_202403 where ma_Nv = 'CTV021955';
select* from ttkd_bsc.tl_giahan_tratruoc where ma_nv = 'CTV021955' and thang = 202403;              
COMMIT;
---------------Ty le cua To truong ----- HCM_TB_GIAHA_029
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_022')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_029')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_029'
;
update ttkd_Bsc.bangluong_kpi set diem_cong = 5 where  tyle_thuchien >= 80 and thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
update ttkd_Bsc.bangluong_kpi set diem_tru = 5 where  tyle_thuchien < 75 and thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
update ttkd_Bsc.bangluong_kpi set diem_tru = 0, diem_Cong = 0 where  tyle_thuchien >= 75 and tyle_thuchien <80  and thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_029' ; 
update ttkd_Bsc.bangluong_kpi set diem_tru =null, diem_cong = null where  thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_029' and ma_nv='VNP017445';
commit;
SELECT a.ten_Nv, a.diem_cong, b.diem_Cong, a.diem_tru, b.diem_tru , a.tyle_Thuchien , b.tyle_thuchien 
FROM TTKD_bSC.BANGLUONG_KPI a
join bk_kpi_full b on a.thang = b.thang and a.ma_kpi = b.ma_kpi and a.ma_nv = b.ma_nv
WHERE a.THANG = 202410 AND  b.ma_kpi = 'HCM_TB_GIAHA_029'; and ma_nv='VNP017445';
--1. 60 ngay
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023' )
    , giao =  (select tong from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023' )
    ,thuchien =  (select da_giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_023' )

where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_023') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_023';
--2. 30 ngay
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_022' )
    , giao =  (select tong from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_022' )
    ,thuchien =  (select da_giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_022' )

where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_022') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_022';
-- 3. home comebo
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select sum(DA_GIAHAN_DUNG_HEN) from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv and ma_kpi = 'HCM_SL_COMBO_006')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006'
;
select* from ttkd_bct.bangiao_chungtu_tinhbsc where ma_ct='VCB_20240907_336680';
rollback;
select* from ttkd_Bsc.nhanvien_202403 where ma_Nv = 'CTV021955';
select* from ttkd_bsc.tl_giahan_tratruoc where ma_nv = 'CTV021955' and thang = 202403;              
COMMIT;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to
and ma_kpi = 'HCM_SL_COMBO_006')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_SL_COMBO_006')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006'
;
--------------Ty le cua Pho GD --------------
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_SL_COMBO_006' )
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_SL_COMBO_006') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006';
commit;
delete from ttkd_bsc.bangluong_kpi where thang =202410 and ma_nv in (select ma_Nv from nhuy.obghtt ) and ma_kpi not in ('HCM_CL_TNGOI_004','HCM_SL_ORDER_001');
---4. OB
ROLLBACK;
update TTKD_BSC.bangluong_kpi a set 
        giao = 400,
        thuchien = (select sum(DA_GIAHAN_DUNG_HEN) from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_kpi = 'HCM_SL_ORDER_001' and ma_nv = a.ma_nv )
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_ORDER_001' and thuchien is null
;
--- 5. CHUNG TU
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_NV' and ma_nv = a.ma_nv
and ma_kpi = 'HCM_SL_HOTRO_006')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi in ('HCM_SL_HOTRO_006')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_HOTRO_006'
;
update  TTKD_BSC.bangluong_kpi set giao = 1650 WHERE THANG = 202410 AND ma_kpi = 'HCM_SL_HOTRO_006';
commit;
select* from TTKD_BSC.bangluong_kpi a where thang = 202410 and ma_kpi = 'HCM_SL_ORDER_001';
select * from ttkd_bsc.tl_giahan_tratruoc where
        ma_nv = 'CTV086123' and ma_kpi = 'HCM_SL_ORDER_001';
select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang = 202410 ;
rollback;
select distinct ma_vtcv, ten_vtcv from ttkd_Bsc.nhanvien where thang = 202410 and ma_pb not in ('VNP0703000','VNP0702400','VNP0702500','VNP0702300') and donvi = 'TTKD';
select* from TTKD_BSC.bangluong_kpi a where thang = 202410 and ma_kpi = 'HCM_SL_ORDER_001';
select* from ttkd_bsc.nhanvien where ma_Nv = 'CTV086123';
select* from qltn.v_Db_Datcoc@dataguard where rownum  =1;
select* from ttkd_Bsc.bangluong_kpi  where thang = 202410 and ma_kpi ='HCM_CL_TONDV_003';
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 5, tyle_Thuchien = 0 where thang = 202410 and ma_kpi ='HCM_CL_TONDV_003' and ten_Nv like '%Y?n';
COMMIT;
--- order
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_NV' and ma_nv = a.ma_nv
and ma_kpi = 'HCM_SL_ORDER_001')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi in ('HCM_SL_ORDER_001')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_ORDER_001';
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to
and ma_kpi = 'HCM_SL_ORDER_001')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_SL_ORDER_001')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_ORDER_001'
;
select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202410  and ma_kpi = 'HCM_SL_ORDER_001';
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_SL_ORDER_001' )
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_SL_ORDER_001') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_ORDER_001';
COMMIT;
select* from TTKD_BSC.bangluong_kpi where thang = 202410  AND ma_kpi = 'HCM_CL_TONDV_003';  
SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' --and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_SL_COMBO_006';
select* from nhanvien_202410 where mail_vnpt = 'thgiang.hcm@vnpt.vn';
----- ti le phieu ton
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_Nv
and ma_kpi = 'HCM_CL_TONDV_003')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where   ma_kpi in ('HCM_CL_TONDV_003')
    and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_CL_TONDV_003'
;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_CL_TONDV_003' )
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_CL_TONDV_003') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_CL_TONDV_003';
SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' --and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_CL_TONDV_001' ;
commit;
---- 
-- BSC 30 ngay'
select distinct ten_vtcv , ma_vtcv from TTKD_BSC.bangluong_kpi_202403 where HCM_TB_GIAHA_022 is not null;
update TTKD_BSC.bangluong_kpi a set 
thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv
and ma_kpi = 'HCM_TB_GIAHA_022')
-- select * from TTKD_BSC.bangluong_kpi a 
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_022'
;
select* from css_hcm.khuvuc;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to 
and ma_kpi = 'HCM_TB_GIAHA_022')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_022'
;   
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_Kpi = 'HCM_TB_GIAHA_023' AND LOAI_TINH = 'KPI_NV';
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv 
and ma_kpi = 'HCM_TB_GIAHA_022' )
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpupdate TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_024' ;i_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_022') and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_022';
commit;
update TTKD_BSC.bangluong_kpi 
set thuchien = null, TYLE_THUCHIEN = null, MUCDO_HOANTHANH = null, DIEM_CONG = null, DIEM_TRU = null
where thang = 202410 and ma_kpi in ('HCM_TB_GIAHA_026','HCM_TB_GIAHA_027','HCM_TB_GIAHA_024');
-- CA - thang T
-- NHAN VIEN 
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_nv = 'VNP017400'; --and ma_vtcv = 'VNP-HNHCM_BHKV_18'


SELECT* FROM TTKD_bSC.NHANVIEN_202406 WHERE TEN_nV LIKE '%Hi?n';
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'
;
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_024';

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        ,thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_024';
                    select * from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_kpi = 'HCM_TB_GIAHA_024'       ;                 
-- CA THANG T-1
update TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_025')
where thang = 202403 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_025') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_025'

rollback;
COMMIT;
-- TO TRUONG
select* from ttkd_Bsc.nhanvien where thang = 202410;
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_025')
where thang = 202403 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_025') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_025';

commit;
update TTKD_BSC.bangluong_kpi  set tyle_thuchien = thuchien where ma_kpi = 'HCM_TB_GIAHA_024' and tyle_thuchien is null and thang = 202410 ;
rollback;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202403 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_025' )

where thang = 202403 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_025')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_025';
     COMMIT;
;
-- TEN MIEN
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_nv = 'VNP017400'; --and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi a 
set 
 tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang = 202410 and MA_VTCV = a.MA_VTCV) 
--and ma_pb in ('VNP0702400','VNP0701100')
and ma_kpi = 'HCM_TB_GIAHA_026';
select tyle_thuchien from  TTKD_BSC.bangluong_kpi a where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang = 202410 and MA_VTCV = a.MA_VTCV)  
;
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18';

update TTKD_BSC.bangluong_kpi a 
set  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
 , tyle_thuchien= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')                                                                               
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' --and ma_pb in ('VNP0702400','VNP0701100')
;

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202410 and loai_tinh ='KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_026'
                and ma_nv = a.ma_nv  )
    ,  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
        , giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                and ma_kpi in ('HCM_TB_GIAHA_026')  
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' ;and ma_pb in ('VNP0702400','VNP0701100')
;
commit;
-- TEN MIEN
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 2
                           when (tyle_Thuchien) >= 95 and tyle_thuchien < 100 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202410; and ma_pb in ('VNP0702400','VNP0701100')
;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 80 and tyle_thuchien < 95 then 1
                        when (tyle_Thuchien) < 80 then 2 end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202410 ;and ma_pb in ('VNP0702400','VNP0701100')
  ;
  select* from ttkd_Bsc.bangluong_kpi  where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202410 and ten_nv like '%Linh';
  select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202410 and ma_Kpi ='HCM_TB_GIAHA_022' and loai_tinh = 'KPI_TO';
-- ty le thuc hien
;
select* from ttkd_Bsc.bangluong_kpi where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202410 and ten_nv like '%Giang H?i';
select * from ttkd_Bsc.bangluong_kpi where thang = 202403 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025','HCM_TB_GIAHA_026',
'HCM_TB_GIAHA_022','HCM_TB_GIAHA_023') ; -- and thuchien is not null;

update ttkd_Bsc.bangluong_kpi 
set tyle_Thuchien = thuchien where thang = 202410 and ma_kpi in ('','HCM_TB_GIAHA_024')
--;,'HCM_TB_GIAHA_026',
--'HCM_TB_GIAHA_022','HCM_TB_GIAHA_023')
and tyle_Thuchien is null AND THUCHIEN IS NOT NULL;

commit;
update ttkd_Bsc.bangluong_kpi set thuchien = 16 where thang = 202410 and ma_kpi ='HCM_SL_COMBO_006' and ma_nv ='VNP017740';
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202410 and ma_pb = 'VNP0703000';
-- muc do hoan thanh -
-- 30 NGAY
--- cac phong khac
update ttkd_Bsc.bangluong_kpi  
set mucdo_hoanthanh = case when tyle_Thuchien >= 80 then 120
                        when tyle_thuchien >= 65 and tyle_thuchien < 80 then 100 + (tyle_thuchien - 65)
                        when tyle_thuchien >= 40 and tyle_thuchien < 65 then 100 - 2*(65 - tyle_thuchien)
                        when tyle_thuchien < 40 then 0 end
where ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202410 and ma_pb != 'VNP0703000';
-- bhol
update ttkd_Bsc.bangluong_kpi  
set mucdo_hoanthanh = case when tyle_Thuchien >= 80 then 120
                        when tyle_thuchien >= 65 and tyle_thuchien < 80 then 100 + 1.1*(tyle_thuchien - 65)
                        when tyle_thuchien >= 45 and tyle_thuchien < 65 then 50 + 2*(tyle_thuchien-45)
                        when tyle_thuchien >= 30 and tyle_thuchien < 45 then 20 + 1.5*(tyle_thuchien-30)
                        when tyle_thuchien < 30 then 0 end
where ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202410 and ma_pb = 'VNP0703000';
select * from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi in ('HCM_TB_GIAHA_026') ;
-- 60 NGAY
update ttkd_Bsc.bangluong_kpi  
set mucdo_hoanthanh = case when tyle_Thuchien >= 90 then 120 
                        when tyle_thuchien >= 70 and tyle_thuchien < 90 then 100 + (tyle_thuchien - 70)
                        when tyle_thuchien >= 65 and tyle_thuchien < 70 then 70+5.8*(tyle_thuchien-65)
                        when  tyle_thuchien >= 60 and tyle_thuchien < 65 then 35+6.8*(tyle_thuchien-60)
                        when tyle_thuchien < 60 then 0 end
where ma_kpi ='HCM_TB_GIAHA_023' AND THANG = 202410;
commit;
ROLLBACK;
update ttkd_Bsc.bangluong_kpi set tyle_Thuchien = 77.2 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_022' AND MA_PB = 'VNP0703000' AND CHITIEU_gIAO IS NOT NULL;
SELECT* FROM  ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_022' AND MA_PB = 'VNP0703000';
-- CA T
SELECT* FROM  ttkd_Bsc.bangluong_kpi  where ma_kpi ='HCM_TB_GIAHA_023' AND THANG = 202410 ; and thuchien != tyle_thuchien;
update ttkd_Bsc.bangluong_kpi set tyle_thuchien = thuchien where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410 and tyle_thuchien is null;
UPDATE  ttkd_Bsc.bangluong_kpi SET TYLE_THUCHIEN = THUCHIEN WHERE ma_kpi IN ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026') AND THANG = 202410;
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when nvl(tyle_Thuchien,thuchien) >= 100 then 5
                        when nvl(tyle_Thuchien,thuchien) >= 85 and tyle_thuchien < 100 then 3
                        when nvl(tyle_Thuchien,thuchien) > 70 and tyle_thuchien < 85 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when nvl(tyle_Thuchien,thuchien) >= 60 and tyle_thuchien < 70 then 1  
                        when nvl(tyle_Thuchien,thuchien) >= 45 and tyle_thuchien < 60 then 3
                        when nvl(tyle_Thuchien,thuchien) < 45 then 5 end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410  ;
commit;
update ttkd_bsc.bangluong_kpi set diem_Cong = 5 where   ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410  and ma_nv = 'VNP027256';
-- CA T+1
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when tyle_Thuchien >= 70 then 0.05 
                        when tyle_thuchien >= 55 and tyle_thuchien < 70 then 0.03
                        when tyle_thuchien > 40 and tyle_thuchien < 55 then 0.01
                        end
where ma_kpi ='HCM_TB_GIAHA_025' AND THANG = 202403;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when tyle_thuchien >= 30 and tyle_thuchien < 40 then 0.01   
                        when tyle_thuchien >= 20 and tyle_thuchien <30 then 0.03
                        when tyle_thuchien where ma_kpi ='HCM_TB_GIAHA_025' AND THANG = 202403  ;
< 20 then 0.05 end

commit;
select* from ttkd_Bsc.bangluong_kpi where  ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202410 ;
SELECT* FROM TTKD_bSC.TL_GIAHAN_tRATRUOC where  ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202410 ;
--select* from ttkd_Bsc.blkpi_danhmuc_kpi where ten_kpi like '%t?n%';
select* from ttkd_Bsc.bangluong_kpi where ten_kpi like '%?ch%' and thang = 202410 and ten_kpi = 'S? l??ng ch?ng t? g?ch n? trong tháng';
select* from ttkd_Bsc.blkpi_danhmuc_kpi_vtcv where ma_kpi = 'HCM_CL_TONDV_001';
commit;
--- update so giao home combo
update ttkd_bsc.bangluong_kpi a
SET GIAO =
(SELECT SOGIAO FROM TTKDHCM_KTNV.ID372_GIAO_C2_CHOTTHANG WHERE THANG = 202410 AND TEN_KPI = '2.CT PTM thuê bao gói Home Sành/Home ch?t'
                                                    and a.ma_pb = ma_pb  and ma_nv is null )
where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' and giao is null; ma_nv in
(
SELECT ma_nv FROM TTKDHCM_KTNV.ID372_GIAO_C2_CHOTTHANG WHERE THANG = 202410 AND TEN_KPI = '2.CT PTM thuê bao gói Home Sành/Home ch?t' group by ma_Nv having count(ma_Nv)>1
);
---- NV, TT
update ttkd_bsc.bangluong_kpi a
SET GIAO =
(SELECT MAX(SOGIAO) FROM TTKDHCM_KTNV.ID372_GIAO_C2_CHOTTHANG WHERE THANG = 202410 AND TEN_KPI = '2.CT PTM thuê bao gói Home Sành/Home ch?t'
                                                    and  ma_NV = a.ma_NV )
                                                    
where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' ;and ma_vtcv = 'VNP-HNHCM_BHKV_2';ma_nv in
(
SELECT ma_nv FROM ttkd_bsc.tonghop_giao_372 WHERE THANG = 202410 AND TEN_KPI = '1.CT PTM thuê bao gói Home Combo, Home Sành/Home ch?t' group by ma_Nv having count(ma_Nv)> 1
);
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006'  ;and ma_vtcv = 'VNP-HNHCM_BHKV_2';; 
SELECT ma_nv FROM ttkd_bsc.tonghop_giao_372 WHERE THANG = 202410 AND TEN_KPI = '1.CT PTM thuê bao gói Home Combo, Home Sành/Home ch?t' group by ma_Nv having count(ma_Nv)=1;
select* from ttkd_bsc.tonghop_giao_372 WHERE THANG = 202410 AND TEN_KPI = '1.CT PTM thuê bao gói Home Combo, Home Sành/Home ch?t'  and ma_to = 'VNP07015C0';
select* from ttkd_Bsc.nhanvien where thang = 202410;
select ma_Nv,MA_PB from ttkd_Bsc.tl_giahan_tratruoc  where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' and loai_tinh ='KPI_PB';  
UPDATE ttkd_bsc.bangluong_kpi a
SET GIAO = NULL where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' and ma_vtcv = 'VNP-HNHCM_BHKV_2' AND MA_NV NOT IN 
(select NVL(ma_Nv,'A') from ttkd_Bsc.tl_giahan_tratruoc  where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' and loai_tinh ='KPI_PB');
COMMIT;
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202410 and ma_nv in( 'VNP016898','VNP016983');
update ttkd_bsc.bangluong_kpi set giao = null where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006'  and ma_vtcv = 'VNP-HNHCM_BHKV_2' and ma_nv in( 'VNP016898','VNP016983');
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_SL_COMBO_006' and ma_pb = 'VNP0701500';
select * from   TTKDHCM_KTNV.ID372_GIAO_C2_CHOTTHANG WHERE THANG = 202410 AND TEN_KPI = '2.CT PTM thuê bao gói Home Sành/Home ch?t';
UPDATE ttkd_Bsc.bangluong_kpi A
SET TYLE_tHUCHIEN =  NULL--(SELECT TYLE_tHUCHIEN FROM  ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_024' AND MA_NV = A.MA_NV),
 ,DIEM_CONG = NULL--(SELECT DIEM_CONG FROM  ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_024' AND MA_NV = A.MA_NV),
 ,DIEM_TRU = (SELECT DIEM_TRU FROM  ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_024' AND MA_NV = A.MA_NV)


where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_027';
COMMIT;
SELECT* FROM TTKD_BSC.bangluong_kpi WHERE THANG = 202410 and ma_kpi= 'HCM_CL_TONDV_003';
SELECT* FROM TTKD_bSC.TL_gIAHAN_TRATRUOC WHERE THANG = 202410 AND MA_NV= 'VNP017528';
update ttkd_Bsc.bangluong_kpi 
set mucdo_hoanthanh = -2.98
where thang = 202410 and ma_kpi= 'HCM_CL_TONDV_003' and ma_Vtcv = 'VNP-HNHCM_BHKV_2' and ten_nv like '%H??ng';
commit;
update ttkd_Bsc.bangluong_kpi 
set mucdo_hoanthanh = -0.95
where thang = 202410 and ma_kpi= 'HCM_CL_TONDV_003' and ma_Vtcv = 'VNP-HNHCM_BHKV_2' and ma_Nv = 'VNP017528'; ten_nv like '%Minh';
update ttkd_Bsc.bangluong_kpi 
set mucdo_hoanthanh = -3.3
where thang = 202410 and ma_kpi= 'HCM_CL_TONDV_003' and ma_Vtcv = 'VNP-HNHCM_BHKV_2' and ten_nv like '%Hà';
commit;
SELECT* FROM ttkd_Bsc.bangluong_kpi WHERE THANG = 202410 AND MA_KPI = 'HCM_SL_COMBO_006'; AND DIEM_cONG IS NULL AND DIEM_TRU IS NULL;

update TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')

where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_027') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_027'; AND MA_nV != 'VNP017793';
SELECT* FROM TTKD_bSC.NHANVIEN_202406 WHERE TEN_nV LIKE '%Hi?n';
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'
;
select* from ttkd_Bsc.bangluong_kpi where ma_to = 'VNP0702308' and ma_kpi ='HCM_TB_GIAHA_027';
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
    ,tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_027') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_027';

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024'),
                thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_027')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_027';
                    select * from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_kpi = 'HCM_TB_GIAHA_024'       ;   
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when thuchien >= 100 then 5
                        when  thuchien >= 85 and thuchien < 100 then 3
                        when  thuchien > 70 and thuchien < 85 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_027' AND THANG = 202410;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when  thuchien >= 60 and thuchien < 70 then 1  
                        when  thuchien >= 45 and thuchien < 60 then 3
                        when  thuchien< 45 then 5 end
where ma_kpi ='HCM_TB_GIAHA_027' AND THANG = 202410  ;
COMMIT;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026','HCM_TB_GIAHA_027');
--- QLDL

update TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_028') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_028' ;
SELECT* FROM TTKD_bSC.NHANVIEN_202406 WHERE TEN_nV LIKE '%Hi?n';
rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202403 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'
;
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_028') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_028' and ten_nv like '%Kiên';

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        ,thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_024';
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when nvl(tyle_Thuchien,thuchien) >= 100 then 5
                        when nvl(tyle_Thuchien,thuchien) >= 85 and tyle_thuchien < 100 then 3
                        when nvl(tyle_Thuchien,thuchien) > 70 and tyle_thuchien < 85 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202410;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when nvl(tyle_Thuchien,thuchien) >= 60 and tyle_thuchien < 70 then 1  
                        when nvl(tyle_Thuchien,thuchien) >= 45 and tyle_thuchien < 60 then 3
                        when nvl(tyle_Thuchien,thuchien) < 45 then 5 end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202410  ;
COMMIT;
select* from ttkd_bsc.tl_giahan_tratruoc where thang =202410 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_To = 'VNP0702416';
SELECT* FROM TTKD_bSC.bangluong_kpi WHERE THANG = 202410 AND  ma_kpi ='HCM_TB_GIAHA_028';
SELECT *
  FROM dba_objects
 WHERE
    owner = 'NHUY'
   AND object_type = 'TABLE';
update ttkd_Bsc.NHANVIEN  A SET THAYDOI_VTCV = (sELECT THAYDOI FROM TINHBSC WHERE MA_NV = A.MA_NV) WHERE THANG = 202410;
SELECT* FROM ttkd_Bsc.NHANVIEN WHERE THANG = 202410;
COMMIT;
----- muc do hoan thanh CA
rollback;
update ttkd_Bsc.bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien = 100 then 120 
                        when tyle_Thuchien >= 85 and tyle_Thuchien < 100 then 100+1.1*(tyle_Thuchien-85)
                        when tyle_Thuchien > 70 and tyle_Thuchien < 85  then 80+1.1*(tyle_Thuchien-70)
                        when tyle_Thuchien = 70 then 80
                        when tyle_Thuchien >= 60 and tyle_Thuchien < 70 then 70 - 1.1*(60 - tyle_Thuchien) 
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 60  then 50 - 1.1*(45 - tyle_Thuchien) 
                        else 0 end
where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_027';
update  ttkd_Bsc.bangluong_kpi set diem_tru = 10 where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_028';
commit  ;
update ttkd_Bsc.bangluong_kpi a
set mdht = (select mdht from
where thang = 202410 and ma_kpi = 'HCM_CL_TONDV_003' and mucdo_hoanthanh is not null;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202410;
select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_nv = 'VNP017639' ;
select* from ttkd_Bsc.TL_GIAHAN_TRATRUOC where thang = 202410 and MA_nV ='VNP027256';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202410 and ma_to = 'VNP0702308' and loaitb_id not in (147,148);
select * from ttkdhcm_ktnv.ghtt_chotngay_271 where trunc(ngay_chot)=to_date('02/09/2024','dd/mm/yyyy') and thang_kt=202410 and ma_to = 'VNP0702308' and loaitb_id not in (147,148);