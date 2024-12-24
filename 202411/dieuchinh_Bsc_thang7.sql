-- lao dong dinh bien
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and ma_kpi = 'HCM_SL_ORDER_001' AND LOAI_TINH = 'KPI_TO';
update ttkd_bsc.tl_giahan_tratruoc set tyle = round((da_giahan_dung_hen/(660*7)),2) where thang = 202406 and ma_kpi = 'HCM_SL_ORDER_001' AND LOAI_TINH = 'KPI_TO' and ma_pb = 'VNP0702100';
;
select HCM_SL_ORDER_001 from ttkd_Bsc.bangluong_kpi_202406;
select sum(TIEN_THUYETPHUC*HESO_DICHVU*HESO_CHUKY) from ttkd_bsc.ct_dongia_tratruoc where NHANVIEN_XUATHD = 'VNP016881' and thang = 202405 and loai_Tinh in( 'DONGIATRA_OB') ;and ipcc= 1;
select* from ttkd_bsc.bangluong_dongia_202405 where ma_Nv = 'VNP016881';
--
select a.ma_Nv,  a.ten_donvi, a.HCM_SL_ORDER_001, b.thuchien from ttkd_bsc.bangluong_kpi_202406 a join ttkd_bsc.bangluong_kpi b 
on a.ma_nv = b.ma_Nv and b.thang = 202406 and b.ma_kpi = 'HCM_SL_ORDER_001'
where a.HCM_SL_ORDER_001 !=b.thuchien ;
select* from  ttkd_bsc.bangluong_kpi b where b.thang = 202406 and b.ma_kpi = 'HCM_SL_ORDER_001';
rollback;
commit;
update   ttkd_bsc.bangluong_kpi b set MUCDO_HOANTHANH = round(thuchien*100/660,2) where b.thang = 202406 and b.ma_kpi = 'HCM_SL_ORDER_001' and b.ma_Vtcv = 'VNP-HNHCM_BHKV_47';
INSERT INTO TTKD_bSC.tl_giahan_Tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN);
select THANG, LOAI_TINH,'HCM_CL_TONDV_001' MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN
 from ttkd_bsc.tl_giahan_Tratruoc where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_022';
 alter table TTKD_bSC.tl_giahan_Tratruoc add (sl_phieuton number(6));
 
 ---- NGO THI VAN VNP027256 VNP0702509 VNP0702500
 delete;
 delete from ttkd_Bsc.ct_BsC_giahan_cntt where thang = 202406 and ma_Tb in ('hcm_ca_ivan_00021689','hcm_ca_00042655','hcm_ca_00042654','hcm_ca_00079272','hcm_ca_00079273','hcm_ca_00079730'
 ,'hcm_ca_00079727');
 commit;
 
  delete from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202406 and ma_Tb in ('hcm_ca_ivan_00021689','hcm_ca_00042655','hcm_ca_00042654','hcm_ca_00079272','hcm_ca_00079273','hcm_ca_00079730'
 ,'hcm_ca_00079727');
 
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024';
commit;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and loai_tinh = 'DONGIATRU_CA';
commit;
update ttkd_bsc.bangluong_kpi_202406 a set hcm_tb_Giaha_024 =  (select nvl(diem_cong,-diem_tru) from ttkd_bsc.bangluong_kpi where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024'
and ma_nv = a.ma_nv)
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null and MA_VTCV = a.MA_VTCV);
commit;
---- TY LE PHIEU TON
select* from ttkd_Bsc.nhanvien where sdt ='0888514509';
drop table ct_Bsc_slpt_chua_xuly;
select* from ttkd_Bsc.nhanvien_202406 where ma_nv = 'VNP017585';
select* from ttkd_Bsc.bangluong_kpi where ma_nv ='VNP017585' and thang = 202406;
rollback;
---- THAI SON
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and ma_kpi = 'HCM_CL_TONDV_001' and ma_pb = 'VNP0702100' and loai_tinh = 'KPI_PB';
update ttkd_Bsc.tl_giahan_tratruoc   set ma_Nv = 'VNP017585' where thang = 202406 and ma_kpi = 'HCM_CL_TONDV_001' and ma_pb = 'VNP0702100' and loai_tinh = 'KPI_PB';
CREATE TABLE bangluong_kpi_202406 AS SELECT* FROM TTKD_BSC.bangluong_kpi_202406;
select* from bangluong_kpi_202406;
update TTKD_BSC.bangluong_kpi_202406 a 
set HCM_CL_TONDV_001 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_CL_TONDV_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_CL_TONDV_001') and thang_kt is null and MA_VTCV = a.MA_VTCV) AND  ma_nv ='VNP017585' ;
COMMIT;

update TTKD_BSC.bangluong_kpi_202406 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_TB_GIAHA_023' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV) AND  ma_nv ='VNP017585' ;

update TTKD_BSC.bangluong_kpi_202406 a 
set HCM_SL_ORDER_001 = (select da_giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_SL_ORDER_001' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_SL_ORDER_001') and thang_kt is null and MA_VTCV = a.MA_VTCV) AND  ma_nv ='VNP017585' ;
select HCM_TB_GIAHA_023,HCM_TB_GIAHA_022 ,HCM_SL_ORDER_001,HCM_CL_TONDV_001 from  TTKD_BSC.bangluong_kpi_202406 a where  ma_nv ='VNP017585' ;
update ttkd_bsc.bangluong_kpi set thuchien = 1894
, tyle_Thuchien = round(1894/(660*7),4)*100  
where thang =202406 and MA_KPI in ('HCM_SL_ORDER_001') and ma_nv = 'VNP017585';
commit;
--- UPDATE TRUONG LINE, PHO GIAM DOC
Select* from ttkd_Bsc.blkpi_danhmuc_kpi_vtcv where ma_kpi = 'HCM_TB_GIAHA_024';
update TTKD_BSC.bangluong_kpi_202406 a 
set  HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to  AND MA_PB =A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024','') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV) AND MA_VTCV = 'VNP-HNHCM_KHDN_4';
update TTKD_BSC.bangluong_kpi_202406 a 
                set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                                                 and ma_nv = a.ma_nv_hrm AND MA_PB  = A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_024' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024', '')  
                                            and thang_kt is null and MA_VTCV = a.MA_VTCV) AND MA_VTCV = 'VNP-HNHCM_KHDN_2';
commit;
select a.MA_NV, a.MA_NV_HRM, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_DONVI, a.TEN_DONVI, a.MA_TO, a.TEN_TO,  b.HCM_TB_GIAHA_024 from bangluong_kpi_202406 a
    join TTKD_BSC.bangluong_kpi_202406 b on a.ma_nv = b.ma_nv
where nvl(a.HCM_TB_GIAHA_024,0) != nvl(b.HCM_TB_GIAHA_024,0)
;
select  a.MA_NV, a.MA_NV_HRM, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_DONVI, a.TEN_DONVI, a.MA_TO, a.TEN_TO,HCM_TB_GIAHA_023,HCM_TB_GIAHA_022 ,HCM_SL_ORDER_001,HCM_CL_TONDV_001
from  TTKD_BSC.bangluong_kpi_202406 a where  ma_nv ='VNP017585' ;
select * from ttkd_Bsc.nhanvien_202406 where ten_Nv like '%Hi?n';
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202406; ma_to = 'VNP0702302';
insert into ct_Bsc_giahan_cntt;
select* from ct_Bsc_giahan_cntt where thang = 202406 and ma_tb= 'hcm_ca_00057807';
commit;
rollback;
delete from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202406 and ma_Tb in
('hcm_ca_00042834',
'hcm_ca_00042870',
'hcm_ca_00057807',
'hcm_ca_00057810',
'hcm_ca_00057811',
'hcm_ca_00057812',
'hcm_ca_00057814',
'hcm_ca_00057817',
'hcm_ca_00058082',
'hcm_ca_00058274',
'hcm_ca_00058482',
'hcm_ca_00058483',
'hcm_ca_00058543',
'hcm_ca_00078692',
'hcm_ca_00078770',
'hcm_ca_00078772',
'hcm_ca_00078868',
'hcm_ca_00078869',
'hcm_ca_00079317',
'hcm_ca_00079538',
'hcm_ca_00079754');
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv = 'VNP017793';
update ttkd_Bsc.tl_giahan_tratruoc set tong = 2, tyle = 0 where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv = 'VNP017793';
commit;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_to = 'VNP0702302' and loai_tinh = 'KPI_TO';
update ttkd_Bsc.tl_giahan_tratruoc set tong = 17, TYLE = 47.06 where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_to = 'VNP0702302' and loai_tinh = 'KPI_TO';
update ttkd_Bsc.tl_giahan_tratruoc set tong = 17, TYLE = 47.06 where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_pb = 'VNP0702300' and loai_tinh = 'KPI_PB'
and ma_nv = 'VNP022082';
commit;
select* from ttkd_Bsc.tl_giahan_tratruoc set tong = 17, TYLE = 47.06 where thang = 202406 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv = 'VNP017793';