update ttkd_bsc.bangluong_kpi_202402
set HCM_TB_GIAHA_023 = null
where HCM_TB_GIAHA_023 is not null;

select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO,HCM_TB_GIAHA_022, 
HCM_TB_GIAHA_023 from ttkd_Bsc.bangluong_kpi_202402   where ma_nv in ('VNP016863','VNP027588')
commit;
-- BSC 60 ngay

select distinct ma_to, ten_to from ttkd_bsc.nhanvien_202312 where ma_pb = 'VNP0702200'
select* from ttkd_bsc.nhanvien_202401 where ma_pb = 'VNP0702200' 
select* from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where thang_KT IS NULL and  ma_kpi = 'HCM_TB_GIAHA_022'
select* from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202402 and km = 1 and loaibo = 0 and tratruoc = 1
select  ma_Vtcv from TTKD_BSC.bangluong_kpi_202402 where HCM_TB_GIAHA_023 is not null
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_Kpi = 'HCM_TB_GIAHA_023' and ma_to = 'VNP0702407';
select a.*, b.ma_vtcv , b.ten_vtcv from ttkd_bsc.tl_giahan_tratruoc a
join ttkd_bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv 
where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023'
select * from ttkd_Bsc.bangluong_kpi_202402 where HCM_TB_GIAHA_023 is not null and ma_vtcv in ('VNP-HNHCM_KHDN_4','VNP-HNHCM_KHDN_3')
update TTKD_BSC.bangluong_kpi_202402 a set 
        HCM_TB_GIAHA_023 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV) --AND MA_NV = 'VNP017875'
;
CREATE TABLE BU_BL AS
select HCM_TB_GIAHA_023, TEN_VTCV, MA_VTCV from ttkd_Bsc.bangluong_kpi_202402  WHERE ma_TO ='VNP0702407'
SELECT* FROM TTKD_BSC.NHANVIEN_202402 WHERE ma_TO ='VNP0702407'
COMMIT;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_023' and thang_kt is null

--------------- Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_023')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) --AND ma_TO ='VNP0702407'
;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV) AND 

-- BSC 30 ngay'
select distinct ten_vtcv , ma_vtcv from TTKD_BSC.bangluong_kpi_202402 where HCM_TB_GIAHA_022 is not null
update TTKD_BSC.bangluong_kpi_202402 a set 
HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)

;
select* from css_hcm.khuvuc
---------------Ty le cua To truong -----
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202402 and ma_nv = ''
update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
and thang_kt is null and MA_VTCV = a.MA_VTCV)
;   
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_Kpi = 'HCM_TB_GIAHA_023' AND LOAI_TINH = 'KPI_NV'
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)

commit;
-- CA 
-- NHAN VIEN 
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_nv = 'VNP017400' --and ma_vtcv = 'VNP-HNHCM_BHKV_18'
select HCM_TB_GIAHA_025,HCM_TB_GIAHA_024 from TTKD_BSC.bangluong_kpi_202402  where ma_vtcv = 'VNP-HNHCM_BHKV_41'
update TTKD_BSC.bangluong_kpi_202402 a 
set 
 HCM_TB_GIAHA_025 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025')
, HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') and thang_kt is null and MA_VTCV = a.MA_VTCV)
and A.ma_vtcv = 'VNP-HNHCM_BHKV_41'
--and hcm_tb_giaha_025 is  null or hcm_tb_giaha_024 is null
select distinct a.ma_nv from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv where thang = 202402 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
and b.ma_vtcv  = 'VNP-HNHCM_BHKV_41'
rollback;
COMMIT;
-- TO TRUONG
select* from ttkd_bsc.nhanvien_202402 where ma_vtcv = 'VNP-HNHCM_BHKV_41'

SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_024')
, HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to  AND MA_PB =A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_025')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV);

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi_202402 a 
                set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202402 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024' )
             , HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                    where thang = 202402 and loai_tinh ='KPI_PB' 
                                                 and ma_nv = a.ma_nv_hrm AND MA_PB  = A.MA_DONVI and ma_kpi = 'HCM_TB_GIAHA_025' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025')  
                                            and thang_kt is null and MA_VTCV = a.MA_VTCV)
     COMMIT;
;
-- TEN MIEN
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_nv = 'VNP017400' --and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202402 a 
set 
 HCM_TB_GIAHA_026 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026')

where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang_kt is null and MA_VTCV = a.MA_VTCV)  
and ma_Vtcv = 'VNP-HNHCM_BHKV_41'

rollback;
COMMIT;
-- TO TRUONG
SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202402 a 
set HCM_TB_GIAHA_026= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV);

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi_202402 a 
                set HCM_TB_GIAHA_026 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202402 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_026')  
                                            and thang_kt is null and MA_VTCV = a.MA_VTCV)
commit;