update ttkd_bsc.bangluong_kpi_202311
set HCM_TB_GIAHA_023 = null
commit;
                        -- BSC 60 ngay
                        
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312 and ma_kpi = 'HCM_TB_GIAHA_023'
update TTKD_BSC.bangluong_kpi_202403 a set 
HCM_TB_GIAHA_023 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
COMMIT;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_023')
    and thang_kt is null and MA_VTCV = a.MA_VTCV)

;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV)

                -- BSC 30 ngay
                
                
update TTKD_BSC.bangluong_kpi_202403 a set 
HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)

;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_Kpi = 'HCM_TB_GIAHA_023' AND LOAI_TINH = 'KPI_NV'
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and 
loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)

commit;
-- CA THANG T
-- NHAN VIEN 
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024'
update TTKD_BSC.bangluong_kpi_202403 a set 
HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null and MA_VTCV = a.MA_VTCV)
and HCM_TB_GIAHA_024 is null
;
COMMIT;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
 and ma_to = 'VNP0702216' and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_024')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and HCM_TB_GIAHA_024 is null and ma_to = 'VNP0702219'
;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm and ma_to = a.ma_donvi and ma_kpi = 'HCM_TB_GIAHA_024' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null and MA_VTCV = a.MA_VTCV) and HCM_TB_GIAHA_024 is null
rollback;


-- CA THANG T-1


select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312 and ma_kpi = 'HCM_TB_GIAHA_025'
update TTKD_BSC.bangluong_kpi_202403 a set 
HCM_TB_GIAHA_025 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_025') and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
COMMIT;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and  ma_to = 'VNP0702216' and ma_kpi = 'HCM_TB_GIAHA_025')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_025')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and ma_to = 'VNP0702219'

;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and
ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_025') and thang_kt is null and MA_VTCV = a.MA_VTCV)
-- TEN MIEN 

-- nhan vien
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312 and ma_kpi = 'HCM_TB_GIAHA_023'
update TTKD_BSC.bangluong_kpi_202403 a set 
        HCM_TB_GIAHA_026 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
COMMIT;
---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_026 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO'
and ma_to = a.ma_to and ma_to = 'VNP0702216' and ma_kpi = 'HCM_TB_GIAHA_026')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_026')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and ma_to = 'VNP0702219'
    rollback;
;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_026 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_026') and thang_kt is null and MA_VTCV = a.MA_VTCV)    

select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where thang_kt is null and ma_kpi = 'HCM_TB_GIAHA_025';
select distinct ma_vtcv from ttkd_Bsc.bangluong_kpi_202403 where HCM_TB_GIAHA_025 is not null ;
select HCM_TB_GIAHA_024, HCM_TB_GIAHA_025, HCM_TB_GIAHA_026 from ttkd_Bsc.bangluong_kpi_202403 where   ma_nv in ('VNP017662','VNP027588')

select * from ttkd_Bsc.nhanvien_202312 where ma_To = 'VNP0702216'  
;
