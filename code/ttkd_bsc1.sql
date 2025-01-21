update TTKD_BSC.bangluong_kpi_202403 a 
set 
 HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') and thang_kt is null and MA_VTCV = a.MA_VTCV) and ma_Nv ='VNP027256'
and A.ma_vtcv = 'VNP-HNHCM_BHKV_41'
--and hcm_tb_giaha_025 is  null or hcm_tb_giaha_024 is null
select distinct a.ma_nv from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202402 b on a.ma_nv = b.ma_nv where thang = 202402 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
and b.ma_vtcv  = 'VNP-HNHCM_BHKV_41'
rollback;
COMMIT;
-- TO TRUONG
select* from ttkd_bsc.nhanvien_202402 where ma_vtcv = 'VNP-HNHCM_BHKV_41'

SELECT hcm_tb_giaha_026 FROM TTKD_BSC.bangluong_kpi_202402 where ma_to = 'VNP07012H0' and ma_vtcv = 'VNP-HNHCM_BHKV_18'

update TTKD_BSC.bangluong_kpi_202403 a 
set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_donvi and ma_kpi = 'HCM_TB_GIAHA_024')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV) and ma_To ='VNP0702509';

select* FROM ttkd_bsc.tl_giahan_tratruoc WHERE THANG = 202403 AND LOAI_TINH = 'DONGLUCTS' and ma_nv = 'VNP016582'

