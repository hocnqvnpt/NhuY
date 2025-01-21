select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202405 and ma_to = 'VNP0702504' and thang_ktdc_cu = 202405;
update ttkd_Bsc.bangluong_kpi_202405 SET HCM_TB_GIAHA_026= 100
--select* from  ttkd_Bsc.bangluong_kpi_202405
WHERE MA_vTCV IN (SELECT ma_Vtcv FROM TTKD_BSC.blkpi_danhmuc_kpi_vtcv WHERE THANG_KT IS NULL AND GIAMDOC_PHOGIAMDOC  =1  AND MA_KPI = 'HCM_TB_GIAHA_026')
AND MA_nv NOT IN (SELECT nvl(MA_nv,'a') FROM TTKD_bSC.TL_GIAHAN_tRATRUOC WHERE THANG = 202405 AND MA_KPI = 'HCM_TB_GIAHA_026' AND LOAI_TINH = 'KPI_PB') 
and HCM_TB_GIAHA_026 is null;
commit;
create table backup_kpi as select* from  ttkd_Bsc.bangluong_kpi_202405;
select* from TTKD_bSC.TL_GIAHAN_tRATRUOC WHERE THANG = 202405 AND LOAI_TINH = 'DONGIATRA_OB' ;and ma_nv = 'CTV084801';
select* from ttkd_Bsc.bangluong_kpi_202405;

SELECT * FROM TTKD_BSC.blkpi_danhmuc_kpi_vtcv WHERE THANG_KT IS NULL AND MA_KPI = 'HCM_TB_GIAHA_022';
select* from ttkd_bct.hocnq_cp_nhancong_hoahong;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_SL_COMBO_005' and thang_kt is null
;
select * from ttkd_bsc.v_dthu_ptm_mangphutrach_pdn where thang=202405