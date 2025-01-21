select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' ;
SELECT* FROM RMP_BSC_PHONG WHERE THANG = 202410 AND MA_PB = 'VNP0702100' AND LOAI_TINH = 'KPI_NV';
SELECT SUM(DA_GIAHAN_DUNG_hEN) FROM TTKD_bSC.TL_gIAHAN_tRATRUOC where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006' AND MA_PB = 'VNP0702100' AND LOAI_TINH = 'KPI_NV';
rollback;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202410 and ma_kpi = 'HCM_CL_TONDV_003' ;
update ttkd_Bsc.bangluong_kpi set GIAO = 883, THUCHIEN = 131 , TYLE_tHUCHIEN = 14.84
where thang = 202410 and ma_kpi ='HCM_CL_TONDV_003' and ma_nv = 'VNP017621';

commit;
UPDATE ttkd_Bsc.bangluong_kpi A
SET TYLE_THUCHIEN = (SELECT TYLE  FROM TTKD_bSC.TL_GIAHAN_tRATRUOC 
WHERE 202410 = THANG AND MA_kPI = 'HCM_TB_GIAHA_022' AND LOAI_tINH ='KPI_TO' AND MA_TO = A.MA_TO and ma_pb = a.ma_pb)
    where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_029' ;
    commit;
    select * from ttkd_bsc.BLkpi_thuchien_202410_20241120_2026;
SELECT* FROM TTKD_bSC.TL_GIAHAN_TRatruoc where thang = 202410 and ma_Kpi = 'HCM_TB_GIAHA_024' and ma_To = 'VNP0702406';

select * from ttkd_Bsc.bangluong_dongia_202410 where ten_Nv like '%Thu Hi?n';
205,571,777
227,631,705