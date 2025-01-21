select* from ttkd_Bsc.bangluong_kpi where thang = 202410;
update ttkd_bsc.bangluong_kpi set chitieu_Giao = 1650 where  ma_kpi = 'HCM_SL_HOTRO_006' and thang = 202410 ;and ma_Nv not in ('CTV087758','CTV087789','CTV087779');
select* from ttkd_bsc.bangluong_kpi where  ma_kpi = 'HCM_SL_HOTRO_006' and thang = 202410;
rollback;
SELECT* FROM TTKD_bSC.BLKPI_DANHMUC_KPI WHERE THANG = 202410;
commit;
update ttkd_bsc.bangluong_kpi set chitieu_Giao = 100 where thang = 202410 and ma_kpi in ('HCM_CL_TONDV_003','HCM_TB_GIAHA_023');