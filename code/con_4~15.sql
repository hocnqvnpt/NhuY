update ttkd_bsc.bangluong_kpi a
set CHITIEU_gIAO = 100 
where thang = 202411 and ma_Kpi = 'HCM_TB_GIAHA_027' ;and ma_nv  in 
(select ma_nv from ttkd_Bsc.bangluong_kpi where thang = 202410 and ma_kpi = a.ma_kpi and chitieu_giao is not null);
commit;
select distinct ma_nv  from ttkd_Bsc.blkpi_dm_to_pgd where thang in ( 202410,202411) and dichvu = 'CSKH';
select* from ;
update ttkd_Bsc.bangluong_kpi 
set chitieu_Giao = null
where thang >=202410 and ten_Nv like '%Kiên' and ma_kpi = 'HCM_TB_GIAHA_027';
update ttkd_Bsc.bangluong_kpi  A
set giao = (select MAX(SOGIAO) from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG 
where thang = 202411 and ten_kpi = '2.CT PTM thuê bao gói Home Sành/Home ch?t' AND MA_NV = A.MA_nV)
where thang = 202411 and ma_kpi = 'HCM_SL_COMBO_006';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 AND   ma_kpi in ( 'HCM_TB_GIAHA_027','HCM_SL_BHOL_002','HCM_TB_GIAHA_022','HCM_TB_GIAHA_023') AND CHITIEU_GIAO IS NOT NULL;
select *from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG 
where thang = 202410 and ten_kpi = '2.CT PTM thuê bao gói Home Sành/Home ch?t';
SELECT* FROM 

;
commit;
update ttkd_Bsc.bangluong_kpi set chitieu_giao = 90 where thang = 202411 and ma_kpi ='HCM_SL_BHOL_002';
update TTKD_bSC.BANGLUONG_KPI
set giao = 25 
WHERE   ma_kpi = 'HCM_SL_COMBO_006' AND TEN_NV LIKE '%Tuy?t' and thang >=202410 and ma_Vtcv = 'VNP-HNHCM_KDOL_2';