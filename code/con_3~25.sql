,update ttkd_bsc.ct_bsc_Giahan_cntt 
set tien_khop = null,
ma_tb_khac = null
where thang = 202404 and ma_Tb in ('hcm_ca_00039226','hcm_ca_00055400');

commit;
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 and ma_To = 'VNP0702509' and ma_pb = 'VNP0702500' and ma_kpi = 'HCM_TB_GIAHA_024';
DELETE  FROM ttkd_bsc.CT_dONGIA_TRATRUOC WHERE THANG = 202404 and ma_Tb in ('hcm_ca_00039226','hcm_ca_00055400');
select* from  ttkd_bsc.ct_bsc_Giahan_cntt  where thang = 202404 and ma_Tb in ('hcm_ca_00039226','hcm_ca_00055400');
select* from ttkd_bsc.tl_giahan_Tratruoc where thang = 202404 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_to = 'VNP0702509' and loai_tinh = 'KPI_TO';

update ttkd_bsc.tl_giahan_Tratruoc
set DA_GIAHAN_DUNG_HEN = 1,
tyle = round(1*100/4,2) where thang = 202404 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_Nv = 'VNP027256' and loai_tinh = 'KPI_NV';