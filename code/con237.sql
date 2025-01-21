select* from ttkd_bsc.ct_bsc_Giahan_cntt where thang = 202404 and loaitb_id not in (147,148) and ma_Tb in ('hcm_ca_00054808',
'hcm_ca_00054804',
'hcm_ca_00054819',
'hcm_ca_00054801',
'hcm_ca_00054817',
'hcm_ca_00054810',
'hcm_ca_00054814',
'hcm_ca_00054809',
'hcm_ca_00054806',
'hcm_ca_00054803',
'hcm_ca_00054795');
and thang_ktdc_cu = 202404 and  thuebao_id not in (
select * from ttkdhcm_ktnv.ghtt_chotngay_271 where loaitb_id not in (147,148) and thang_kt in (202405) and ngay_chot=to_date('20240503','yyyymmdd')
and ma_Tb in ('hcm_ca_00054808',
'hcm_ca_00054804',
'hcm_ca_00054819',
'hcm_ca_00054801',
'hcm_ca_00054817',
'hcm_ca_00054810',
'hcm_ca_00054814',
'hcm_ca_00054809',
'hcm_ca_00054806',
'hcm_ca_00054803',
'hcm_ca_00054795');
ROLLBACK;
DELETE from ttkd_Bsc.ct_bsc_Giahan_cntt where manv_Giao = 'VNP017930' and thang = 202404 and thang_ktdc_cu = 202405 AND ma_Tb in ('hcm_ca_00054808',
'hcm_ca_00054804',
'hcm_ca_00054819',
'hcm_ca_00054801',
'hcm_ca_00054817',
'hcm_ca_00054810',
'hcm_ca_00054814',
'hcm_ca_00054809',
'hcm_ca_00054806',
'hcm_ca_00054803',
'hcm_ca_00054795');
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202404 and ma_To = 'VNP0702308' and ma_pb = 'VNP0702300' and ma_kpi = 'HCM_TB_GIAHA_025';
COMMIT;
SELECT* FROM TTKD_bSC.TL_GIAHAN_tRATRUOC WHERE THANG = 202404 AND MA_nV = 'VNP017930' AND ma_kpi = 'HCM_TB_GIAHA_025';
SELECT* FROM TTKD_bSC.TL_GIAHAN_tRATRUOC WHERE THANG = 202404 AND ma_To = 'VNP0702308' AND ma_kpi = 'HCM_TB_GIAHA_025' AND LOAI_TINH = 'KPI_TO';
SELECT* FROM TTKD_bSC.TL_GIAHAN_tRATRUOC WHERE THANG = 202404 AND ma_PB = 'VNP0702300' AND ma_kpi = 'HCM_TB_GIAHA_025' AND LOAI_TINH = 'KPI_PB' AND MA_nV='VNP017763';


UPDATE TTKD_bSC.TL_GIAHAN_tRATRUOC SET TONG = 2 ,TYLE = 100 WHERE THANG = 202404 AND MA_nV = 'VNP017930' AND ma_kpi = 'HCM_TB_GIAHA_025';
UPDATE TTKD_bSC.TL_GIAHAN_tRATRUOC  SET TONG = 3 ,TYLE = 100 WHERE THANG = 202404 AND ma_To = 'VNP0702308' AND ma_kpi = 'HCM_TB_GIAHA_025' AND LOAI_TINH = 'KPI_TO';
UPDATE TTKD_bSC.TL_GIAHAN_tRATRUOC SET TONG = 175 ,TYLE = 8.65  WHERE THANG = 202404 AND ma_PB = 'VNP0702300' AND ma_kpi = 'HCM_TB_GIAHA_025' AND LOAI_TINH = 'KPI_PB' AND MA_nV='VNP017763';
ROLLBACK;

