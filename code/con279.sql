select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023';
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202407 and ma_nv = 'CTV046582';
UPDATE ttkd_bsc.bangluong_kpi SET GIAO = (SELECT GIAO FROM ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_NV ='VNP017853')
, THUCHIEN = (SELECT THUCHIEN FROM ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_NV ='VNP017853')
, TYLE_tHUCHIEN = (SELECT TYLE_tHUCHIEN FROM ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_NV ='VNP017853')
, MUCDO_HOANTHANH = (SELECT MUCDO_HOANTHANH FROM ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_NV ='VNP017853')
WHERE THANG =  202408 and ma_kpi = 'HCM_TB_GIAHA_023' AND MA_NV = 'VNP017072';
COMMIT;
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202408 and ten_Nv like '%Bá V?%';
select* from ttkd_Bsc.nhanvien where ten_nv like '%H?ng Duyên';