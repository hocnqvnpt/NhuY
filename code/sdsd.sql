select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_Vtcv ='VNP-HNHCM_BHKV_41' and thang_kt is null

select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023';
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_023' and thang_kt is null ;--and nvl(thang_kt,202401) = 202401;

select b.* from ttkd_bsc.nhanvien_202402 a join ttkd_Bsc.tl_giahan_tratruoc b
on a.ma_nv=  b.ma_nv 
where a.ma_vtcv = 'VNP-HNHCM_BHKV_41' aNd  MA_KPI LIKE 'HCM_TB_GIAHA_%'

select * from ttkd_bsc.ct_bsc_giahan_Cntt where thang = 202402  AND LOAITB_ID IN (147,148) and thang_ktdc_cu = 202402
and ma_Tb  in ( 'hcm_tmvn_00003356','hcm_tmvn_00004344');

select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_022' and loai_tinh = 'KPI_PB';

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_024' and thang_kt is null

