select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang_kt is null
VNP0701410      
VNP0702219
select * from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and ma_to = 'VNP0702216'
VNP0701400
VNP0702200
select* from ttkd_bsc.nhanvien_202401 where ma_nv in ('VNP017072','VNP027588')

select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202401 and  ma_to in( 'VNP0702216','VNP0702219') and loai_tinh = 'KPI_TO';
select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_pb = 'VNP0701400'
select* from ttkd_Bsc.tl_ where thang = 202401 and thang_ktdc_cu = 202401 and MAnv_giao = 'VNP027588'--ma_pb in ('VNP0701400
VNP0702200','')'
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202401 and ma_To = 'VNP0702216' and thang_ktdc_cu  = 202401 --and thuebao_id in (4610740,9413332,7254767,9413328,1894862,9137696,8210680)
select* from css_hcm.db_datcoc where rkm_id in (6444606,6434377,6379166,6436374,6379160,6417289,6436379)
select distinct  a.ma_nv
from ttkd_bsc.nhanvien_202401 a
join ttkd_Bsc.ct_bsc_giahan_cntt b on a.ma_nv = b.manv_giao and b.thang_ktdc_cu = 202401 and loaitb_id not in (147,148)  and thang = 202401
where  ma_vtcv in ('VNP-HNHCM_BHKV_18','VNP-HNHCM_BHKV_2')
(select ma_vtcv from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where thang_kt is null and ma_kpi = 'HCM_TB_GIAHA_024')

select* from ttkd_bsc.tl_Giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024'