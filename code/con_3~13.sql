select* from ttkd_bsc.ct_Dongia_Tratruoc where thang =202404 and ma_Tb = 'hcm_ca_ivan_00015680';
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where  ma_Tb = 'hcm_ca_ivan_000156';

select* from  ttkdhcm_ktnv.ghtt_giao_688 where ma_Tb in ( 'nqdai2022' ,'ants2021','xd-anduc','phongduc2312','560e-pvd');

select* from ds_Giahan_tratruoc2  where ma_Tb in ( 'nqdai2022' ,'x','xd-anduc','phongduc2312','560e-pvd');
select* from ttkd_Bsc.ct_bsc_tratruoc_moi_tr30day where thang = 202404;
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202404 ;
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where ma_tb = 'hcm_ca_00076811';

select * from ttkd_Bsc.bangluong_kpi_202404 where  ma_vtcv = 'VNP-HNHCM_BHKV_48' AND ;
select * from ttkd_Bsc.tl_Giahan_Tratruoc where THANG = 202404 AND ma_Kpi = 'HCM_TB_GIAHA_022' AND LOAI_TINH = 'KPI_TO' AND MA_TO IN (select MA_tO from ttkd_Bsc.bangluong_kpi_202404 where  ma_vtcv = 'VNP-HNHCM_BHKV_48');