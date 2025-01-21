select ngay_tt from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202402 and ngay_tt is not null-- ma_tb in ('hcm_ca_00073395','hcm_ca_00095463','hcm_ca_00095460','hcm_ca_00072884','hcm_ca_00039067','hcm_ca_00039209');
select* from css_hcm.doituong
select ngay_kt, ma_Tb from css_hcm.db_cntt a left join css_hcm.db_Thuebao b on a.thuebao_id =  b.thuebao_id
where
ma_tb in ('hcm_ca_00073395','hcm_ca_00095463','hcm_ca_00095460','hcm_ca_00072884','hcm_ca_00039067','hcm_ca_00039209');
    select  max(ngay_bddc),thuebao_id from v_Thongtinkm_all a where thuebao_id in 
    (select thuebao_id from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202403 and rkm_id is null ) and hieuluc = 1 and ttdc_id = 0
    group by thuebao_id;