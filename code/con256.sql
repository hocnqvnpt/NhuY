select * from v_thongtinkm_all b where (thuebao_id,rkm_id) in (select thuebao_id, rkm_id from tmp_3 a where rkm_id not in (select rkm_id from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang in  (202405,202406)) and exists (select 1 from v_Thongtinkm_all where 
a.thuebao_id = thuebao_id and rkm_id > a.rkm_id and ttdc_id = 0 and hieuluc = 1)) ;

select * from css_Hcm.bd_goi_Dadv  where thuebao_id=1070148 and trangthai = 1;
select* from css_hcm.db_thuebao where thuebao_id=1070148;

