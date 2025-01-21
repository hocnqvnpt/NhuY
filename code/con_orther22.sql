select* from qrcode where thang = 202410;
update qrcode set thang= 202411 where thang is null;
commit;
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202411 and ma_Gd in (select ma_gd from qrcode );
and tien_khop ;
select distinct kenhthu_id, ht_Tra_id from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202411 and tien_khop = 0;
select* from css_hcm.trangthai_Tb ;
