delete
select* from DS_GIAHAN_TRATRUOC_AAA where rkm_id = 6103313 and rownum = 1;
commit;

select thuebao_id, tien_thanhtoan, thang_ktdc_cu , a.* from ttkd_bsc.ct_bsc_giahan_cntt a where loaitb_id in (147,148) and thang = 202402 and tien_khop = 1
select* from css_hcm.db_cntt where thuebao_id =12148091;

select* from css_hcm.db_thuebao where ten_tb like '%Bích Nhung'