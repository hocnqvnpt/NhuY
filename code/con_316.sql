select* from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where thang = 202404 and rkm_id is not null and tien_khop is null;
select* from tmp3_30ngay;

select* from 
rename  ct_bsc_tratruoc_moi_thang_t_1 to ct_bsc_tratruoc_moi_tr30day
;
SELECT owner, table_name
  FROM dba_tables;
  update nhuy_ct_bsc_ipcc_obghtt set thang = 202404;
  commit;