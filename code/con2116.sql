update ttkd_bsc.ct_Bsc_giahan_cntt set tien_khop = 1 where thang = 202412 and ma_Tb in ('hcm_ca_00052507','hcm_ca_00067140','hcm_ivan_00029960');
commit;

select* from ttkd_Bsc