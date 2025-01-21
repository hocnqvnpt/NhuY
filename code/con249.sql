select b.ma_nv
from css_hcm.phieutt_hd a 
join admin_hcm.nhanvien b on a.thungan_tt_id = b.nhanvien_id
where a.ma_gd = 'HCM-TT/02803930';

select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405;