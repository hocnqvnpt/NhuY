select* from css.v_phieutt_hd@dataguard where phieutt_id = 8908296;
from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001';
update ttkd_Bsc.bangluong_kpi set giao = 660, tyle_thuchien = round(thuchien*100/660,2), mucdo_hoanthanh = round(thuchien*100/660,2) where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001';
commit;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202408;

select* from ttkdhcm_ktnv.ghtt_chotngay_271 where ma_Tb = 'hcm_ca_00062200' and to_char(ngay_chot,'yyyymmdd') = '20240902';
