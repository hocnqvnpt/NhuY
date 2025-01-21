select* from nhuy_ct_bsc_ipcc_obghtt where thang = 202404 and rkm_id is not null;

select b.thuebao_id, c.goi_id, g.nhomtb_id, d.thuebao_id,b.ma_tb,d.ma_Tb, tt.ma_tt, t.ma_tt 
from ttkd_bsc.ct_bsc_homecombo a
join css_hcm.db_thuebao b on a.ma_tb = b.ma_tb and b.loaitb_id in (58,59)
join css_hcm.bd_goi_dadv c on b.thuebao_id = c.thuebao_id and trangthai = 1
join css_hcm.bd_goi_dadv g on c.nhomtb_id = g.nhomtb_id
join css_hcm.db_Thuebao d on g.thuebao_id = d.thuebao_id and g.thuebao_id != b.thuebao_id
join css_hcm.db_Thanhtoan tt on b.thanhtoan_id = tt.thanhtoan_id
join css_hcm.db_thanhtoan t on d.thanhtoan_id = t.thanhtoan_id
where thang = 202402 and loai_kpi = 'Fiber_hh'  and t.ma_tt != tt.ma_tt
;
select ma_tt from css_hcm.db_thuebao a join css_hcm.db_Thanhtoan b on a.thanhtoan_id = b.thanhtoan_id 
where a.ma_tb in ('hcm_thibay_17','thbay1696601','832322722');