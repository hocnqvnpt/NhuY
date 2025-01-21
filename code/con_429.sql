select my.*, dv.ten_dv
from my 
    join css_hcm.db_Thuebao a on my.acc_mytv = a.ma_Tb
    join admin_hcm.donvi dv on a.donvi_id = dv.donvi_id;
    select distinct loai_tinh, ma_kpi from ttkd_bsc.tl_giahan_Tratruoc where thang= 202410;