select a.*, c.muccuoc muccuoc_cu, c.cuoc_tg cuoc_Cu,  e.muccuoc muccuoc_moi, e.cuoc_tg cuoc_moi
from x_xgspon a 
    left join css_hcm.bd_Thuebao b on a.hdtb_id = b.hdtb_id
    left join css_hcm.muccuoc_Tb c on b.mucuoctb_id = c.mucuoctb_id
    left join css_hcm.db_Thuebao d on a.ma_tb = d.ma_Tb and d.loaitb_id in (58,59)
    left join css_Hcm.muccuoc_Tb e on d.mucuoctb_id = e.mucuoctb_id;
    
    select a.*, b.donvi_id , c.ma_Tb , dv1.ten_dv
    from sotb a
        join css_hcm.hd_khachhang b on a.MÃ_GT_?HSXKD = b.ma_Gd
        join css_hcm.hd_thuebao c on b.hdkh_id = c.hdkh_id
        left join admin_hcm.donvi dv on b.donvi_id = dv.donvi_id
        left join admin_hcm.donvi dv1 on dv.donvi_cha_id =dv1.donvi_id;
        
select * from qltn_hcm.ct_no where ma_Tb= '918016578' and ky_cuoc = 20241101 ;

select* from  ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a