select* from css_Hcm.phieutt_hd where ma_Gd ='HCM-TT/03074836';

select 
a.phieutt_id, a.hdkh_id, a.ma_gd, b.hdtb_id, c.thuebao_id, c.ma_tb, c.ten_tb, c.diachi_tb, e.loaihinh_Tb, a.ngay_tt, b.tien+b.vat tong_Tien
, b.tien, b.vat, b.khoanmuctt_id, f.ma_kmtt, a.ht_Tra_id, g.ht_Tra, a.seri,a.soseri, a.ngay_hd, a.trangthai, a.thungan_Tt_id, a.donvi_tt_id, 
a.kenhthu_id, a.ungdung_id, d.ten_ungdung
from css.v_phieutt_hd@dataguard a
    join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id
    join css.v_hd_thuebao@dataguard c on b.hdtb_id = c.hdtb_id
    join css.ds_ungdung_online@dataguard d on a.ungdung_id = d.ungdung_id
    join css.loaihinh_Tb@dataguard e on c.loaitb_id =e.loaitb_id
    join css.v_khoanmuc_Tt@dataguard f on b.khoanmuctt_id = f.khoanmuctt_id
    join css.hinhthuc_Tra@dataguard g on a.ht_Tra_id = g.ht_Tra_id
where a.ma_gd in ('HCM-TT/03072453','HCM-TT/03073100','HCM-TT/03094497','HCM-TT/03044543','HCM-TT/03074836');
select* from css_hcm.ds_ungdung_online;

