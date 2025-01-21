select b.ma_Tb, a.ma_Gd, d.tien, d.vat, d.tien+d.vat  tong_Tien
from dichvucA C 
    JOIN css_Hcm.hd_khachhang a on a.ma_gd = c.ma_Gd
    join css_hcm.hd_Thuebao b on a.hdkh_id = b.hdkh_id
    left join css_Hcm.ct_phieutt d on b.hdtb_id = d.hdtb_id
order by  a.ma_gd
