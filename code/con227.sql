select distinct a.ht_Tra_id, a.kenhthu_id, c.kenhthu, d.ht_Tra
from css_hcm.phieutt_hd a
    join qrcode b on a.ma_gd = b.ma_gd
    join css_hcm.kenhthu c on a.kenhthu_id = c.kenhthu_id
    join css_hcm.hinhthuc_tra d on a.ht_tra_id = d.ht_tra_id;
select* from css_hcm.hinhthuc_tra where ht_Tra_id in (   
select * from ttkd_Bsc.ct_bsc_Tratruoc_moi_30day where thang = 202404 and ht_tra_id in (204,214,216)
);
 select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where phieu_id in 
 (   
select * from ct_bsc_trasau_tp_tratruoc where thang = 202404 --and ht_tra_id in (204,214,216)
);--