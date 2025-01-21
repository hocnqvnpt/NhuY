-- LAY THONG TIN HOP DONG CUA THUE BAO CO TRA TRUOC 
--SELECT THUEBAO_ID FROM (
with tthd as (
select ghtt.thuebao_id, mc.muccuoc , td.tocdo, td.thuonghieu, c.tenmuc, hdtb.tthd_id,dt.ten_dt, hdtb.ten_tb,hdtb.hdkh_id, hdtb.hdtb_id, hdtb.diachi_ld,  lkh.ten_loaikh
        , hdkh.mst, hdkh.ma_kh, hdkh.so_gt
from ttkdhcm_ktnv.ghtt_giao_688 ghtt  
    join css_hcm.hd_Thuebao hdtb on ghtt.thuebao_id  = ghtt.thuebao_id
    join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
    left join css_hcm.muccuoc_tb mc on hdtb.mucuoctb_id = mc.mucuoctb_id
    left join css_hcm.doituong dt on hdtb.doituong_id = dt.doituong_id 
    left join css_hcm.loai_kh lkh on hdkh.loaikh_id = lkh.loaikh_id 
    left join css_hcm.muccuoc c on mc.muccuoc_id = c.muccuoc_id
    left join css_hcm.tocdo_adsl td on mc.tocdo_id = td.tocdo_id
where  ghtt.tratruoc = 1 and ghtt.loaibo = 0 
),
--) GROUP BY THUEBAO_ID HAVING COUNT(THUEBAO_ID) > 1
-- LAY THONG TIN TRA TRUOC 
tttt as 
(
    select  ptt.phieutt_id,ptt.hdkh_id,ptt.ma_gd, ptt.tien tong_tien,  ctp.tien ct_tien,ctp.hdtb_id, ctp.khoanmuctt_id,   ptt.trangthai,ptt.ngay_tt ,
    htt.ht_tra,kt.kenhthu, dsud.ten_ungdung 
    from  css_hcm.phieutt_hd ptt 
        join css_hcm.ct_phieutt ctp on ptt.phieutt_id = ctp.phieutt_id
        left join css_hcm.hinhthuc_tra htt on ptt.ht_tra_id = htt.ht_tra_id
        left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
        left join css_hcm.DS_UNGDUNG_ONLINE dsud on ptt.ungdung_id = dsud.ungdung_id
    where  ptt.tien > 0 and ctp.tien > 0 
)
select *
from tthd join tttt on tthd.hdkh_id = tttt.hdkh_id and tthd.hdtb_id = tttt.hdtb_id
