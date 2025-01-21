select a.PHIEUTT_ID ,nvl(d.hdkh_id,a.HDKH_ID)hdkh_id ,nvl(d.MA_GD,a.ma_Gd) ma_Gd ,b.HDTB_ID ,c.THUEBAO_ID ,c.MA_TB  ,c.tEN_TB ,db.DIACHI_TB,e.LOAIHINH_TB ,a.NGAY_TT,b.TIEN ,b.VAT  
,b.KHOANMUCTT_ID  ,f.MA_KMTT  ,
a.HT_TRA_ID  ,h.HT_TRA  ,a.SERI  ,a.SOSERI ,a.NGAY_HD ,a.TRANGTHAI  ,a.THUNGAN_TT_ID ,a.DONVI_TT_ID,a.KENHTHU_ID  ,a.SO_CT,a.UNGDUNG_ID, u.TEN_UNGDUNG
from css_hcm.phieutt_Hd a
    left join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id
    left join css_hcm.hd_thuebao c on b.hdtb_id = c.hdtb_id
    left join css_hcm.hd_khachhang d on c.hdkh_id = d.hdkh_id
    left join css_hcm.loaihinh_Tb e on c.loaitb_id = e.loaitb_id
    left join css_hcm.khoanmuc_tt f on b.khoanmuctt_id = f.khoanmuctt_id
    left join css_hcm.hinhthuc_Tra h on a.ht_tra_id = h.ht_Tra_id
    left join css_hcm.ds_ungdung_online u on a.ungdung_id = u.ungdung_id
    left join css_hcm.db_Thuebao_sub db on c.thuebao_id = db.thuebao_id
where a.trangthai = 1 and (a.ngay_tt is null or thungan_tt_id is null) and b.tien>0;
select owner , table_name from all_tab_columns where column_name = upper('ungdung_id');
