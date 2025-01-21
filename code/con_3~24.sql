select * from css_hcm.hinhthuc_tra
drop table chut_xoa
(;
create table hai as 
with  ddd as (
select thuebao_id,loaitb_id, ma_tb, ma_gd, phieutt_id, ngay_tt, TIEN_THANHTOAN, VAT_THANHTOAN, ht_Tra_id, kenhthu_id from tmp3_ts
union all
select thuebao_id, loaitb_id, ma_tb, ma_gd,phieutt_id, ngay_tt, TIEN_THANHTOAN, vat VAT_THANHTOAN, ht_Tra_id, kenhthu_id  from tmp3
union all
select thuebao_id, loaitb_id, ma_tb, ma_gd, phieutt_id,ngay_tt,TIEN_THANHTOAN, VAT_THANHTOAN, ht_Tra_id, kenhthu_id  from tmp_ob)
select   THUEBAO_ID, MA_TB, a.LOAITB_ID,  MA_GD, phieutt_id, to_number(to_char(ngay_tt,'yyyymm'))thang_tt, NGAY_TT, TIEN_THANHTOAN, VAT_THANHTOAN, KENHTHU, HT_TRA, b.loaihinh_Tb, c.ten_Dvvt
from ddd a
    left join css_hcm.loaihinh_Tb b on a.loaitb_id =b.loaitb_id
    left join css_hcm.dichvu_vt c on b.dichvuvt_id = c.dichvuvt_id 
    left join css_hcm.hinhthuc_tra d on a.ht_Tra_id = d.ht_Tra_id
    left join css_hcm.kenhthu k on a.kenhthu_id = k.kenhthu_id
union all 
create table mot as
with ds as (
select thuebao_id, ma_Tb,loaitb_id, ma_gd, phieutt_id,  to_number(to_char(ngay_tt,'yyyymm')) thang_Tt,ngay_tt, tien_thanhtoan, vat, kenhthu, TEN_HT_TRA  from ttkd_bsc.ct_bsc_Tratruoc_moi a where thang = 202404 and phieutt_id is not null and ten_Ht_tra = 'QRCode '
union all
select thuebao_id, ma_Tb, loaitb_id, ma_gd, phieutt_id, to_number(to_char(ngay_tt,'yyyymm')) thang_Tt, ngay_tt,tien_thanhtoan, vat, kenhthu, TEN_HT_TRA  from ttkd_bsc.ct_bsc_Tratruoc_moi_30day a where thang = 202404 and phieutt_id is not null and ten_Ht_tra = 'QRCode '
)
select a.*,  b.loaihinh_Tb, c.ten_Dvvt
from ds a
  left join css_hcm.loaihinh_Tb b on a.loaitb_id =b.loaitb_id
    left join css_hcm.dichvu_vt c on b.dichvuvt_id = c.dichvuvt_id 
    select distinct THUEBAO_ID, MA_TB, LOAITB_ID, MA_GD, PHIEUTT_ID, THANG_TT, NGAY_TT, TIEN_THANHTOAN, VAT, KENHTHU, TEN_HT_TRA, LOAIHINH_TB, TEN_DVVT from (
select* from mot where thang_tt = 202404
union all 
select* from hai where thang_tt = 202404 )
--select count (distinct ma_Gd) from hopdong;
select distinct a.ma_Gd, a.ngay_tt , a.tien + a.vat tongtien_vat
from css_Hcm.phieutt_hd a join hopdong b on a.ma_gd = b.ma_gd ;
65010;
select* from chut_xoa where TONGTIEN_VAT > 0 and ma_gd in (select ma_Gd from chut_xoa group by ma_Gd having count (ma_gd) > 1 )-- from tmp3 where ma_gd in ('C','c') --and thang = 202404;
update tmp3 set ma_Gd = 'HCM-TT/02676208'
where phieutt_id = '8219246' --and thang = 202404;
;
select ma_gd, max(ngay_tt), sum(tongtien_Vat) from chut_xoa where TONGTIEN_VAT > 0 --and ma_gd in (select ma_Gd from chut_xoa group by ma_Gd having count (ma_gd) > 1 )
group by ma_gd
commit
;
select count(distinct ma_gd) from chut_xoa;
select 1 from css_hcm.phieutt_hd a join  css_hcm.hd_khachhang b on a.hdkh_id = b.hdkh_id
where phieutt_id in (8281344,8198331);
update 
select* from css_hcm.phieutt_hd where  phieutt_id in (8281344,8198331); hdkh_id in (21773355,22077754);
update hopdong set 
;
HCM-TT/02662321
HCM-TT/02687078
HCM-TT/02684069
HCM-TT/02658187