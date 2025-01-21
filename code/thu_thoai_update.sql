with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc
                        from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
)
, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css.v_khuyenmai_dbtb@dataguard 
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
)
-- tien tra truoc
,tra_truoc as (
select a.hdkh_id, a.khachhang_id, b.thuebao_id, b.ma_Tb, a.ngay_yc, d.ngay_tt, a.ma_gd,c.tien, c.vat, tt.ten_kmtt
from css_hcm.hd_khachhang a
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id and b.tthd_id = 6
    join css_Hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id and c.tien > 0
    join css_Hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id
--    left join hddc on b.hdtb_id = hddc.hdtb_id
--    left join kmtb on b.hdtb_id = kmtb.hdtb_id
    left join css_hcm.khoanmuc_tt tt on c.khoanmuctt_id = tt.khoanmuctt_id
where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405 and d.trangthai =1 and d.kenhthu_id != 6
)
, phan_bo as (
select kyhoadon,thuebao_id,ma_tb,rkm_id,cuoc_tk,ton_ck, tien_thoai
    from qltn.v_db_datcoc@dataguard where phanvung_id=28 and thuebao_id = 9651660 and kyhoadon=20240501 
)
, thoai as (
select a.hdkh_id, a.khachhang_id, b.thuebao_id, b.ma_Tb, a.ngay_yc, d.ngay_tt, a.ma_gd,c.tien, c.vat,  tt.ten_kmtt
from css_hcm.hd_khachhang a
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id and b.tthd_id = 6
    join css_Hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id and  c.tien < 0
    join css_Hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id
--    left join hddc on b.hdtb_id = hddc.hdtb_id
--    left join kmtb on b.hdtb_id = kmtb.hdtb_id
    left join css_hcm.khoanmuc_tt tt on c.khoanmuctt_id = tt.khoanmuctt_id
where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405  and b.kieuld_id in (553,551) and d.trangthai = 1
)
select* from tra_Truoc
