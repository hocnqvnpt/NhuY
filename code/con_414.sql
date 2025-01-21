select* from ttkd_Bsc.nhanvien_202407;

select* from v_Thongtinkm_All where ma_Tb = 'ansinhchuthapdo';

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
select kyhoadon,thuebao_id,ma_tb,rkm_id,cuoc_tk,ton_ck
     from qltn.v_db_datcoc@dataguard where phanvung_id=28  and kyhoadon=20240501 
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
where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405  and b.kieuld_id in (553,551)
);
update  ttkd_bsc.nhanvien set thang = 202406, donvi='VTTP' where thang is null;
commit;
select* from phan_bo
;
select* from ttkd_Bsc.nhanvien_202405 where ten_Nv like '%Nh?';
-- check tien thoai
--with thoai as 
--(
--    select thuebao_id, sum(tien_thoai) tien_thoai
--    from qltn.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20240401 and tien_Thoai is not null
--    group by thuebao_id
--)
--, tt as (
--select b.thuebao_id, sum(c.tien+ c.vat) tien
--from css_hcm.hd_khachhang a
--    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id and b.tthd_id = 6
--    join css_Hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id and c.khoanmuctt_id = 11 and c.tien < 0
--    join css_Hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id
--where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405 and b.kieuld_id in (553,551)
--group by b.thuebao_id
--)
--select *
--from thoai a
--    left join tt on a.thuebao_id= tt.thuebao_id
--where nvl(-a.tien_thoai,0) != nvl(tt.tien,0)
;

select a.hdkh_id, a.khachhang_id, b.thuebao_id, b.ma_Tb, a.ngay_yc, d.ngay_tt, a.ma_gd,c.tien, c.vat
from css_hcm.hd_khachhang a
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id and b.tthd_id = 6
    join css_Hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id and c.khoanmuctt_id = 11 and c.tien < 0
    join css_Hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id
where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405 and b.kieuld_id in (553,551);
;

select* from qltn.v_db_Datcoc@dataguard where phanvung_id=28 and kyhoadon=20240401 and tien_Thoai is not null and thuebao_id = 8361155; and rkm_id in (6881742,6420632);
select a.hdkh_id, a.khachhang_id, b.thuebao_id, b.ma_Tb, a.ngay_yc, d.ngay_tt, a.ma_gd,c.tien, c.vat
from css_hcm.hd_khachhang a
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id and b.tthd_id = 6
    join css_Hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id and c.khoanmuctt_id = 11 and c.tien < 0
    join css_Hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id
where to_number(to_char(d.ngay_Tt,'yyyymm')) = 202405 and b.kieuld_id in (553,551);

select* from v_Thongtinkm_all where rkm_id in ( 6881742,6420632);

select* from css_hcm.trangthai_hd;

select * from ttkd_Bsc.nhanvien where  ma_nv = 'CTV021936';
update ttkd_Bsc.nhanvien set ma_to = 'VNP0701890' , ten_To = 'C?a Hàng 492 Nguy?n Duy Trinh' where ma_Nv = 'CTV021936' and thang = 202406;
commit;