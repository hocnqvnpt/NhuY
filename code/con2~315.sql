with tbi as (
   select db.ma_tb, a.thuebao_id, a.SDVT_ID, a.THIETBI_ID, a.NGAY_SD, a.TRANGBI_ID, a.SERIAL, db.trangthaitb_id,db.donvi_id
                                    , b.KHO_ID, b.VATTU_ID
                                    , c.TEN_VT, c.LOAITBI_ID, c.NHOMVT_ID, rank() over (partition by db.thuebao_id order by a.SDVT_ID desc) rnk
                       from qlvt.v_sudung_vt@dataguard a
                                        join css.v_db_thuebao@dataguard db on a.thuebao_id = db.thuebao_id 
                                        left join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id
                                        left join qlvt.v_vattu@dataguard c on b.vattu_id = c.vattu_id
                       where c.loaitbi_id = 2 and c.nhomvt_id = 1 and db.loaitb_id in (58, 59) --and db.trangthaitb_id not in (7,8,9)
                        and c.vattu_id = 17676 
)
, ds as (
select 
    b.ma_tb, a.ngay_Yc, b.ngay_ht , a.ma_gd, ten_kieuld, ten_loaihd , nvl(pb.ten_dv,pbrp.ten_dv) phong_Bh, tbi.ten_vt thietbi
    ,trangthai_Hd, trangthai_Tb, row_number() over (partition by b.thuebao_id order by a.hdkh_id desc) rn, ttvt.ten_Dv ttvt,cn.congnghe
from css.v_hd_khachhang@dataguard a
    left join css.v_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    join css.v_db_adsl@dataguard c on b.thuebao_id = c.thuebao_id and c.congnghe_id = 11
    left join css.loai_hd@dataguard d on a.loaihd_id = d.loaihd_id
    left join css.kieu_ld@dataguard e on b.kieuld_id = e.kieuld_id
    left join admin.nhanvien@dataguard nvtp on a.ctv_id = nvtp.nhanvien_id
    left join admin.donvi@dataguard dv on nvtp.donvi_id = dv.donvi_id
    left join admin.donvi@dataguard pb on dv.donvi_cha_id = pb.donvi_id
    left join admin.donvi@dataguard t on a.donvi_id = t.donvi_id
    left join admin.donvi@dataguard pbrp on t.donvi_cha_id = pbrp.donvi_id
    join tbi on b.thuebao_id = tbi.thuebao_id
    left join css.trangthai_hd@dataguard tthd on b.tthd_id = tthd.tthd_id
    left join css.trangthai_Tb@dataguard tt on tbi.trangthaitb_id =tt.trangthaitb_id
    left join css.congnghe@dataguard cn on c.congnghe_id =cn.congnghe_id
    left join admin.donvi@dataguard ttvt on tbi.donvi_id = ttvt.donvi_id

where  d.loaihd_id in ( 8,1) 
)
select* from ds where rn = 1;
select* from css.v_hd_khachhang@dataguard where ma_gd ='HCM-TD/00717017';
select* from css.v_hd_Thuebao@dataguard where ma_Tb='philong9684337';
select* from css.v_giaophieu_tbi@dataguard;
select ngay_yc from css_hcm.hd_khachhang where ma_gd='HCM-LD/01934683';
select* from css.v_Giaophieu@dataguard;
select* from qlvt.v_phieu_vt@dataguard where  hdtb_id in(26889819,26863639,26706757)