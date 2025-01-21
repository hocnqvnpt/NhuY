with can_gh as (
     select  thuebao_id, khachhang_id, thanhtoan_id, ma_tb, ngay_ktdc, ngay_insert, pbh_id, donvi_id,thang_kt
             , so_dt, sdt_lh, ma_tt, doivt_id, khuvuc_id 
     from report.ghtt_ds_thuebao@dataguard a
     where ngay_ktdc >= to_date('20241101','yyyymmdd') and ngay_ktdc < add_months(to_date('20241101','yyyymmdd'),1) and ko_theodoi = 0
           and thuebao_id in (12209814,4510551) and phanvung_id = 28
)
,ghtt as
(
    SELECT a.thuebao_id, a.khachhang_id, a.thanhtoan_id, db.ma_tb, a.ngay_ktdc, a.ngay_insert, a.pbh_id, a.donvi_id,a.thang_kt,
            a.so_dt, a.sdt_lh, a.ma_tt, a.doivt_id, a.khuvuc_id,
            db.trangthaitb_id, db.ten_tb, db.diachi_ld, db.ngay_td , cq.chuquan_id, 
            db.dichvuvt_id, db.loaitb_id
    FROM can_gh a
    join css.v_db_thuebao@dataguard db on db.thuebao_id=a.thuebao_id and db.loaitb_id in (58,61,171,210,274,222,224)
    join css.v_db_adsl@dataguard  cq on cq.thuebao_id = a.thuebao_id 
         and cq.phanvung_id = 28  and db.phanvung_id = 28
                       and cq.chuquan_id in (145, 264, 266)
)
,hdtb as
  (
       select 
             hd.tthd_id, hd.hdtb_id, hd.hdkh_id, hd.kieuld_id
           , kh.ma_gd, kh.loaihd_id, hd.ngay_tt, hd.ngay_ht
           , kh.ngay_yc ngay_tao_dh, kh.ctv_id, hd.ngay_ins
           , kh.nhanvien_id nhanvien_id_taodh, nv.donvi_id donvi_id_taodh -- kh.donvi_id donvi_id_taodh --don vi tao don hang
           , hd.ngay_cn, hd.may_cn, hd.nguoi_cn, hd.ip_cn
           , ds.thuebao_id, ds.ma_tb, ds.khachhang_id, ds.trangthaitb_id
           , ds.loaitb_id
       from ghtt ds
       join css.v_hd_thuebao@dataguard  hd on hd.thuebao_id=ds.thuebao_id and hd.tthd_id <>7
       join css.hd_khachhang@dataguard kh on  kh.hdkh_id=hd.hdkh_id and kh.phanvung_id = 28  and hd.phanvung_id = 28
                    and ( (kh.ngay_yc >= ds.ngay_ktdc-45 and kh.ngay_yc < add_months(to_date('20241101','yyyymmdd'),2))
                         )
       left join admin.nhanvien@dataguard nv on kh.nhanvien_id = nv.nhanvien_id and nv.phanvung_id = 28
   ) 
, phieu_dd as (
     select h.*, row_number() over (partition by h.thuebao_id order by h.thuebao_id, h.hdkh_id desc) tt_hd --h.thuebao_id,count(*)
            , case when h.loaihd_id in (31,32) then 1 else 0 end phieu_tt
     from hdtb h
     where 
            (h.loaihd_id in (17) -- phi?u TP dang d? + hoàn thành    -> tr? sau
             or (h.loaihd_id in (31,32) and tthd_id not in (6))      -- Phi?u TT dang d?
             or (h.loaihd_id =8 and tthd_id=6) -- Phi?u TD hoàn thành -> tr? sau
            )  
           and tthd_id not in (7) 
     ) 
,trasau_kotamthu_cn as (
     select g.khachhang_id,g.ma_tb,h.*, row_number() over (partition by h.thuebao_id order by h.thuebao_id, h.ngay_cn desc) tt_tb, 1 tb_trasau_kotamthu, hdtb.hdtb_id
     from ghtt g
    join css.v_ghtt_chuyendoi@dataguard  h on h.thuebao_id=g.thuebao_id and h.ngay_cn >= g.ngay_ktdc-45 and h.ngay_cn<add_months(to_date('20241101','yyyymmdd'),+2) and h.kieutra_id = 2
     left join hdtb on g.thuebao_id = hdtb.hdtb_id
     where g.trangthaitb_id not in (6,7,9) and h.phanvung_id = 28
           and not exists ( select 1 from phieu_dd b where tt_hd=1 and phieu_tt=0 and b.thuebao_id=h.thuebao_id)
) select* from phieu_dd;
, xd_kq as (
 select thuebao_id,ma_tb, khachhang_id,ma_gd, 'that bai: phieu TP' ketqua_gh, loaihd_id, kieuld_id, ctv_id, tthd_id, donvi_id_taodh, nhanvien_id_taodh, hdtb_id, hdkh_id, ngay_tt, ngay_tao_dh, NGAY_CN, MAY_CN, NGUOI_CN, IP_CN
 from phieu_dd b where tt_hd=1 and phieu_tt=0
 union all
 select thuebao_id,ma_tb, khachhang_id,'ghtt_chuyendoi', 'that bai: TS ko t?m thu' ,0 loaihd_id, 0 kieuld_id, 0 ctv_id, 0 tthd_id, 0 donvi_id_taodh, 0 nhanvien_id_taodh, hdtb_id, 0 hdkh_id, null ngay_tt, null ngay_tao_dh, NGAY_CN, MAY_CN, NGUOI_CN, IP_CN
 from trasau_kotamthu_cn b where tt_tb=1                 
)        
select * from xd_kq;