drop table tmp3_ts -- purge;
;
select chuquan_id from css_hcm.db_Thuebao a 
join css_Hcm.db_adsl b on a.thuebao_id = b.thuebao_id 
where ma_Tb = 'fvn_tuan6a69'
commit;
update tmp3_ts set lan =5;
alter table tmp3_ts add lan number(3) ;
create table tmp3_ts as;
insert into tmp3_Ts
with db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202401
)
, km1 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202402 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km2 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, cuoc_Dc ,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202403 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km3 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and cuoc_dc > 0 and
    202401 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, ts as (
select db.thuebao_id
from db 
    left join km1 on db.thuebao_id = km1.thuebao_id and km1.rn = 1
    left join km2 on db.thuebao_id = km2.thuebao_id and km2.rn = 1
    left join km3 on db.thuebao_id = km3.thuebao_id and km3.rn = 1
where km1.rkm_id is null and km2.rkm_id is null and km3.rkm_id is null)
,hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where h.thang_bd > 202307
                                            ) 
   , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                                and thang_bddc > 202307
                        )
    , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
group by bb.phieu_id)
    , kq_ghtt as (select 
    hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai,  ct.ngay_nganhang
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, 
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 7 lan
                     from css_hcm.ct_tienhd a
                            left join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11-- and b.tien < 0
                            left join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id
                            left join hddc on a.hdtb_id = hddc.hdtb_id
                            join css_hcm.hd_thuebao hdtb on a.hdtb_id = hdtb.hdtb_id  and hdtb.kieuld_id in (551, 550, 24, 13080) 
                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id  
                            left join kmtb on a.hdtb_id = kmtb.hdtb_id
                            left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
                            left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
                            join  ts on hdtb.thuebao_id = ts.thuebao_id 
                            left join ct on p.phieutt_id = ct.phieu_id

                     where   to_number(to_char(hdkh.ngay_yc,'yyyymm')) = 202404  and a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20240401 and 20240502)                     ----change--2 thang- ngay 02
                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20240401 and 20240502)) 
                                    )  

select *
from kq_ghtt a
where ngay_bd_moi is not null and not exists (select rkm_id from tmp3_ts where rkm_id = a.rkm_id) ; ;
select * from tmp3_60ngay -- group by rkm_id having count(rkm_id) > 1;
select* from v_thongtinkm_all where ma_Tb ='hongdinh1887510'