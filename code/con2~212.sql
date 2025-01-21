drop table donhang;
create table donhang as (select 
                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id--, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
--                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
--                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 308 lan
                     from  css.v_hd_khachhang@dataguard hdkh
													join css.v_hd_thuebao@dataguard hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
--													join tt  on hdtb.thuebao_id = tt.thuebao_id-- and gh.thang >= 202403
													join css.v_ct_tienhd@dataguard a on hdtb.hdtb_id = a.hdtb_id
													   join css.v_ct_phieutt@dataguard b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0
													   join css.v_phieutt_hd@dataguard p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
--													   left join hddc on hdtb.hdtb_id = hddc.hdtb_id
--													   left join kmtb on hdtb.hdtb_id = kmtb.hdtb_id
													   left join css.kenhthu@dataguard kt on kt.kenhthu_id = p.kenhthu_id
													   left join css.v_nganhang@dataguard nh on nh.nganhang_id = p.nganhang_id
													   left join css.hinhthuc_tra@dataguard ht on ht.ht_tra_id = p.ht_tra_id
				    
                     where  a.khoanmuctt_id = 11 and 
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224,136) and a.tien > 0 
                                      and ( to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20240901 and 20240930)  --    and p.ma_Gd = 'HCM-TT/02814791'            
)  ;
create table tt as;
WITH db as 
(
    select a.thuebao_id, a.loaitb_id, ma_tb
    from css.v_db_Thuebao@dataguard a
--        join css.v_db_adsl@dataguard b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9)  and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202406
) 
,km as (select rkm_id, thuebao_id, ma_tb, loaitb_id, ngay_bddc, ngay_ktdc, ngay_kt, ngay_huy, ngay_thoai
                            , thang_bddc, thang_ktdc, thang_kt_mg, thang_huy, thang_kt_dc                                                            
                            , case when ngay_huy is not null then ngay_huy
                                        when ngay_huy is null and thang_huy > 0 then last_day(to_date(thang_huy, 'yyyymm'))+1
                          when ngay_huy is null and thang_huy = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_huy_x
                            , case when ngay_thoai is not null then ngay_thoai
                                        when ngay_thoai is null and thang_kt_dc > 0 then last_day(to_date(thang_kt_dc, 'yyyymm'))+1
                          when ngay_huy is null and thang_kt_dc = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_thoai_x
                            , tien_td, cuoc_dc, tyle_sd, tyle_tb, thangdc so_thangdc, thangkm so_thangkm, congvan_id, khuyenmai_id, chitietkm_id, nhom_datcoc_id
                from v_thongtinkm_all_ol
                where  thang_ktdc > 202301 and cuoc_dc > 0 --and  thuebao_id = 11736290
            )
, km1 as(select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                , to_date(thang_bd_mg, 'yyyymm') ngay_bd_mg
                                , last_day(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm'))  ngay_kt_mg
                                , last_day(to_date(thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai
                       from v_thongtinkm_all_ol
                       where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) and (tyle_sd = 100 or tyle_tb = 100) 
                       and cuoc_dc = 0 and thang_bd_mg > 202201 
                )
, dc as (
        ----------------TT Thang tang tren 1 dong-------------
             select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id
                            , km.ngay_bddc, least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , least(km.ngay_kt, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x ngay_thoai
                            , km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km 
            where (km.tyle_sd = 100 or km.tyle_tb = 100)
                            and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR))  >= ngay_bddc
                            and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                            
                        --    and km.thuebao_id = 9085442
           union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) end ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x NGAY_THOAI, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km left join km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  km1.ngay_bd_mg
            where (km.tyle_sd + km.tyle_tb < 100)
                    and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) >= ngay_bddc
                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                         --   and km.thuebao_id = 8131788
            
             union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_ktdc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg
                            , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km
            where (km.tyle_sd + km.tyle_tb < 100)
                            and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                            and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                          --  and km.thuebao_id = 9085442
    ) --select* from dc where  ma_Tb = 'nhakhoaphuchau';
, tt1 as (
    select thuebao_id, rkm_id ,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202406 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
) 
, tt2 as 
(
    select thuebao_id, rkm_id,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202408 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt3 as (
    select thuebao_id, rkm_id,  ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202407 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
) 
,tt as (
    select  db.thuebao_id,ma_Tb
    from db
        join tt1  on db.thuebao_id = tt1.thuebao_id and tt1.rn = 1
        join tt2 on db.thuebao_id = tt2.thuebao_id and tt2.rn = 1
        join tt3 on db.thuebao_id = tt3.thuebao_id and tt3.rn = 1
) --select a.* from tmp3_ob a join tt on a.thuebao_id = tt.thuebao_id;
select* from dc; where rownum =1;
drop table tt;
--select* from tt where ma_Tb = 'hcm2nhvu_2021';
, 

 hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc , h.thang_bd
                                            from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where thuebao_id = 9731692
 ) 
, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css.v_khuyenmai_dbtb@dataguard
                            where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                            and thang_bddc > 202307
) 
;
select a.*, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
from donhang a
    join tt on a.thuebao_id = tt.thuebao_id
    left join hddc on a.hdtb_id = hddc.hdtb_id
    left join kmtb on a.hdtb_id = kmtb.hdtb_id