create table tmp3_tp as;
WITH 
db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202403
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
                from v_thongtinkm_all
                where  thang_ktdc > 202301 and cuoc_dc > 0 --and  thuebao_id = 11736290
            )
, km1 as    (
                        select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                , to_date(thang_bd_mg, 'yyyymm') ngay_bd_mg
                                , last_day(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm'))  ngay_kt_mg
                                , last_day(to_date(thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai
                       from v_thongtinkm_all
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
    ) 
, tt1 as (
    select thuebao_id, rkm_id ,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202403 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt2 as 
(
    select thuebao_id, rkm_id,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202404 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt3 as (
    select thuebao_id, rkm_id,  ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202405 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
,ts as (
    select  db.thuebao_id
    from db
        left join tt1  on db.thuebao_id = tt1.thuebao_id and tt1.rn = 1
        left join tt2 on db.thuebao_id = tt2.thuebao_id and tt2.rn = 1
        left join tt3 on db.thuebao_id = tt3.thuebao_id and tt3.rn = 1
    where tt1.rkm_id is  null and tt2.rkm_id is  null and tt3.rkm_id is  null
) 
,hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css_hcm.hdtb_datcoc g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where h.thang_bd > 202307
 ) 
, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                            where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                            and thang_bddc > 202307
) 
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--           join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--        group by bb.phieu_id) 
, kq_ghtt as (select 
                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai--,  ct.ngay_nganhang
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 111 lan, A.KHOANMUCTT_ID
                     from  css_hcm.hd_khachhang hdkh
													join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id  =136 and hdtb.tthd_id = 6
--													join ts  on hdtb.thuebao_id = ts.thuebao_id-- and gh.thang >= 202403
													join css_hcm.ct_tienhd a on hdtb.hdtb_id = a.hdtb_id
                                                   join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and a.khoanmuctt_id = b.khoanmuctt_id and b.tien > 0
                                                   join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
                                                   left join hddc on hdtb.hdtb_id = hddc.hdtb_id
                                                   left join kmtb on hdtb.hdtb_id = kmtb.hdtb_id
                                                   left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
                                                   left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
                                                   left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
--													   left join ct on p.phieutt_id = ct.phieu_id
				    
                     where  a.khoanmuctt_id = 3130 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and  to_number(to_char(p.ngay_tt, 'yyyymm')) >= 202405                ----change--2 thang- ngay 02
--                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
)  

select *
from kq_ghtt a;