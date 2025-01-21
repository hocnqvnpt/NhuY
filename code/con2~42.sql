select* from css_hcm.db_Thuebao where ma_Tb = 'tn072022';
select* from bang_gom where thuebao_id in (select thuebao_id
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and  not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id));
--    and ma_Tb in (select ma_Tb from ma_Tb where nguon = 'nsg')
    union all 
    select thuebao_id
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id);

WITH db as 
(
    select a.thuebao_id, a.loaitb_id, ma_tb
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202402
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
, km1 as(select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
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
    ) --select* from dc;
, tt1 as (
    select thuebao_id, rkm_id ,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202402 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt2 as 
(
    select thuebao_id, rkm_id,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202403 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt3 as (
    select thuebao_id, rkm_id,  ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202404 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm')) and dc.so_Thangdc >= 3
)
,tt as (
    select  db.thuebao_id,ma_Tb, tt3.rkm_id
    from db
--        join tt1  on db.thuebao_id = tt1.thuebao_id and tt1.rn = 1
--        join tt2 on db.thuebao_id = tt2.thuebao_id and tt2.rn = 1
        join tt3 on db.thuebao_id = tt3.thuebao_id and tt3.rn = 1 
)
--,
--create table kmm as (
-- select ngay_ktdc, thuebao_id, row_number() over (partition by thuebao_id order by rkm_id desc) rn from v_Thongtinkm_all a where exists 
--    (select 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405 and thuebao_id = a.thuebao_id) 
--)
select a.* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
--    join kmm b on a.thuebao_id = b.thuebao_id --and b.rn = 2
where thang = 202405
and exists (select 1 from tt where a.rkm_id = rkm_id) and ob_id is null ;
--and rkm_id in (select rkm_id from ttkd_Bsc.ct_bsc_trasau_tp_Tratruoc where thang = 202404);
--delete from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a where rkm_id in (select rkm_id from ttkd_Bsc.ct_bsc_trasau_tp_Tratruoc where thang = 202404) and ob_id is null;
--rollback;
--commit;
and thuebao_id not in (4574714,4290616,7206932,8525983,9316626,11830328,11786498,9412365,9408402,11831127,11831129,11813059,9632515,9211945,9211948,11846631,8226065,8363632,
9211945,9211948,9237029,9412365,11780597,11786498,11813059,11830328,11831127,11846631);
delete  from ttkd_Bsc.ct_bsc_trasau_tp_Tratruoc where thang = 202405 and thuebao_id = 1896856;