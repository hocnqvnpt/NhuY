select* from hocnq_ttkd.ds_giahan_Tratruoc where ma_Tb = 'nguyenbros2023';
select * from v_Thongtinkm_all  where ma_Tb = 'kyluong_2019';
select tratruoc, km,loaibo, thang_kt from ttkdhcm_ktnv.ghtt_giao_688 where ma_Tb = 'masterisehomes1738339';

    with dc as (
    ----------------TT Thang tang tren 1 dong-------------
                     select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc, least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , least(km.ngay_kt, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_kt_mg, km.ngay_huy, km.NGAY_THOAI
                                    , km.tien_td, km.cuoc_dc
                                    , km.thangdc + km.thangkm so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km 
                    where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))  >= ngay_bddc
                                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                                    and km.thang_ktdc > 202310 and ma_Tb = 'nguyenbros2023'
                                --    and km.thuebao_id = 9085442
                   union all
--                                                                 -------KM 100, nhung khong co datcoc              
--                                                                               select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, to_date(km.thang_bd_mg, 'yyyymm') ngay_bd
--                                                                                                , LAST_DAY(to_date(km.thang_kt_mg, 'yyyymm')) ngay_ktdc
--                                                                                                , LAST_DAY(to_date(km.thang_kt_mg, 'yyyymm')) ngay_kt_mg, km.ngay_huy, km.NGAY_THOAI                                                                                               
--                                                                                                , km.tien_td, km.cuoc_dc
--                                                                                                , km.thangdc + km.thangkm so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
--                                                                                from v_thongtinkm_all_ol km 
--                                                                                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc =0
--                                                                                                and hieuluc = 1 and ttdc_id = 0
--                                                                                                and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
--                                                                                                and km.thang_kt_mg > 202307
--                                                                                                and km.thuebao_id = 8131788
--                                                                               union all
    ----------------TT giam cuoc or thang tang tren 2 dong-------------
                    select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) ngay_ktdc
                                    , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) end ngay_kt_mg
                                    , km.ngay_huy, km.NGAY_THOAI, km.tien_td, km.cuoc_dc
                                    , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km left join (select thuebao_id, thang_bd_mg, LAST_DAY(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg, rkm_id, thangkm
                                    from v_thongtinkm_all_ol
                                    where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                    and (tyle_sd = 100 or tyle_tb = 100) and thang_bd_mg > 202310 
                                ) km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  to_date(km1.thang_bd_mg, 'yyyymm')
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc
                                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)     
                                    and km.thang_ktdc > 202310
                                 --   and km.thuebao_id = 8131788
                    
                     union all
    ----------------TT giam cuoc or thang tang tren 2 dong-------------
                    select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
                                    , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) thang_ktdc
                                    , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) thang_kt_mg
                                    , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
                                    , km.thangdc so_thangdc, km.thangkm so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
                    from v_thongtinkm_all_ol km 
                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0
                                    and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                    and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)
                                    and km.thang_ktdc > 202310 and    ma_tb = '2-cxlg10b'
                                  --  and km.thuebao_id = 9085442
            ) select* from dc where   ma_tb = 'kidsplaza_q8';