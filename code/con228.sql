              with 
                                km as (select rkm_id, thuebao_id, ma_tb, loaitb_id, ngay_bddc, ngay_ktdc, ngay_kt, ngay_huy, ngay_thoai
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
                                                       where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) and (tyle_sd = 100 or tyle_tb = 100) and cuoc_dc = 0 and thang_bd_mg > 202201 
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
--                                                                                
--                                                                                 union all
--                                                                ----------------TT giam cuoc or thang tang tren 2 dong-------------
--                                                                                select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
--                                                                                                , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_ktdc
--                                                                                                , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg
--                                                                                                , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
--                                                                                                , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
--                                                                                from km
--                                                                                where (km.tyle_sd + km.tyle_tb < 100)
--                                                                                                and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                                                                                and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                                                                                              --  and km.thuebao_id = 9085442
                                                                ) select* from dc where thuebao_id=8066931;
                                                                (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                , to_date(thang_bd_mg, 'yyyymm') ngay_bd_mg
                                                                , last_day(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm'))  ngay_kt_mg
                                                                , last_day(to_date(thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai
                                                       from v_thongtinkm_all
                                                       where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) and (tyle_sd = 100 or tyle_tb = 100) and cuoc_dc = 0 and thang_bd_mg > 202201 
                                                );
                                                (select rkm_id, thuebao_id, ma_tb, loaitb_id, ngay_bddc, ngay_ktdc, ngay_kt, ngay_huy, ngay_thoai
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
                                            );