create table dongia_moi as
        select 
        THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, MANV_OB, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv, NHOMTB_CU_ID, NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC, 
        HT_TRA_ONLINE, kenhthu_tainha,HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, NHOMTB_ID, TIEN_KHOP, RNK,
                     case  when KENHTHU_tainha >0 then 12000
                         WHEN KENHTHU_tainha = 0 and ht_Tra_online >0  then 7500
                         when KENHTHU_tainha = 0 and ht_Tra_online = 0 then 15000
                         else 0
                         end dongia ;
                         delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202404;
                         insert into ttkd_Bsc.tl_giahan_tratruoc 
                         select 202404 thang,
                         'KPI_NV' loai_tinh,
                         'HCM_SL_ORDER_001' ma_kpi,
                         a.manv_thuyetphuc ma_nv, ma_to, ma_pb,
                         36 tong,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan,
                           round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong
                                      , null tyle, null dongia, null,null
--      select*
        from (
--        with hs as (select thang, khachhang_id from nhuy_ct_bsc_ipcc_obghtt xu
--                where xu.rkm_id is not null and xu.loaitb_id in (58,59) group by thang, khachhang_id
--                ), 
--            ms as (select thang, khachhang_id from nhuy_ct_bsc_ipcc_obghtt xu
--                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
--            )
--            , goi as (select nhomtb_id , thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
--                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
--                                   --  and nhomtb_id not in (2691065)
--                ) 
--                
--            , km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
--         from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100 and thang_bddc > 202301)
--             , km as (
--                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
--                                , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
--                                , km.thangdc + km.thangkm SOTHANG_DC, km.khuyenmai_id
--                from v_thongtinkm_all km 
--                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
--                                --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
--                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
--                                and thang_bddc > 202310
--                union all
------------------TT giam cuoc or thang tang tren 2 dong-------------
--                
--                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
--                                , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
--                                , km.thangdc + nvl(km1.thangkm, 0) SOTHANG_DC, km.khuyenmai_id
--                from v_thongtinkm_all km left join km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
--                where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
--                               -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
--                               and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
--                               and km.thang_bddc > 202301
--             )
             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_Nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , a.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
                                     ,sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                   , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                    * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                            , 0)

                                            ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                            ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                            when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from nhuy_ct_bsc_ipcc_obghtt xu
                                                                                                where xu.loaitb_id in (58, 59)
                                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                        else 0 
                                       end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.nhomtb_id
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from nhuy_ct_bsc_ipcc_obghtt a 
--                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join ms on ms.thang = a.thang and ms.khachhang_id = a.khachhang_id

--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.thuebao_id, a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id, a.khachhang_id
--                                            , hs.khachhang_id, ms.khachhang_id
        ) a
        where rnk = 1 and dthu > 0 
        group by a.thang, a.manv_thuyetphuc, ma_to, ma_pb
;
select* from tmp_ob where 1=1;
select count(distinct thuebao_id) from nhuy_ct_Bsc_ipcc_obghtt where manv_ob='VNP016953' and nhanvien_ob_id = nvtuvan_id;
        commit;
        rollback;
        select* from ttkd_bsc.nhuy_ct_ipcc_obghtt where thuebao_id in (8750890,8753100,8759484,8768511,8768729,8769408,8774615,8777384,8781511,8784580,8796616,8859435,8871794,8879150,8879389,8882934)
        select* from tmp_ob where nvtuvan_id is null;--  = 4511026;
        select* from nhuy_ct_bsc_ipcc_obghtt where thuebao_id=11838773;-- where MANV_THUYETPHUC is null and rkm_id is not null --THUEBAO_ID  = 12062776;
        select * from nhuy_ct_bsc_ipcc_obghtt where loaitb_id in (210,61, 171, 18,58,59) and rkm_id is not null --group by khachhang_id  having count(distinct loaitb_id) > 2; -- where thang = 202302 and khachhang_id = 7320163;
--        select khachhang_id from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202302 group by khachhang_id having count(khachhang_id) > 3;
   select * from ttkd_bsc.nhuy_ct_ipcc_obghtt where to_number(to_char(td_th,'yyyymm')) != thang--and thuebao_id in (select thuebao_id from xsmax)-- loaitb_id in (210,61, 171, 18,58,59) and khachhang_id in 
   (select khachhang_id from nhuy_ct_bsc_ipcc_obghtt where loaitb_id in (210,61, 171, 18,58,59) group by khachhang_id  having count(distinct loaitb_id) > 1) --loaitb_id in (210,61, 171, 18) group by khachhang_id  having count(distinct loaitb_id) > 1  -- where thang = 202302 and khachhang_id = 7320163;
