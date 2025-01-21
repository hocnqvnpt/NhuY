select* from ttkd_bsc.nhanvien_202404;
select thang, loai_kpi,ma_tb,homecombo,
            to_char(ngay_dk_goi,'dd/mm/yyyy') ngay_dk_goi,
            to_char(ngay_sd_fiber,'dd/mm/yyyy') ngay_sd_fiber,
            fiber, a.ma_nv,a.ma_to,a.ma_pb, b.ten_to,b.ten_pb
from ttkd_bsc.ct_bsc_homecombo a
         join  ttkd_Bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv          
where loai_kpi  in ('Fiber_hh','Fiber_moi','HomeTV') ;      
where loai_kpi like in ('Fiber_hh','Fiber_moi','HomeTV'
select* from css_hcm.goi_dadv_lhtb where goi_id = 19153;
        select 
        THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, LOAITB_ID, THANHTOAN_ID, ma_Nv, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv, NHOMTB_CU_ID, NHOMTB_MOI_ID, KHACHHANG_ID, SOTHANG_DC, 
        HT_TRA_ONLINE, kenhthu_tainha,HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, NHOMTB_ID, TIEN_KHOP, RNK,
                     case  when tien_khop = 1 then 7500
                         WHEN tien_khop =2 then 15000
                         when tien_khop = 3 then 12000
                         when tien_khop = 4 then 6000
                         else 0
                         end dongia 
        from (
        with hs as (select thang, khachhang_id from nhuy_ct_bsc_ipcc_obghtt xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
--            , goi as (select nhomtb_id , thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 
--                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
--                                   --  and nhomtb_id not in (2691065)
--                ) 
                
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
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , hs.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.so_Thangdc_moi) sothang_dc
                                     ,sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
                                    , case
                                            when max(a.so_Thangdc_moi) >=12 then 1.2
                                            when max(a.so_Thangdc_moi) < 12 and max(a.so_Thangdc_moi) >= 6 then 1
                                            when max(a.so_Thangdc_moi) < 6 and max(a.so_Thangdc_moi) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.nhomtb_id
                                    , max(nvl(tien_khop,0)) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from ct_bsc_trasau_tp_tratruoc a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.thang = 202404 and a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.thuebao_id, a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id
        ) a
        where rnk = 1 and dthu > 0 ;