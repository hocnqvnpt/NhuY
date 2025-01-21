with a as (
             select     
             a.thang, a.thuebao_id, a.ma_to, a.ma_pb
                                        ,a.MANV_THUYETPHUC, nhomtb_id_cu, nhomtb_id
                                          ,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
--                                     ,sum(case 
--                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
--                                            else 0 end) ht_tra_online
--                                    
--                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
--                                                        else 0 end) kenhthu_tainha
--                                    
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                   , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.nhomtb_id_cu, 1, 0)  ---co goi giao = 1
                                                                                                    * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                            , 0)

                                            ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                            when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from chot_final xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                            ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                            when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from chot_final xu
                                                                                                where xu.loaitb_id in (58, 59)
                                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                        else 0 
                                       end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU--, max(ngay_tt) ngay_tt, a.nhomtb_id, max(ngay_yc)
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(rkm_id)) rnk
                        from bang_gom a 
                                join ttkd_bsc.nhanvien_202404 b on a.MANV_THUYETPHUC = b.ma_nv                              
                        where a.rkm_id is not null and thang = 202404 and b.ma_pb = 'VNP0701400' and a.thuebao_id not in 
                        (select thuebao_id from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202404 and tien_khop > 0) and a.ma_tb in (select ma_Tb from ma_tb where nguon = 'nsg')--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, nhomtb_id_cu, nhomtb_id, loaitb_id
--                   
        ) 
 select thang,
                         'KPI_NV' loai_tinh,
                         'HCM_SL_ORDER_001' ma_kpi,
                         a.manv_thuyetphuc ma_nv, a.ma_to, a.ma_pb,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan
--                        ,   round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
--                                      , sum(dthu) DTHU_thanhcong
--      select *
        from  a 
        where rnk = 1 and dthu > 0 
        group by a.thang, a.manv_thuyetphuc, a.ma_to, a.ma_pb;