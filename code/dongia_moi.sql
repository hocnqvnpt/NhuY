select sum(dongia*heso_chuky*heso_dichvu) from dongia_moi where thang = 202404;
commit;
delete from dongia_moi where thang = 202404;
insert into dongia_moi
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

             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                        ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id nhomtb_cu_id ,  a.nhomtb_id nhomtb_moi_id
                                            , hs.khachhang_id,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
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
                        from nhuy_ct_bsc_ipcc_obghtt a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.rkm_id is not null --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_nv, a.ma_to, a.ma_pb
                                            , a.thuebao_id, a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id
        ) a
        where rnk = 1 and dthu > 0 ;
        

INSERT INTO ttkd_bsc.tl_giahan_Tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                        , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
SELECT thang, loai_tinh, ma_kpi,  ma_Nv, MA_tO, MA_PB
                    , count(thuebao_id) tong
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan--, null
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong--, null
                      , round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
                      , round(sum(dongia*heso_chuky*heso_dichvu*tien_khop)) tien
-- select * from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--from TTKD_BSC.ct_dongia_tratruoc  a
from dongia_moi  a
where ma_kpi = 'DONGIA' --and loaitb_id in (58,59)
            and loai_tinh in ('DONGIA_TS_TT') and thang = 202404
group by thang, loai_tinh, ma_kpi, ma_Nv, MA_TO, MA_PB;
COMMIT;
rollback;
