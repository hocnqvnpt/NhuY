select * from tmp3_30ngay-- where thang = 202401-- and loai_tinh = 'DONGIATRA30D' --and ma_tb = 'ltchanhlho'
select* from css_hcm.hinhthuc_tra where ht_tra_id in (1,2,4,5,7)
select* from ct_bsc_tratruoc_moi_30day where thang = 202308 and ma_tb = 'ltchanhlho'
delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRA30D'
--commit;
--select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRA'
--(select thuebao_id, phieutt_id from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310 and manv_thphuc = 'khongco' and rkm_id is not null)
--update  ttkd_bsc.ct_bsc_tratruoc_moi 
--set manv_thphuc = 'khongco'
--where thang = 202310 and manv_thphuc is null
--update s
select distinct ht_tra_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202402-- and rkm_id is not null and manv_thphuc = 'khongco'
select* from css_hcm.hinhthuc_Tra where ht_Tra_id in (1,2,4,7)
insert into TTKD_BSC.ct_dongia_tratruoc
--insert into ct_dongia_tratruoc

--select* from ttkd_bsc.ct_bsc_Tratruoc_moi_30day where thang = 202309 and rkm_id is not null and ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
            with dc as (select /*+ RESULT_CACHE */ dctb.thuebao_id, 'CanGio' ten_quan from css_hcm.diachi_tb dctb, css_hcm.diachi dc 
            where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998))
            , hs as (select thang, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day xu
            where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id)
            select THANG, cast('DONGIATRA30D' as varchar(30)) LOAI_TINH,  cast('DONGIA' as varchar(30)) ma_kpi, MANV_DONGIA ma_nv, nv.ma_to, nv.ma_pb
            , PHONG_cs, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, null MANV_CN, MANV_THPHUC
            , SOTHANG_DC, HT_TRA_ONLINE, Khuvuc, dongia, DTHU, NGAY_TT, heso_giao, khdn, nhomtb_id, khachhang_id, heso, tien_khop
            from (
            
            select a.khachhang_id, a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
            , a.manv_giao, a.manv_thphuc
            /*       , case
                    
            */
            -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
            , case when a.mapb_thphuc = 'VNP0703000' then a.manv_thphuc
                      --  when a.ma_pb is null then a.manv_giao
                        when a.ma_pb is not null and to_number(to_char(max(a.ngay_tt), 'yyyymm')) <= 202309 then a.manv_giao
                     --   manv_thphuc = 'MEDIAPAY'
                        else a.manv_thphuc
                end manv_dongia
            
            , max(so_thangdc) sothang_dc
            
            --      , sum(decode(bo_dau(ten_ht_tra), 'Tien mat', 0, 1)) ht_tra_online
            
            , sum(case when a.ht_tra_id in (1) then 0
                            when a.ht_tra_id in (2,7,4) then 1 end) ht_tra_online
            -- , (select decode(quan_id, 16, 'CanGio') from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) Khuvuc
            , dc.ten_quan khuvuc
            , case 
                   
                    ----thanh toan nhan cong + ap dung khu vuc Can Gio
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in (2,4) then 1 end) = 0 and max(so_thangdc) < 3  
                                           -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                              and dc.ten_quan is not null
                                                        then 8700*0.5
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6  
                                            --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                            and dc.ten_quan is not null
                                                        then 8700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12  
                                            --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                            and dc.ten_quan is not null
                                                        then 11700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 12  
                                            --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                            and dc.ten_quan is not null   
                                                        then 16700
                   ----thanh toan nhan cong
                   when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) < 3 then 5700 * 0.5
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 5700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 8700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in (2,4) then 1 end) = 0 and max(so_thangdc) >= 12 then 13700
                    ----thanh toan online---
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) < 3 then 15700  * 0.5
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 15700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 18700
                    when sum(case when a.ht_tra_id in (1, 7) then 0
                                                        when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 12 then 23700
            end 
                    ------tb thanh toan trong 30 ngay thang T het han
                    * case when max(ngay_tt) between add_months(trunc(sysdate, 'month'), -1) and trunc(sysdate, 'month') -1 then 1.05
                    ------tb thanh toan truoc thang T het han
                                    when max(ngay_tt) < add_months(trunc(sysdate, 'month'), -1) then 1.1
                            else 1
                        end
            dongia
            
            , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                            when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0)
                                                                            , 0)
                                                            -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                        , 0)            
                            ----Dich vu Mesh he so 0.2
                            when a.loaitb_id = 210 then 0.2  
                            ---MyTV he so 0.25 
                            when a.loaitb_id in (61, 171, 18) then 0.25  
                        else 0 
            end Heso
            ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.khdn, a.nhomtb_id, a.heso_giao
            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop ,  row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
            from ttkd_bsc.ct_bsc_tratruoc_moi_30day a 
                    left join dc on a.thuebao_id = dc.thuebao_id 
                    left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
            where rkm_id is not null and a.thang = 202402 --            change
            group by a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_thphuc, a.manv_cs, a.ma_to, a.ma_pb, a.mapb_thphuc
                , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.nhomtb_id, a.loaitb_id, a.khachhang_id, a.goi_old_id, dc.ten_quan
                , a.heso_giao, hs.khachhang_id
            ) a
            , ttkd_bsc.nhanvien_202402 nv               -- change
            where a.manv_dongia = nv.ma_nv (+) and dthu > 0 and a.rnk = 1 --and tien_khop > 0
            and a.manv_dongia is not null
            and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
            --       and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
            --                   and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
            and nvl(nv.ma_vtcv, 'khongco')  not in ('VNP-HNHCM_BHKV_18',
                                                        'VNP-HNHCM_BHKV_2.2',
                                                        'VNP-HNHCM_BHKV_14',
                                                        'VNP-HNHCM_BHKV_2',
                                                        'VNP-HNHCM_BHKV_12',
                                                        'VNP-HNHCM_BHKV_18.1')
            ;
select* from ct_dongia_tratruoc where thang = 202401 and loai_tinh = 'DONGIATRA30D'
commit;
select* from ttkd_bsc.dongia_vttp where thang = 202310 and ma_nv = 'khongco'
------------- TINH TIEN CHI TRA DON GIA CHO NHAN VIEN THUYET PHUC
select* from  ttkd_bsc.tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA30D'
delete  from  ttkd_bsc.tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA30D'
commit;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI
                                                                                                        , DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
select* from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRU30D'
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI
                                                                                                        , DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)       
--insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)                                                                                                             
select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                , count(thuebao_id) tong
                  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                  , sum(dthu) DTHU_thanhcong
                  , round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
                  , round(sum(dongia*heso*tien_khop), 0) dongia
-- select * 
--from ttkd_bsc.ct_dongia_tratruoc  a
from ttkd_bsc.ct_dongia_tratruoc  a
where ma_kpi = 'DONGIA' 
        and loai_tinh = 'DONGIATRA30D' and thang = 202402
group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
commit;
select* from tl_giahan_tratruoc where loai_tinh = 'DONGIATRA30D' and thang = 202312