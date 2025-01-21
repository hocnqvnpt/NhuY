-- TINH DON GIA
select *  from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'-- group by thuebao_id having count(thuebao_id) > 1
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRA'
delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRA'
select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRA'
delete from ct_dongia_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
commit;
select* from css_hcm.tocdo_adsl
rollback;
create table backup_ct_dongia_Tratruoc as select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202312
delete from ct_dongia_tratruoc where thang = 202310 and loai_tinh like 'DONGIA%'
select* from backup_ct_dongia_Tratruoc where thang = 202312 and loai_tinh like 'DONGIA%'
insert into ttkd_bsc.ct_dongia_tratruoc
--insert into ct_dongia_tratruoc
--delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202312 and loai_tinh like 'DONGIA%'
with dc as (select /*+ RESULT_CACHE */ dctb.thuebao_id, 'CanGio' ten_quan from css_hcm.diachi_tb dctb, css_hcm.diachi dc 
                                        where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998))
, hs as (select thang, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi xu
                where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id)
                
        select THANG, cast('DONGIATRA' as varchar(30)) LOAI_TINH,  cast('DONGIA' as varchar(30)) ma_kpi, MANV_DONGIA ma_nv, nv.ma_to, nv.ma_pb
                             , PHONG_cs, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, null MA_NV_CN, MANV_THPHUC
                             , SOTHANG_DC, HT_TRA_ONLINE, Khuvuc, dongia, DTHU, NGAY_TT, heso_giao, khdn, nhomtb_id, khachhang_id, heso, tien_khop, nv.ten_pb
        from (select a.khachhang_id, a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                    , a.manv_giao, a.manv_thphuc
                          
                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                , case when a.mapb_thphuc = 'VNP0703000' then a.manv_thphuc
                                          --  when a.ma_pb is null then a.manv_giao
                                            when a.ma_pb is not null and to_number(to_char(max(a.ngay_tt), 'yyyymm')) <= 202309 then a.manv_giao
                                         --   manv_thphuc = 'MEDIAPAY'
                                            else a.manv_thphuc
                                    end manv_dongia
                                    , max(so_thangdc) sothang_dc
                                     , sum(case when a.ht_tra_id in (1) then 0
                                                        when a.ht_tra_id in (2,7,204,4) then 1 end) ht_tra_online
                                    
                               --     , (select decode(quan_id, 3998, 'CanGio') from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998) and a.thuebao_id = dctb.thuebao_id) Khuvuc
                                    , dc.ten_quan khuvuc
                                    , case
                                               -- when nhomtb_id is not null and a.loaitb_id in (61) then 0
                                                ----thanh toan nhan cong + ap dung khu vuc Can Gio
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) < 3  
                                                                        -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                          and dc.ten_quan is not null
                                                                                    then 8700*0.5
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6  
                                                                        -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                          and dc.ten_quan is not null
                                                                          then 8700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12  
                                                                        -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                          and dc.ten_quan is not null
                                                                          then 11700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 12  
                                                                        -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                          and dc.ten_quan is not null
                                                                          then 16700
                                               ----thanh toan nhan cong
                                               when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) < 3 then 5700 * 0.5
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 5700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 8700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) = 0 and max(so_thangdc) >= 12 then 13700
                                                ----thanh toan online---
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) < 3 then 15700  * 0.5
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 15700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 18700
                                                when sum(case when a.ht_tra_id in (1,204, 7) then 0
                                                                                    when a.ht_tra_id in  (2,4) then 1 end) > 0 and max(so_thangdc) >= 12 then 23700
                                    end 
                                                ------tb thanh toan trong 30 ngay trong thang T het han
                                                * case when max(ngay_tt) between add_months(trunc(sysdate, 'month'), -2) and add_months(trunc(sysdate, 'month'), -1) -1 then 1.05
                                                ------tb thanh toan truoc  thang T het han
                                                                when max(ngay_tt) < add_months(trunc(sysdate, 'month'), -2) then 1.1
                                                        else 1
                                                    end
                                    dongia
                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)

                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, 0 KQ_TH_Dai, a.khdn, a.nhomtb_id, a.heso_giao
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from ttkd_bsc.ct_bsc_tratruoc_moi a 
                                            left join dc on a.thuebao_id = dc.thuebao_id 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                        where rkm_id is not null and a.thang = 202404---        CHANGE
                        group by a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_thphuc, a.manv_cs, a.ma_to, a.ma_pb, a.mapb_thphuc
                                            , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.nhomtb_id, a.loaitb_id, a.khachhang_id, a.goi_old_id, dc.ten_quan
                                            , a.heso_giao, hs.khachhang_id
        ) a
, ttkd_bsc.nhanvien_202404 nv       -- CHANGE
where a.manv_dongia = nv.ma_nv(+) and dthu > 0 And rnk = 1
                and a.manv_dongia is not null
                and nv.ma_nv is not null
                and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
--                and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
--                                        and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
--                and nvl(nv.ma_vtcv, 'khongco') not in ('VNP-HNHCM_BHKV_18',
--                                                                                                    'VNP-HNHCM_BHKV_2.2',
--                                                                                                    'VNP-HNHCM_BHKV_14',
--                                                                                                    'VNP-HNHCM_BHKV_2',
--                                                                                                    'VNP-HNHCM_BHKV_12',
--                                                                                                    'VNP-HNHCM_BHKV_18.1')
---loai tru dongia 30ngay voi don gia > 0 thang n-1
and not exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRA30D' and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id)
and nv.ma_pb not in ('VNP0701100','VNP0701200','VNP0701300','VNP0701400','VNP0701500','VNP0701600','VNP0701800','VNP0702100','VNP0702200')
--    and not exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang = to_char(add_months(trunc(sysdate, 'month'), -3), 'yyyymm') and loai_tinh = 'DONGIATRA_TR30D' and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id)
;commit;
select * from ttkd_bsc.ct_dongia_Tratruoc where thang = 202403 and loai_tinh = 'DONGIATRA' and ma_nv = 'HCM003024'
---- TRA LUONG DON GIA
select* from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--delete 
from tl_giahan_tratruoc where thang = 202312 and loai_tinh like 'DONGIA%'
commit;
select rkm_id from v_thongtinkm_All where ma_Tb ='nguyenthithanhbinh';

select d.ngay_tt , a.ma_Tb , ma_gd,trangthai from css_hcm.db_datcoc a left join css_hcm.hdtb_datcoc b on a.thuebao_dc_id = b.thuebao_dc_id
left join css_hcm.hd_thuebao c on b.hdtb_id = c.hdtb_id
left join css_hcm.ct_phieutt e on c.hdtb_id = e.hdtb_id
left join css_hcm.phieutt_hd d on e.phieutt_id = d.phieutt_id
where a.rkm_id  =6759926;

select hdtb_id from
select*  from ttkd_bsc.tl_giahan_tratruoc where thang = 202312 and ma_kpi = 'DONGIA'
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                        , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
--                                                        
--insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)        
select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                    , count(thuebao_id) tong
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan
                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                      , sum(dthu) DTHU_thanhcong
                      , round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
                      , round(sum(dongia*heso *tien_khop),0) dongia
-- select * from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--from TTKD_BSC.ct_dongia_tratruoc  a
from ttkd_bsc.ct_dongia_tratruoc  a
where ma_kpi = 'DONGIA' 
            and loai_tinh in ('DONGIATRA') and thang = 202403
group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
order by 2; 
select sum(tien) from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA' --and ma_kpi = 'HCM_TB_GIAHA_022'
commit;
select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202312 and mapb_thphuc
select sum(tien) from tl_giahan_tratruoc where thang = 202312 and loai_tinh LIKE 'DONGIA%';
select count( distinct thuebao_id ) from ttkd_bsc.ct_bsc_tratruoc_moi_30day where ma_pb is null and thang = 202309
Select * from ct_dongia_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA30D'

select* from ttkd_bsc.ct_Dongia_tratruoc where thang = 202403