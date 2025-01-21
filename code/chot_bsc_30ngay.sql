create table tmp3_30ngay as select* from hocnq_ttkd.tmp3_30ngay where 1 =2
delete from tmp3_30ngay
select* from tmp3_30ngay where rkm_id is null
commit;
insert /*+ ENABLE_PARALLEL_DML  parallel */ into tmp3_30ngay

WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                )
           , gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt 
                                from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0  and thang_kt = 202311 )
           , kmtb as (select * from css_hcm.khuyenmai_dbtb 
                                        where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                )
           , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                            join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                            group by bb.phieu_id)
                            
            , kq_ghtt as (select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, 05 lan
                                    --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                                                , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                                                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                                                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                                                , a.phieutt_id, a.trangthai
                                                                , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                                                , (select b.kenhthu from css_hcm.kenhthu b where b.kenhthu_id=a.kenhthu_id) kenhthu   
                                                                , (select b.ten_nh from css_hcm.nganhang b where b.nganhang_id=a.nganhang_id) ten_nganhang
                                                                , (select b.ht_tra from css_hcm.hinhthuc_tra b where b.ht_tra_id=a.ht_tra_id) ten_ht_tra
                                                         --       , kmtb.rkm_id a1, hddc.rkm_id a2
                                                               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                                                 from css_hcm.phieutt_hd a
                                                                        join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                                                        left join hddc on b.hdtb_id = hddc.hdtb_id
                                                                        join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                                                        join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                                                        join gh on hdtb.thuebao_id = gh.thuebao_id
                                                                        left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                                                        left join ct on a.phieutt_id = ct.phieu_id
                                                                        left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                                                        left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                                                        left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                                                 where a.kenhthu_id not in (6) and a.trangthai = 1                                                                        
                                                                and ((a.ht_tra_id not in (2, 5,4,9) and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20231001 and 20231202)                     ----change--2 thang- ngay 02
                                                                                    or (a.ht_tra_id in (2, 4,5, 9) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20231001 and 20231202))                     ----change--2 thang- ngay 02
                                                             --   and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20230427 and 20230701                     ----change--2 thang-
                                                          --      and gh.ma_tb in ('ketnoi_thoitrang')   ----loai taykhi can---
                                                            --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                                             --and hddc.rkm_id is not null
                                            )
 select * from kq_ghtt a
where ngay_bd_moi is not null 
                            and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where rkm_id = a.rkm_id and thang >=202309)
                            and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >=202309)
                            and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                   --     and not exists (select rkm_id from tmp3_30ngay where rkm_id = a.rkm_id) 
                       commit;
                       
 update tb
 set thang = (select a, b from tb2 where tb.id = tb2.id)
-- BANG CHOT
select distinct thang from ct_bsc_tratruoc_moi_30day 
create table ct_bsc_tratruoc_moi_30days as select* from ttkd_bsc.ct_bsc_Tratruoc_moi_30day where 1 = 2
  insert into ct_bsc_tratruoc_moi_30day
   --insert into hocnq_ttkd.ct_bsc_tratruoc_moi_30day
            (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
                , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
                , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
                , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
                )
            with t0 as (select c0.thang_kt, c0.thuebao_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                                                            , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                                                                        --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                                                            , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                                                            , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
                                                           from tmp3_30ngay c0
                                                                        left join admin_hcm.nhanvien nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                                                        left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                                                        left join admin_hcm.nhanvien nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                                                                        left join admin_hcm.nhanvien nv2 on nv2.nhanvien_id = c0.nvthu_id
                                                                        left join admin_hcm.nhanvien nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                                )
                         , km0 as (  
                                        ----------------TT Thang tang tren 1 dong-------------
                                                                        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                                                                        , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                                                                        , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                                                                        from v_thongtinkm_all km 
                                                                        where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0 
                                                                                        --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                                                                        and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                                                       union all
                                            ----------------TT giam cuoc or thang tang tren 2 dong-------------
                                                                        select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                                                                        , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                                                                        , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                                                                                        select*
                                                                        from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                                                                                                        from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                                                                                                                    ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                                                                        where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0 and km.rkm_id = 6217981
                                                                                       -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                                                                       and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                             )select* from km0 where thuebao_id = 8061869
                    , ds as (select min(ghtt_id) gh_id, donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_nv_th, donvi_oa, nhanvien_oa, nvl(duan_id, 0) duan_id, ma_nv, tbh_ql_id, pbh_ql_id
                                        , ma_tb, heso, min(to_char(ngay_kt_mg, 'yyyymmdd')) thang_ktdc_cu, max(SO_THANGDC) thangdc, sum(cuoc_dc) cuoc_dc, khachhang_id, thuebao_id, loaitb_id, goi_id, thang_kt
                            from ttkdhcm_ktnv.ghtt_giao_688
                            where tratruoc = 1 and km = 1 and loaibo = 0 
                            group by donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_tb, ma_nv_th, donvi_oa, nhanvien_oa, heso, khachhang_id, thuebao_id, loaitb_id, goi_id, duan_id, ma_nv, tbh_ql_id, pbh_ql_id, thang_kt
                            )
                          
                    , c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                        , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                        , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                                from t0
                                            join km0 on t0.rkm_id = km0.rkm_id
                        )  
            select a.thang_kt, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
                        , a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
                        , a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
                         , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
                         , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
                         , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
                         , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                         
                         , nvtp.ma_pb MAPB_THPHUC
                        , c.manv_gt, c.manv_thungan
                         , lkh.khdn, a.heso
                        , a.THANG_KTDC_CU, a.cuoc_dc TIEN_DC_CU
                        , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.ngay_nganhang
                                            , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                        , tt.trangthai_tb, a.thuebao_id, a.loaitb_id, donvi_oa pbh_oa_id, nhanvien_oa manv_oa
                         , (select  nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 and a.thuebao_id = thuebao_id 
                            and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                            ) nhomtb_id
--                        , 2816536 nhomtb_id
                        , a.khachhang_id, a.goi_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                        
                        , case when c.rkm_id is null then null
                                        when c.ht_tra_id in (1, 7) then 1
                                        when c.ht_tra_id in (2, 9, 4, 5) then 0 else null end tien_khop
                        , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                        
            from ds a
                                        join css_hcm.db_thuebao dbtb
                                            on a.thuebao_id = dbtb.thuebao_id
                                        join css_hcm.db_khachhang dbkh
                                                on dbtb.khachhang_id = dbkh.khachhang_id
                                        join css_hcm.loai_kh lkh
                                                on dbkh.loaikh_id = lkh.loaikh_id
                                        join css_hcm.trangthai_tb tt
                                            on dbtb.trangthaitb_id = tt.trangthaitb_id
                                        left join ttkd_bsc.dm_phongban pb
                                            on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                          
                                        left join  ttkd_bct.phongbanhang pb_giao
                                            on a.donvi_giao = pb_giao.pbh_id
                                        left join  ttkd_bct.phongbanhang pb_th
                                            on a.pbh_id_th = pb_th.pbh_id
                                        left join c 
                                            on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                        left join ttkd_bsc.nhanvien_202311 nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv
                                
            where  a.thang_kt = 202311--and rkm_id != 3369696 --a.ma_tb  in ('')
                   --      and a.gh_id is null
                            and a.khachhang_id  <> 9798610  -- and a.thuebao_id  =     9091836
commit;
select* from ct_bsc_tratruoc_moi_30day where thang = 202311
select thuebao_id, nhomtb_id
from css_Hcm.bd_Goi_dadv
where trangthai = 1 
    and dichvuvt_id = 4 and thuebao_id = 9091836
    and goi_id not between 1715 
    and 1726 and goi_id not in (15414, 16221) 
    and goi_id < 100000 and thuebao_id in (select thuebao_id from tmp3_30ngay)
    group by thuebao_id  having count (Thuebao_id) > 1
    select khachhang_id from css_hcm.db_thuebao where ma_tb=  'hcm_minhnam_11'
    select ma_kh from css_hcm.db_khachhang where khachhang_id = 9796535