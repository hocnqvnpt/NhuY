create table tmp3_30ngay as
--Buoc1--tao danh sach cac hop dong tra truoc
-- insert /*+ ENABLE_PARALLEL_DML  parallel */ into tmp3_30ngay

            select *--/*+ no_parallel */*
            from (
            with
            kmtb as (select * from css_hcm.khuyenmai_dbtb where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc),                                                           

            ct as 
            (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                                                                        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                                        group by bb.phieu_id),
            gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0  and thang_kt = 202311) ---change n-1
            select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, 05 lan
            --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                        , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                        , a.phieutt_id, a.trangthai
                                        , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                        , y.kenhthu kenhthu   
                                        ,  z.ten_nh ten_nganhang
                                        , t.ht_tra ten_ht_tra
                                 --       , kmtb.rkm_id a1, hddc.rkm_id a2
                                       , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                         from css_hcm.phieutt_hd a, css_hcm.ct_phieutt b
                                        , (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                            ) hddc --, css_hcm.hdtb_datcoc hdttdc
                                    , css_hcm.hd_thuebao hdtb, css_hcm.hd_khachhang hdkh
                                    ,  gh     
                                    ,  kmtb
                                    ,  ct 
                                    , css_hcm.kenhthu y, css_hcm.nganhang z, css_hcm.hinhthuc_tra t
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                        and b.hdtb_id = hdtb.hdtb_id and kieuld_id in (551, 550, 24, 13080) and tthd_id <> 7
                                        and hdtb.hdkh_id = hdkh.hdkh_id
                                        and b.hdtb_id = hddc.hdtb_id (+)
                                        and b.hdtb_id = kmtb.hdtb_id (+)
                                        and a.kenhthu_id = y.kenhthu_id (+)
                                        and a.nganhang_id = z.nganhang_id (+)
                                        and a.ht_Tra_id = t.ht_Tra_id (+)
                                    
                                      --  and hddc.thuebao_dc_id = hdttdc.THUEBAO_THOAITRA_ID (+) 
                      
                                      --and (hdttdc.THUEBAO_THOAITRA_ID is null or (hdttdc.THUEBAO_THOAITRA_ID is not null and hdttdc.thang_bd > hdttdc.thang_thoaitra))
                                        and hdtb.thuebao_id = gh.thuebao_id
                                        and a.phieutt_id = ct.phieu_id (+)
                                        and ((a.ht_tra_id <> 2 and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20231001 and 20231202)                     ----change--2 thang- ngay 02
                                                            or (a.ht_tra_id in (2, 4, 9) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20231001 and 20231202))                     ----change--2 thang- ngay 02
                                     --   and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20230427 and 20230701                     ----change--2 thang-
                                  --      and gh.ma_tb in ('ketnoi_thoitrang')   ----loai taykhi can---
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                     --and hddc.rkm_id is not null
                        ) a
            where ngay_bd_moi is not null 
                                        and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where rkm_id = a.rkm_id) 
                                        and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id)
                                        and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                                   --     and not exists (select rkm_id from tmp3_30ngay where rkm_id = a.rkm_id) 
                     