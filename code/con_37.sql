WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where h.thang_bd > 202310
                                            )
                       , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css.v_khuyenmai_dbtb@dataguard 
                                                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                                                    and thang_bddc > 202310
                                            )
                        , kq_ghtt as (select 
                        hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                                                            , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
                                                                            , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt
                                                                            , a.phieutt_id, a.trangthai
                                                                            , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
--                                                                            , (select b.kenhthu from css_hcm.kenhthu b where b.kenhthu_id=a.kenhthu_id) kenhthu   
--                                                                            , (select b.ten_nh from css_hcm.nganhang b where b.nganhang_id=a.nganhang_id) ten_nganhang
--                                                                            , (select b.ht_tra from css_hcm.hinhthuc_tra b where b.ht_tra_id=a.ht_tra_id) ten_ht_tra
                                                                           , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                                                             from css.v_phieutt_hd@dataguard a
                                                                                    join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11-- and b.tien < 0
                                                                                    left join hddc on b.hdtb_id = hddc.hdtb_id
                                                                                    join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id = 6 --and hdtb.kieuld_id in (551, 550, 24, 13080) 
                                                                                    join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                                                                    left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                                                                    left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
--                                                                                    left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
--                                                                                    left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                                                             where a.kenhthu_id not in (6) and a.trangthai = 1                                                                        
                                                                            and to_number(to_char(nvl(a.ngay_tt, hdtb.ngay_ht), 'yyyymm')) between 202401 and 202401
                                                        ) 
             select a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt
                            , sum(case when tien_thanhtoan >0 then tien_thanhtoan end) tien_thu
                            , sum(case when tien_thanhtoan <0 then tien_thanhtoan end) tien_chi
             from kq_ghtt a 
                left join css.v_db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id
                left join css.v_db_thanhtoan@dataguard c on b.thanhtoan_id = c.thanhtoan_id
             group by a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt;
