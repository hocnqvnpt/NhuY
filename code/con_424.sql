create table tien_thu_v2 as;
WITH 
--hddc as (select distinct hdtb_id,h.rkm_id b, g.rkm_id a, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc
--                     , g.thang_thoaitra,g.ngay_thoai
--         from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
--) --12379099 --25722563
--, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc,thuebao_id
--           from css.v_khuyenmai_dbtb@dataguard 
--           where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--)
--,
ct as (select distinct MA_CT_ONEB, ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
, kq_ghtt as (select 202401 thang ,hdtb.thuebao_id,hdtb.ma_tb, hdkh.khachhang_id, hdtb.kieuld_id,hdtb.loaitb_id--, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
--                    , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
--                    , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt
                    , a.phieutt_id, a.trangthai, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                    , a.ngay_hd, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                    , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id
                    , kt.kenhthu, ht.ht_Tra, km.ten_kmtt, lh.loaihinh_Tb, a.ngay_Tt, hdtb.ngay_ht ngay_kichhoat, hdkh.ma_gd, km.khoanmuctt_id, dv.ten_dvvt, ma_kmtt,nv.TEN_dvql
             from css.v_phieutt_hd@dataguard a
                            join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and b.tien > 0
--                            left join hddc on b.hdtb_id = hddc.hdtb_id
                            join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id = 6 --and hdtb.kieuld_id  in (551, 550, 24, 13080) 
                            join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
                            join css.kenhthu@dataguard kt on a.kenhthu_id = kt.kenhthu_id
                            join css.hinhthuc_Tra@dataguard ht on a.ht_Tra_id = ht.ht_Tra_id
                            join css.v_khoanmuc_tt@dataguard km on b.khoanmuctt_id = km.khoanmuctt_id
                            join css.loaihinh_Tb@dataguard lh on hdtb.loaitb_id = lh.loaitb_id
                            left join css.dichvu_vt@dataguard dv on hdtb.dichvuvt_id = dv.dichvuvt_id
--                            left join ct on a.so_ct = ct.ma_ct_oneb
                            left join admin.v_donvi@dataguard nv on hdkh.donvi_id = nv.donvi_id
--                            left join ttkd_Bsc.nhanvien tp on nv.ma_nv =tp.ma_nv and tp.thang = 202407 and donvi = 'TTKD'
             where a.trangthai = 1 
                        and to_number(to_char(a.ngay_tt,'yyyymm')) = 202401
--                        and to_number(to_char(hdtb.ngay_ht, 'yyyymm')) = 202406
--                                                      `   and to_number(to_char(nvl(a.ngay_tt, hdtb.ngay_ht), 'yyyymm')) = 202401
)                              


select * from kq_Ghtt; 