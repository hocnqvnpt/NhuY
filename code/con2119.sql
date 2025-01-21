drop table ktkh
;
select* from  ktkh;
create table ktkh as 
with ct as (select distinct MA_CT_ONEB, ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
, hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id,h.thang_bd  ngay_bddc,h.thang_kt ngay_ktdc , nvl (h.chitietkm_id, g.chitietkm_id) chitietkm_id
    from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id)
           ---change n-1
, kmtb as (select * from css.v_khuyenmai_dbtb@dataguard
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
, dadv as (
    select g.goi_id, g.thuebao_id, db.ten_goi, g.nhomtb_id, row_Number() over (partition by g.thuebao_id order by nhomtb_id desc) rnk
    from css.v_bd_goi_Dadv@dataguard g
        join css.v_goi_dadv@dataguard db on g.goi_id = db.goi_id
    where g.trangthai = 1
)
select to_char(p.ngay_tt,'yyyymm') thang,
p.donvi_id, p.ht_Tra_id, p.trangthai,  dv3.ten_dvql ten_dv, dv5.ten_dv phong_Bh, p.nguoi_cn nguoi_tt, hdkh.ten_kh, hdkh.diachi_kh, hdkh.mst,p.ngay_tt,p.seri, p.soseri hoadon,
p.ngay_hd, 1 sophieu,  p.so_ct so_ct,hdkh.ma_Gd, dv.ten_dvvt, lh.loaihinh_Tb, 'CK' httt, km.ten_kmtt, hdkh.loaihd_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb,
(b.tien + b.vat )tien_thanhtoan, b.tien, b.vat, kt.kenhthu,tt.ma_tt, hdkh.ghichu, p.ghichu ghichu_tt, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id, ten_ctkm, dadv.ten_goi,dadv.nhomtb_id
, tthd.trangthai_hd, nv1.ten_nv nv_raphieu, rp.ten_dv donvi_raphieu,  p.thungan_tt_id, nd.ma_Nv manv_Tt, nd.ten_Nv tennv_tt
--                                            
--    hdkh.khachhang_id, hdtb.thuebao_id, cast(null as varchar(20)) ma_tt, hdtb.ma_tb, kld.ten_kieuld, lh.loaihinh_tb
--                , hdkh.ten_kh, hdkh.diachi_kh
--			 , p.phieutt_id
--                , p.ma_gd, hdkh.ngay_yc, p.ngay_tt, b.tien tien_thanhtoan,b.vat vat_thanhtoan
--               , nv1.ma_nv manv_xly, dv1.ten_dvql tendv_xly, nv1.ma_nv manv_tuvan
--          --    , p.thungan_tt_id, p.thungan_hd_id, p.DONVI_TT_ID, p.DONVI_HD_ID
--		    , nv3.ma_nv manv_tt, dv5.ten_dv phongban_tt
--		     , nv4.ma_nv manv_hd, dv4.ten_dvql tendv_hd, dv6.ten_dv phongban_hd
--              , ht.ht_tra ten_ht_tra,  nh.ten_nh ten_nganhang,  p.ngay_nh, ct.ma_ct, p.ghichu, b.khoanmuctt_id
 from  css.v_ct_phieutt@dataguard b --on a.hdtb_id = b.hdtb_id and  b.tien > 0
	    join css.v_phieutt_hd@dataguard p on b.phieutt_id = p.phieutt_id
	   join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id  --and hdtb.kieuld_id in (551, 550, 24, 13080) 
	   join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id  
		left join css.kieu_ld@dataguard kld on hdtb.kieuld_id = kld.kieuld_id
		left join css.loaihinh_tb@dataguard lh on hdtb.loaitb_id = lh.loaitb_id
	   left join css.kenhthu@dataguard kt on kt.kenhthu_id = p.kenhthu_id
	   left join css.v_nganhang@dataguard nh on nh.nganhang_id = p.nganhang_id
	   left join css.hinhthuc_tra@dataguard ht on ht.ht_tra_id = p.ht_tra_id
	   left join admin.v_nhanvien@dataguard nv1 on hdkh.nhanvien_id = nv1.nhanvien_id
	   left join admin.v_donvi@dataguard dv1 on hdkh.donvi_id = dv1.donvi_id
	   left join admin.v_nhanvien@dataguard nv2 on hdkh.ctv_id = nv2.nhanvien_id
	   left join admin.v_nhanvien@dataguard nv3 on p.thungan_tt_id = nv3.nhanvien_id
	   left join admin.v_donvi@dataguard dv3 on p.DONVI_TT_ID = dv3.donvi_id
       left join admin.v_nhanvien@dataguard nd on p.thungan_tt_id = nd.nhanvien_id
--	   
	   left join admin.v_nhanvien@dataguard nv4 on p.thungan_hd_id = nv4.nhanvien_id
	   left join admin.v_donvi@dataguard dv4 on p.donvi_hd_id = dv4.donvi_id
--	   left join ct on p.ma_gd = ct.ma_gd
       left join admin.v_donvi@dataguard dv5 on dv3.donvi_cha_id = dv5.donvi_id
       left join admin.v_donvi@dataguard dv6 on dv4.donvi_cha_id = dv6.donvi_id
--       
       left join css.dichvu_vt@dataguard dv on hdtb.dichvuvt_id = dv.dichvuvt_id
       left join css.v_khoanmuc_tt@dataguard km on b.khoanmuctt_id = km.khoanmuctt_id 
       left join css.v_db_Thuebao@dataguard db on hdtb.thuebao_id = db.thuebao_id
       left join css.v_db_thanhtoan@dataguard tt on db.thanhtoan_id = tt.thanhtoan_id
       left join css.trangthai_Hd@dataguard tthd on hdtb.tthd_id = tthd.tthd_id
       left join hddc on b.hdtb_id = hddc.hdtb_id
       left join kmtb on b.hdtb_id = kmtb.hdtb_id
       left join css.v_ct_khuyenmai@dataguard ctkm on nvl(hddc.chitietkm_id,kmtb.chitietkm_id) = ctkm.chitietkm_id
        left join admin.v_donvi@dataguard rp on dv1.donvi_cha_id = rp.donvi_id
       left join dadv on db.thuebao_id = dadv.thuebao_id and rnk = 1 
        
 where   to_number(to_char(p.ngay_tt,'yyyymm')) between 202409 and 202412 and p.trangthai = 1 and p.ht_Tra_id = 2;
    
    
    
    
    with  ct as (select distinct  ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
    select thang, DONVI_ID, HT_TRA_ID, TRANGTHAI, TEN_DV, PHONG_BH, NGUOI_TT, TEN_KH, DIACHI_KH, MST, NGAY_TT, SERI, HOADON, NGAY_HD, SOPHIEU,  a.MA_GD, TEN_DVVT, LOAIHINH_TB, 
    HTTT, TEN_KMTT, LOAIHD_ID, HDTB_ID, THUEBAO_ID, MA_TB, TIEN_THANHTOAN, TIEN, VAT, KENHTHU, MA_TT, GHICHU, GHICHU_TT, RKM_ID, TEN_CTKM, TEN_GOI, NHOMTB_ID, TRANGTHAI_HD, 
    NV_RAPHIEU, DONVI_RAPHIEU, THUNGAN_TT_ID, MANV_TT, TENNV_TT,  nvl(ct.ma_Ct,a.so_Ct) so_Ct
    from ktkh a 
        left join ct on a.ma_gd = ct.ma_gd
;
select trangthai, ht_Tra_id, ngay_Tt from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02952933';
    select * from ttkdhcm_ktnv.DM_MVIEW where error_Get = 1 or error_cn = 1
;

with  ct as (select distinct  ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
    select DONVI_ID, HT_TRA_ID, TRANGTHAI, TEN_DV, PHONG_BH, NGUOI_TT, TEN_KH, DIACHI_KH, MST, NGAY_TT, SERI, HOADON, NGAY_HD, SOPHIEU,  a.MA_GD, TEN_DVVT, LOAIHINH_TB, 
    HTTT, TEN_KMTT, LOAIHD_ID, HDTB_ID, THUEBAO_ID, MA_TB, TIEN_THANHTOAN, TIEN, VAT, KENHTHU, MA_TT, GHICHU, GHICHU_TT, RKM_ID, TEN_CTKM, TEN_GOI, NHOMTB_ID, TRANGTHAI_HD, 
    NV_RAPHIEU, DONVI_RAPHIEU, THUNGAN_TT_ID, MANV_TT, TENNV_TT,  nvl(ct.ma_Ct,a.so_Ct) so_Ct
    from ktkh a 
        left join ct on a.ma_gd = ct.ma_gd