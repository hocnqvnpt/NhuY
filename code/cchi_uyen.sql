selecT* from css_hcm.phieutt_hd;
select* from admin_hcm.nguoidung;
with ct as (select distinct MA_CT_ONEB, ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
, hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id,h.thang_bd  ngay_bddc,h.thang_kt ngay_ktdc , nvl (h.chitietkm_id, g.chitietkm_id) chitietkm_id
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
           ---change n-1
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
, dadv as (
    select g.goi_id, g.thuebao_id, db.ten_goi, g.nhomtb_id, row_Number() over (partition by g.thuebao_id order by nhomtb_id desc) rnk
    from css_hcm.bd_goi_Dadv g
        join css_hcm.goi_dadv db on g.goi_id = db.goi_id
    where g.trangthai = 1
)
select p.donvi_id, p.ht_Tra_id, p.trangthai,  dv3.ten_dvql ten_dv, dv5.ten_dv phong_Bh, nd.ma_nd nguoi_tt, hdkh.ten_kh, hdkh.diachi_kh, hdkh.mst,p.ngay_tt,p.seri, p.soseri hoadon,
p.ngay_hd, null sophieu,  nvl(ct.ma_ct,p.so_ct) so_ct,hdkh.ma_Gd, dv.ten_dvvt, lh.loaihinh_Tb, 'CK' httt, km.ten_kmtt, hdkh.loaihd_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb,
(b.tien + b.vat )tien_thanhtoan, b.tien, b.vat, kt.kenhthu,tt.ma_tt, hdkh.ghichu, p.ghichu ghichu_tt, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id, ten_ctkm, dadv.ten_goi,dadv.nhomtb_id
, tthd.trangthai_hd, nv1.ten_nv nv_raphieu, rp.ten_dv donvi_raphieu
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
 from  css_hcm.ct_phieutt b --on a.hdtb_id = b.hdtb_id and  b.tien > 0
	    join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id
	   join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id  --and hdtb.kieuld_id in (551, 550, 24, 13080) 
	   join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id  
		left join css_hcm.kieu_ld kld on hdtb.kieuld_id = kld.kieuld_id
		left join css_hcm.loaihinh_tb lh on hdtb.loaitb_id = lh.loaitb_id
	   left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
	   left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
	   left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
	   left join admin_hcm.nhanvien_onebss nv1 on hdkh.nhanvien_id = nv1.nhanvien_id
	   left join admin_hcm.donvi dv1 on hdkh.donvi_id = dv1.donvi_id
	   left join admin_hcm.nhanvien_onebss nv2 on hdkh.ctv_id = nv2.nhanvien_id
	   left join admin_hcm.nhanvien_onebss nv3 on p.thungan_tt_id = nv3.nhanvien_id
	   left join admin_hcm.donvi dv3 on p.DONVI_TT_ID = dv3.donvi_id
       left join admin_hcm.nguoidung nd on p.thungan_tt_id = nd.nhanvien_id
--	   
	   left join admin_hcm.nhanvien_onebss nv4 on p.thungan_hd_id = nv4.nhanvien_id
	   left join admin_hcm.donvi dv4 on p.donvi_hd_id = dv4.donvi_id
	   left join ct on p.ma_gd = ct.ma_gd
       left join admin_hcm.donvi dv5 on dv3.donvi_cha_id = dv5.donvi_id
       left join admin_hcm.donvi dv6 on dv4.donvi_cha_id = dv6.donvi_id
       
       left join css_hcm.dichvu_vt dv on hdtb.dichvuvt_id = dv.dichvuvt_id
       left join css_hcm.khoanmuc_tt km on b.khoanmuctt_id = km.khoanmuctt_id 
       left join css_hcm.db_Thuebao db on hdtb.thuebao_id = db.thuebao_id
       left join css_hcm.db_thanhtoan tt on db.thanhtoan_id = tt.thanhtoan_id
       left join css_hcm.trangthai_Hd tthd on hdtb.tthd_id = tthd.tthd_id
       left join hddc on b.hdtb_id = hddc.hdtb_id
       left join kmtb on b.hdtb_id = kmtb.hdtb_id
       left join css_hcm.ct_khuyenmai ctkm on nvl(hddc.chitietkm_id,kmtb.chitietkm_id) = ctkm.chitietkm_id
        left join admin_hcm.donvi rp on dv1.donvi_cha_id = rp.donvi_id
       left join dadv on db.thuebao_id = dadv.thuebao_id and rnk = 1 
        
    where   to_number(to_char(p.ngay_tt,'yyyymm'))= 202406 and p.trangthai = 1 and p.ht_Tra_id = 2;
								and b.tien > 0 and hdtb.tthd_id = 6 -- and ma_Tb = 'hcmthiha-f26'
	;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb;
select* from css_hcm.trangthai_hd;

select a.ma_Ct, a.ngay_ct ngay_nh, b.ma_gd, nv.ten_nv nv_thanhtoan, dv5.ten_dv phong_tt, dv2.ten_dv donvi_raphieu, nv.ten_Nv nv_raphieu
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
    join ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb b on a.chungtu_id = b.chungtu_id
    join css_hcm.phieutt_hd c on b.ma_Gd = c.ma_gd
    left join admin_hcm.nhanvien nv on c.thungan_tt_id = nv.nhanvien_id
    left join admin_hcm.donvi dv3 on c.DONVI_TT_ID = dv3.donvi_id
    left join admin_hcm.donvi dv5 on dv3.donvi_cha_id = dv5.donvi_id
    left join css_hcm.hd_khachhang hd on c.hdkh_id = hd.hdkh_id
    left join admin_hcm.donvi dv1 on hd.donvi_id =dv1.donvi_id
    left join admin_hcm.donvi dv2 on dv1.donvi_cha_id = dv2.donvi_id
    left join admin_hcm.nhanvien nv on hd.nhanvien_id = nv.nhanvien_id
where to_number(to_char(ngay_ct,'yyyymm')) = 202406
