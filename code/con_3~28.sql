with ct as (select distinct MA_CT_ONEB, ma_ct
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
					)
select 
    hdkh.khachhang_id, hdtb.thuebao_id, cast(null as varchar(20)) ma_tt, hdtb.ma_tb, kld.ten_kieuld, lh.loaihinh_tb
                , hdkh.ten_kh, hdkh.diachi_kh
			 , p.phieutt_id
                , p.ma_gd, hdkh.ngay_yc, p.ngay_tt, b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , nv1.ma_nv manv_xly, dv1.ten_dvql tendv_xly, nv1.ma_nv manv_tuvan
          --    , p.thungan_tt_id, p.thungan_hd_id, p.DONVI_TT_ID, p.DONVI_HD_ID
		    , nv3.ma_nv manv_tt, dv3.ten_dvql tendv_tt, dv5.ten_dv phongban_tt
		     , nv4.ma_nv manv_hd, dv4.ten_dvql tendv_hd, dv6.ten_dv phongban_hd
              , ht.ht_tra ten_ht_tra, kt.kenhthu, nh.ten_nh ten_nganhang,  p.ngay_nh, nvl(ct.ma_ct,p.so_ct) so_ct, ct.ma_ct, p.ghichu, b.khoanmuctt_id
 from    css_hcm.ct_phieutt b --on a.hdtb_id = b.hdtb_id and  b.tien > 0
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
--	   
	   left join admin_hcm.nhanvien_onebss nv4 on p.thungan_hd_id = nv4.nhanvien_id
	   left join admin_hcm.donvi dv4 on p.donvi_hd_id = dv4.donvi_id
	   left join ct on p.so_ct = ct.MA_CT_ONEB
       left join admin_hcm.donvi dv5 on dv3.donvi_cha_id = dv5.donvi_id
       left join admin_hcm.donvi dv6 on dv4.donvi_cha_id = dv6.donvi_id
    where   to_number(to_char(p.ngay_tt,'yyyymmdd')) between  20240701 and 20240731 and p.trangthai = 1
								and b.tien > 0 and hdtb.tthd_id = 6 -- and ma_Tb = 'hcmthiha-f26'
                                
;
with a as (Select a.PHIEU_ID, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ngay_cn, a.nguoidung_id, a.chungtu, a.nganhang_id, a.ngaynganhang, c.httt_id, c.hinhthuc, sum(b.tragoc) TRAGOC, sum(b.trathue)trathue
					  From Qltn_hcm.bangphieutra partition for(20240601) A, Qltn_hcm.ct_tra partition for(20240601) B, Qltn_Hcm.Hinhthuc_Tt C
					  WHERE a.PHIEU_ID=B.PHIEU_ID and a.httt_id=c.httt_id(+)
										and c.httt_id in (125, 192, 200, 16, 6, 116, 171, 167, 170, 4) --and to_number(to_char(a.ngay_cn,'yyyymmdd')) = 20240612
--					    and a.ma_tt = 'HCM009935696'
					  group by a.PHIEU_ID, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ngay_cn, a.nguoidung_id, a.chungtu, a.nganhang_id, a.ngaynganhang, c.hinhthuc, c.httt_id
					 -- having sum(b.tragoc) >0
					)
select a.ma_tt, a.ma_tb, 1 c, lh.loaihinh_tb, db.ten_tb, db.diachi_tb, a.PHIEU_ID, 1 d
				, 1 e, a.ngay_cn, 1 f, 1 g, TRAGOC, TRATHUE
				, nv1.ma_nv manv_gach, dv1.ten_dvql tendv_gach
				, a.HINHTHUC, 1 s, nh.ten_nh, a.ngaynganhang, a.chungtu, ct.ma_ct, 1 vv--, httt_id
from a
				left join css_hcm.nganhang nh on a.nganhang_id=nh.nganhang_id
				left join admin_hcm.nguoidung c on (c.trangthai=1 and a.nguoidung_id=c.nguoidung_id)
				left join admin_hcm.nhanvien_onebss nv1 on (c.nhanvien_id=nv1.nhanvien_id)
				left join admin_hcm.donvi dv1 on (nv1.donvi_id=dv1.donvi_id)
				left join css.v_db_thuebao@dataguard db on (a.ma_tb= db.ma_tb and a.dichvuvt_id=db.dichvuvt_id)
				left join css_hcm.loaihinh_tb lh on (db.loaitb_id=lh.loaitb_id)
				left join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb ct on (a.chungtu=to_char(ct.chungtu_id))