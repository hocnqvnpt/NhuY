select* from ttkd_bsc.ct_dongia_tratruoc ds where thang = 202409 and loai_tinh = 'DONGIATRA_OB' and ma_pb = 'VNP0701100' and  ma_to ='VNP0701104' and ipcc = 1;  AND GHICHU LIKE '%BHKV%' and not exists (
select 1
--                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id--, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
----                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
----                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
--                , p.phieutt_id, p.trangthai
--                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
--               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
--              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
--               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 308 lan
                     from  css_hcm.hd_khachhang hdkh
													join css_hcm.hd_thuebao  hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 7
													join ttkd_Bsc.ct_dongia_tratruoc tt on tt.thuebao_id = hdtb.thuebao_id  and tt.thang =202409 and tt.loai_tinh = 'DONGIATRA_OB' AND tt.GHICHU LIKE '%BHKV%' AND IPCC = 0-- and gh.thang >= 202403
													join css_hcm.ct_tienhd  a on hdtb.hdtb_id = a.hdtb_id
--													   join css_hcm.ct_phieutt  b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0
--													   join css_hcm.phieutt_hd  p on b.phieutt_id = p.phieutt_id --and p.kenhthu_id not in (6) 
												
--													   left join css_hcm.kenhthu  kt on kt.kenhthu_id = p.kenhthu_id
--													   left join css_hcm.nganhang  nh on nh.nganhang_id = p.nganhang_id
--													   left join css_hcm.hinhthuc_tra  ht on ht.ht_tra_id = p.ht_tra_id
--													   left join ct on p.phieutt_id = ct.phieu_id
				    
                     where  a.khoanmuctt_id = 11 and  tt.thuebao_id = ds.thuebao_id and
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224,136) and a.tien > 0 
                                      and ( to_number(to_char(hdkh.ngay_yc, 'yyyymmdd')) between 20240901 and 20240930) ) --    and p.ma_Gd = 'HCM-TT/02814791'            
  and not exists (
                select A.hdtb_id,A.thuebao_id,A.ma_gd,A.ma_tt,A.ghichu,A.ngay_insert,A.user_insert--,tt_huy,loai_huy
                 from ttkdhcm_ktnv.ghtt_huy_hdtb_688 a
                    left join CSS_HCM.HD_KHACHHANG B ON A.MA_GD = B.MA_GD
                    LEFT JOIN ADMIN_HCM.DONVI C ON B.DONVI_ID = C.DONVI_ID
                 where loai_huy=2  AND (c.donvi_id = 2941 or C.DONVI_CHA_ID = 2941)
                 and tt_huy=1 and thuebao_id in (select THUEBAO_ID from ttkd_bsc.ct_dongia_tratruoc
                 where thang =202409 and loai_tinh = 'DONGIATRA_OB' and ma_pb = 'VNP0701100' and  ipcc = 0 )
             and thuebao_id = ds.thuebao_id);
             select *  from ttkd_bsc.ct_dongia_tratruoc
             where thang =202409 and loai_tinh = 'DONGIATRA_OB' and ma_tb in ('long-2711','luongaihoa2408');
             select* from v_Thongtinkm_all where ma_Tb= 'long-2711';
             select* from ttkd_Bsc.nhanvien where thang = 202409 and ma_pb ='VNP0701100';
             select* from giao_bhkv a join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_nv where thang = 202409 and ma_pb ='VNP0701100' and  ma_to ='VNP0701104' ;
        select ngay_Tt from css.v_phieutt_hd@dataguard where ma_Gd = 'HCM-TT/02999100';
        select* from ttkd_Bsc.BANGLUONG_KPI where thang = 202409 and ma_KPI= 'HCM_SL_COMBO_006' ;
     select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202409   and ma_Nv in ( 'CTV082532', 'CTV085701', 'VNP027577');