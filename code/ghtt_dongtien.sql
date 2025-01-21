WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc, nvl(h.nhom_datcoc_id,g.nhom_datcoc_id) nhom_datcoc_id
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                        --    where h.thang_bd > 202310
                                            )
, kmtb as (select a.hdtb_id, a.rkm_id, a.ngay_bddc, a.ngay_ktdc, b.nhom_datcoc_id from css_hcm.khuyenmai_dbtb a
    left join css_hcm.ct_khuyenmai b on a.chitietkm_id = b.chitietkm_id
where a.datcoc_csd > 0 and least(a.thang_ktdc, nvl(a.thang_kt_dc, 999999), nvl(a.thang_huy, 999999)) >= a.thang_bddc
   --     and thang_bddc > 202310
),

ptt as (
    select   a.phieutt_id, a.trangthai, a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri,sum(b.tien)  +sum(b.vat) tien_thanhtoan
               , a.kenhthu_id, a.nganhang_id,a.ht_tra_id, b.hdtb_id, a.thungan_tt_id
    from css_hcm.phieutt_hd a
        join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11
    group by  a.phieutt_id, a.trangthai, a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri
               , a.kenhthu_id, a.nganhang_id,a.ht_tra_id, b.hdtb_id,a.thungan_tt_id
        
)
, kq_ghtt as (select hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt,
                nvl(kmtb.nhom_datcoc_id, hddc.nhom_datcoc_id) nhom_datcoc_id
                , a.phieutt_id, a.trangthai
                , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, a. tien_thanhtoan
                , (select b.kenhthu from css_hcm.kenhthu b where b.kenhthu_id=a.kenhthu_id) kenhthu   
                , (select b.ten_nh from css_hcm.nganhang b where b.nganhang_id=a.nganhang_id) ten_nganhang
                , (select b.ht_tra from css_hcm.hinhthuc_tra b where b.ht_tra_id=a.ht_tra_id) ten_ht_tra
               , a.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
 from ptt a-- and b.tien < 0
                        left join hddc on a.hdtb_id = hddc.hdtb_id
                        join css_hcm.hd_thuebao hdtb on a.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id = 6 --and hdtb.kieuld_id in (551, 550, 24, 13080) 
                        join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                        left join kmtb on a.hdtb_id = kmtb.hdtb_id
                        left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                        left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                        left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
 where a.kenhthu_id not in (6) and a.trangthai = 1                                                                        
                and to_number(to_char(nvl(a.ngay_tt, hdtb.ngay_ht), 'yyyymm')) between 202401 and 202401
)
             select * from kq_ghtt where ma_Tb = 'lhoa6650586';

select* from css_hcm.phieutt_hd where phieutt_id = 7979546;

select thang_ktdc,thang_kt_dc,thang_huy , thang_bddc from css_hcm.khuyenmai_dbtb where hdtb_id = 23224317