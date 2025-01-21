select hdkh_id, ngay_kh, ma_tb from css_hcm.hd_Thuebao where THUEBAO_ID = 9584647;
select donvi_id, nhanvien_id, ma_gd from css_hcm.hd_khachhang where  ma_gd = 'HCM-LD/00909312';
select* from admin_hcm.nhanvien where nhanvien_id = 8685
select * from css_hcm.ct_phieutt where phieutt_id = 7982241
select ma_gd,manv_thphuc, phieutt_id,thuebao_id , ma_tb from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202312 and tien_khop = 1-- ma_gd = 'HCM-TT/02301380';
select* from css_hcm.dichvu_Vt-- where phieutt_id = 7546480
select * from ttkd_bsc.ct_Bsc_tratruoc_moi where rownum = 1;
select thanhtoan_id, khachhang_id from css_hcm.db_Thuebao where thuebao_id = 9298182;
select mst from css_hcm.db_Thanhtoan where thanhtoan_id = 9033809;
select mst from css_hcm.db_khachhang where khachhang_id = 9726101;
select ten_dv from admin_hcm.donvi where donvi_id = 892082;
select* from ttkd_bsc.ct_bsc_giahan_cntt where ma_Tb = 'hcm_tmvn_00002669' and thang = 202312 and loaitb_id in (147,148)
select * from css_hcm.hd_thuebao where hdtb_id = 23231090;
select ma_Tb, thuebao_id from css_hcm.db_Thuebao where dichvuvt_id in (7,8,9) 
select* from css_hcm.db_mgwan where thuebao_id= 9584647
select* from css_hcm.tocdo_kenh where tocdo_id  =125
select* from hocnq_ttkd.ds_giahan_tratruoc2 where rkm_id = 5747847
  select rkm_id from ds_giahan_tratruoc group by rkm_id having count(rkm_id) > 1
  select thuebao_dc_id from css_hcm.db_datcoc where rkm_id = 5747847;
  select hdtb_id from css_hcm.hdtb_datcoc where thuebao_dc_id = 4401738;

create table ttin as select* from (
with dc as (select distinct g.hdtb_id, hd.thuebao_id , nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id 
                                join css_Hcm.hd_thuebao hd on hd.thuebao_id = h.thuebao_id and g.hdtb_id = hd.hdtb_id
            union all
            select hdtb_id, thuebao_id, rkm_id, ngay_Bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb 
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
            )   -- select  thuebao_id ,hdtb_id from dc where rkm_id in  (5747847,6490666,5942183,5836941,6301975,5803981,5201083)            
        , ttin as (
            select dc.rkm_id, c.thuebao_id, a.kenhthu_id, a.ht_tra_id--, ptt.ht_tra_id, ptt.kenhthu_id,ptt.phieutt_id, hd.hdkh_id,hd.hdtb_id--, ptt.hdtb_id
            from css_hcm.phieutt_hd a
                join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.tien> 0 and b.khoanmuctt_id = 11
                join dc on b.hdtb_id = dc.hdtb_id
                join ds_giahan_tratruoc2 c on dc.rkm_id = c.rkm_id and dc.thuebao_id = c.thuebao_id
                join css_hcm.hd_thuebao hd on b.hdtb_id = hd.hdtb_id and dc.thuebao_id = hd.thuebao_id
            where a.trangthai = 1 and a.kenhthu_id <> 6
        )select* from ttin)        
