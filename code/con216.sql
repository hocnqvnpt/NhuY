select* from v_Thongtinkm_all where rkm_id = 6103313;

select hdtb_id from css_hcm.khuyenmai_dbtb where rkm_id =5800352;
select b.* from css_hcm.db_Datcoc a left join css_hcm.hdtb_datcoc b on a.thuebao_dc_id = b.thuebao_dc_id where a.rkm_id =6103313;


select ht_tra_id,kenhthu_id from css_hcm.phieutt_hd a left join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id where hdtb_id = 22155694 --and a.phieutt_id = 7622099;

delete from DS_GIAHAN_TRATRUOC_AAA where rkm_id = 6103313 and ht_Tra_id = 1 and kenhthu_id = 1;
commit;

select* from css.v_loai_tbi@dataguard where loaitbi_id in (10106, 10130);
select * from css_hcm.hdtb_datcoc where hdtb_id  =24328081;
select ma_gd from css_hcm.hd_khachhang where hdkh_id = 21999529
select* from  css.v_hdtb_cntt@dataguard where hdtb_id = 24328081;

aselect* from  css.v_loai_tbi@dataguard where loaitbi_id in (10106, 10130)

select* from css.v_db_cntt@dataguard where thuebao_id = 8440468;

select* from css.v_khuyenmai_dbtb@dataguard where loaitb_id in (80,140) ;
select * from v_Thongtinkm_all --where loaitb_id = 58 and cuoc_sd > 0;-- where ma_tb = 'giaoduclyon'
select ma_gd, ngay_ht
from css_hcm.hd_thuebao a 
    left join css_hcm.hd_khachhang b 
    on a.hdkh_id = b.hdkh_id
    where a.loaitb_id  in (80,140) and a.kieuld_id in (551, 550, 24, 13080);
select c.ma_gd,  b.ngay_ht,b.kieuld_id, d.tien, d.vat, e.ten_ctkm ,a.datcoc_csd, a.tien_Td
from css_hcm.khuyenmai_dbtb a 
left join css_hcm.hd_Thuebao b on a.hdtb_id = b.hdtb_id 
left join css_hcm.hd_khachhang c
    on b.hdkh_id = c.hdkh_id
left join css_hcm.ct_phieutt d on b.hdtb_id = d.hdtb_id
left join css_hcm.ct_khuyenmai e on a.chitietkm_id = e.chitietkm_id
where b.loaitb_id  in (80,140) and a.datcoc_csd > 0 and d.tien > 0  order by a.rkm_id desc;
select * from css_Hcm.ct_phieutt where hdtb_id = 22324035 ;
select* from css_hcm.ct_khuyenmai where datcoc_csd > 0
select* from css_hcm.db_datcoc where loaitb_id  in (80,140);
select* from css_hcm.db_thuebao  where loaitb_id  in (80,140) and trangthaitb_id = 1;
select* from css_hcm.loaihinh_Tb where loaitb_id in(80,140)