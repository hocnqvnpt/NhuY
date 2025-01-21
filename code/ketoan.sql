--with ct as (																									
--select hdtb_id,phieutt_id, sum(case when khoanmuctt_id = 19 then tien else 0 end) km_lapdat, sum(case when khoanmuctt_id = 19 then vat else 0 end) vat_km,																									
--sum(case when khoanmuctt_id <> 19 then tien else 0 end) tien_Thu,sum(case when khoanmuctt_id <> 19 then vat else 0 end) vat_thu																									
--from css_hcm.ct_phieutt																									
--group by hdtb_id, phieutt_id																									
--)		
---- THANH TOAN BANG TIEN MAT
--- tien dat coc tra truoc 
select a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id,  d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id	--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
--left join css_hcm.db_adsl br on b.thuebao_id = br.thuebao_id																									
--left join css_hcm.db_cntt t on b.thuebao_id = t.thuebao_id																									
--left join css_hcm.db_tsl tsl on b.thuebao_id = tsl.thuebao_id																									
--join css_hcm.chuquan cq on cq.chuquan_id =  coalesce(br.chuquan_id, t.chuquan_id, tsl.chuquan_id )																									
where to_Number(to_char(b.ngay_tt,'yyyymm')) = 202406	and c.khoanmuctt_id = 11 and d.ht_Tra_id = 1;--and  coalesce(br.chuquan_id, t.chuquan_id, tsl.chuquan_id ) in (145, 266,	264)																							
and a.loaihd_id in (1,3,8,6,7)	;		
select* from csS_hcm.khoanmuc_tt where ten_kmtt like '%oken%';
select donvi_id from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02791837';
--- tien thiet bi CNTT
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, to_Number(to_char(d.ngay_tt,'yyyymmdd')), d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id 
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and b.loaitb_id in (61) and c.tien >0 and c.khoanmuctt_id =5 and d.ht_Tra_id = 1 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316);
--- tien dat coc tra truoc fiber: ?úng
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, to_Number(to_char(d.ngay_tt,'yyyymmdd')), d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id 
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and b.loaitb_id in (210) and c.tien >0 and c.khoanmuctt_id =11 and d.ht_Tra_id = 1 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316);
--- tien dich vu CA: dung
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv, th.loaihinh_Tb, a.ma_kh--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id
    left join css_hcm.loaihinh_Tb th on b.loaitb_id =th.loaitb_id
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and b.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,318 ) and c.tien >0 and --d.ht_Tra_id = 2 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316) and c.khoanmuctt_id = 11;
select *
     from qltn.v_db_datcoc@dataguard where phanvung_id=28  and kyhoadon=20240601 ;
---
--- thu tien GTGT HDDT: dung
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv, th.loaihinh_Tb, a.ma_kh--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id
    left join css_hcm.loaihinh_Tb th on b.loaitb_id =th.loaitb_id
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and b.loaitb_id in (122 ) and c.tien >0 and --d.ht_Tra_id = 2 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316) and c.khoanmuctt_id = 11;
---- thu tien token: DHSX: dung
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv, th.loaihinh_Tb, a.ma_kh--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id
    left join css_hcm.loaihinh_Tb th on b.loaitb_id =th.loaitb_id
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and c.tien >0 and --d.ht_Tra_id = 2 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316) and c.khoanmuctt_id = 5;
select* from css_hcm.khoanmuc_Tt where ten_kmtt = 'Mua Thêm Thi?t B?';
--- THU TIEN THIET BI TEN MIEN VIET Nam
select --a.ma_Gd
--distinct dv.donvi_id
a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id, d.ngay_tt, c.tien, c.vat,																									
d.thungan_tt_id,d.thungan_hd_id, d.ht_tra_id,d.kenhthu_id, d.trangthai,b.loaitb_id,c.khoanmuctt_id	,e.ten_kmtt, d.donvi_id, dv.ten_dv, th.loaihinh_Tb, a.ma_kh--, cq.tenchuquan																								
from css_hcm.hd_khachhang a																									
    join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id																									
    join css_hcm.ct_phieutt c on b.hdtb_id = c.hdtb_id																									
    join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id 																						
	join css_hcm.khoanmuc_tt e on c.khoanmuctt_id =e.khoanmuctt_id	
    left join admin_Hcm.donvi dv on d.donvi_tt_id = dv.donvi_id
    left join css_hcm.loaihinh_Tb th on b.loaitb_id =th.loaitb_id
where  to_Number(to_char(d.ngay_tt,'yyyymmdd')) between 20240601 and 20240630 and c.tien >0 and --d.ht_Tra_id = 2 and 
(dv.donvi_cha_id = 284316 or dv.donvi_id = 284316) and c.khoanmuctt_id = 52;