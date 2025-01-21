update ttkd_Bsc.nhanvien A
set tinh_Bsc = (select tinh_Bsc from u where a.ma_nv = mÃ_Nv_hrm)
where thang= 202408 and donvi = 'TTKD';
COMMIT;
SELECT* FROM ttkd_Bsc.nhanvien A where thang= 202408 and donvi = 'TTKD';
update ttkd_Bsc.ct_Bsc_giahan_cntt
set tien_khop = 1 where thang = 202408 and tien_khop = 0 and ma_Tb in ('',
'hcm_ca_ivan_00018004',
'hcm_ca_ivan_00018054',
'hcm_ca_ivan_00018096',
'hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098',
'');
commit;
select* from rmp_bsc_phong;
update rmp_bsc_phong a
set da_giahan = (select z from tlt_2 where y= a.ma_pb) 
, mdht = (select x from tlt_2 where y= a.ma_pb )
where ten_kpi ='T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' and ma_pb not in ('VNP0702300','VNP0702400','VNP0702500');
select* from rmp_bsc_phong;
update rmp_bsc_phong a
set mdht = 5 
where  ten_kpi ='T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' and ma_pb  in ('VNP0702200');,'VNP0702400','VNP0702500');
;
commit;

select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202408 and ma_tb in ('hcm_ca_ivan_00017589',
'hcm_ca_ivan_00018004',
'hcm_ca_ivan_00018054',
'hcm_ca_ivan_00018096',
'hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098',
'hcm_ivan_00016978',
'hcm_ca_ivan_00017310','hcm_ca_00037631');
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202408 and tien_khop = 0 and ma_Tb in ('',
'hcm_ca_ivan_00018004',
'hcm_ca_ivan_00018054',
'hcm_ca_ivan_00018096',
'hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098',
'');
update ttkd_Bsc.ct_dongia_Tratruoc
set tien_khop = 1, dongia = 0 where thang = 202408 and tien_khop = 0 and ma_Tb in ('',
'hcm_ca_ivan_00018004',
'hcm_ca_ivan_00018054',
'hcm_ca_ivan_00018096',
'hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098',
'');
commit;
select khachhang_id , mst from css_hcm.db_thuebao where ma_tb in ('hcm_ca_ivan_00018004','hcm_ca_00116314');
select * from ttkdhcm_ktnv.ghtt_chotngay_271 where trunc(ngay_chot)=to_date('02/09/2024','dd/mm/yyyy')
and ma_tb='hcm_ca_ivan_00018004';
select* from ttkd_Bsc.nhanvien where thang = 202408 and ma_Nv = 'HCM004899';
update ttkd_Bsc.ct_Bsc_giahan_cntt 
set tien_khop = 1
where thang = 202408 and ht_Tra_id = 2 and kenhthu_id = 10;
select * from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408 and  ht_Tra_id = 2 and kenhthu_id = 10 ;

SELECT* FROM RMP_BSC_PHONG;
select* from css_hcm.kenhthu;
select* from rmp_bsc_phong where ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)';
create table bk_up as;
select a.phong_giao, a.tyle, b.tyle from rmp_bsc_phong a join bk_up b on a.ma_pb= b.ma_pb and a.ten_kpi = b.ten_kpi 
where a.ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? VNPT CA-IVAN)' and a.tyle = b.tyle;
commit;
select* from ct_Bsc_giahan_cntt where thang = 202409 ;and ma_gd not in (select ma_gd from ttkd_bsc.ct_bsc_giahan_cntt where thang= 202408);
select * from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202408 and tien_khop = 0 and ma_pb = 'VNP0701100'; and ma_Gd in (select * from qrcode); 
insert into ct_Bsc_tratruoc_moi where thang = 202408 and tien_khop = 0;
select* from css_Hcm.kenhthu where kenhthu_id in (26,23,25, 29);
update rmp_bsc_phong
set mdht = 2.37--, ghi_chu = 'ki?m soát t?n t?t, t? l? t?n hàng ngày c?a P.BH = 0, +5%'
where ma_pb = 'VNP0701400' and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
select* from ct_Bsc_giahan_cntt where thang = 202409 and ma_Tb = 'hcm_ca_00037631';
select* from rmp_bsc_phong where ma_pb = 'VNP0701400' and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
delete from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202408 and ma_tb = 'hcm_tmqt_00000343' ;and loaitb_id not in (147,148);
update ttkd_bsc.ct_Bsc_giahan_cntt set tien where thang = 202408 and ma_tb = 'hcm_tmqt_00000343' ;
commit;
select* from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202408 and ma_tb = 'hcm_tmqt_00000343' ;
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202408 and ma_nv ='CTV085685';
select* from rmp_bsc_phong where thang = 202408 and ma_pb = 'VNP0702400';
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202408 and ma_PB='VNP0702400' and ma_kpi = 'HCM_TB_GIAHA_026';
select* from ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202408 and ma_Tb in ('quangvu938','long00610','lnduyen-0321','minhphat060317','vu5724_05','mesh_long00610','nguyentrai93730582','thuy556a')