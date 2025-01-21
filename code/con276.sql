select* from rmp_bsc_phong where ma_pb = 'VNP0701300' ;
update rmp_bsc_phong set da_giahan = 22, tyle = 100 where ma_pb = 'VNP0701300' and ten_kpi ='T? l? thuy?t ph?c khách hàng GHTT TC tháng T (D?ch v? Tên mi?n)';
update ttkd_Bsc.ct_Bsc_giahan_cntt
set tien_khop = 1
where thang = 202408 and ma_pb = 'VNP0701300' and loaitb_id in (147,148);
select* from ttkdhcm_ktnv.ds_chungtu_Nganhang_oneb where ma_Ct = 'VCB_20240729_265676';
commit;

select distinct ma_pb, ten_pb from ttkd_bsc.nhanvien_202405 where ten_pb like 'Phòng Bán Hàng K%';
select* from rmp_bsc_phong where ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh'
;
update rmp_bsc_phong a
set mdht = 5
--da_giahan = (select dgh from fi where ma_pb = a.ma_pb)
--, tyle = (select tyle from fi where ma_pb = a.ma_pb)
--, mdht = (select mdht from fi where ma_pb = a.ma_pb)
where ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh'
and ma_pb  in ('VNP0702200');,'VNP0702300','VNP0702500')