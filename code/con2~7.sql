	select * from rmp_bsc_phong;
    select* from tlto;
    update rmp_bsc_phong a 
    set tong = (select tong from tlto where ma_pb = a.ma_pb),
     da_giahan = (select da_hoanthanh from tlto where ma_pb = a.ma_pb),
     tyle =  (select tyle_ from tlto where ma_pb = a.ma_pb),
      mdht = (select mdht from tlto where ma_pb = a.ma_pb)
    where  ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
commit;
select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi IN ('','HCM_TB_GIAHA_027') ;and ma_to = 'VNP0702311';
select* from ttkd_Bsc.nhanvien where thang = 202408 and ma_Nv = 'VNP020742';
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202408 and ma_nv = 'VNP017942';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408;
select* from ttkd_Bsc.bangluong_kpi where ma_nv = '';
commit;
rollback;
insert into ct_bsc_tratruoc_moi_30day select* from  ttkd_Bsc.ct_bsc_tratruoc_moi_30day where thang = 202408;
update ttkd_Bsc.ct_bsc_tratruoc_moi_30day
set tien_khop = 1
--select* from  ttkd_Bsc.ct_bsc_tratruoc_moi
where thang = 202408 and tien_khop = 0 and ht_Tra_id = 207 and kenhthu_id in  (26,25);
update ttkd_Bsc.bangluong_kpi
set tyle_thuchien = 100, mucdo_hoanthanh = 100, diem_Cong = 5
where thang = 202408 and ma_nv = 'VNP020742'   and ma_kpi IN ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_027') ;
select* from rmp_bsc_phong where  ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
update ttkd_Bsc.bangluong_kpi set 