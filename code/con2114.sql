select* from ttkd_Bsc.nhanvien where thang = 202412 and ma_nv in ('CTV059592',
'VNP017771',
'VNP017439',
'CTV076445',
'CTV075798',
'VNP017719',
'CTV080087',
'CTV086096',
'CTV086124',
'CTV082214',
'CTV082384',
'CTV082209',
'CTV082664',
'CTV080931',
'CTV081976',
'CTV086121',
'CTV086129',
'CTV076800',
'CTV074751',
'CTV082208',
'CTV021804',
'CTV086448',
'VNP017327',
'VNP017319');
select* from nhuy.nhanvien_202412_l3
;
select* from ttkd_Bsc.nhanvien where thang = 202412 and user_ccos is null and ma_nv in (select manv_hrm from ttkd_Bsc.dm_Ccos where thang = 202412);
update  ttkd_Bsc.nhanvien a
set( user_Ccos , trangthai_Ccos) = (select user_ccos, trangthai from ttkd_Bsc.dm_Ccos where thang = 202412 and a.ma_nv = manv_hrm)
where thang = 202412 and user_ccos is null and ma_nv in (select manv_hrm from ttkd_Bsc.dm_Ccos where thang = 202412);
commit;
update ttkd_Bsc.nhanvien a
set (USER_CCBS, TR_THAI)= (select USER_CCBS, TR_THAI from ttkd_Bsc.nhanvien where thang = 202411 and user_ccbs is not null and ma_Nv = a.ma_nv);
select* from ttkd_Bsc.nhanvien a
where thang = 202412 and user_ccbs is null and ma_nv in (select ma_Nv from ttkd_Bsc.nhanvien where thang = 202409 and user_ccbs is not null);

delete from ttkd_Bsc.nhanvien where thang = 202412 and ma_Nv in  ('CTV087922','CTV087161');
commit;

select* from ttkd_bsc.x_nhanvien_202412_kogiao ;
select* from ttkd_Bsc.nhanvien where thang = 202412 and ma_Nv='VNP016548';

select* from nhanvien_202412_l3 where ;
select* from nhanvien_202412_l3 where ma_Nv in (Select ma_nv from dsnv_16ol);
update nhanvien_202412_l3 a
set (ma_Vtcv, ten_vtcv) = (select ma_Vtcv, ten_vtcv  from dsnv_16ol where ma_nv = a.ma_nv)
 where ma_Nv in (Select ma_nv from dsnv_16ol);
select* from nhanvien_202412_l3
;
commit;
select* from css_hcm.huonggiao;
update nhanvien_202412_l3 set ma_vtcv = upper(ma_vtcv);