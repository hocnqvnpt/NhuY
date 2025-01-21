select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang =202412 and nguoi_xuly = 'Nh? Ý' and ma_kpi = 'HCM_SL_BHOL_009';;
commit;
update  ttkd_Bsc.blkpi_danhmuc_kpi set chitieu_giao = 1, mucdo_hoanthanh = 1
where thang =202412 and nguoi_xuly = 'Nh? Ý' and ma_kpi ='HCM_SL_BHOL_009'; in ('HCM_TB_GIAHA_030','HCM_TB_GIAHA_031');

update ttkd_bsc.bangluong_kpi set chitieu_giao = 100 where thang = 202412 and ma_kpi = 'HCM_SL_BHOL_009';

commit;

update ttkd_bsc.bangluong_kpi set chitieu_giao = 100 where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_027';
select* from ttkd_bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_027';

delete from ttkd_bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_nv ='VNP017445';

update  ttkd_bsc.bangluong_kpi set chitieu_giao = 100 where thang = 202412 and ma_kpi in ( 'HCM_TB_GIAHA_022','HCM_TB_GIAHA_023');
select* from   ttkd_bsc.bangluong_kpi  where thang = 202412 and ma_kpi in ( '','HCM_TB_GIAHA_023') and ma_nv not in ('VNP016983','VNP017496','VNP017585',
'VNP017947','VNP017729','VNP016659','VNP016898','VNP017621','VNP017699');
update ttkd_bsc.bangluong_kpi set chitieu_giao = null where thang = 202412 and ma_kpi in ( '','HCM_TB_GIAHA_023') and ma_nv not in ('VNP016983','VNP017496','VNP017585',
'VNP017947','VNP017729','VNP016659','VNP016898','VNP017621','VNP017699');
commit;

select* from   ttkd_bsc.bangluong_kpi  where thang = 202412 and ma_kpi in ( '','HCM_TB_GIAHA_022')
;

update ttkd_Bsc.bangluong_kpi set giao = 600 where thang = 202412 and ma_kpi = 'HCM_SL_ORDER_001';

select* from   ttkd_bsc.bangluong_kpi  where thang = 202412 and ma_kpi in ( '','HCM_SL_ORDER_001')
;
commit;

update ttkd_Bsc.bangluong_kpi b set giao = (select max(soGiao) from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a 
where a.thang =202412 and a.ten_kpi ='2.CT PTM thuê bao gói Home Sành/Home ch?t' and b.ma_nv = a.ma_nv)
where thang = 202412 and ma_kpi = 'HCM_SL_COMBO_006';
commit;
select* from css_hcm.;
select* from   ttkd_bsc.bangluong_kpi  where thang = 202412 and ma_kpi in ( '','HCM_SL_COMBO_006') and ma_nv in ('VNP017721','VNP017813',
'VNP020231','VNP017528','VNP019931','VNP017853','VNP017601','VNP017604','VNP017305') ;and ten_Vtcv like 'Phó%'; and ma_vtcv = 'VNP-HNHCM_BHKV_2';

select* from  ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a where a.thang =202412 and  a.ten_kpi ='2.CT PTM thuê bao gói Home Sành/Home ch?t';
and ten_to ='Lãnh ??o Phòng';
update ttkd_Bsc.bangluong_kpi set chitieu_giao = 100 where thang = 202412 and ma_kpi = 'HCM_SL_BHOL_002';
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_SL_BHOL_002';
commit;

update ttkd_Bsc.blkpi_danhmuc_kpi set mucdo_hoanthanh= 1, tyle_Thuchien = 0 where  thang = 202412 and ma_kpi = 'HCM_SL_BHOL_002';


--- ty trong
update ttkd_bsc.bangluong_kpi a
set tytrong = (select tytrong from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_nv = a.ma_nv)
where thang = 202412 and ma_kpi in ('HCM_SL_COMBO_006');,'HCM_TB_GIAHA_022');
commit;
select* from ttkd_bsc.bangluong_kpi a where thang = 202412 and ma_kpi in ('HCM_SL_COMBO_006');,'HCM_TB_GIAHA_022');
update ttkd_Bsc.bangluong_kpi set chitieu_Giao = null  where thang = 202412 and ma_kpi in ('HCM_TB_GIAHA_027') and ten_Nv like '%Kiên';

--
update ttkd_bsc.bangluong_kpi a
set tytrong = (select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_COMBO_006');

update ttkd_bsc.bangluong_kpi a
set tytrong = null --(select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_COMBO_006') and ma_vtcv ='VNP-HNHCM_BHKV_2' and giao is null;
commit;
select * from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi  in ('HCM_SL_COMBO_006') ;and ma_vtcv ='VNP-HNHCM_BHKV_2' ;and tytrong is not null;

update ttkd_bsc.bangluong_kpi a
set tytrong = 50--(select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_ORDER_001');

update ttkd_bsc.bangluong_kpi a
set tytrong = 15--(select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_BHOL_009');

COMMIT;

update ttkd_bsc.bangluong_kpi a
set tytrong = 15--(select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_BHOL_009');

update ttkd_bsc.bangluong_kpi a
set tytrong = 10--(select distinct(tytrong) from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = a.ma_kpi and ma_VTCV = a.ma_vtcv and tytrong is not null)
where thang = 202412 and ma_kpi in ('HCM_SL_BHOL_002');