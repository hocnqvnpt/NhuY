select * from ttkd_Bsc.nhanvien where thang = 202411 and ma_Nv ='CTV087496';
select * from nhanvien_202411_l2 where ma_Nv ='CTV087496';
select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202411 and ma_tb='hcm_tmqt_00000505';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_Nv in ('VNP017793','VNP020233','VNP017748','VNP017331'
,'VNP020233','CTV071063', 'VNP017798','VNP017728','VNP035858','VNP030414','VNP030414','CTV076270','VNP031585','VNP039527','CTV086828','VNP039527',
'CTV082537','VNP019947','VNP027799','VNP027799','CTV084651','CTV062104','VNP042847','CTV082818','CTV062104','VNP016764','CTV53229','VNP016548',
'CTV087192','CTV078282','CTV080745','CTV021955');
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027';

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026'
and ma_Nv in ('VNP017263','VNP017748','VNP017938','VNP024921','VNP029929','CTV080856','CTV080937','VNP016881','VNP017479');
select* from ttkd_Bsc.bangluong_kpi 
where thang = 202411 and ma_kpi in ('HCM_TB_GIAHA_023','HCM_TB_GIAHA_022') and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_028'