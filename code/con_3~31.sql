select* from ttkd_bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_SL_COMBO_006'; and giao is null and ma_nv in (select ma_nv from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG
where thang = 202410 and ten_kpi like '%Home%');
update  ttkd_bsc.bangluong_kpi a
set giao = (select max(sogiao) from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG
where thang = 202411 and ten_kpi like '%Home%' and ma_Nv=  a.ma_Nv)
where thang = 202411 and ma_kpi ='HCM_SL_COMBO_006' ;and giao is null and ma_nv in (select ma_nv from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG
where thang = 202410 and ten_kpi like '%Home%');
select* from   ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG
where thang = 202411 and ten_kpi like '%Home%' and ma_Nv ='CTV087844';ten_nv in ('Tr?n Th? H??ng','Nguy?n Th? Thu H??ng');
commit;
create table bu_kpi as;
select* from ttkd_bsc.bangluong_kpi where thang = 202410 and ma_kpi ='HCM_SL_COMBO_006' ;
select a.ten_nv, a.ten_vtcv, a.giao, b.giao giao_moi, a.ma_nv
from bu_kpi a
    join  ttkd_bsc.bangluong_kpi b on a.ma_nv = b.ma_Nv and a.ma_kpi  = b.ma_kpi and a.thang = b.thang 
    where nvl(a.giao,0)!= nvl(b.giao,0);
    select* from  ttkd_bsc.bangluong_kpi a 
        join ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG b on a.thang =b.thang and a.ma_nv= b.ma_nv
    where a.ma_kpi ='HCM_SL_COMBO_006' and b.ten_kpi like '%Home%' and a.giao != b.sogiao and a.thang = 202411;
    select* from ttkd_bsc.nhanvien where thang = 2024;
    update ttkd_bsc.bangluong_kpi set giao = 25 where ma_nv = 'VNP017740' and thang = 202410 and ma_kpi ='HCM_SL_COMBO_006';
    select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202410 and ma_pb ='VNP0703000';
    select* from admin_hcm.nhanvien_onebss where ma_Nv='daily_bocau1';
    select * from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202409 and loai_tinh in ('THUHOI_DONGIA_GHTT','DONGIA_CHUNG_TU');
    select* from thuhoi_Bsc_dongia where thang = 202408;
    delete from ttkd_bsc.ct_Bsc_Giahan_cntt where thang = 202410 and ma_tb in ('hcm_ca_00038039',
'hcm_ca_00042771',
'hcm_ca_00042765',
'hcm_ca_00042928',
'hcm_ca_00042763',
'hcm_ca_00042752',
'hcm_ca_00043102',
'hcm_ca_00042963',
'hcm_ca_00042770',
'hcm_ca_00042976',
'hcm_ca_00042745',
'hcm_ca_00042927',
'hcm_ca_00042937',
'hcm_ca_00043130',
'hcm_ca_00042932',
'hcm_ca_00042941',
'hcm_ca_00042754',
'hcm_ca_00042965',
'hcm_ca_00043226',
'hcm_ca_00042978',
'hcm_ca_00042905',
'hcm_ca_00043005',
'hcm_ca_00042982',
'hcm_ca_00042907',
'hcm_ca_00043097',
'hcm_ca_00042910',
'hcm_ca_00042929',
'hcm_ca_00042766',
'hcm_ca_00042920',
'hcm_ca_00042981',
'hcm_ca_00042930',
'hcm_ca_00042936',
'hcm_ca_00043116',
'hcm_ca_00043248',
'hcm_ca_00042939',
'hcm_ca_00042760',
'hcm_ca_00042692',
'hcm_ca_00043178',
'hcm_ca_00042898',
'hcm_ca_00042693',
'hcm_ca_00042975',
'hcm_ca_00043268',
'hcm_ca_00042780',
'hcm_ca_00042972',
'hcm_ca_00042888',
'hcm_ca_00042897',
'hcm_ca_00042903',
'hcm_ca_00042757',
'hcm_ca_00042983',
'hcm_ca_00042753',
'hcm_ca_00043162',
'hcm_ca_00042918',
'hcm_ca_00043001',
'hcm_ca_00043312',
'hcm_ca_00042973',
'hcm_ca_00043395',
'hcm_ca_00043300',
'hcm_ca_00043361',
'hcm_ca_00043353',
'hcm_ca_00043338',
'hcm_ca_00043447',
'hcm_ca_00043266',
'hcm_ca_00043126',
'hcm_ca_00043336',
'hcm_ca_00043350',
'hcm_ca_00043389',
'hcm_ca_00043264',
'hcm_ca_00043314',
'hcm_ca_00043345',
'hcm_ca_00043406',
'hcm_ca_00043153',
'hcm_ca_00043391',
'hcm_ca_00043354',
'hcm_ca_00043316',
'hcm_ca_00043334',
'hcm_ca_00043294',
'hcm_ca_00043507',
'hcm_ca_00043272',
'hcm_ca_00043436',
'hcm_ca_00043475',
'hcm_ca_00043381',
'hcm_ca_00043134',
'hcm_ca_00043383',
'hcm_ca_00043471',
'hcm_ca_00043441',
'hcm_ca_00043258',
'hcm_ca_00043366',
'hcm_ca_00043149',
'hcm_ca_00043442',
'hcm_ca_00043400',
'hcm_ca_00043347',
'hcm_ca_00043317',
'hcm_ca_00043341',
'hcm_ca_00043343',
'hcm_ca_00043382',
'hcm_ca_00043310',
'hcm_ca_00043151',
'hcm_ca_00043318',
'hcm_ca_00043261',
'hcm_ca_00043388',
'hcm_ca_00043402',
'hcm_ca_00043271',
'hcm_ca_00043304',
'hcm_ca_00043311',
'hcm_ca_00043253',
'hcm_ca_00043340',
'hcm_ca_00043398',
'hcm_ca_00043412',
'hcm_ca_00043349',
'hcm_ca_00043147',
'hcm_ca_00043309',
'hcm_ca_00043319',
'hcm_ca_00043145',
'hcm_ca_00043469',
'hcm_ca_00043380',
'hcm_ca_00043390',
'hcm_ca_00043146',
'hcm_ca_00043367',
'hcm_ca_00043384',
'hcm_ca_00043358',
'hcm_ca_00062552',
'hcm_ca_00065939',
'hcm_ca_00066456',
'hcm_ca_00065717',
'hcm_ca_00065753',
'hcm_ca_00065754',
'hcm_ca_00065755',
'hcm_ca_00065758',
'hcm_ca_00065757',
'hcm_ca_00065759',
'hcm_ca_00065760',
'hcm_ca_00065761',
'hcm_ca_00065763',
'hcm_ca_00065762',
'hcm_ca_00065764',
'hcm_ca_00065765',
'hcm_ca_00065766',
'hcm_ca_00065768',
'hcm_ca_00065770',
'hcm_ca_00066061',
'hcm_ca_00064863',
'hcm_ca_00085989',
'hcm_ca_00086219',
'hcm_ca_00087229',
'hcm_ca_00087228',
'hcm_ca_00087231',
'hcm_ca_00087225',
'hcm_ca_00087230',
'hcm_ca_00087223',
'websitevnpt.vn',
'hcm_ca_00042917',
'hcm_ca_00043028',
'hcm_ca_00043063',
'hcm_ca_00042835',
'hcm_ca_00043066',
'hcm_ca_00042735',
'hcm_ca_00043048',
'hcm_ca_00042707',
'hcm_ca_00042825',
'hcm_ca_00043032',
'hcm_ca_00043059',
'hcm_ca_00043457',
'hcm_ca_00043476',
'hcm_ca_00044145',
'hcm_ca_00087011',
'hcm_ca_00087013',
'hcm_ca_00087014',
'hcm_ca_00065666',
'hcm_ca_00065651',
'hcm_ca_00065636',
'hcm_ca_ivan_00019037',
'hcm_ca_ivan_00019265',
'hcm_ca_00086846',
'hcm_ca_00087103',
'hcm_ca_00087119',
'hcm_ca_00086057',
'hcm_ca_00045574 ',
'hcm_ca_00047304 ',
'hcm_ca_00061060 ',
'hcm_ivan_00020028',
'hcm_ca_00038514 ',
'hcm_ca_ivan_00018867',
'hcm_ca_ivan_00017493 ',
'hcm_ca_00045241',
'hcm_ca_ivan_00019690',
'hcm_ca_00075031',
'hcm_ivan_00028943 ',
'hcm_ivan_00018722',
'hcm_ca_00049532 ',
'hcm_ca_ivan_00019268',
'hcm_ca_ivan_00019375',
'hcm_ca_00119825',
'hcm_ivan_00043045 ',
'hcm_ca_00119621',
'hcm_ivan_00042962',
'hcm_ca_00076624',
'hcm_ivan_00029407 ');
rollback;
commit;
select distinct chungtu_id from ttkd_bct.bangiao_chungtu_tinhbsc where thang = 202410 and xuly_tudong = 1;