delete from ;
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where chungtu_id in (492023,520452,526656,533277,539266,538688,538782,539262,536808);
create table bangluong_kpi_130125 as select* from ttkd_bsc.bangluong_kpi where thang = 202412;
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202412 and ma_Tb in ('kizuna_est2',
'loidof30',
'aladdin2.thoaingochau',
'vy181-4',
'hcmloidof30',
'taladdin06aquy',
'ctykimngan',
'nafoods_80',
'kldq2022',
'han16',
'han12a',
'perfectcompanion',
'ksluxury',
'sontien_q3',
'hcm_khanhluong',
'ctykizuna_2019');
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202412 and ma_Nv = 'CTV072537';
select* from ttkd_bsc.bangluong_kpi where thang = 202412 and ma_Nv = 'CTV082537';
select* from ttkd_bsc.ct_bsc_tratruoc_moi_1tr where manv_Giao ='CTV082537';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1 where ma ='HCM-TT/03084396';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where chungtu_id =471442;
select* from ttkd_Bsc.nhanvien where thang = 202412 and ten_nv like '%H?nh';
commit;
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where ma_Tb ='hcm_ca_00048621';
update ttkd_Bsc.nhanvien set thaydoi_vtcv = 0 where thang = 202501 and ma_Nv in ('VNP017623','VNP027585','CTV087844');
select* from ttkd_Bsc.nhanvien where thang = 202501 and ma_Nv in ('VNP017623','VNP027585','CTV087844');