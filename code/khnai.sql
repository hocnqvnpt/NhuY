--- TD GGS
update ttkd_Bsc.ct_BsC_giahan_cntt
set tien_khop = 1
where thang = 202412 and ma_tb in ('hcm_ca_00048301','hcm_ca_00047813','hcm_ca_00048007');
update ttkd_Bsc.ct_dongia_tratruoc
set tien_khop = 1, dongia = 0
where thang = 202412 and ma_tb in ('hcm_ca_00048301','hcm_ca_00047813','hcm_ca_00048007');
commit;
select * from  ttkd_Bsc.ct_dongia_tratruoc where thuebao_id in (9131997,9143349,9152762);
--- TD GGS
update ttkd_Bsc.ct_bsc_Giahan_cntt set tien_khop = 1 where thang = 202412 and ma_Tb= 'hcm_ca_ivan_00020560';
update ttkd_Bsc.ct_dongia_tratruoc
set tien_khop = 1, dongia = 0
where thang = 202412 and ma_tb = 'hcm_ca_ivan_00020560';

;
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where thang = 202412 and ma_Tb ='hcm_ca_00068919';
 tasco-tanbinh, solution_binhduong, deheus_tanan
---- fiber
delete from ttkd_Bsc.ct_Bsc_tratruoc_moi_1tr where thang = 202412 and ma_Tb = 'tasco_2023';
commit;


select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1 where ma ='HCM-TT/03084396'
;
select* from ttkd_Bsc.bangluong_kpi where ma_nv ='VNP020225' and thang = 202412;
update ttkd_Bsc.bangluong_kpi set giao = 4, tyle_thuchien = 25 where ma_nv ='VNP020225' and thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_031';
update ttkd_Bsc.TL_GIAHAN_tRATRUOC set TONG = 4, tyle = 25 where ma_nv ='VNP020225' and thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_031';


---- ca
update ttkd_Bsc.ct_Bsc_giahan_Cntt set tien_khop = 1, ma_Gd = '01062022' where thang = 202412 and ma_tb='hcm_ca_00048621';
commit;
update ttkd_Bsc.ct_dongia_tratruoc set tien_khop = 1, dongia = 0 where thang = 202412 and ma_tb='hcm_ca_00048621';
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_Nv= 'VNP028833';
rollback;
update ttkd_Bsc.bangluong_kpi set thuchien = 3, tyle_thuchien = 100,diem_Cong = 5, diem_tru = 0
where ma_nv ='VNP028833' and thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_024';
update ttkd_Bsc.TL_GIAHAN_tRATRUOC set da_Giahan_dung_Hen = 3, tyle = 100 where ma_nv ='VNP028833' and thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_024';

select* from ttkd_Bsc.ct_bsc_Giahan_Cntt where thang =202412 and manv_giao = 'VNP028833';
select* from ttkd_Bsc.bangluong_kpi where ma_kpi = 'HCM_SL_COMBO_006' and thang = 202412