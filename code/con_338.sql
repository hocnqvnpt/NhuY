select a.*, c.thuonghieu from chiduyen a 
    left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
    left join css_hcm.tocdo_adsl c on b.tocdo_id = c.tocdo_id
;
select* from ttkd_Bsc.nhanvien where sdt = '0914606064';
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202408 and ma_to = 'VNP0702416' and ma_kpi = 'HCM_TB_GIAHA_024';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202408 and ma_to = 'VNP0702416';
update ttkd_Bsc.bangluong_kpi 
set tyle_thuchien = 100 
--;SELECT* FROM  ttkd_Bsc.bangluong_kpi
where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_027' and tyle_thuchien is null;
commit;
ROLLBACK;
select* from ttkd_Bsc.nhanvien where ma_nv = 'CTV081714';
select* from ttkd_bsc.tl_Giahan_Tratruoc where thang = 202408 and ma_kpi= 'HCM_TB_GIAHA_024' and ma_nv = 'CTV081714';
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and  ma_to = 'VNP0702416';;
update ttkd_Bsc.bangluong_kpi set diem_cong = 0 ,diem_tru = 0 where tyle_thuchien is null AND THUCHIEN IS NULL and thang = 202408 and ma_kpi='HCM_TB_GIAHA_024';
select* from css_hcm.tocdo_adsl;
select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi in ('HCM_TB_GIAxHA_024','HCM_TB_GIAHxA_026','HCM_TB_GIAHA_027') ;AND THUCHIEN IS NULL AND
tyle_Thuchien IS NOT NULL;
UPDATE ttkd_bsc.bangluong_kpi 
SET tyle_thuchien = thuchien
where thang = 202408 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAxHA_026','HCM_TB_xGIAHA_027') AND TYLE_THUCHIEN IS NULL AND
THUCHIEN IS NOT NULL;
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_027';