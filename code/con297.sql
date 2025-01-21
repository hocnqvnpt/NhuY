select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202410 and ma_Nv ='VNP017793';
select * from ttkd_Bsc.nhanvien where  thang = 202410 and ma_Nv ='VNP017793';
update ttkd_bsc.tl_giahan_Tratruoc set tong = 34, DA_GIAHAN_DUNG_HEN = 4, tyle = 11.76 , ma_to ='VNP0702302' 
where thang = 202410 and ma_Nv ='VNP017793' and ma_kpi = 'HCM_TB_GIAHA_024';
commit;
DELETE from ttkd_bsc.tl_giahan_Tratruoc where thang = 202410 and  ma_to ='VNP0702302'  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_TO'  ;