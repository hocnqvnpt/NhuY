select ten_nv,ma_To, ten_to,ten_donvi, ten_Vtcv,ma_Vtcv ,hcm_tb_Giaha_022,hcm_tb_Giaha_023 from ttkd_bsc.bangluong_kpi_202403 where ten_Nv like '%Nhiên';
commit;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_023' and thang_kt is null;
insert into tl_giahan_tratruoc select* from ttkd_bsc.tl_Giahan_tratruoc where thang = 202403
select* from ttkd_Bsc.bangluong_kpi where thang = 202403 and ma_kpi ='HCM_TB_GIAHA_023' ;
insert into ct_Bsc_tratruoc_moi select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202403
select* from ttkd_Bsc.nhanvien_202403 where  ma_nv in('CTV082094','CTV081987','VNP016671','VNP020236','VNP017365','VNP016984','CTV082017','CTV085345')
select* from ttkd_Bsc.ct_bsc_giahan_Cntt where thang = 202403 and manv_giao in('CTV082094','CTV081987','VNP016671','VNP020236','VNP017365','VNP016984','CTV082017','CTV085345')
and tien_khop is null and rkm_id is not null;
commit;
update ttkd_bsc.ct_Bsc_giahan_cntt set ma_to = 'VNP0702222' where 
thang = 202403 and manv_giao in('CTV082094','CTV081987','VNP016671','VNP020236','VNP017365','VNP016984','CTV082017','CTV085345')