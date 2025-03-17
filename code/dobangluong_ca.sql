--- 024
update TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select (sum(DA_GIAHAN_DUNG_HEN))
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_024' ;
rollback;
COMMIT;
-- TO TRUONG

update TTKD_BSC.bangluong_kpi a 
set thuchien = (select da_giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_024';

commit;
-- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        ,thuchien = (select da_giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_024';
-- diem cong tru
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 5
                        when (tyle_Thuchien) > 75 and tyle_thuchien < 100 then 1
                        when (tyle_Thuchien) = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410;
commit;
update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 65 and tyle_thuchien < 75 then 1  
                        when (tyle_Thuchien) >= 45 and tyle_thuchien < 65 then 3
                        when (tyle_Thuchien) >= 30 and tyle_thuchien < 45 then 5
                        when (tyle_Thuchien) < 30 then 7 end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202410  ;
commit;
-- 027 
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select da_giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
    ,tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_027') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_027' and ma_nv != 'VNP017445';
                                
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024'),
                thuchien = (select da_giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_027')  
                                            and thang = 202410 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_027' and ma_nv != 'VNP017445' ;
commit;

--- cong tru



-- mdht
update ttkd_Bsc.bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_027' and ma_nv != 'VNP017445' ;
commit;
--- QLDL
update TTKD_BSC.bangluong_kpi a 
set 
 thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_028') and thang = 202410 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_028'  ;

update TTKD_BSC.bangluong_kpi a 
set thuchien = (select da_giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202410 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_028') 
                                and thang = 202410 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_028';

commit;
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when tyle_thuchien >= 100 then 7
                        when  tyle_thuchien >= 75 and tyle_thuchien < 100 then 3
                        when  tyle_thuchien = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202410 and ( ma_nv = 'VNP017445' or ma_Vtcv !='VNP-HNHCM_KHDN_4');

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when  tyle_thuchien >= 65 and tyle_thuchien < 75 then 3  
                        when  tyle_thuchien >= 45 and tyle_thuchien < 65 then 5
                        when  tyle_thuchien >= 30 and tyle_thuchien < 45 then 7
                        when  tyle_thuchien< 30 then 10 end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202410  and ( ma_nv = 'VNP017445' or ma_Vtcv !='VNP-HNHCM_KHDN_4');
commit;
update   ttkd_Bsc.bangluong_kpi set diem_Tru = null, diem_Cong = null , thuchien = null, tyle_Thuchien = null, mucdo_hoanthanh = null
where ma_kpi ='HCM_TB_GIAHA_027' AND THANG = 202410 and ma_Nv = 'VNP017445';
select* from ttkd_Bsc.bangluong_kpi where ma_kpi ='HCM_TB_GIAHA_027' AND THANG = 202410 ;
select* from  ttkd_Bsc.bangluong_kpi  where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202410  and ma_nv ='CTV062104';
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_nv ='CTV062104';
select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202410 and ma_Tb in ('hcm_ca_00085885','hcm_ca_00067371','hcm_ca_00065551');
