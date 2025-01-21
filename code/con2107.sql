update ttkd_bsc.bangluong_kpi a
set GIAO = (select sl from manpn.HCM_CL_DHQLY_006 where a.ma_nv = ma_nv)
    , THUCHIEN = (select soluong from manpn.HCM_CL_DHQLY_006 where a.ma_nv = ma_nv)
    , TYLE_THUCHIEN = (select tyl from manpn.HCM_CL_DHQLY_006 where a.ma_nv = ma_nv)
    , DIEM_TRU = (select case when tyl < 50 then 20 else null end from manpn.HCM_CL_DHQLY_006 where a.ma_nv = ma_nv)
    , DIEM_CONG = (select case when tyl >= 100 AND MUCDO_HOANTHANH >= 100 then 20 else null end 
                                                from manpn.HCM_CL_DHQLY_006 B 
                                                    LEFT JOIN (
                                                                            select MA_NV, MUCDO_HOANTHANH
                                                                            from  ttkd_bsc.bangluong_kpi
                                                                            where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' ) C ON B.MA_NV = C.MA_NV
                                     where a.ma_nv = B.ma_nv)
where ma_nv in (select ma_nv from manpn.HCM_CL_DHQLY_006) and thang = 202411 and ma_kpi = 'HCM_CL_DHQLY_006' 
;


---YEEEEEEEE: chay xong chay doan nay coi co phai 2 nguoi dat 100%: -> 2 nguoi +20
select * from ttkd_bsc.bangluong_kpi 
where ma_kpi = 'HCM_CL_DHQLY_006' and thang =202411     
    and ma_vtcv = 'VNP-HNHCM_BHKV_17';
    

-----------------------------------------------062: chay lan luot la dc 
update ttkd_bsc.bangluong_kpi a -----------s? l?i b?ng temp_HCM_DT_PTMOI_062_cn1
        set CHITIEU_GIAO = 100
            , GIAO =  (select ROUND(NHOMVINA_DTGIAO/1000000,3) from ttkd_bsc.dinhmuc_giao_dthu_ptm B where thang = 202411 AND A.MA_NV = B.MA_NV)
            , THUCHIEN = (select round(sum(DTHU_KPI)/1000000,3) from manpn.temp_HCM_DT_PTMOI_062_cn where ma_nv=a.ma_nv) 
    where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' ;

    
--to truong
update ttkd_bsc.bangluong_kpi a -----------s? l?i b?ng temp_HCM_DT_PTMOI_062_cn1
           set  CHITIEU_GIAO = 100
            , GIAO =  (select ROUND(NHOMVINA_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm B where thang = 202411 AND A.MA_NV = B.MA_NV)
            , thuchien = (select round(sum(DTHU_KPI)/1000000,3) from manpn.temp_HCM_DT_PTMOI_062 where ma_to=a.ma_to) 
    where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' and MA_VTCV = 'VNP-HNHCM_BHKV_17';
    
TONG_DTGIAO   nvl(NHOMVINATS_KQTH,0)+nvl(NHOMVINATT_KQTH,0)
    
update  ttkd_bsc.bangluong_kpi a
set  TYLE_THUCHIEN = case when (MA_VTCV ='VNP-HNHCM_BHKV_15.1' and GIAO < 32 and THUCHIEN < 32 
                                                                        or (MA_VTCV ='VNP-HNHCM_BHKV_15' and GIAO < 30 and THUCHIEN < 30) 
                                                                        or (MA_VTCV ='VNP-HNHCM_BHKV_17' and GIAO < 240 and THUCHIEN < 240))
                                                                and ROUND(THUCHIEN/GIAO *100,3) > 100 then 100
                                                       when (MA_VTCV ='VNP-HNHCM_BHKV_15.1' and GIAO < 32 and THUCHIEN > 32) then ROUND(THUCHIEN/32 *100,3)
                                                       when (MA_VTCV ='VNP-HNHCM_BHKV_15' and GIAO < 30 and THUCHIEN > 30) then ROUND(THUCHIEN/30 *100,3)
                                                       when (MA_VTCV ='VNP-HNHCM_BHKV_17' and GIAO < 240 and THUCHIEN > 240) then ROUND(THUCHIEN/240 *100,3)
                                                       when (MA_VTCV ='VNP-HNHCM_BHKV_15.1' and GIAO > 39) then ROUND(THUCHIEN/39 *100,3)
                                                       when (MA_VTCV ='VNP-HNHCM_BHKV_15' and GIAO > 37) then ROUND(THUCHIEN/37 *100,3)
                                                       -- (MA_VTCV ='VNP-HNHCM_BHKV_17' and GIAO < 240) then ROUND(THUCHIEN/240 *100,3)
                                                                   else ROUND(THUCHIEN/GIAO *100,3) end 
--select * from ttkd_bsc.bangluong_kpi
where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' ;


update  ttkd_bsc.bangluong_kpi a
set MUCDO_HOANTHANH = CASE WHEN TYLE_THUCHIEN < 30 THEN 0 
                    WHEN TYLE_THUCHIEN > 150 THEN 150
                    ELSE TYLE_THUCHIEN END 
where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' ;

commit;
select* from  manpn.ptmhh_goi_tonghop a; where thang = 202411 and ma_kpi = 'HCM_DT_PTMOI_062' ;

----------------------------------------------------
Select * from ttkd_bsc.bangluong_kpi
    where  thang =202411 and ma_kpi = 'HCM_DT_VNPTT_007';
update ttkd_bsc.bangluong_kpi set thuchien = 8483.8
    where  thang =202411 and ma_kpi = 'HCM_DT_VNPTT_007';    