select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and ma_Kpi = 'HCM_SL_ORDER_001'  and loai_Tinh = 'KPI_TO'; 
update ttkd_bsc.tl_giahan_tratruoc set tyle = round(nvl(DA_GIAHAN_DUNG_HEN,0)/660,2) 
where thang = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' and loai_tinh = 'KPI_NV';
commit;
UPDATE ttkd_bsc.tl_giahan_tratruoc a set tyle =(select  round(sum(DA_GIAHAN_DUNG_HEN)/ (660*5),2)
                                                from ttkd_bsc.tl_giahan_tratruoc 
                                                where thang = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' and MA_nV = A.MA_NV and loai_Tinh = 'KPI_PB'
                                                group by ma_pb, ma_to, thang, loai_tinh, ma_kpi)

where thang  = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' and ma_pb in ('VNP0701300','','') and loai_tinh in ('KPI_PB');

UPDATE ttkd_bsc.tl_giahan_tratruoc a set tyle = (select  round(sum(DA_GIAHAN_DUNG_HEN)/ (660*6),2)
                                                from ttkd_bsc.tl_giahan_tratruoc 
                                                where thang = 202406 and ma_Kpi = 'HCM_SL_ORDER_001'  and  MA_nV = A.MA_NV and loai_Tinh = 'KPI_PB'
                                                group by ma_pb, ma_to, thang, loai_tinh, ma_kpi)
where thang  = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' and ma_pb in ('VNP0701200','VNP0702200','VNP0702100','VNP0701500','VNP0701600') and loai_tinh in ('KPI_PB');

UPDATE ttkd_bsc.tl_giahan_tratruoc a set tyle = (select  round(sum(DA_GIAHAN_DUNG_HEN)/ (660*8),2)
                                                from ttkd_bsc.tl_giahan_tratruoc 
                                                where thang = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' AND MA_nV = A.MA_NV and loai_Tinh = 'KPI_PB'
                                                group by ma_pb, ma_to, thang, loai_tinh, ma_kpi)
where thang  = 202406 and ma_Kpi = 'HCM_SL_ORDER_001' and ma_pb in ('VNP0701100','VNP0701400','VNP0701800','','') and loai_tinh in ('KPI_PB');
update ttkd_bsc.;
select distinct ma_pb, ten_pb from ttkd_bsc.nhanvien_202406;
commit;
select * from TTKD_BSC.bangluong_kpi where thang = 202406 and  ma_Kpi = 'HCM_SL_ORDER_001';
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and ma_kpi = 'HCM_SL_ORDER_001') ,
        tyle_thuchien = (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and ma_kpi = 'HCM_SL_ORDER_001'),
        mucdo_hoanthanh =  (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and ma_kpi = 'HCM_SL_ORDER_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_ORDER_001') and thang_kt is null and MA_VTCV = a.MA_VTCV) and thang = 202406 
 and  ma_Kpi = 'HCM_SL_ORDER_001';
;
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001') ,
        tyle_thuchien = (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001'),
        mucdo_hoanthanh =  (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_to = a.ma_to and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_SL_ORDER_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_SL_ORDER_001')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and thang = 202406 and  ma_Kpi = 'HCM_SL_ORDER_001';
COMMIT;
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_SL_ORDER_001') ,
        tyle_thuchien = (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_SL_ORDER_001'),
        mucdo_hoanthanh =  (select tyle  from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_SL_ORDER_001')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 and ma_kpi in ('HCM_SL_ORDER_001')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) and thang = 202406 and  ma_Kpi = 'HCM_SL_ORDER_001';
    update ttkd_Bsc.bangluong_kpi set tyle_thuchien = mucdo_hoanthanh = 1.5 where thuchien is null and ma_vtcv = '';