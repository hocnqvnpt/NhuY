select  from ttkd_Bsc.bangluong_kpi_202404 where HCM_TB_GIAHA_027 is not null;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_tr30day where thang = 202405;
select* from  ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
and ma_kpi = 'HCM_TB_GIAHA_027';
update TTKD_BSC.bangluong_kpi_202405 a set 
        HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm')
        and ma_nv = a.ma_nv_hrm 
        and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV); AND MA_NV = 'VNP027256';
;
select nhanvien_giao from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202406;
commit;
VNP0702306
VNP0702407
VNP0702507;
select* from TTKD_BSC.bangluong_kpi_202405 where ma_to =  'VNP0702507';

update TTKD_BSC.bangluong_kpi_202405 a 
set HCM_TB_GIAHA_022 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_pb = a.ma_Donvi
and ma_kpi = 'HCM_TB_GIAHA_022')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
    and thang_kt is null and MA_VTCV = a.MA_VTCV) ;
update TTKD_BSC.bangluong_kpi_202405 a 
set HCM_TB_GIAHA_027 = (select tyle from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
and ma_nv = a.ma_nv_hrm 
and ma_kpi = 'HCM_TB_GIAHA_027' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_TB_GIAHA_027') and thang_kt is null and MA_VTCV = a.MA_VTCV) ;
