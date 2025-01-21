select  distinct loai_tinh, ma_kpi from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202403 and loai_tinh = 'DONGIATRA30D' and ma_tb not in (select ma_tb from ttkd_Bsc.ct_dongia_tratruoc WHERE THANG = 202404);

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_027' and thang_kt is null;

select* from dhsx.v_ghtt_ipcc@dataguard  where thang = 202404;