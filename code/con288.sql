update ttkd_Bsc.tl_giahan_tratruoc A set ma_Nv = (select m√_Nv_HRM from pgd_Cskh where ma_pb = a.ma_pb)
where thang = 202409 and ma_kpi = 'HCM_CL_TONDV_003' AND LOAI_TINH='KPI_PB';
DELETE FROM ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi = 'HCM_CL_TONDV_003' AND LOAI_TINH='KPI_PB';
COMMIT;

SELECT* FROM pgd_Cskh