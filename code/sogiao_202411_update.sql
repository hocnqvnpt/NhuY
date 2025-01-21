select * from ttkd_Bsc.bangluong_kpi where thang = 202411 and  ma_kpi in ('','HCM_SL_COMBO_006') --HCM_SL_COMBO_006 HCM_SL_ORDER_001
and ma_nv in 
(select ma_nv from ttkd_bsc.nhanvien_202411_dieuchuyen
union all
select ma_nv from ttkd_bsc.nhanvien_202411_moi );
-- Nguy?n Anh Phúc
-- Nguy?n Th? Th?o Vi
-- Ngô Thanh Lan;
update ttkd_Bsc.bangluong_kpi 
set giao = 400
where thang = 202411 and  ten_nv not in ('Nguy?n Anh Phúc','Nguy?n Th? Th?o Vi','Ngô Thanh Lan')and  ma_kpi in ('','HCM_SL_ORDER_001') --HCM_SL_COMBO_006
and ma_nv in 
(select ma_nv from ttkd_bsc.nhanvien_202411_dieuchuyen
union all
select ma_nv from ttkd_bsc.nhanvien_202411_moi );
commit;
select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202411 and ten_kpi ='2.CT PTM thuê bao gói Home Sành/Home ch?t' and ma_nv in 
(select ma_nv from ttkd_Bsc.bangluong_kpi where thang = 202411 and  ma_kpi in ('','HCM_SL_COMBO_006') --HCM_SL_COMBO_006 HCM_SL_ORDER_001
and ma_nv in 
(select ma_nv from ttkd_bsc.nhanvien_202411_dieuchuyen
union all
select ma_nv from ttkd_bsc.nhanvien_202411_moi ));

