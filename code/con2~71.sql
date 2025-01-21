select * from ttkd_bsc.bangluong_kpi where thang = 202409  and ma_kpi in ('HCM_SL_ORDER_001');
SELECT* FROM ttkd_Bsc.blkpi_danhmuc_kpi
where thang = 202409 and NGUOI_XULY = 'Nh? Ý'; ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026','HCM_TB_GIAHA_028');
UPDATE ttkd_bsc.bangluong_kpi SET CHITIEU_GIAO = 100 
                                                        where thang = 202409 and ma_kpi in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023','HCM_TB_GIAHA_027','HCM_CL_TONDV_003');
                                                        COMMIT;
                                                        update ttkd_bsc.bangluong_kpi set giao = 0 where thang = 202409  and ma_kpi in ('HCM_SL_COMBO_006') and ma_Nv in 
                                                        (select mã_Nv_hrm from pgd_Cskh);
                                                        select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202409 and ma_To ='VNP0702309';
                                                        update ttkd_bsc.bangluong_kpi set tyle_Thuchien = 22.4, diem_tru = 5 where thang = 202409  and ma_kpi in ('HCM_TB_GIAHA_028')  and ma_To ='VNP0702309';;
select* from ttkd_Bsc.bangluong_kpi where thang = 202409 and ma_kpi ='HCM_TB_GIAHA_022' and ten_Nv like '%Duyên';
select* from rmp_Bsc_phong;
insert into rmp_bsc_phong(THANG, MA_PB, PHONG_GIAO, TEN_KPI, TONG, DA_GIAHAN, TYLE)
select thang, ma_pb, 'Phòng Bán Hàng Online' phong_giao, 'T? l? thuy?t ph?c khách hàng GHTT TC tháng T ( D?ch v? Fiber, MyTV, Mesh)' ten_kpi, tong, da_giahan_dung_hen, tyle
from ttkd_Bsc.tl_giahan_tratruoc where  thang = 202409 and ma_kpi ='HCM_TB_GIAHA_022' and ma_pb ='VNP0703000';
commit;
