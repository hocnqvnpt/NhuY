select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202410;
update ttkd_Bsc.bangluong_kpi A SET GIAO = (SELECT MAX(SOGIAO) FROM  ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202410
                                            AND TEN_KPI = '2.CT PTM thuê bao gói Home Sành/Home ch?t' AND MA_NV = A.MA_NV 
                                            )
where thang = 202410 and ma_kpi = 'HCM_SL_COMBO_006';
COMMIT;
select * from ttkd_bsc.nhanvien where thang = 202410 and ngaysinh like '11/02%';
select *from ttkd_bct.bangiao_chungtu_tinhbsc a;
select* from ct_Bsc_Chungtu where thang = 202409;
select distinct chungtu_id from ttkdhcm_ktnv.ds_chungtu_Nganhang_oneb where to_char(ngay_Ht,'yyyymm')='202410';
select * from ttkdhcm_ktnv.ds_chungtu_nganhang_nhom; where chungtu_id=383076
and loai_nhom=0;