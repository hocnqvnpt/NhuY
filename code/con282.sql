select
ten_Nv, ten_pb, diem_Cong, diem_tru, mucdo_Hoanthanh from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_022'; AND diem_cong IS NOT NULL;
update ttkd_bsc.bangluong_kpi set diem_tru = null where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003' and ten_nv = 'H? Anh Toàn';
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001';
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202404 and loai_tinh = 'DONGIA_OB_VTTP' ;ma_Tb ='trucsuong1986_19';
select* from ttkd_Bsc.dongia_Vttp where ma_Tb ='hcmtrucsuong22';
select* from ttkd_Bsc.nhanvien_vttp_potmasco;
select* from ttkd_Bsc.nhanvien where ten_Nv like '%Trinh';
select* from css_Hcm.goi_Dadv where upper(ten_goi) like '%HOME NET%';
select a.*, c.ten_Goi from chiduyen a
    left join css_hcm.bd_goi_dadv b on a.thuebao_id = b.thuebao_id and b.trangthai = 1 and b.goi_id not between 1715 and 1726 and 
                b.goi_id not in (15414, 16221) and b.goi_id < 100000
    left join css_Hcm.goi_Dadv c on b.goi_id = c.goi_id
;



