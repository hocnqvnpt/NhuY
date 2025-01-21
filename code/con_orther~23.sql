select* from admin_hcm.nhanvien_onebss where  ma_Nv ='CTV033341';
select* from admin_hcm.donvi;
select* from bu_kpi;
select a.ma_nv, a.ten_pb, a.HCM_SL_COMBO_006, b.giao from ttkd_bsc.BLkpi_giao_202411_20241108_2317 a
    join ttkd_Bsc.bangluong_kpi b on a.ma_nv = b.ma_Nv and b.thang = 202411 and b.ma_kpi = 'HCM_SL_COMBO_006'
where nvl(a.HCM_SL_COMBO_006 ,0)!= nvl(b.giao,0);
select* from ttkd_Bsc.bangluong_Kpi where thang = 202411 and ma_kpi = 'HCM_SL_COMBO_006';
select* from ttkd_bsc.BLkpi_giao_202411_20241108_2317 where ten_Nv like '%Khuy?n'