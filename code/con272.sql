select a.ma_nd, b.so_Dt, c.TEN_DV from admin_hcm.nguoidung a
    join admin_hcm.nhanvien_onebss b on a.nhanvien_id = b.nhanvien_id
    join admin_hcm.donvi c on b.donvi_id = c.donvi_id 
    where   upper(ten_Dv) like '%THU?N TÍN THÀNH%';
    select* from admin_hcm.donvi where upper(ten_Dv) like '%THU?N TÍN THÀNH%';
    select* from ttkd_Bsc.bangluong_kpi where ma_To = 'VNP0702509' and thang = 202408;
    select* from ttkd_bsc.ct_Bsc_giahan_cntt where manv_Giao = 'VNP020228' and thang = 202408;
    UPdate ttkd_Bsc.bangluong_kpi set tyle_thuchien = 100, thuchien = 100, diem_Cong = 5, diem_tru = null where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_024' AND TEN_nV LIKE '%V?nh Nghi?p';
    select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_024' AND TEN_nV LIKE '%V?nh Nghi?p';
    commit;
    rollback;
    select* from ttkd_bsc.ct_dongia_Tratruoc where thang = 202408 and loai_tinh = 'DONGIATRA_OB';
    with tien as 
    (
        select sum(tien_thuyetphuc)
        from ttkd_Bsc.ct_dongia_Tratruoc a join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and a.thang = b.thang
        where a.thang = 202408 and loai_tinh = 'DONGIATRA_OB' and a.ma_nv = 'CTV040725'
    );
    select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202408 and loai_tinh = 'DONGIATRA_OB' and ma_pb = 'VNP0701100';
    select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001' and ma_pb='VNP0703000';
    select* from ttkd_Bsc.nhanvien where thang = 202408 and ma_Nv = 'CTV082204';ma_vtcv = 'VNP-HNHCM_KDOL_15';
    select ma_nv,da_giahan_dung_hen from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001' and ma_pb = 'VNP0703000' and loai_tinh = 'KPI_NV';
    select* from ttkd_Bsc.nhanvien where ten_Nv = 'Ngô Th? Vân'