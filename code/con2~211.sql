with ds as (
select tap, khachhang_id, thuebao_id, ma_tb, DICHVUVT_ID, LOAITB_ID, NGAY_SD, TRANGTHAITB_ID, SD_SMARTTV, MYTV, sl_tbao_mesh
                                , ONT_HiB
                                , (select loai_gt from css_hcm.loai_gt where loaigt_id = a.loaigt_id) loai_gt, so_gt, TOANHA_ID, TOCDO_ID, TENGOI_TBAO
                                , TOCDOTHUC, CUOC_SAUKM, iptinh, TEN_GOI_DADV, TIEN_DADV, ngay_ktdc, LUU_LUONG_FIBER
                                , HT_THUTIEN, NHOM_HTTT, HINHTHUC_TT, MATB_DADV_MYTV, MATB_DADV_DD
                                , KN_BANGRONG, KN_DIDONG, KN_SOHOA, KN_CHUANHOA_KH
						   , MANV_CSKH, TENNV_CSKH
						, TO_CSKH, PHONG_CSKH, TEN_TOVT, TEN_TTVT, MANV_TTVT, TENNV_TTVT, MA_KV
                from hocnq_ttkd.fiber_GD a 
			 union all
                select tap, khachhang_id, thuebao_id, ma_tb, DICHVUVT_ID, LOAITB_ID, NGAY_SD, TRANGTHAITB_ID, SD_SMARTTV, MYTV, sl_tbao_mesh
                                , ONT_HiB
                                , (select loai_gt from css_hcm.loai_gt where loaigt_id = a.loaigt_id) loai_gt, so_gt, TOANHA_ID, TOCDO_ID, TENGOI_TBAO
                                , TOCDOTHUC, CUOC_SAUKM, iptinh, TEN_GOI_DADV, TIEN_DADV, ngay_ktdc, LUU_LUONG_FIBER
                                , HT_THUTIEN, NHOM_HTTT, HINHTHUC_TT, MATB_MYTV, MATB_DADV_DD
                                , KN_BANGRONG, KN_DIDONG, KN_SOHOA, KN_CHUANHOA_KH
						   , MANV_CSKH, TENNV_CSKH
						, TO_CSKH, PHONG_CSKH, TEN_TOVT, TEN_TTVT, MANV_TTVT, TENNV_TTVT, MA_KV
                     from hocnq_ttkd.VinaPhone_noFiber a union all
                select tap, khachhang_id, thuebao_id, ma_tb, DICHVUVT_ID, LOAITB_ID, NGAY_SD, TRANGTHAITB_ID, SD_SMARTTV, MYTV, sl_tbao_mesh
                                , ONT_HiB
                                , (select loai_gt from css_hcm.loai_gt where loaigt_id = a.loaigt_id) loai_gt, so_gt, TOANHA_ID, TOCDO_ID, TENGOI_TBAO
                                , TOCDOTHUC, CUOC_SAUKM, iptinh, TEN_GOI_DADV, TIEN_DADV, ngay_ktdc, LUU_LUONG_FIBER
                                , HT_THUTIEN, NHOM_HTTT, HINHTHUC_TT, MATB_MYTV, MATB_DADV_DD
                                , KN_BANGRONG, KN_DIDONG, KN_SOHOA, KN_CHUANHOA_KH
						   , MANV_CSKH, TENNV_CSKH
						, TO_CSKH, PHONG_CSKH, TEN_TOVT, TEN_TTVT, MANV_TTVT, TENNV_TTVT, MA_KV
                from hocnq_ttkd.MyTV_donle a
)
select ds.*, kh.so_Dt sdt_kh, tt.so_dt sdt_tt
from ds
    left join css_hcm.db_khachhang kh on ds.khachhang_id = kh.khachhang_id
    left join css_hcm.db_Thuebao tb on ds.thuebao_id = tb.thuebao_id
    left join css_hcm.db_Thanhtoan tt on tb.thanhtoan_id = tt.thanhtoan_id;
select* from hocnq_ttkd.fiber_GD;
select * from ttkd_Bsc.tl_giahan_Tratruoc A
    JOIN TTKD_bSC.NHANVIEN B ON A.MA_nV = B.MA_nV AND A.THANG = B.THANG where A.thang = 202408 and loai_tinh = 'DONGIATRA_OB' and a.ma_pb = 'VNP0700900';
    select* from ttkd_Bsc.ct_Dongia_tratruoc a where A.thang = 202408 and loai_tinh = 'DONGIATRA_OB' and ma_Tb = 'cty-nova4105';
    
    select* from ttkd_Bsc.nhanvien where ma_Nv ='VNP017254';
    UPDATE ttkd_bsc.bangluong_kpi SET DIEM_CONG = 5 where ten_Nv = 'Ngô Th? Vân' and thang =  202408 and ma_kpi ='HCM_TB_GIAHA_024';
    COMMIT ;
    select* from ttkd_bsc.bangluong_kpi where ten_Nv = 'Ngô Th? Vân' and thang =  202408 and ma_kpi ='HCM_TB_GIAHA_024';
    select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003';
    UPDATE  ttkd_bsc.bangluong_kpi SET DIEM_CONG = CASE WHEN MUCDO_HOANTHANH > 0 THEN MUCDO_HOANTHANH ELSE NULL END 
    where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003';
     UPDATE  ttkd_bsc.bangluong_kpi SET DIEM_TRU = CASE WHEN MUCDO_HOANTHANH < 0 THEN MUCDO_HOANTHANH ELSE NULL END 
    where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003';
    update ttkd_Bsc.blkpi_danhmuc_kpi set diem_Cong = 1, diem_tru = 1 , mucdo_hoanthanh = 0 where thang = 202408 and ma_kpi = 'HCM_CL_TONDV_003';
    commit;
    select* from  ttkd_Bsc.blkpi_danhmuc_kpi  where thang = 202408 AND NGUOI_xULY = 'Nh? Ý';
    update ttkd_bsc.bangluong_kpi SET giao = 1650 
    where thang = 202408 and ma_kpi = 'HCM_SL_HOTRO_006';
update  ttkd_Bsc.blkpi_danhmuc_kpi set giao = 1 where thang = 202408 and ma_kpi = 'HCM_SL_ORDER_001';
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi in ('HCM_TB_GIAHA_027');,'HCM_TB_GIAHA_026','HCM_TB_GIAHA_027','HCM_TB_GIAHA_028');
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ten_Nv like '%Vân';
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 120 where thang = 202408 and ma_kpi in ('HCM_TB_GIAHA_027') and tyle_Thuchien is null;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202408 and ma_Nv = 'VNP029065';
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_SL_COMBO_006';
UPDATE  ttkd_Bsc.bangluong_kpi SET DIEM_TRU = -DIEM_TRU where thang = 202408 and ma_kpi = 'HCM_SL_COMBO_006';
ROLLBACK;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_SL_COMBO_006'; AND GIAO IS NULL AND MA_NV IN (
SELECT MA_nV FROM ttkd_bsc.tonghop_giao_372  WHERE TEN_KPI ='1.CT PTM thuê bao gói Home Combo, Home Sành/Home ch?t');

select* from ttkd_bsc.