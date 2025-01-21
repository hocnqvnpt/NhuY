select a.ma_gd, b.ma_tb, b.ngay_ht, a.ngay_yc ngay_radon, c.trangthaitb_id
from css.v_hd_khachhang@dataguard a
    left join  css.v_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    left join css.v_db_Thuebao@dataguard c on b.thuebao_id = c.thuebao_id
where a.loaihd_id = 4 and to_number(to_char(a.ngay_yc,'yyyymm')) = 202408 and a.nhanvien_id = 7820 and b.tthd_id = 6 ;
select* from admin_hcm.nhanvien_onebss where ma_Nv = 'CTV021892';
select* from css_hcm.loai_Hd ;
select* from css_hcm.trangthai_tb where trangthaitb_id = 9;
update ttkd_bsc.blkpi_danhmuc_kpi a
   set giao=(select sum(case when giao is not null then 1 end) from bangluong_kpi where thang=a.thang and ma_kpi=a.ma_kpi)
      ,tyle_th=(select sum(case when tyle_thuchien is not null then 1 end) from bangluong_kpi where thang=a.thang and ma_kpi=a.ma_kpi)
      ,mucdo_ht=(select sum(case when mucdo_hoanthanh is not null then 1 end) from bangluong_kpi where thang=a.thang and ma_kpi=a.ma_kpi)
      ,diemcong=(select sum(case when diem_cong is not null then 1 end) from bangluong_kpi where thang=a.thang and ma_kpi=a.ma_kpi)
      ,diemtru=(select sum(case when diem_tru is not null then 1 end) from bangluong_kpi where thang=a.thang and ma_kpi=a.ma_kpi)
          --select * from ttkd_bsc.blkpi_danhmuc_kpi a
 where thang=202408 and nguoi_xuly = 'Nh? Ý';
 select* from ttkd_bsc.blkpi_danhmuc_kpi  where thang=202408 and nguoi_xuly = 'Nh? Ý' ;
update ttkd_bsc.blkpi_danhmuc_kpi
set THUCHIEN = 1
where thang=202408 and nguoi_xuly = 'Nh? Ý' and ma_kpi in ('HCM_SL_HOTRO_006');
commit;
select * from  ttkd_bsc.blkpi_danhmuc_kpi where thang = 202408 and ma_kpi in ( 'HCM_DT_PTMOI_055');
select* from ttkd_bsc.tonghop_giao_372;
UPDATE ttkd_bsc.bangluong_kpi SET GIAO = 176-22 where thang = 202408 and MA_nV = 'VNP017601' AND ma_kpi = 'HCM_SL_COMBO_006';
COMMIT;
select* from  ttkd_bsc.bangluong_kpi where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_026'; and ma_nv = 'VNP027256';
UPDATE TTKD_bSC.BANGLUONG_KPI SET DIEM_CONG = 5 WHERE THANG = 202408 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv = 'VNP027256';
commit;
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_026' and ma_pb = 'VNP0701300';
update ttkd_bsc.tl_giahan_tratruoc
set da_giahan_Dung_hen = tong, tyle = 100
where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_026' and ma_pb = 'VNP0701300';
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_pb = 'VNP0701300' and ma_nv in
(select ma_Nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_026' and ma_pb = 'VNP0701300'
)
and ma_kpi = 'HCM_TB_GIAHA_026' and diem_cong is null;
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202408 and loai_tinh = 'DONGIA_CHUNG_TU';
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi = 'HCM_TB_GIAHA_022';
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB,TIEN)

select thang, 'DONGIA_CHUNG_TU'loai_tinh, 'DONGIA_CHUNG_TU' ma_kpi,MA_NV, MA_TO, MA_PB ,TIEN from final_ctu_Tien where thang = 202408 and ghichu = 'bosung';
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202408  and ma_kpi = 'HCM_TB_GIAHA_026' and ma_Nv in ('VNP017827','VNP024915'); ma_To in ('VNP0701301','VNP07013H0')