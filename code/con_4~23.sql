select * from THu_T1 w;

select* from v_Thongtinkm_all where rkm_id in (6435437,6435438);
select a.* from tmp3_ob a
join ttkdhcm_ktnv.ghtt_giao_688 b on a.thuebao_id = b.thuebao_id
where b.thang_kt = 202405 and km = 1 and tratruoc = 1 and loaibo = 0
and not exists (select 1 from tmp3_30ngay where rkm_id = a.rkm_id);

select* from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where ma_Tb=  'lacnuithanh' and thang = 202405;

select* from TTKD_BSC.blkpi_danhmuc_kpi where thang_kt is null;
select* from 
with tien as
(select ma_nv,TIEN_DONGIA   from ttkd_bsc.dongia_Vttp where thang = 202404 
--where loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') and (tien_thuyetphuc > 0 or tien_xuathd > 0)
union all 
select MANV_XHDON, TIEN_XHDON  from ttkd_bsc.dongia_Vttp where thang = 202404
--where loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') and (tien_thuyetphuc > 0 or tien_xuathd > 0)
)
select sum(TIEN_DONGIA)
from tien a
join ttkd_Bsc.nhanvien_vttp b on a.ma_nv = b.ma_nv and b.thang = 202404;
select* from ttkd_Bsc.tl_Giahan_Tratruoc where ma_nv = 'VNP027795';
select THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA, DTHU, 
NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON,
case when nvl(manv_xhdon,'a') in (select ma_Nv from ttkd_Bsc.nhanvien_vttp where thang = 202404) then tien_xhdon else 0 end tien_xhdon
from ttkd_bsc.dongia_Vttp where thang = 202404 ;and thuebao_id not in
(select * from ct_dongia_temp where loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') and (tien_thuyetphuc > 0 or tien_xuathd > 0)
); and loai_tinh in ('DONGIA_OB_VTTP','DONGIA_VTTP') ;
select* from ttkd_Bsc.nhanvien_202404 where ten_nv like '%Trinh';
select* from ttkd_Bsc.dongia_Vttp  where thang = 202404 and dongia*heso*heso_chuky!= round(tien_xhdon+TIEN_DONGIA);