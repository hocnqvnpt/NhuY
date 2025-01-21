with tien as (
    select ma_nv,sum(tien) tien 
    from ttkd_Bsc.tl_giahan_tratruoc 
                        where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB', 'DONGIA_TS_TP_TT')
    group by ma_nv
)
select *
from ttkd_Bsc.bangluong_dongia_202408 a
    join tien b on a.ma_Nv = b.ma_Nv
where a.luong_dongia_ghtt <> b.tien;
insert into ttkd_Bsc.ct_dongia_tratruoc
select 202408 THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, -DONGIA, 
DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU, TIEN_KHOP,'BOI HOAN' GHICHU, TYLE_THANHCONG, HESO_CHUKY, NHANVIEN_XUATHD, TIEN_XUATHD, TIEN_THUYETPHUC, IPCC
from ttkd_Bsc.ct_dongia_tratruoc where thang = 202407 and ma_tb in ('hcm_smartca_00017765',
'hcm_smartca_00017772',
'hcm_smartca_00017763',
'hcm_smartca_00017764',
'hcm_smartca_00017775',
'hcm_smartca_00017770',
'hcm_smartca_00017754',
'hcm_smartca_00017776',
'hcm_smartca_00017766',
'hcm_smartca_00017769'
);
update ttkd_bsc.tl_giahan_tratruoc set tien = 0 where thang = 202408 and ma_nv = 'VNP029065' and loai_tinh = 'DONGIATRU_CA';
SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc where thang = 202408 and ma_nv = 'VNP029065' and loai_tinh = 'DONGIATRU_CA';
COMMIT;
ROLLBACK;
rollback;
select distinct loai_tinh from  ttkd_Bsc.tl_giahan_tratruoc 
                        where thang = 202408 and ma_kpi = 'DONGIA' ;
with tien as (
    select  ma_nv,-sum(tien) tien 
    from ttkd_Bsc.tl_giahan_tratruoc 
         where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRU_CA')
    group by ma_nv
)
select *
from ttkd_Bsc.bangluong_dongia_202408 a
    join tien b on a.ma_Nv = b.ma_Nv
where a.giamtru_ghtt_cntt <> b.tien;

select distinct loai_tinh from  ttkd_Bsc.tl_giahan_tratruoc 
                        where thang = 202408 and ma_kpi = 'DONGIA' ;
with tien as (
    select  ma_nv,sum(tien) tien 
    from ttkd_Bsc.tl_giahan_tratruoc 
         where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIA_CHUNG_TU')
    group by ma_nv
)
select *
from ttkd_Bsc.bangluong_dongia_202408 a
    join tien b on a.ma_Nv = b.ma_Nv
where a.giamtru_ghtt_cntt <> b.tien;
;
with tien as (
    select  ma_nv,sum(tien) tien 
    from ttkd_Bsc.tl_giahan_tratruoc 
         where thang = 202408 and ma_kpi = 'DONGIA' and loai_tinh in ('THUHOI_DONGIA_GHTT')
    group by ma_nv
)
select *
from ttkd_Bsc.bangluong_dongia_202408 a
    join tien b on a.ma_Nv = b.ma_Nv
where a.THUHOI_DONGIA_GHTT <> b.tien;
select* from ttkd_Bsc.bangluong_kpi where thang = 202408 and ma_kpi ='HCM_TB_GIAHA_024';in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023','HCM_TB_GIAHA_024','HCM_TB_GIAHA_026','HCM_TB_GIAHA_027');