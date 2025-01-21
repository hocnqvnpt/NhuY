SELECT a.* , b.da_giahan sltb_Nhap, b.tien_xp , b.sltb_quydoi sltb_quydoi_nhap FROM CT_TIEN_TP_TEMP a
    left join CT_tIEN_XP b on a.ma_nv = b.NHANVIEN_XUAT_HD and a.loai_tinh = b.loai_tinh and a.ma_kpi = b.ma_kpi 
    where a.loai_tinh in ('DONGIATRA_OB')
union all
select a.thang, a.loai_tinh ,a.ma_kpi, a.NHANVIEN_XUAT_HD, null,null, null,null,null,da_giahan,tien_xp,sltb_quydoi 
from CT_tIEN_XP a where NHANVIEN_XUAT_HD not in (select ma_nv from CT_TIEN_TP_TEMP where loai_tinh in   ('DONGIATRA_OB')   )
and loai_tinh in  ('DONGIATRA_OB');

with tp as 
(
    select ma_nv, SUM(NVL(tien_thuyetphuc,0)*heso_chuky*heso_dichvu) TIEN, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ipcc = 1
    GROUP BY MA_nV
)
,
XP AS 
(
    select NHANVIEN_XUATHD, SUM(NVL(TIEN_XUATHD,0)*heso_chuky*heso_dichvu) TIEN, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' --AND NHANVIEN_XUATHD IS NOT NULL
    GROUP BY NHANVIEN_XUATHD
)
, TIEN AS (
SELECT a.MA_NV, A.TIEN, A.SLTB_QUYDOI, A.SLTB , b.SLTB sltb_Nhap, b.tien TIEN_XUATHD , b.sltb_quydoi sltb_quydoi_nhap FROM TP a
    left join XP b on a.ma_nv = b.NHANVIEN_XUATHD 
union all
select   a.NHANVIEN_XUATHD,  null,null,null,SLTB,tien TIEN_XUATHD,sltb_quydoi 
from XP a 
where NHANVIEN_XUATHD not in (select ma_nv from TP) --) select* from tien;
) --select distinct ma_nv from tien;
--select sum(tien) from tp;
SELECT A.*, B.MA_TO,B.TEN_tO,B.MA_PB, B.TEN_PB, B.MA_VTCV, B.TEN_vTCV
FROM TIEN A
    JOIN TTKD_bSC.NHANVIEN_202405 B ON A.MA_nV= B.MA_NV
where b.ma_pb not in ('VNP0702500','VNP0702400','VNP0702300')
;

;
SELECT* FROM TTKD_bSC.NHANVIEN_202405 WHERE MA_nV=  'CTV079059';
select* from ttkd_bsc.ct_Dongia_Tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB'  ;
select SUM(TIEN) from TTKD_bSC.TL_gIAHAN_TRATRUOC where thang = 202405 AND LOAI_TINH = 'DONGIATRA_OB'-- AND MA_PB IN ('VNP0702500','VNP0702400','VNP0702300');
SELE