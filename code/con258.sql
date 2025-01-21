update ttkd_bsc.bangluong_kpi_202406 a
set hcm_tb_giaha_026 =  (select nvl(diem_cong,-diem_tru) from ttkd_bsc.bangluong_KPI where thang = 202406  and ma_kpi = 'HCM_TB_GIAHA_026' and a.ma_nv = ma_nv)
WHERE  exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi in ('HCM_TB_GIAHA_026')
and thang_kt is null and MA_VTCV = a.MA_VTCV);
COMMIT;
COMMIT;
rollback;
insert into ipcc;
select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang = 202406 and loai_tinh = 'DONGIATRA_OB';
select 289480+227714777.5 from DUAL;
select* from ttkd
create table no_ipcc as ;
		select* from TTKD_BSC.blkpi_danhmuc_kpi where ma_kpi ='HCM_SL_COMBO_006' and thang_kt is null

with tp as 
(
    select ma_nv, SUM(NVL(tien_thuyetphuc,0)*heso_chuky*heso_dichvu) TIEN, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202406 and loai_tinh = 'DONGIATRA_OB' --and ipcc = 1
    GROUP BY MA_nV
)
,
XP AS 
(
    select NHANVIEN_XUATHD, SUM(NVL(TIEN_XUATHD,0)*heso_chuky*heso_dichvu) TIEN, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202406 and loai_tinh = 'DONGIATRA_OB' --AND NHANVIEN_XUATHD IS NOT NULL
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
    JOIN TTKD_bSC.NHANVIEN B ON A.MA_nV= B.MA_NV and 202406 = b.thang and b.donvi = 'TTKD'
where b.ma_pb not in ('VNP0702300','VNP0702400','VNP0702500')