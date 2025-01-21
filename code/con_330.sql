select LUONG_DONGIA_GHTT_bsT4
        from ttkd_bsc.bangluong_dongia_202404 WHERE TEN_DONVI LIKE 'Phòng Khách Hàng Doanh Nghi?p%'
;
with ti as (
    select ma_Nv, sum(tien) tien from (
    select a.ma_nv, tien
    from TTKD_bSC.tl_Giahan_Tratruoc a
        join ttkd_Bsc.nhanvien_202404 b on a.ma_nv =b.ma_nv 
    WHERE THANG = 202404 and loai_tinh in ('DONGIATRA_OB') and ma_vtcv != 'VNP-HNHCM_BHKV_47'
    union all
    select ma_nv, tien
    from TTKD_bSC.tl_Giahan_Tratruoc a
    WHERE THANG = 202404 and loai_tinh in ('DONGIA_TS_TP_TT')) group by ma_Nv

)
select a.ma_nv, a.ten_donvi, LUONG_DONGIA_GHTT_bsT4, b.tien,TEN_VTCV, MA_VTCV from ttkd_Bsc.bangluong_dongia_202405 a join ti b on a.ma_nv = b.ma_nv
where nvl(a.LUONG_DONGIA_GHTT_bsT4,0) = nvl(b.tien,0);
AND giamtru_ghtt_cntt!=-TIEN;
SELECT* FROM TTKD_bSC.CT_DONGIA_tRATRUOC WHERE THANG = 202405 AND MA_NV ='VNP019520';
SELECT* FROM TTKD_bSC.tl_Giahan_Tratruoc WHERE THANG = 202405 AND MA_NV ='VNP019520';

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
with tp as 
(
    select ma_nv, SUM(NVL(tien_thuyetphuc,0)*heso_chuky*heso_dichvu) TIEN, SUM(HESO_CHUKY*HESO_DICHVU) SLTB_QUYDOI, COUNT(THUEBAO_ID) SLTB
    FROM TTKD_BSC.CT_DONGIA_tRATRUOC
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB' --and ipcc = 1
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
where b.ma_pb not in ('VNP0702500','VNP0702400','VNP0702300');


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
SELECT a.MA_NV, A.TIEN, A.SLTB_QUYDOI, A.SLTB , b.SLTB sltb_Nhap, b.tien TIEN_XUATHD , b.sltb_quydoi sltb_quydoi_nhap 
FROM TP a
    left join XP b on a.ma_nv = b.NHANVIEN_XUATHD 
union all
select   a.NHANVIEN_XUATHD,  null,null,null,SLTB,tien TIEN_XUATHD,sltb_quydoi 
from XP a 
where NHANVIEN_XUATHD not in (select nvl(ma_nv,'a') from TP) --) select* from tien;
) --select distinct ma_nv from tien;
--select sum(tien) from tp;
SELECT A.*, B.MA_TO,B.TEN_tO,B.MA_PB, B.TEN_PB, B.MA_VTCV, B.TEN_vTCV
FROM TIEN A
    JOIN TTKD_bSC.NHANVIEN_202405 B ON A.MA_nV= B.MA_NV
where b.ma_pb not in ('VNP0702500','VNP0702400','VNP0702300');

select* from ttkd_bct.hocnq_cp_nhancong_hoahong; where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
insert into ttkd_bct.hocnq_cp_nhancong_hoahong ;
select thang, thang, a.ma_Tb, null,null, null, c.loaihinh_Tb , ma_pb, a.ma_nv , ten_nv, null, tien_thanhtoan, sothang_Dc,null, null, null, round( from ttkd_bsc.ct_dongia_tratruoc a
left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
left join css_hcm.loaihinh_Tb c on loaitb_id = c.loaitb_id
left join ttkd_Bsc.nhanvien_202405 d on a.ma_nv = d.ma_Nv 
where thang = 202404 and loai_tinh = 'DONGIATRA_OB';
