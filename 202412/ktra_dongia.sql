--- kiem tra tien GHTT
WITH TIE AS (
    SELECT MA_nV, SUM(TIEN) TIEN
    FROM ttkd_Bsc.tl_Giahan_Tratruoc A
where thang = 202412 and loai_Tinh IN( 'DONGIATRA_OB' ,'DONGIA_TS_TP_TT','DONGIATRA_OB_BS')
GROUP BY MA_nV
)
SELECT A.*, B.LUONG_DONGIA_GHTT
FROM TIE A 
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202412 B ON A.MA_NV = B.MA_NV
where NVL((A.TIEN),0) != NVL(B.LUONG_DONGIA_GHTT,0);
select sum(tien) from ttkd_Bsc.tl_giahan_tratruoc where thang = 202411 and loai_tinh ='DONGIATRA_OB';


--- kiem tra don gia thu hoi GHTT
select A.TIEN, B.THUHOI_DONGIA_GHTT, B.TEN_nV, B.TEN_PB
from ttkd_Bsc.tl_Giahan_Tratruoc A
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202412 B ON A.MA_NV = B.MA_NV
where thang = 202412 and loai_Tinh = 'THUHOI_DONGIA_GHTT' AND NVL((A.TIEN),0) != NVL(B.THUHOI_DONGIA_GHTT,0);
--- kiem tra don gia gach no chung tu
select A.TIEN, B.LUONG_DONGIA_CHUNGTU, B.TEN_nV, B.TEN_PB
from ttkd_Bsc.tl_Giahan_Tratruoc A
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202412 B ON A.MA_NV = B.MA_NV
where thang = 202412 and loai_Tinh = 'DONGIA_CHUNG_TU' AND NVL((A.TIEN),0) = NVL(B.LUONG_DONGIA_CHUNGTU,0);
--- kiem tra don gia thu hoi CNTT
WITH TIE AS (
    SELECT MA_nV, SUM(TIEN) TIEN
    FROM ttkd_Bsc.tl_Giahan_Tratruoc A
where thang = 202412 and loai_Tinh IN( 'DONGIATRU_CA' ,'','')
GROUP BY MA_nV
)
SELECT A.*, B.GIAMTRU_GHTT_CNTT
FROM TIE A 
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202412 B ON A.MA_NV = B.MA_NV
where  NVL((-A.TIEN),0) != NVL(B.GIAMTRU_GHTT_CNTT,0);
select GIAMTRU_GHTT_CNTT from ttkd_Bsc.bangluong_dongia_202412 where ma_nv = 'VNP017855';
(select -sum(tien) from ttkd_bsc.tl_giahan_tratruoc 
                        where thang = 202412 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_nv = 'VNP017855'
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0) ;
select sum(tien) from ttkd_bsc.tl_giahan_Tratruoc where loai_tinh = 'DONGIATRU_CA' and thang = 202412 and ma_kpi = 'DONGIA' and ma_Nv  = 'VNP017855';