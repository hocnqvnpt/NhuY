--- kiem tra tien GHTT
WITH TIE AS (
    SELECT MA_nV, SUM(TIEN) TIEN
    FROM ttkd_Bsc.tl_Giahan_Tratruoc A
where thang = 202411 and loai_Tinh IN( 'DONGIATRA_OB' ,'DONGIA_TS_TP_TT')
GROUP BY MA_nV
)
SELECT A.*, B.LUONG_DONGIA_GHTT
FROM TIE A 
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202411 B ON A.MA_NV = B.MA_NV
where NVL((A.TIEN),0) != NVL(B.LUONG_DONGIA_GHTT,0);
--- kiem tra don gia thu hoi GHTT
select A.TIEN, B.THUHOI_DONGIA_GHTT, B.TEN_nV, B.TEN_PB
from ttkd_Bsc.tl_Giahan_Tratruoc A
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202411 B ON A.MA_NV = B.MA_NV
where thang = 202411 and loai_Tinh = 'THUHOI_DONGIA_GHTT' AND NVL((A.TIEN),0) = NVL(B.THUHOI_DONGIA_GHTT,0);
--- kiem tra don gia gach no chung tu
select A.TIEN, B.LUONG_DONGIA_CHUNGTU, B.TEN_nV, B.TEN_PB
from ttkd_Bsc.tl_Giahan_Tratruoc A
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202411 B ON A.MA_NV = B.MA_NV
where thang = 202411 and loai_Tinh = 'DONGIA_CHUNG_TU' AND NVL((A.TIEN),0) = NVL(B.LUONG_DONGIA_CHUNGTU,0);
--- kiem tra don gia thu hoi CNTT
select A.TIEN, B.GIAMTRU_GHTT_CNTT, B.TEN_nV, B.TEN_PB
from ttkd_Bsc.tl_Giahan_Tratruoc A
    JOIN TTKD_bSC.BANGLUONG_DONGIA_202411 B ON A.MA_NV = B.MA_NV
where thang = 202411 and loai_Tinh = 'DONGIATRU_CA' AND NVL((-A.TIEN),0) != NVL(B.GIAMTRU_GHTT_CNTT,0);