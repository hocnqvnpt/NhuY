select a.ma_gd, b.soseri, b.seri, phieutt_id
from vi_cn a
    left join css_hcm.phieutt_hd b on a.ma_gd = b.ma_gd
    order by b.soseri;
    
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB'
union all
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202404 and loai_tinh in ('DONGIATRA_OB', 'DONGIATRA_OB_BS')
UNION ALL 
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202403 and loai_tinh IN ('DONGIATRA','DONGIATRA30D')


;
select DISTINCT A.MA_PB, A.MA_TO, A.MA_nV, TEN_nV, C.TEN_TO, D.TEN_PB, MA_KPI, THANG
from ttkd_bsc.blkpi_dm_to_pgd A
    LEFT JOIN TTKD_BSC.NHANVIEN_202406 B ON A.MA_NV = B.MA_nV 
    LEFT JOIN (SELECT DISTINCT MA_TO, TEN_TO FROM TTKD_BSC.NHANVIEN_202406 ) C ON A.MA_TO = C.MA_TO
    LEFT JOIN (SELECT DISTINCT MA_PB, TEN_PB FROM TTKD_BSC.NHANVIEN_202406 ) D ON A.MA_PB = D.MA_PB
where thang = 202406 and ma_kpi = 'HCM_SL_ORDER_001';