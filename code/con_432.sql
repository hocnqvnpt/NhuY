with ds as (
select thuebao_id, rkm_id, to_Char(ngay_kt_mg,'yyyymm') thang_kt  from ds_giahan_Tratruoc2 a
) 
, ten_pb as (
    select distinct ma_pb, ten_pb
    from ttkd_Bsc.nhanvien where thang = 202411 and donvi = 'TTKD'
)
select a.thang_kt, C.TEN_PB, count(distinct a.thuebao_id) as sltb
    from ds a
    LEFT join ttkd_Bct.db_Thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    LEFT JOIN TEN_PB C ON B.MAPB_QL = C.MA_PB
WHERE THANG_KT BETWEEN 202501 AND 202512

    group by a.thang_kt, C.TEN_PB
    ORDER BY THANG_KT, TEN_PB;
    
    select* from css_hcm.congnghe;