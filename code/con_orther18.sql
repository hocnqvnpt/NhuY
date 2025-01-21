select  a.rkm_id,a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, SO_THANGKM, b.ton_ck, b.ngay_cn, b.thang_bd,b.thang_kt
from tmp_mesh_ton a
    join qltn_hcm.db_Datcoc b on  a.thuebao_id = b.thuebao_id and  b.kyhoadon = 20240901
where b.thang_Bd >= 202410 --and to_number(to_char(ngay_cn,'yyyymmdd')) > 20241001
;
select sum(ton_ck) from qltn_hcm.db_Datcoc where thang_bd >= 202410 and kyhoadon = 20240901 and loaitb_id = 210 ;and  to_number(to_char(ngay_cn,'yyyymmdd')) <= 20241010
