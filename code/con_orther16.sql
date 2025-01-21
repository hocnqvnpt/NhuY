with tttb as (
    select DISTINCT THUEBAO_ID, trangthai_Tb, A.TRANGTHAITB_ID
    from tinhcuoc_hcm.dbtb partition for (20231201) a
        left join css_hcm.trangthai_Tb b on a.trangthaitb_id = b.trangthaitb_id

)   
SElect a.thang, A.THUEBAO_ID, a.rkm_id, a.ma_tt,a.MA_TB, a.TEN_DVVT, a.LOAIHINH_TB,  a.THANG_BD, a.THANG_KT, a.TON_CK, a.CUOC_TK, a.NGAY_KTDC, b.trangthaitb_id, B.trangthai_Tb 
from PB a
    left join TTTB b on a.THUEBAO_ID= b.THUEBAO_ID  
where a.thang = 202312;