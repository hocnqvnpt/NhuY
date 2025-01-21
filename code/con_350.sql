select  a.*, q.quan_id, q.ten_quan, da.ma_duan ,case when KT_TRATRUOC_FIBER is not null then
                                        MONTHS_BETWEEN(TO_DATE(KT_TRATRUOC_FIBER, 'YYYYMM'), TO_DATE(202411, 'YYYYMM')) end AS sothang_conlai
from fina a
    left join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id
    left join css_Hcm.diachi_Tb c on a.thuebao_id= c.thuebao_id
    left join css_hcm.diachi d on c.diachild_id = d.diachi_id
    left join css_hcm.quan q on d.quan_id = q.quan_id
    left join css_hcm.toanha t on b.toanha_id = t.toanha_id
    left join css_hcm.duan da on t.duan_id = da.duan_id
    
    ;