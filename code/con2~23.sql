select count(thuebao_id) sl_record_trung, 202403 thang_kt 
from (select  thuebao_id, thang_kt, count(thuebao_id) sl_record_trung
                from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202403
                group by thuebao_id, thang_kt
                having count(thuebao_id) > 1)
                select* from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202403;
                
                select* from tmp3_60ngay