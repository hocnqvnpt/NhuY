 (select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck
from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101
and ngay_cn <to_date('20241202','yyyyMMdd')
and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20241201','yyyyMMdd'))
and tiengach is null
union all
/*Danh sách rkm_id có thoái c?c t? 1/12/2024*/
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck
from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101
and ngay_cn <to_date('20241202','yyyyMMdd')
and trunc(ngay_thoai)>= to_date('20241201','yyyyMMdd')
and tiengach is null
union all
/*Danh sách rkm_id có g?ch n? lùi k? (g?ch trên form g?ch n? ti?n m?t)*/
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck
from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101
and ngay_cn <to_date('20241202','yyyyMMdd')
and tiengach >=0
union all
/*Danh sách rkm_id có g?ch n? lùi k? (g?ch theo d/s ch? Thanh gui)*/
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(cuoc_tk_lui,0) ton_ck
from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101
and ngay_cn <to_date('20241202','yyyyMMdd')
and cuoc_tk_lui<>0) ;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_SL_COMBO_006' AND TEN_VTCV LIKE 'Phó%' and giao is not null;
select * from ttkd_bsc.bangluong_kpi_202411 where thang = 202411;