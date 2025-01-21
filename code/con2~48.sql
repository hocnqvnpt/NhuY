--select sum(ton_Ck) from (
create table ton_202308_Erp as 
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20230801

and ngay_cn <to_date('20230902','yyyyMMdd')

and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20230901','yyyyMMdd'))

and tiengach is null

union all

/*Danh s�ch rkm_id c� tho�i c?c t? 1/11/2024*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20230801

and ngay_cn <to_date('20230902','yyyyMMdd')

and trunc(ngay_thoai)>= to_date('20230901','yyyyMMdd')

and tiengach is null

union all

/*Danh s�ch rkm_id c� g?ch n? l�i k? (g?ch tr�n form g?ch n? ti?n m?t)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck

from qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20230801

and ngay_cn <to_date('20230902','yyyyMMdd')

and tiengach >=0