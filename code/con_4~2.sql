select loaitb_id from css_hcm.loaihinh_Tb where dichvuvt_id in (7,8,9)

select distinct dichvuvt_id from css_hcm.db_cntt a join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id;
select * from css_hcm.db_thuebao where thuebao_id = 3318353
select dichvuvt_id from css_hcm.db_Datcoc --where loaitb_id in (select loaitb_id from css_hcm.loaihinh_Tb where dichvuvt_id in (7,8,9)
)

select a.thuebao_id, a.loaitb_id, greatest(b.thang_kt_Mg,b.thang_kt) thang_kt
from css_hcm.db_Thuebao a
    join css_hcm.db_datcoc b on a.thuebao_id = b.thuebao_id and b.hieuluc = 1 and b.ttdc_id = 0
where a.dichvuvt_id in (7,8,9) and thang_kt between 202401 and 202412 -- and b.thang_kt < b.thang_kt_Mg  --and a.trangthaitb_id not in (7,8,9)
union all
select a.thuebao_id, a.loaitb_id, b.*--greatest(b.thang_kt_Mg,b.thang_kt) thang_kt
from css_hcm.db_Thuebao a
    join css_hcm.khuyenmai_dbtb b on a.thuebao_id = b.thuebao_id  and b.hieuluc = 1 and b.ttdc_id = 0
where a.dichvuvt_id in (7,8,9) and thang_kt between 202401 and 202412;

select a.thuebao_id , a.loaitb_id, greatest(to_Number(to_Char(b.ngay_kt,'yyyymm')), thang_kt,thang_kt_mg) thang_kt
from css_hcm.db_Thuebao a
    join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.db_datcoc c on b.thuebao_id = c.thuebao_id  and c.hieuluc = 1 and c.ttdc_id = 0
where a.dichvuvt_id in (13,14,15,16) and thang_kt between 202401 and 202412;
union all
select a.thuebao_id , a.loaitb_id, c.*--greatest(to_Number(to_Char(b.ngay_kt,'yyyymm')), thang_kt,thang_ktdc) thang_kt
from css_hcm.db_Thuebao a
    join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.khuyenmai_dbtb c on b.thuebao_id = c.thuebao_id  and c.hieuluc = 1 and c.ttdc_id = 0
where a.dichvuvt_id in (13,14,15,16) and thang_kt between 202401 and 202412;
select* from css_hcm.db_datcoc where thuebao_id = 8805523