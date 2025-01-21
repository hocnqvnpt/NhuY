select* from (
select  a.thuebao_id ,a.dichvuvt_id, a.loaitb_id,c.rkm_id, greatest(c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.db_datcoc c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and a.trangthaitb_id not in (7,9) and
    greatest (c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412
union all
select  a.thuebao_id ,a.dichvuvt_id, a.loaitb_id,c.rkm_id, greatest(c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.khuyenmai_Dbtb c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and  a.trangthaitb_id not in (7,9) and
    greatest (c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412
  ) where thuebao_id not in (select thuebao_id from (
     
with km as (
            	select thuebao_id, thang_bddc, thang_ktdc, hieuluc, ttdc_id, datcoc_csd from css_hcm.khuyenmai_dbtb where hieuluc=1 and ttdc_id=0
            	union all
            	select thuebao_id, thang_bd, thang_kt, hieuluc, ttdc_id, cuoc_dc from css_hcm.db_datcoc where hieuluc=1 and ttdc_id=0
        	)
select b.dichvuvt_id
             	, a.datcoc_csd, b.loaitb_id,a.thuebao_id
               	, a.thang_bddc,a.thang_ktdc
              	, null ngay_bd, null ngay_kt ,thang_ktdc thang_kt
  from km a
                	, css_hcm.db_thuebao b
  where a.thuebao_id=b.thuebao_id              	 
                  	and b.trangthaitb_id not in (7,9)   and   b.dichvuvt_id  in (7,8,9)
                 	and  thang_ktdc  between 202401 and 202412
union all
select b.dichvuvt_id
             	, a.datcoc_csd, b.loaitb_id,a.thuebao_id
               	, a.thang_bddc,a.thang_ktdc
              	, d.ngay_bd,d.ngay_kt , to_number(to_char(d.ngay_kt, 'yyyymm')) thang_kt
  from km a
                	, css_hcm.db_thuebao b
                	, css_hcm.db_cntt d      	 
  where a.thuebao_id=b.thuebao_id              	 
                  	and b.trangthaitb_id not in (7,9)   and   b.dichvuvt_id  in (13,14,15,16) 
                  	and a.thuebao_id=d.thuebao_id (+)
                 	and  to_number(to_char(d.ngay_kt, 'yyyymm'))  between 202401 and 202412
              	));

select* from css_Hcm.db_datcoc where rkm_id = 8226755;
