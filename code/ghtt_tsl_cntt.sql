with km as (
            	select a.thuebao_id, a.thang_bddc, a.thang_ktdc, a.hieuluc, a.ttdc_id, a.datcoc_csd, b.chitietkm_id, b.ten_Ctkm
                from css_hcm.khuyenmai_dbtb a
                    join css_hcm.ct_khuyenmai b on a.chitietkm_id = b.chitietkm_id 
                    where a.hieuluc=1 and a.ttdc_id=0
            	union all
            	select a.thuebao_id, a.thang_bd, a.thang_kt, a.hieuluc, a.ttdc_id, a.cuoc_dc , b.chitietkm_id, b.ten_Ctkm
                from css_hcm.db_datcoc a
                    join css_hcm.ct_khuyenmai b on a.chitietkm_id = b.chitietkm_id
                where a.hieuluc=1 and a.ttdc_id=0
        	)
select b.dichvuvt_id
             	, a.datcoc_csd, b.loaitb_id,a.thuebao_id
               	, a.thang_bddc,a.thang_ktdc
              	, null ngay_bd, null ngay_kt ,thang_ktdc thang_kt, a.chitietkm_id, a.ten_Ctkm
  from km a
                	, css_hcm.db_thuebao b
  where a.thuebao_id=b.thuebao_id              	 
                  	and b.trangthaitb_id not in (7,9)   and   b.dichvuvt_id  in (7,8,9)
                 	and  thang_ktdc  between 202401 and 202412
union all
select b.dichvuvt_id
             	, a.datcoc_csd, b.loaitb_id,a.thuebao_id
               	, a.thang_bddc,a.thang_ktdc
              	, d.ngay_bd,d.ngay_kt , to_number(to_char(d.ngay_kt, 'yyyymm')) thang_kt, a.chitietkm_id, a.ten_Ctkm
  from km a
                	, css_hcm.db_thuebao b
                	, css_hcm.db_cntt d      	 
  where a.thuebao_id=b.thuebao_id              	 
                  	and b.trangthaitb_id not in (7,9)   and   b.dichvuvt_id  in (13,14,15,16) 
                  	and a.thuebao_id=d.thuebao_id (+)
                 	and  to_number(to_char(d.ngay_kt, 'yyyymm'))  between 202401 and 202412;