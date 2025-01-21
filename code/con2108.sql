select* from css_hcm.db_Thanhtoan;
select ma_Tb, b.trangthaitb_id from css.v_db_adsl@dataguard a
    join css.v_Db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id 
    where congnghe_id = 11 and loaitb_id in (58,59) and ma_Tb ='720_groupeseb'
    ;
    select* from css.v_db_thuebao@dataguard where ma_Tb='1467807';
-- thu duc ggs
delete from ttkd_Bsc.ct_dongia_tratruoc where thang = 202411 and  ma_tb = 'hcm_ca_ivan_00019903';
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_Nv ='VNP020789' and ma_kpi = 'HCM_TB_GIAHA_024';
UPDATE TTKD_bSC.BANGLUONG_KPI SET GIAO = 8, TYLE_tHUCHIEN = ROUND(3*100/8,2) WHERE thang = 202411 and ma_Nv ='VNP020789' and ma_kpi = 'HCM_TB_GIAHA_024';
COMMIT;

-- dn3 ggs
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and ma_Tb = 'hcm_ca_ivan_00020231';

update ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop  = 1, ma_tb_khac = 'hcm_ca_00112483' where thang = 202411 and ma_Tb = 'hcm_ca_ivan_00020231';

commit;
update ttkd_Bsc.ct_dongia_tratruoc set tien_khop  = 1, dongia = 0 where thang = 202411 and ma_Tb = 'hcm_ca_ivan_00020231';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_Nv ='CTV073464' and ma_kpi = 'HCM_TB_GIAHA_024';
UPDATE TTKD_bSC.BANGLUONG_KPI SET thuchien = 3, TYLE_tHUCHIEN = ROUND(3*100/4,2), diem_tru = 0
WHERE thang = 202411 and ma_Nv ='VNP020789' and ma_kpi = 'HCM_TB_GIAHA_024';


--- 49 TB tu du dn3
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and kenhthu = 'TT cham thanh toan';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and TEN_nV LIKE '%Hi?n' and ma_Kpi = 'HCM_TB_GIAHA_024';
update ttkd_Bsc.bangluong_kpi set giao = 5+152, thuchien = 5+38, tyle_thuchien = round((5+38)/(5+152),2) where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024'
AND MA_nV = 'VNP017793';
COMMIT;
update  ttkd_Bsc.ct_Bsc_giahan_cntt x 
set kenhthu = 'TT cham thanh toan',
ma_Gd = (select ma_gd from  ttkdhcm_ktnv.id271_dn3_bvtudu_0112 a ,css_hcm.hd_thuebao b ,css_hcm.phieutt_hd c 
where a.matb=b.ma_tb and b.kieuld_id=13248 and b.ngay_ht >=to_date('01/05/2024','dd/mm/yyyy') 
and b.hdkh_id=c.hdkh_id and b.ma_Tb = x.ma_tb)
where thang = 202411 and tien_khop = 0 and ma_gd is null;
update ttkd_Bsc.ct_bsc_Giahan_cntt set tien_khop = 1  where thang = 202411 and kenhthu = 'TT cham thanh toan';
commit;
---
update ttkd_Bsc.ct_dongia_tratruoc set dongia = 0, tien_khop = 1 where thang = 202411 and thuebao_id in (
select thuebao_id from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and kenhthu = 'TT cham thanh toan'
);
commit;
---




