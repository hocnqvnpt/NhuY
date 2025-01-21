select * from ttkd_Bsc.ct_dongia_tratruoc where ma_Tb = 'hcm_ca_00014592';
select distinct ten_vtcv from ttkd_bsc.nhanvien_202405 where ma_nv in  (
select * from ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRU_CA' and ma_Tb in('hcm_ca_00048047','hcm_ca_00077204','hcm_ca_00056148'));
('hcm_ca_00040009',
'hcm_ca_ivan_00016229',
'hcm_ivan_00020602',
'hcm_ca_ivan_00016226',
'hcm_ca_ivan_00015956',
'hcm_ivan_00015947',
'hcm_ca_00040007',
'hcm_ca_00039956',
'hcm_ivan_00010195',
'hcm_ca_00056072',
'hcm_ca_00041260',
'hcm_ca_ivan_00026680',
'hcm_ca_ivan_00026737',
'hcm_ca_00077450');
COMMIT;
SELECT * FROM all_tab_modifications;
insert into TL_GIAHAN_TRATRUOC DELETE FROM ttkd_bsc.TL_GIAHAN_TRATRUOC where thang = 202405 and loai_tinh = 'DONGIATRU_CA';

select a.ma_gd, b.soseri, b.seri
from vi_dn a
left join css_hcm.phieutt_hd b on a.ma_gd = b.ma_Gd
;
select * from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202405; and thuebao_id in (select thuebao_id from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202404);
select* from ttkd_Bsc.nhanvien_202405 where ma_nv in ( 'VNP019948','VNP019530');
select * from ttkd_Bsc.CT_DONGIA_TRATRUOC where thang = 202405 and loai_tinh = 'DONGIATRA_OB_NSG_H'; and rownum = 1;
rollback
;
select HCM_TB_GIAHA_024 from ttkd_Bsc.BANGLUONG_KPI_202405 where     ma_nv in ( 'VNP019948','VNP019530') ;and ma_kpi = 'HCM_TB_GIAHA_024' ;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_024' and thang_kt is null;
SELECT* FROM  TTKD_BSC.TL_gIAHAN_tRATRUOC WHERE THANG = 202405 AND MA_NV= 'VNP019948';
commit;
select* from ttkd_bsc.CT_dONGIA_tRATRUOC where thang = 202405 and ma_Tb=  'hcm_ca_00056377';
UPDATE  ttkd_bsc.CT_dONGIA_tRATRUOC SET DONGIA = 0, GHICHU = '110000;VI TRI KHONG BI TRU DON GIA, CO THAY DOI NHAN VIEN GIAO NGAY' where thang = 202405 and ma_Tb=  'hcm_ca_00056377';
UPDATE TTKD_BSC.TL_gIAHAN_tRATRUOC SET TONG = 2 ,TYLE = 100 , TIEN = 0 WHERE MA_NV = 'VNP019948' AND LOAI_TINH = 'DONGIATRU_CA' AND THANG = 202405;
update ttkd_bsc.CT_dONGIA_tRATRUOC  set MA_NV ='VNP019530' , ma_to = 'VNP0702509' where thang = 202405  and  ma_Tb=  'hcm_ca_00056377';
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202405 and ma_kpi = 'HCM_TB_GIAHA_024';