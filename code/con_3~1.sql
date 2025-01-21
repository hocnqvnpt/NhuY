
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403 and phieutt_id is not null-- and tien_khop is not null


select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202403 and ma_Tb in ('hcm_ca_00073385',
'hcm_ca_ivan_00015264',
'hcm_ca_ivan_00023068',
'hcm_ca_ivan_00025246',
'hcm_ivan_00024765',
'hcm_ivan_00024762',
'hcm_ca_00063093',
'hcm_ca_00062880',
'hcm_smartca_00003831',
'hcm_ca_ivan_00024057',
'hcm_ca_00053747',
'hcm_ca_00038918',
'hcm_smartca_00000770',
'hcm_ca_00053760',
'hcm_smartca_00000821',
'hcm_ca_ivan_00014821',
'hcm_ca_00062071',
'hcm_ivan_00009894',
'hcm_ivan_00009895',
'hcm_ca_00073786',
'hcm_ca_00074306',
'hcm_ca_ivan_00015102',
'hcm_ca_00053866',
'hcm_ivan_00025364',
'hcm_ca_00064532',
'hcm_ca_00074250',
'ivan_00008838',
'hcm_ca_ivan_00014845',
'hcm_ca_00050921',
'hcm_ca_00082344',
'hcm_ca_ivan_00014952'
)
;
select phieutt_id, ma_gd, ht_Tra_id_hp, thuebao_id_khac ,a.* from ttkdhcm_ktnv.ghtt_chotngay_271 a where  ma_Tb = 'hcm_ca_ivan_00015102' and  ngay_chot=to_date('20240402','yyyymmdd')
and thang_kt in (202403,202404) and loaitb_id in   (55 ,80 ,116 ,117,132,140,154,181,288,318 )
;
select b.* from css_hcm.db_Thuebao a left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id 
where   ma_Tb = 'hcm_ca_ivan_00015102';

;
select ma_Tb_moi from css_hcm.db_Cntt
select kieuld_id from css_hcm.hd_thuebao where ma_tb ='hcm_ca_00092725';
select* from css_hcm.kieu_ld
-- in ('hcm_ca_00073385',
'hcm_ca_ivan_00015264',
'hcm_ca_ivan_00023068',
'hcm_ca_ivan_00025246',
'hcm_ivan_00024765',
'hcm_ivan_00024762',
'hcm_ca_00063093',
'hcm_ca_00062880',
'hcm_smartca_00003831',
'hcm_ca_ivan_00024057',
'hcm_ca_00053747',
'hcm_ca_00038918',
'hcm_smartca_00000770',
'hcm_ca_00053760',
'hcm_smartca_00000821',
'hcm_ca_ivan_00014821',
'hcm_ca_00062071',
'hcm_ivan_00009894',
'hcm_ivan_00009895',
'hcm_ca_00073786',
'hcm_ca_00074306',
'hcm_ca_ivan_00015102',
'hcm_ca_00053866',
'hcm_ivan_00025364',
'hcm_ca_00064532',
'hcm_ca_00074250',
'ivan_00008838',
'hcm_ca_ivan_00014845',
'hcm_ca_00050921',
'hcm_ca_00082344',
'hcm_ca_ivan_00014952'
) and  ngay_chot=to_date('20240402','yyyymmdd')
and thang_kt in (202403,202404) and loaitb_id in   (55 ,80 ,116 ,117,132,140,154,181,288,318 )
