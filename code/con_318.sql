select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202404 and ma_tb in ('hcm_ca_00013722','hcm_smartca_00000817','hcm_ca_00054001','hcm_ca_00055697');

select* from css_hcm.hd_Thuebao where ma_Tb = 'kimchuc93756723';
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202404 and thuebao_id in  (3044739,9677586);
select* from temp_tt where thuebao_id = 9677586;
select* from css_Hcm.tb_Uudai where thuebao_id = 2650771;
select* from css_hcm.ct_phieutt where  hdtb_id = 19959059;
select* from css_hcm.db_cntt where thuebao_id in (
select thuebao_id from ttkd_Bsc.ct_bsc_giahan_cntt where ma_tb in ('hcm_ca_ivan_00015680',
'hcm_smartca_00000817',
'hcm_ca_00054001',
'hcm_ca_00055697',
'hcm_smartca_00000316',
'hcm_ca_00039240',
'hcm_ca_ivan_00015722',
'hcm_ca_00014592',
'hcm_ca_00054943') and thang = 202404
);

select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202404 and ma_Tb in 
('hcm_ca_00054808','hcm_ca_00054804','hcm_ca_00054819','hcm_ca_00054801','hcm_ca_00054817','hcm_ca_00054810',
'hcm_ca_00054814','hcm_ca_00054809','hcm_ca_00054806','hcm_ca_00054803','hcm_ca_00054795');
select* from ttk;
SELECT* from  ttkd_Bsc.ct_Bsc_giahan_cntt where MANV_GIAO = 'VNP028210'and thang = 202404;
insert into ct_Bsc_giahan_cntt ;
select* from  ct_bsc_giahan_cntt where ma_tb = 'hcm_ca_00013722' and thang = 202404;
rollback;
;VNP0702500;
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 and ma_To = 'VNP0702508' and ma_pb = 'VNP0702500' and ma_kpi = 'HCM_TB_GIAHA_024'

select* from ttkd_Bsc.tl_giahan_tratruoc where ma_PB = 'VNP0702500' and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_024' AND THANG = 202404;
update  ttkd_Bsc.tl_giahan_tratruoc  ;
set tong = 33 , TYLE = ROUND(14*100/33,2)
where ma_to = 'VNP0702508' and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_TB_GIAHA_024' AND THANG = 202404;
select hcm_tb_giaha_024 from ttkd_bsc.bangluong_kpi_202404 where ma_Nv_hrm = 'VNP028210';
commit;
INSERT INTO tl_giahan_tratruoc;
DELETE from TTKD_BSC.tl_giahan_tratruoc where thang = 202404 and loai_tinh = 'DONGIATRU_CA';
select * from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240503','yyyymmdd') and  ma_tb ='hcm_ca_00013722'-- in ('hcm_ca_ivan_00015680',
'hcm_smartca_00000817',
'hcm_ca_00054001',
'hcm_ca_00055697',
'hcm_smartca_00000316',
'hcm_ca_00039240',
'hcm_ca_ivan_00015722',
'hcm_ca_00014592',
'hcm_ca_00054943') ;
update  ttkd_bsc.ct_dongia_tratruoc set tien_dc_cu = 550000, dongia = 55000 where ma_Tb = 'hcm_smartca_00000817';
commit;
select* from css_hcm.db_thuebao  where ma_tb ='hcmlqhung12';
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202404 and ma_tb in ('cchuynhvanchinh2_d4.14',
'tdng261220',
'vphuong5242673',
'hcmmnhut_vnpt',
'ml0305',
'vbn1112',
'ml0305',
'hcm_bnhthun_3',
'manhhung93747451',
'hcm0ngoctuyet',
'hcmkht84a',
'hcm.nvtan.ncck12',
'huyen814',
'tngocminhtrang15',
'dungk1t14',
'mesh0122661',
'nttphuong270422',
'vovanthao789',
'mesh0055360',
'hcm_tngkhi',
'mesh0055359',
'ngocthenguyen',
'b1ccdt_8b',
'bnhthun_16',
'mesh0118336',
'mesh0118337',
'chau0304',
'phamvanan86',
'hcmhuyen814',
'hcm_manhhung21',
'lienoik',
'thanhphong_85',
'ctytuyetlights',
'tphuong0422',
'dqv114a',
'htkp75',
'hcm_bnhthun_3',
'bnhthun_16',
'lht2808',
'nhtphuong93746346',
'hung107',
'thanhphong_85',
'tnquyen',
'hcmdqv114a',
'hcm-nhhung-hkn',
'hcmminhlocgd0',
'hcm_bnhthun_3',
'hcmphamvanan86',
'27-ac',
'hcm_chau0304',
'kht84a',
'ctycongnhan_f26',
'lphuocthang2022',
'hcm0ngoctuyet',
'ohthu90',
'bnhthun_16',
'mesh0118336',
'ngodieuvan09',
'hcmtminh2016',
'hcm8b',
'nhhung-hkn',
'hcm_vphuong_4',
'nttphuong270422',
'dung_2018',
'phamvanan86',
'kdt72',
'nvtan.ncck12',
'lequoctri2015',
'hcmthhoanh34110',
'173-kv3e',
'tonglb113',
'ptthanhtuyen',
'hcmlan76-40',
'lqhung12',
'vtnyen5765',
'ngocthenguyen',
'mesh0056162',
'qdkhoa507',
'mesh0118337',
'hcmhung107',
'lamxahao55',
'mesh0056164',
'ntminh2016',
'hcmnttphuong27',
'hcmphamvanan86',
'hcmnttphuong27',
'dung298-2',
'hcmdqv114a',
'dung298-5',
'tngkhi93866303',
'32-oik20',
'lan76-40');