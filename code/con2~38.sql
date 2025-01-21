update ttkd_Bsc.ct_bsc_giahan_cntt 
set tien_khop = 1 ,
ma_Gd = ,
matb_khac = ''
where thang = 202404 and ma_tb in ('hcm_ca_00039226');
select* from ttkd_bsc.ct_dongia_tratruoc where ma_Tb in ('lytuthanh',
'fiber_vanhien',
'hcmlht2808',
'dung298-3',
'minhlocgd0',
'dung_2018',
'hcmvanthao789',
'hcmtphu-htv5',
'dung298-1',
'tphu-htv5',
'dung298-4',
'mnhut_vnpt',
'thihoanh34110',
'hcmlpthang2022',
'lulehoa',
'htkp75',
'hcmlqhung12',
'cchuynhvanchinh2_d4.14',
'tdng261220',
'vphuong5242673') and thang = 202403;
select distinct ten_vtcv , ma_vtcv from ttkd_Bsc.nhanvien_202403 where ma_Nv = 'VNP016578';
 ma_Vtcv in ('VNP-HNHCM_BHKV_18',
                                                    'VNP-HNHCM_BHKV_2.2',
                                                    'VNP-HNHCM_BHKV_14',
                                                    'VNP-HNHCM_BHKV_2',
                                                    'VNP-HNHCM_BHKV_12',
                                                    'VNP-HNHCM_BHKV_18.1');
select* from ttkd_bsc.ct_dongia_tratruoc where thang >= 202403 and ma_nv = 'VNP016578';

