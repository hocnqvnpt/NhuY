select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202408 and manv_giao = 'CTV078025';ma_Tb in ('hcm_ca_ivan_00017589',
'hcm_ca_ivan_00018004',
'hcm_ca_ivan_00018054',
'hcm_ca_ivan_00018096',
'hcm_ca_ivan_00018097',
'hcm_ca_ivan_00018098',
'hcm_ca_ivan_00017310',
'hcm_ca_00037631','hcm_ca_00083578');
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409;
select* from ttkd_Bsc.nhanvien where thang = 202409; and ten_nv like '%Duyên';
select* from ttkd_Bsc.nhanvien where thang in ( 202408,202409) and ma_nv in ('VNP019500','CTV021826');
select ma_Gd from css_hcm.phieutt_hd where phieutt_id = 8495476;
select distinct ma_nv, ten_pb, ten_Nv from ttkd_bsc.blkpi_dm_to_pgd where thang = 202409 and (ten_to like '%Sau Bán Hàng%' or ten_to like '%Online%'); ma_nv in ('VNP016983','VNP017496','VNP017366','VNP017456','VNP017585','VNP017947','VNP017729','VNP016898',
'VNP016659','VNP017072','VNP022074','VNP017621','VNP017699');
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202409 and 'HCM-TT/02970518' = ma_gd;
select* from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202409 and ma_Tb = 'mtdung_basic';