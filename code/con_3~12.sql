select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202404 and ma_Tb in ('mesh0135060','hcmmich2015','hcmthanhphungf49','thanhtung9','ncthe2011','mesh0187954'
hcmk28-tt
hcmkyson1972);

select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where rkm_id in (select rkm_id from nhuy_ct_Bsc_ipcc_obghtt) and thang = 202404
;

select* from temp_Tt where ma_Tb in ('mesh0135060',
'hcmmich2015',
'hcmthanhphungf49',
'thanhtung9',
'ncthe2011',
'mesh0187954',
'hcmk28-tt',
'hcmkyson1972');

select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202404 and ma_Tb in ('mesh0135060',
'hcmmich2015',
'hcmthanhphungf49',
'thanhtung9',
'ncthe2011',
'mesh0187954',
'hcmk28-tt',
'hcmkyson1972');