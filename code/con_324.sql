select* from ttkd_bsc.nhanvien where ten_Nv like '%Hà';

update ttkd_bsc.CT_BSC_TRASAU_TP_TRATRUOC set tien_khop = 2 where ma_gd in (select ma_tt from qrcode where thang = 202405) and thang = 202405 and tien_khop != 2;
commit;

select* from css_hcm.db_Thuebao where ma_Tb=  'fvn_d1006hp';
select* from css.V_dB_dATCOC@dataguard where thuebao_id = 12263680;
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405 and rkm_id in (select rkm_id from ttkd_bsc.CT_BSC_TRASAU_TP_TRATRUOC where thang >= 202402);

