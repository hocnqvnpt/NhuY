select * from v_thongtinkm_all where thuebao_id = 9197065;
select* from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202408;
select* from v_Thongtinkm_all where ma_tb = 'ctytenwei19';
select so_Ct, ghichu from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02937538';
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_Gd = 'HCM-TT/02937538';
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = 323442;
select distinct thang, thang_ktdc_Cu from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202409;
select* from css_hcm.db_Thuebao where  ma_tb = 'ctytenwei19';
select* from css_hcm.db_adsl where thuebao_id = 8241200;
select* from ttkd_Bsc.nhuy_Ct_ipcc_obghtt where thang = 202408;
update  ttkd_Bsc.ct_Bsc_tratruoc_moi a
set ma_pb = (select distinct ma_pb from ttkd_Bsc.nhanvien where thang = 202408 and ten_pb = a.phong_giao)
where thang = 202409 and ma_pb is null;
rollback;
select distinct phong_giao, ma_pb from  ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202409 ;and ma_pb is null;
commit
;