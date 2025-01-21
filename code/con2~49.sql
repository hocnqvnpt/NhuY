select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a where ma = 'HCM-TT/03101805';
select* from ttkd_bsc.ct_Bsc_trasau_tp_Tratruoc where thang = 202411 and ht_Tra_id = 2 and tien_khop = 0 and ma_gd  in (
select ma_Gd from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a where ma = 'HCM-TT/03101805');

select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a where ma = 'HCM-TT/03079021';
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = 462632