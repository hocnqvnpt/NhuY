select count(distinct chungtu_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202411 and tratruoc = 0 and sl_matb = 0 ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 where chungtu_id = 479884;

select * from qltn_hcm.bangphieutra partition for (20241001) where ma_Tt='HCM010065127';
select* from qltn_hcm.ct_tra partition for (20241001) where phieu_id = 2798936
;
  CREATE INDEX IDX_ghtt ON ds_giahan_tratruoc2_ghtt (thuebao_id);
       CREATE INDEX IDX2_ghtt ON ds_giahan_tratruoc2_ghtt (khachhang_id, ngay_kt_mg);
       CREATE INDEX IDX3_Ghtt ON ds_giahan_tratruoc2_ghtt (thuebao_id, ngay_kt_mg);