create table bsc_ctu  as select* from nhuy.ct_Bsc_Chungtu_temp ;
update ct_Bsc_Chungtu_temp a
set tinh_bsc = 0
where exists (select distinct chungtu_id, ma from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc 
where to_char(ngay_ht,'yyyymm') = '202410' and tudong = 1 and a.chungtu_id = chungtu_id and (a.ma_tt)= ma);
update ct_Bsc_Chungtu_temp set tinh_bsc = 0 where tinh_Dongia = 0 and tra_Truoc = 1 and ma_gd in ('HCM-TP/00068838','HCM-LD/01966542');
select* from  ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc  where ma in ('HCM-TP/00068838','HCM-LD/01966542');
select* from css_Hcm.phieutt_Hd where ma_Gd in ('HCM-TP/00068838','HCM-LD/01966542');
commit;
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202410 and ma_Nv ='CTV041777';
