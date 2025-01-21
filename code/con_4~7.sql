select* from v_Thongtinkm_all where ma_tb = 'tuyen29221';
alter table giao_oneb add thang number(6);
update giao_oneb set thang = 202411,thang_kt = 202411 where thang is null;
commit;
select* from giao_oneb where thang = 202411;
update giao_oneb set thang_kt = 202410 where thang =202411;

select* from css_hcm.hinhthuc_Tra where ht_Tra_id in (207,214,216,204)
;
select* from bhol;


select* from tmp3_60ngay_Bs where ma_Tb='mesh0127210';
select* from css_hcm.hinhthuc_Tra where ht_Tra_id in (1, 7,204,214,216,208);

select count(distinct chungtu_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202411 and tratruoc = 0 and sl_matb = 0 ;