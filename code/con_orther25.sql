select * from bhol_giao 
where luonggiao_ttvt is null and (KETQUA_LSOB_DAUTIEN!= 'Kh�ng g?p/ kh�ng t�m ???c kh�ch h�ng ' or
KETQUA_LSOB_SAUCUNG !='Kh�ng g?p/ kh�ng t�m ???c kh�ch h�ng ');
alter table bhol_giao add (thang_kt number, thang number);
update bhol_giao set thang= 202412, thang_kt = 202412 ;
commit; 
select * from css_hcm.ct_khuyenmai;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202412;