select* from tdoi_vtcv;
update ttkd_Bsc.nhanvien a
set ten_vtcv = (select T�N_VTCV from tdoi_vtcv where a.ma_nv =M�_NV_HRM )
, ma_Vtcv = (select VTCV_HRM from tdoi_vtcv where a.ma_nv =M�_NV_HRM )
where thang in (202410, 202411) and ma_nv in (select m�_nv_hrm from tdoi_vtcv);
select* from  ttkd_Bsc.nhanvien a where thang in (202410, 202411) ; 
commit;
select* from nhanvien_202410_l2 ;where ma_nv in (select m�_nv_hrm from tdoi_vtcv);
rollback;
update ttkd_Bsc.nhanvien a  set ten_to = initcap(ten_to), ten_vtcv = initcap(ten_vtcv) where thang  in (202410, 202411) and ma_Nv ='VNP017097';
select initcap('Chuy�n Vi�n Gi�N Sa?T') from dual;

