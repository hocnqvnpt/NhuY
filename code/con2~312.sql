update  ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop = 1 where ma_tb in ('hcm_smartca_00087421','hcm_smartca_00087416', 'hcm_smartca_00087354') and thang = 202409;
select* from  ttkd_Bsc.ct_dongia_Tratruoc where ma_tb in ('hcm_smartca_00087421','hcm_smartca_00087416', 'hcm_smartca_00087354') and thang = 202409;
select a.ma_nv, a.ten_nv, a.makenh, a.so_dt,b.ten_dv,c.ten_Dv donvi_cha, a.email, a.ngay_login, a.gioitinh, a.ngay_sn , d.ma_nd, d.trangthai , e.hinhthuc_hd
from admin.v_nhanvien@dataguard a
    left join admin.v_donvi@dataguard b on a.donvi_id = b.donvi_id
    left join admin.v_donvi@dataguard c on b.donvi_cha_id = c.donvi_id
    left join admin.v_nguoidung@dataguard d on a.nhanvien_id = d.nhanvien_id
    left join hrm.hinhthuc_Hd@dataguard e on a.hthd_id = e.hthd_id
where c.donvi_id in ( 283426 ); and d.trangthai = 1 and to_numbeR(to_char(a.ngay_login,'yyyymm')) >=202401; and makenh is not null;
    left join ;
select* from  hrm.hinhthuc_Hd@dataguard;
rollback;
select* from ttkd_Bsc.bangluong_kpi where thang = 202409 and ma_kpi ='HCM_SL_COMBO_006' and ten_nv like '%H?ng';
select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202409 and ma_Tb='bht210726'; in ('hcm_ca_ivan_00018768','hcm_ca_ivan_00018661');
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202409 and ma_Tb in ('hcm_smartca_00087354','hcm_smartca_00087416','hcm_smartca_00087421');
update  ttkd_Bsc.ct_dongia_tratruoc set tien_khop = 1, dongia = 0 where thang = 202409 and ma_Tb in ('hcm_smartca_00087354','hcm_smartca_00087416','hcm_smartca_00087421');
update ttkd_Bsc.ct_Bsc_Giahan_cntt set tien_khop = 1 , ma_Gd = '00936983' where thang = 202409 and ma_Tb in ('hcm_smartca_00087421');
commit;
delete from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202409 and ma_Tb in ('hcm_ca_00042917','hcm_ca_00043028','hcm_ca_00043063','hcm_ca_00042835','hcm_ca_00043066',
'hcm_ca_00042735','hcm_ca_00043048','hcm_ca_00042707','hcm_ca_00042825','hcm_ca_00043032','hcm_ca_00043059','hcm_ca_00043457','hcm_ca_00043476');
delete from ttkd_Bsc.ct_dongia_tratruoc where thang = 202409 and ma_Tb in ('hcm_ca_00042917','hcm_ca_00043028','hcm_ca_00043063','hcm_ca_00042835','hcm_ca_00043066',
'hcm_ca_00042735','hcm_ca_00043048','hcm_ca_00042707','hcm_ca_00042825','hcm_ca_00043032','hcm_ca_00043059','hcm_ca_00043457','hcm_ca_00043476');
select* from  admin.v_Donvi@dataguard ;
select owner , table_name from all_tab_columns where column_name = upper('HTHD_ID');
SELECT* FROM ttkd_bsc.bangluong_kpi where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_024' AND TEN_nV = 'Nguy?n Th? Trúc Ph??ng';
SELECT* FROM TTKD_bSC.CT_bSC_GIAHAN_CNTT WHERE THANG = 202409 AND MANV_GIAO ='VNP027831';
UPDATE  ttkd_bsc.bangluong_kpi SET THUCHIEN =NULL, TYLE_THUCHIEN = NULL, DIEM_CONG = NULL, DIEM_tRU = NULL
where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_028' AND MA_VTCV ='VNP-HNHCM_KHDN_4' AND MA_NV!='VNP017445';
SELECT* FROM V_THONGTINKM_ALL WHERE  MA_tB ='sjcgovap_2020';
UPDATE TTKD_BSC.bangluong_kpi set giao = 190 where  thang = 202409 and ma_kpi = 'HCM_SL_COMBO_006' AND ma_Nv ='VNP017601';
commit;