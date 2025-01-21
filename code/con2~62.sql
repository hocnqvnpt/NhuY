select* from css_hcm.ds_ungdung_online where ungdung_id IN(1,3,11,13);
select*  from v_Thongtinkm_all where dulieu = 'km_dbtb' and thang_bddc = 202405;
select owner , table_name from all_tab_columns where column_name = upper('ungdung_id');

select* from css_hcm.loaihinh_Tb where loaihinh_Tb = '2613436';
select ngay_kt from css_hcm.db_Thuebao a join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id and a.ma_tb ='hcm_ca_ivan_00021689';
select* from css_hcm.loai_Hd where loaihd_id IN (1,3,6,8,32,31);
select* from css_hcm.phieutt_hd where ma_gd = 'HCM-LD/01686242';
select* from css_hcm.ct_phieutt where phieutt_id = 8528495;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202406 and ma_Nv = 'VNP027256'
;select* from css_hcm.khoanmuc_Tt where khoanmuctt_id = 1;

select THANG, LOAI_TINH, MA_KPI, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN,SL_PHIEUTON, TYLE
from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202406 and loai_tinh = 'KPI_PB' AND MA_KPI=  'HCM_CL_TONDV_001';
select* from ttkd_Bsc.nhanvien_202406 where ten_Nv like '%Nh?';

select distinct ma_kpi, loai_tinh from ttkd_bsc.tl_giahan_Tratruoc where thang =202406
;
select* from ttkd_Bsc.nhanvien_202406 where ten_Nv like '%Th?o' and ma_pb = 'VNP0700700'