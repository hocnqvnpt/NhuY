select* from ttkdhcm_ktnv.Ghtt_chotngay_271 where ma_Tb ='hcm_ca_plugin_00008819';

update ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt set tien_khop = 1 ;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt
where thang = 202412 and tien_khop = 0 and ma_pb = 'VNP0703000' And ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1);
and thungan_tt_id != nvtuvan_id
;
select* from  ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt
where thang = 202411 and tien_khop = 0 and ma_Gd in (select ma_Gd from css_hcm.phieutt_Hd where so_Ct like '%PQT%' ) 
and ma_gd not in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1);

select a.ma_Tb, b.ma_nv nv_t1, c.ma_nv nv_t12, nv1.ten_to ten_to_t1, nv2.ten_to ten_t12, nv1.ten_pb pb_t1, nv2.ten_pb pb_t12
from chilinh a 
    left join ttkd_bct.db_Thuebao_ttkd b on a.ma_Tb = b.ma_Tb
    left join ttkd_bct.db_thuebao_ttkd_202411 c on a.ma_Tb = c.ma_Tb
    left join ttkd_bsc.nhanvien nv1 on b.ma_Nv_am = nv1.ma_nv and nv1.thang = 202501
    left join ttkd_Bsc.nhanvien nv2 on c.ma_nv_am = nv2.ma_nv and nv2.thang =202412;
    select* from ttkd_bsc.nhanvien where ma_nv = 'VNP028832';
    select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where thang = 202412 and ma_Tb in ('hcm_ca_00047813','hcm_ca_00048007');
    select ngay_kt from css_Hcm.db_cntt where thuebao_id in (9143349,9152762);
    select distinct chungtu_id, nguoi_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc_202412_1 where chungtu_id in (492023,520452,526656,533277,539266,538688,538782,539262,536808);