with ma_gd as (
    select  ma_gd, phieutt_id, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202405 and ht_Tra_id in (2,4,5,9,207)
    union all
    select  ma_gd,phieutt_id, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202405  and ht_Tra_id in (2,4,5,9,207)
    union all
    select  ma_gd,phieutt_id, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day where thang = 202405  and ht_Tra_id in (2,4,5,9,207)
    union all
    select  ma_gd ,phieutt_id, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day where thang = 202405  and ht_Tra_id in (2,4,5,9,207)
    union all
    select  ma_gd,phieutt_id, khachhang_id from ttkd_bsc.ct_bsc_trasau_tp_Tratruoc where thang = 202405  and ht_Tra_id in (2,4,5,9,207)
    union all
    select  ma_gd,phieutt_id, khachhang_id from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405  and ht_Tra_id in (2,4,5,9,207)
    union all
    select ma_Gd, phieutt_id, khachhang_id from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202405 and ht_Tra_id in (2,4,5,9,207)
)
--select distinct ht_Tra_id, ten_ht_Tra from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405 ;
,ct as (select MA_CT_ONEB, ma_ct
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
					)
select distinct a.ma_Gd, nvl(ct.ma_ct,b.so_ct) so_ct, ct.ma_ct, (b.tien+b.vat)  tongtien, dv2.ten_dv phong_Tt, b.ngay_tt 
from ma_gd a
    left join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
    left join admin_hcm.nhanvien_onebss nv on b.thungan_tt_id = nv.nhanvien_id
    left join admin_hcm.donvi dv on nv.donvi_id =dv.donvi_id
    left join admin_hcm.donvi dv2 on dv.donvi_cha_id = dv2.donvi_id
    left join ct on b.so_ct = ct.MA_CT_ONEB
    ;
select* from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405;
update ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a
set manv_ob = (select distinct manv_ob from  ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt
        where thang = 202405 and manv_ob is not null and a.phieutt_id = phieutt_id and a.nvtuvan_id = nvtuvan_id and a.khachhang_id = khachhang_id)
where thang = 202405 and rkm_id is not null;