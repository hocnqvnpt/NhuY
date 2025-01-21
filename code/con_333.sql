select * from ttkdhcm_ktnv.DM_MVIEW; where MVIEW_NAME= upper('duan');
select* from ttkd_bsc.blkpi_danhmuc_kpi where ma_kpi = 'HCM_CL_TONDV_001';
select * from ttkd_Bsc.nhanvien_202406 WHERE MA_VTCV = 'VNP-HNHCM_BHKV_48';
select* from v_Thongtinkm_all where ma_Tb = 'phanlan1958';
select* from  ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202406 and ma_Tb = 'thanhthuy3026048';
update ttkd_bsc.ct_bsc_tratruoc_moi_30day
set ma_to = 'VNP0701204'
where thang = 202406 and ma_To is null and ma_pb = 'VNP0701200';
select* from ttkd_Bsc.nhanvien where thang = 202406 and ma_nv in ( 'VNP016551' , 'HCM014350');
commit;
select* from css_hcm.phieutt_hd where ma_Gd = 'HCM-LD/01686242';
select* from css_hcm.ct_phieutt where phieutt_id = 8528495;
select* from css_hcm.khoanmuc_tt where khoanmuctt_id = 19;
with ct as (
    select hdtb_id,phieutt_id, sum(case when khoanmuctt_id = 19 then tien else 0 end) km_lapdat, sum(case when khoanmuctt_id = 19 then vat else 0 end) vat_km,
    sum(case when khoanmuctt_id <> 19 then tien else 0 end) tien_Thu,sum(case when khoanmuctt_id = 19 then vat else 0 end) vat_thu
    from css_hcm.ct_phieutt 
    group by hdtb_id, phieutt_id
)
select a.ma_gd, b.hdtb_id, b.thuebao_id, b.ma_tb, a.loaihd_id,b.kieuld_id, a.ngay_yc, b.ngay_ht, a.ctv_id, a.nhanviengt_id,  d.ngay_tt, c.tien_thu, c.vat_thu,c.km_lapdat, c.vat_km,
d.thungan_tt_id, d.ht_tra_id,d.kenhthu_id, d.trangthai
from css_hcm.hd_khachhang a
join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
join ct c on b.hdtb_id = c.hdtb_id
join css_hcm.phieutt_hd d on c.phieutt_id = d.phieutt_id and  (c.tien_thu <> 0 or km_lapdat <> 0) --d.tien <> 0
--left join css_hcm.db_thuebao e on b.thuebao_id = e.thuebao_id
where to_Number(to_char(b.ngay_ht,'yyyymm')) = 202406
 and a.loaihd_id = 1  ; -- del fiber"