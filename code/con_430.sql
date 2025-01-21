
select* from ttkd_Bsc.CT_BSC_GIAHAN_CNTT where thang = 202411 and MANV_gIAO = 'VNP027796'; MA_TB='hcm_ca_00034804';

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and MA_NV ='VNP020233' AND MA_KPI = 'HCM_TB_GIAHA_024';
select* from ttkd_Bsc.nhanvien where thang = 202411 and ma_Vtcv='VNP-HNHCM_BHKV_36'; ten_Nv = '?oàn Th? Duyên';
select* from rmp_Bsc_phong where thang = 202411 and ma_pb = 'VNP0702300';

select* from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where thang = 202411 and ma_pb = 'VNP0702300' and ma_Tb='fptshoptxn.fb.tha';

select distinct a.*,b.thuebao_id,b.hdkh_id,b.ngay_tt,b.ngay_ht,b.kieuld_id,b.hdtb_id, so_Ct
,c.trangthai,c.ma_gd,c.phieutt_id,c.ht_tra_id,c.kenhthu_id
from ttkdhcm_ktnv.id271_dn3_bvtudu_0112 a ,css_hcm.hd_thuebao b ,css_hcm.phieutt_hd c 
where a.matb=b.ma_tb and b.kieuld_id=13248 and b.ngay_ht >=to_date('01/05/2024','dd/mm/yyyy') 
and b.hdkh_id=c.hdkh_id;
update rmp_Bsc_phong a set giao_truoc_loaitru = (select giao from rmp_Bsc_phong b where thang = 20241101 and a.ma_pb = b.ma_pb and a.ten_kpi = b.ten_kpi)
where a.thang = 202411 and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu Tên mien)';
rollback;
commit;
select* from rmp_Bsc_phong a where a.thang = 202411  ;and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T+1 ( Dich vu Fiber, MyTV, Mesh)';
update rmp_Bsc_phong set giao_truoc_loaitru = case when ma_pb = 'VNP0702300' then giao+23 
                                                    when ma_pb = 'VNP0702400' then giao+85 else giao end 
where  ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' and thang = 202411;
update rmp_Bsc_phong set tyle_truoc_loaitru = round(thuchien*100/giao_truoc_loaitru,4)
where  ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T ( Dich vu Fiber, MyTV, Mesh)' and thang = 202411;
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and ma_Tb='hcm_tmqt_00000505  ';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' AND MA_NV='VNP017479';
