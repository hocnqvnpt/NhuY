select* from ct_Bsc_Chungtu where thang = 202411 and tra_Truoc = 1;
with giao as (
    select a.*, b.khachhang_id from giao_oneb a 
        join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
    where thang = 202411 and thang_kt=202411 and a.ma_Tb not in (select ma_Tb from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202411)
)

select a.*,  c.mapb_ql, c.* from giao a 
join ttkd_bct.db_thuebao_ttkd_202410 c on a.thuebao_id = c.thuebao_id
where c.mapb_ql ='VNP0702300';
select* from ttkd_bct.Tobanhang where tbh_id = 313;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' and ma_nv ='VNP017639';
update  ttkd_Bsc.bangluong_kpi set diem_cong = 0 where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' and ma_nv ='VNP017639';
commit;
where thang = 202411 and thang_kt=202411 and a.ma_Tb not in (select ma_Tb from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202411);
delete from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202411 and loai_Tinh = 'DOANHTHU' AND MA_NV NOT IN
(SELECT MA_NV FROM  TTKD_bSC.NHANVIEN WHERE THANG = 202411 AND MA_VTCV = 'VNP-HNHCM_KDOL_17' );
commit;
SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202411 AND MA_PB ='VNP0703000';

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_SL_COMBO_006' AND MA_VTCV ='VNP-HNHCM_BHKV_2' and ma_pb = 'VNP0701100';
select nhanvien_giao, ma_to, donvi_giao from ttkdhcm_ktnv.giahan_cntt_theoky a where a.thuebao_id in ( select b.thuebao_id from (
select distinct a.ma_tb,a.thuebao_id,a.kq_import,a.thang_kt,a.ma_tt
from ttkdhcm_ktnv.kq_loaitru a where user_import=61 and thang_kt=202411 
and trunc(ngay_import) =to_date('09/12/2024','dd/mm/yyyy') ) b  ) and thang_kt=202411
and ghichu_pbh like '%418%'