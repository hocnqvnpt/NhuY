update ttkd_bsc.ct_bsc_giahan_cntt a
set rkm_id = (select rkm_id from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt = 202310 and phieutt_id = a.phieutt_id)

-- rkm_id, ngay_nganhang, ma_gd,
insert into ttkd_bsc.dongia_vttp 
select * from  hocnq_ttkd.DONGIA_VTTP where thang = 202309
--select * from hocnq_ttkd.DONGIA_VTTP where thang = 202309
union all 
create table vttp_t9 as 
select * from ttkd_bsc.DONGIA_VTTP where thang = 202309;
commit;
select phieutt_id from ttkd_bsc.ct_bsc_giahan_cntt where thang_ktdc_cu=202310 --and ma_Tb = 'hcm_smartca_00001899';
select * from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where ma_gd = 'HCM-LD/01555266' and ma_tb = 'hcm_ca_00088615'
    select ma_gd from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt= 202310 group by ma_gd having count(ma_gd) > 1 --and thuebao_id in ( 9236349, 8997936, 11806900, 8682502)

select ma_tt, khachhang_id from css_hcm.db_Thanhtoan
union all 
select ma_tt, khachhang_id from css_hcm.db_khachhang
select * from css_hcm.db_khachhang where khachhang_id =  5006235
select  ma_gd from ttkd_bsc.ct_bsc_Tratruoc_moi where thang = 202310 group by ma_gd having count(ma_gd) > 1
select * from css_hcm.hd_thanhtoan
commit;
rollback;
select 
select ma_gd, ma_tb from ttkd_bsc.ct_bsc_giahan_cntt where thang_ktdc_cu= 202310 and thuebao_id in( 9236349, 8997936, 11806900, 8682502)
union all
select ma_gd, ma_tb from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt= 202310 and thuebao_id in( 9236349, 8997936, 11806900, 8682502)

select thuebao_id from ttkd_bsc.ct_bsc_giahan_cntt where thang_ktdc_cu= 202310 and ma_gd is  null and thuebao_id in 
(select thuebao_id from  ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt=202310 and ma_gd is not null)
9236349 8997936 11806900 8682502
update ttkd_bsc.ct_bsc_giahan_cntt a
set ma_gd  = (select ma_gd from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt= 202310 and thuebao_id = a.thuebao_id)
where thang_ktdc_cu = 202310 and ma_gd is null and 
    a.thuebao_id = (select thuebao_id from  ttkdhcm_ktnv.ghtt_kq_thanhtoan_271 where thang_kt= 202310 and  ma_gd is not null  and thuebao_id = a.thuebao_id ) 