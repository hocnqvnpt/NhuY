select* from ttkd_bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_024' AND TEN_nV LIKE '%Thúy';
select* from css_Hcm.phieutt_Hd where ghichu like '%PQT%' and ma_gd in (
select ma_gd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang =  202411 and tien_khop = 0);
select* from ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_HD_1 where ma='00997429';
select* from ct_homecombo_2024 where thang = 202411;

update ttkd_bsc.ct_Bsc_giahan_cntt set tien_khop = 0 where thang = 202411 and thuebao_id in (
select a.thuebao_id from ttkd_Bsc.ct_Bsc_giahan_cntt a
    join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    where thang = 202411 and loaitb_id not in (147,148) and tien_khop = 1 and ma_Gd is null and ma_tb in (
select ma_Tb from ttkdhcm_ktnv.id271_dn3_bvtudu_0112) and a.khachhang_id = 9639917);
rollback;
commit;






drop table ton_202411 ;
select* from  dhsx.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241101 and ma_Tb= 'hcmthithuy032023';
select* from  qltn.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon<20241101 ;and ma_Tb= 'hcmthithuy032023';

select* from v_Thongtinkm_all where ma_Tb= 'hcmthithuy032023';
create table ton_202411 as;
select* from (
select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck, thang_bd, thang_kt--, trangthaitb_id

from dhsx.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001

and ngay_cn <to_date('20241102','yyyyMMdd')

and (ngay_thoai is null or trunc(ngay_thoai)< to_date('20241101','yyyyMMdd'))

and tiengach is null

union all

/*Danh sách rkm_id có thoái c?c t? 1/12/2024*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tien_thoai,0) ton_ck, thang_bd, thang_kt--, trangthaitb_id

from dhsx.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001

and ngay_cn <to_date('20241102','yyyyMMdd')

and trunc(ngay_thoai)>= to_date('20241101','yyyyMMdd')

and tiengach is null

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch trên form g?ch n? ti?n m?t)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(tiengach,0) ton_ck, thang_bd, thang_kt--, trangthaitb_id

from dhsx.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001

and ngay_cn <to_date('20241102','yyyyMMdd')

and tiengach >=0

union all

/*Danh sách rkm_id có g?ch n? lùi k? (g?ch theo d/s ch? Thanh gui)*/

select kyhoadon,thuebao_id,ma_tb,rkm_id,ton_dk ,cuoc_tk,ton_ck+nvl(cuoc_tk_lui,0) ton_ck, thang_bd, thang_kt--, trangthaitb_id

from dhsx.v_db_datcoc@dataguard where phanvung_id=28 and kyhoadon=20241001

and ngay_cn <to_date('20241102','yyyyMMdd')

and cuoc_tk_lui<>0) where ma_Tb ='vtphuong200723';
;
select* from v_Thongtinkm_all where ma_Tb='vtphuong200723';
 select* from ton_202411 where ma_tb='vtphuong200723'; 
select ngay_Tt from css.v_phieutt_Hd@dataguard where 
 select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202411 and ma_gd='HCM-TT/03065332';
 select* from css_hcm.hinhthuc_Tra where ht_Tra_id = 214;
 select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where  ma_Ct ='VCB_20241130_484271';
  select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb  where  ma_Ct ='VCB_20241130_484271';
  select* from ton_202411 where ma_tb='vtphuong200723'; ton_dk!= ton_ck+cuoc_Tk;
  ( select a.*, d.trangthai_Tb,c.ma_tt, e.loaihinh_Tb  from ton_202411 a
    left join ttkd_bct.db_thuebao_ttkd_202410 b on a.thuebao_id  = b.thuebao_id
    left join css_hcm.db_thanhtoan c on b.thanhtoan_id = c.thanhtoan_id
    left join css_hcm.trangthai_Tb d on b.trangthaitb_id = d.trangthaitb_id
    left join css_hcm.loaihinh_Tb e on b.loaitb_id = e.loaitb_id);
  select* from ttkd_Bsc.ct_bsc_giahan_Cntt where thang = 202411 and ma_Tb='hcm_ca_ivan_00019870';