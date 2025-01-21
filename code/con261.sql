with ct as (
--    select NGANHANG,  MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB,null ma_Gd, LOAIHINH_TB, GHICHU, nguoi_cnt ma_nv,TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, 0 Tra_Truoc
--    from ct_trsau
--    union all 
    -- chi tien cho nhan vien TT
    select ten_nganhang,  ma_Ct MA_CHUNGTU, tien_ct, ten_ht_Tra, NGAY_TT, MA_TT,null, MA_gd, LOAIHINH_TB, GHICHU,manv_tt ma_Nv, TENDV_tt,a.tien_hopdong,a.vat_hopdong, TONGTien, null,1
    from ct_trtruoc a
    where exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma_gd) and manv_tt =nvl(manv_Hd,manv_tt) and manv_tt is not null
) 
select count(distinct MA_CHUNGTU) sl, ma_pb
from ct join ttkd_Bsc.nhanvien_202406 a on ct.ma_Nv = a.ma_Nv 
group by a.ma_pb;--select count(1) from ct;
--create table tunhan_dien  as (
--    select distinct a.* 
--    from nhuy.ds_Chungtu_dongia a
--    left join ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb b on  a.chungtu_id = b.chungtu_id
--where 
-- EXISTS (select 1 from css.v_ghtt_chungtu@dataguard 
--            where nd_ct = b.ma_gd and trangthai=1 and ngay_tao > to_date('20240531','yyyymmdd')
--                and ngay_tao < to_date('20240701','yyyymmdd'))
--);
select a.*, nvl2(c.chungtu_id,1,0) tunhandien
from ct a
    left join tunhan_dien  c on a.ma_chungtu = c.MA_CHUNGTU;
where 
 EXISTS (select 1 from css.v_ghtt_chungtu@dataguard 
            where nd_ct = b.ma_gd and trangthai=1 and ngay_tao > to_date('20240531','yyyymmdd')
                and ngay_tao < to_date('20240701','yyyymmdd'));
select* from ttkd_bsc.nhanvien_2;
                select* from ttkd_Bsc.nhanvien_202406 where ten_Nv like '%Nh?';
                select* from admin_hcm.nhanvien_onebss;
                select* from css_hcm.tocdo_adsl;
    select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202406 and ma_Tb in ('hcm_ca_00078692',
'hcm_ca_00057811',
'hcm_ca_00079317',
'hcm_ca_00078868',
'hcm_ca_00043327',
'hcm_ca_00078772',
'hcm_ca_00079538',
'hcm_ca_00079754',
'hcm_ca_00078869',
'hcm_ca_00057807',
'hcm_ca_00078770',
'hcm_ca_00058482',
'hcm_ca_00058274',
'hcm_ca_00058483',
'hcm_ca_00057814',
'hcm_ca_00058543',
'hcm_ca_00057812',
'hcm_ca_00046622',
'hcm_ca_00057810',
'hcm_ca_00057817',
'hcm_ca_00042870',
'hcm_ca_00058082');
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202406 and ma_kpi = 'DONGIA';
update ttkd_bsc.bangluong_dongia_202406 a set
luong_chungtu =  (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                where thang = 202406 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIA_CHUNGTU' and ma_nv = a.MA_NV_HRM
                                group by ma_nv 
                                having  sum(tien)  <> 0)
                                                            
;
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202406 and hdtb_id = 25343360;
select* from v_thongtinkm_all where rkm_id = 7034840;
select* from css_hcm.hd_Thuebao where hdtb_id = 25343360;
COMMIT;
select* from css_hcm.trangthai_Hd;
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202406 and ma_Tb ='hcm_smartca_00035351';
select* from css_Hcm.khoanmuc_Tt where khoanmuctt_id in (27,36);
select* from   ttkd_Bsc.tl_giahan_Tratruoc
--set loai_tinh = 'DONGIATRA_BS'
where thang = 202406 and loai_tinh = 'DONGIATRA_OB' AND MA_NV IN (SELECT MA_nV FROM TTKD_bSC.NHANVIEN_202406 WHERE TEN_VTCV = 'Nhân Viên Nghi?p V? Ch?m Sóc Khách Hàng Phòng Bán Hàng Khu V?c');