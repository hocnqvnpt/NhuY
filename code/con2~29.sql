select* from css_hcm.khoanmuc_tt; where thang = 202406;
select* from ttkd_Bsc.nhanvien_202406 where ma_vtcv = 'VNP-HNHCM_BHKV_49';

select * from  css_hcm.dichvu_vt; where THUEBAO_ID ='12382886';
select* from admin_hcm.nhanvien_onebss;
with ct as (
    select ten_nganhang,  ma_Ct, tien_ct, ten_ht_Tra, NGAY_TT, MA_TT,null, MA_gd, LOAIHINH_TB, GHICHU,manv_tt, TENDV_tt,a.tien_hopdong,a.vat_hopdong, TONGTien, null d,1
    from ct_trtruoc a
    where exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma_gd) and manv_tt =nvl(manv_Hd,manv_tt) and manv_tt is not null
)
select* from ct join css_hcm.phieutt_hd b on ct.ma_gd = b.ma_gd where not exists (select 1 from css_Hcm.ct_phieutt where phieutt_id = b.phieutt_id and khoanmuctt_id  = 11)
;
select *from (
select THANG, KHACHHANG_ID, THUEBAO_ID, MA_TO, MA_PB, MA_TB, LOAIHINH_TB, MA_NV, NHOMTB_ID_OLD, NHOMTB_ID_NEW, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, TIEN_KHOP
from (
             select     
             a.thang,  a.khachhang_id, a.thuebao_id, a.ma_to, a.ma_pb, ma_Tb, b.loaihinh_tb
                                        ,a.MANV_THUYETPHUC ma_nv, goi_old_id nhomtb_id_old, nhomtb_id nhomtb_id_new, sum( cuoc_dc_cu) tien_Dc_Cu , max(a.SO_THANGDC_MOI) sothang_dc
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
                                   , case 
                                            when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  
                                                                                                    * nvl2(a.nhomtb_id, 0, 1)      
                                                                                            , 0)
                                            when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                            when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                                where xu.loaitb_id in (58, 59)
                                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                        else 0 
                                       end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a 
									left join css_hcm.loaihinh_tb b on a.loaitb_id = b.loaitb_id
                        where a.rkm_id is not null and thang = 202406 and ma_To = 'VNP0701404'
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, a.loaitb_id, b.loaihinh_tb, ma_tb                        
        ) where rnk = 1 and dthu > 0
        order by heso_dichvu 
);

update ttkd_bsc.bangluong_dongia_202406 a set
    luong_dongia_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc
                                                            where thang = 202406 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB' ,'DONGIA_TS_TP_TT','THUHOI_DONGIA_GHTT')
                                                                            and ma_nv = a.MA_NV_HRM
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
     ,giamtru_ghtt_cntt = (select -sum(tien), ma_Nv from ttkd_bsc.tl_giahan_tratruoc
                                                            where thang = 202406 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' 
                                                            and ma_nv = a.MA_NV_HRM
                                                            group by ma_nv 
                                                            having  sum(tien)  <> 0)
                                                            
;
select* from ttkd_bsc.blkpi_dm_to_pgd where  ma_Nv = 'VNP017585' AND THANG = 202406;
SELECT* FROM TTKD_bSC.NHANVIEN WHERE ten_nv like '%Chinh'; = 'VNP017585';
update ttkd_Bsc.tl_giahan_tratruoc  set tien = -21000 where thang = 202406 and loai_Tinh = 'THUHOI_DONGIA_GHTT' and ma_nv ='VNP017064';
select* from ttkd_Bsc.ct_dongia_tratruoc  where thang = 202405 and thuebao_id in (select thuebao_id from thuhoi_bsc_dongia where thang = 202405);
commit;
select* from thuhoi_bsc_dongia where thang = 202405;
select* from ttkd_Bsc.tl_giahan_tratruoc where loai_tinh = 'THUHOI_DONGIA_GHTT' and thang = 202406 ;and loai_Tinh = 'THUHOI_BSC_DONGIA';
select* from ttkd_Bsc.nhanvien_vttp_potmasco where thang is  null;