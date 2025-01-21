drop table tmp3_bhol;

create table tmp3_bhol as
with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left joicss_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                    group by bb.phieu_id)

, kq_ghtt as (select  hdkh.khachhang_id, hdtb.thuebao_id, 0 duan_id, hdtb.ma_tb, 'matt' ma_tt, hdtb.loaitb_id, 0 thang_kt, 22 as lan
                        --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                        , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                        , a.phieutt_id, a.trangthai
                                        , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                        , kt.kenhthu
                                        , nh.ten_nh ten_nganhang
                                        , ht.ht_tra ten_ht_tra
                                        , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id
                                        ,a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                         from css_hcm.phieutt_hd a
                                            join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            left join hddc on b.hdtb_id = hddc.hdtb_id
                                            join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
--                                            left join ct2 on a.phieutt_id = ct2.phieu_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240715 and 20241001                  ----change--3 thang- ngay 2
                                                                 ----change--3 thang- ngay 2
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and a.ma_gd = 'HCM-TT/02290172'
                        )
select * from kq_ghtt a
where  a.ngay_bd_moi is NOT null ;
select a.*  from ttkd_bsc.nhuy_ct_ipcc_obghtt a
--    left join tmp3_bhol b on a.nhanvien_id = b.nvtuvan_id and a.thuebao_id = b.thuebao_id
                        where thang = 202409 and to_char(ngay_ktdc,'yyyymm') = '202409';
select distinct ten_to, ma_to from nhanvien_202409_l4 where ma_pb = 'VNP0703000';
create table tmp_bhol as;
select* from (
select a.*, row_number() over(partition by a.khachhang_id order by tien_thanhtoan desc) rnk
from tmp3_bhol a
) where rnk = 1;
drop table ct_Bsc_ghtt_bhol;
create table ct_Bsc_ghtt_bhol as
with giao as 
(
    select a.* , row_number() over(partition by a.khachhang_id order by ngay_ktdc) rnk
    from ttkd_bsc.nhuy_ct_ipcc_obghtt a
    where thang = 202409 and to_char(ngay_ktdc, 'yyyymm')='202409'
)
, t0 as (select c0.thang_kt, c0.thuebao_id, c0.khachhang_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                                                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                                                , c0.nvtuvan_id, c0.nhanviengt_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
                                               from tmp3_bhol c0
                                                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                                                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nhanviengt_id
                                                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                                                        where lan = 22
                    )
, km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
         from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100 and thang_bddc > 202307
                                                                                        )
, km0 as (  
    
----------------TT Thang tang tren 1 dong-------------5813343
--5865861               
                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                from v_thongtinkm_all km 
                where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                and thang_bddc > 202310
               union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                
                select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                from v_thongtinkm_all km left join km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                               -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                               and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                               and km.thang_bddc > 202307
--         ) km on c0.rkm_id = km.rkm_id 
                                                        --     and not(km.so_thangdc < 3 and c0.khachhang_id <> 2796008     --2-----ket qua tra truoc >= 3T except ma_kh HCM000971574 PBHKVCL yc eO 899817
                                                        --                        and km.so_thangdc < 3 and nvl(c0.duan_id, 0) not in (345, 1382))     --2-----ket qua tra truoc >= 3T xcept 2 ma_duan Tancang, Vinhome PBHKVSG yc eO 944829
                                                        
                )

, c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                            , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nhanviengt_id, t0.thungan_tt_id
                            , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang, t0.khachhang_id
                            , row_number() over(partition by t0.khachhang_id order by t0.tien_thanhtoan desc) rnk
                    from t0
                                join km0 on t0.rkm_id = km0.rkm_id
            ) 
select a.thang, a.ob_id, a.ngay_tao, a.thuebao_id, a.ma_tb, a.loaitb_id, a.khachhang_id, a.thanhtoan_id, a.ngay_ktdc,a.cuoc_Dc cuoc_dc_cu, a.so_thangdc sothang_Dc_cu, a.matb_phu, a.td_th, a.ma_nd, a.ma_nv,
        c.ma_gd, c.tien_Thanhtoan,c.vat, c.ten_nganhang, c.ten_Ht_Tra,c.manv_thuyetphuc,c.manv_Thungan, c.ngay_tt, c.phong_cn, c.phieutt_id, c.ht_tra_id,c.kenhthu_id,
        case when c.rkm_id is null then null
                            when c.ht_tra_id in (1, 7,204,216) then 1
                            when c.ht_tra_id in (2, 4,5, 207,214) then 0 else null end tien_khop
from giao a
    left join  c on a.khachhang_id = c.khachhang_id and a.rnk = c.rnk 
where a.rnk = 1;
update CT_BSC_GHTT_BHOL set tien_khop = 1 where ht_Tra_id = 207 and kenhthu_id in (25,26);
commit;
select* from CT_BSC_GHTT_BHOL where tien_khop = 0;
select* from css_hcm.hinhthuc_tra