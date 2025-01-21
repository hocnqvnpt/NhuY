select* from  temp_ts;
drop table temp_ts;
rollback;
-- BANG DANH SACH HOP DONG
create table tmp_tt as;
with db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202401
) 
--, km1 as (
--    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
--    from v_thongtinkm_all a
--            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
--    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
--    202402 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
--)
, km2 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, cuoc_Dc ,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202403 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
--, km3 as (
--    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
--    from v_thongtinkm_all a
--            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
--    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and cuoc_dc > 0 and
--    202401 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
--)
, tt as (
        select db.thuebao_id
        from db 
--            left join km1 on db.thuebao_id = km1.thuebao_id and km1.rn = 1
            left join km2 on db.thuebao_id = km2.thuebao_id and km2.rn = 1
--            left join km3 on db.thuebao_id = km3.thuebao_id and km3.rn = 1
        where km2.rkm_id is not null ) 
        
,hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where h.thang_bd > 202307
                                            ) 
   , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                                and thang_bddc > 202307
                        )
    , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
               join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
            group by bb.phieu_id)
    , kq_ghtt as (select 
                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai,  ct.ngay_nganhang
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, 
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 111 lan
                     from css_hcm.ct_tienhd a
                            left join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11-- and b.tien < 0
                            left join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id
                            left join hddc on a.hdtb_id = hddc.hdtb_id
                            join css_hcm.hd_thuebao hdtb on a.hdtb_id = hdtb.hdtb_id  and hdtb.kieuld_id in (551, 550, 24, 13080) 
                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id  
                            left join kmtb on a.hdtb_id = kmtb.hdtb_id
                            left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
                            left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
                            join  tt on hdtb.thuebao_id = tt.thuebao_id 
                            left join ct on p.phieutt_id = ct.phieu_id

                     where    a.khoanmuctt_id = 11 and hdtb.thuebao_id = 8413284 and
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymm')) =202404)                     ----change--2 thang- ngay 02
                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
                                    )  ;    

select *
from kq_ghtt a
where ngay_bd_moi is not null;
select THANG, LOAI_TINH, MA_KPI, A.MA_NV, A.MA_TO, A.MA_PB,  DA_GIAHAN_DUNG_HEN , TEN_PB, TEN_TO , TIEN from ttkd_bsc.tl_giahan_tratruoc A 
JOIN TTKD_bSC.NHANVIEN_202404 B ON A.MA_NV = B.MA_nV 
where thang = 202404;
select sum(tien) from (
select thang, loai_Tinh ,ma_kpi, a.ma_nv, a.ma_to, a.ma_pb ,a.ma_tb ,a.sothang_dc, a.heso_dichvu, a.heso_chuky, dongia, b.ten_to,b.ten_pb, round(dongia*heso_chuky*heso_dichvu) tien,
case tien_khop when   1 then 'Chuy?n kho?n' 
    when 2  then 'QR_Code'
    when 3 then 'Thu t?i nhà - Có phí'
    when 4 then 'Thu t?i nhà - Không phí' end tien_khop
        from ttkd_bsc.ct_dongia_Tratruoc a 
    join ttkd_bsc.nhanvien_202404 b on a.ma_nv =b.ma_nv 
where thang = 202404 and loai_tinh in ('DONGIATRA_OB','DONGIA_VTTP','DONGIA_OB_VTTP'));
select* from ttkd_Bsc.nhanvien_202404 where ten_Nv like '%Xuân';
commit;
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO,HCM_SL_HOTRO_005,HCM_SL_HOTRO_004,HCM_CL_TNGOI_004,HCM_CL_TNGOI_004
from ttkd_bsc.bangluong_kpi_202404 where HCM_SL_HOTRO_005 is not null or HCM_SL_HOTRO_004 is not null or HCM_CL_TNGOI_004 is not null or HCM_CL_TNGOI_004 is not null;
---- TAO BANG CHOT
select* from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202404 and so_thangdc_cu = -1;
insert into ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt(THANG, THUEBAO_ID, MA_TB, MA_GD, RKM_ID, NGAY_BD, NGAY_KT, TIEN_HOPDONG, TIEN_THANHTOAN, VAT_thanhtoan, NGAY_TT,
NGAY_HD, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA,  MA_TO, MA_PB, PB_THPHUC, NHOMTB_ID, HDTB_ID, HDKH_ID, NVTUVAN_ID, NVTHU_ID,
THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID,so_thangdc_cu, KHACHHANG_ID, GOI_OLD_ID, 
PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, LOAITB_ID, THANHTOAN_ID, TIEN_KHOP, MA_CHUNGTU)
    with t0 as (select  c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi,c0.ngay_kt_moi, c0.tien_hopdong,c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, 
                                nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
               from temp_tt c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                        )
                 , km0 as (     
                                ----------------TT Thang tang tren 1 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                            , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                            , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km 
                            where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                            --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                            and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                           union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                            , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                            , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                            from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                        ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                            where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                                           -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                     )
--                , ds as (select thang,ob_id, ngay_tao ngay_ob, thuebao_id, ma_Tb, ngay_bddc, ngay_ktdc ngay_kt_dc, td_th, ma_nd, nhanvien_id,ma_Nv,so_thangDc,cuoc_dc,
--                        to_number(to_char(ngay_ktdc,'yyyymm')) thang_kt,nhomtb_id, thanhtoan_id, khachhang_id
--                        from ttkd_bsc.nhuy_ct_ipcc_obghtt
--                        where thang >= 202403
--                        )
--                    select* from ttkd_Bsc.nhuy_ct_ipcc_obghtt;
--                    select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt
            , c as (select  t0.thuebao_id, t0.phieutt_id,  t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.ngay_kt_moi,t0.tien_hopdong,t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang,t0.loaitb_id
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) --select* from c;
            , goi_dadv as  (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rn 
                                from css_hcm.bd_goi_dadv 
                                where trangthai = 1 and dichvuvt_id = 4 and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
            )                  
            , goi_cu as (
                  select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
                  from tinhcuoc.v_sd_goi_dadv@dataguard 
                  where trangthai = 1 and KYHOADON = 20240301
        
            )
    select 202404 thang_ob, c.thuebao_id, dbtb.ma_tb,
            c.ma_gd, c.rkm_id , c.ngay_bd_moi, c.ngay_kt_moi,c.tien_hopdong, c.tien_thanhtoan, 
            c.vat, c.ngay_tt, c.ngay_hd, c.ngay_Nganhang,c.soseri, c.seri, c.kenhthu, c.ten_nganhang, c.ten_ht_Tra,
             nvtp.ma_to, nvtp.ma_pb,nvtp.ma_pb pb_thphuc,   g.nhomtb_id nhomtb_id, c.hdtb_id, c.hdkh_id,
            NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, c.SO_THANGDC so_thangdc_moi, c.AVG_THANG, 
            NVGIAOPHIEU_ID, DVGIAOPHIEU_ID    , -1 thang_kt
              
                , dbkh.khachhang_id, cu.nhomtb_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id, c.loaitb_id, dbtb.thanhtoan_id, 
                
                 case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204) then 1
                                when c.ht_tra_id in (2, 4,5) then 0 else null end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                
    from c
                                join css_hcm.db_thuebao dbtb
                                    on c.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                left join goi_dadv g 
                                    on c.thuebao_id = g.thuebao_id and g.rn =1
                                left join goi_cu cu
                                    on c.thuebao_id = cu.thuebao_id and cu.rnk = 1
--                                left join ttkd_bsc.dm_phongban pb
--                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
--                                left join  ttkd_bct.phongbanhang pb_giao
--                                    on a.donvi_giao = pb_giao.pbh_id
--                                left join  ttkd_bct.phongbanhang pb_th
--                                    on a.pbh_id_th = pb_th.pbh_id
--                                left join c 
--                                    on a.thuebao_id = c.thuebao_id and a.nhanvien_id = c.nvtuvan_id-- and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien_202404 nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv
--                                left join ttkd_bsc.nhanvien_202404 nvob on a.ma_nv = nvob.ma_nv
                    where not exists (select 1 from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202404 and rkm_id = c.rkm_id);
                    
