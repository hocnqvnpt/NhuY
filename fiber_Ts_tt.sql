select distinct ma_kpi, loai_tinh from  temp_ts;
drop table temp_ts;

-- BANG DANH SACH HOP DONG
create table temp_ts as
with db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202401
) 
, km1 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202402 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km2 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, cuoc_Dc ,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202403 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km3 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and cuoc_dc > 0 and
    202401 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, ts as (
        select db.thuebao_id
        from db 
            left join km1 on db.thuebao_id = km1.thuebao_id and km1.rn = 1
            left join km2 on db.thuebao_id = km2.thuebao_id and km2.rn = 1
            left join km3 on db.thuebao_id = km3.thuebao_id and km3.rn = 1
        where km1.rkm_id is null and km2.rkm_id is null and km3.rkm_id is null)
        
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
                            join  ts on hdtb.thuebao_id = ts.thuebao_id 
                            left join ct on p.phieutt_id = ct.phieu_id

                     where   to_number(to_char(hdkh.ngay_yc,'yyyymm')) = 202404  and a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymm')) =202404)                     ----change--2 thang- ngay 02
                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
                                    )  

select *
from kq_ghtt a
where ngay_bd_moi is not null;
insert into ttkd_bsc.ct_bsc_trasau_tp_tratruoc;
--- bang don gia thu phi tra sau
create table temp_tp as
with db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202401
) 
, km1 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202402 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km2 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, cuoc_Dc ,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and  cuoc_dc > 0 and
    202403 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, km3 as (
    select a.thuebao_id, a.rkm_id,thang_bd_mg,thang_kt_mg,thang_kt_dc,thang_huy, ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and cuoc_dc > 0 and
    202401 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
)
, ts as (
        select db.thuebao_id
        from db 
            left join km1 on db.thuebao_id = km1.thuebao_id and km1.rn = 1
            left join km2 on db.thuebao_id = km2.thuebao_id and km2.rn = 1
            left join km3 on db.thuebao_id = km3.thuebao_id and km3.rn = 1
        where km1.rkm_id is null and km2.rkm_id is null and km3.rkm_id is null)
        
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
                            join  ts on hdtb.thuebao_id = ts.thuebao_id 
                            left join ct on p.phieutt_id = ct.phieu_id

                     where   to_number(to_char(hdkh.ngay_yc,'yyyymm')) = 202404  and a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymm')) =202404)                     ----change--2 thang- ngay 02
                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
                                    )  

select *
from kq_ghtt a
where ngay_bd_moi is not null;
select* from css_hcm.khoanmuc_tt where ten_kmtt like '%T?m%';
select* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202408;
commit;
---- TAO BANG CHOT TRA SAU THUYET PHUC TRA TRUOC
insert into ttkd_Bsc.ct_bsc_trasau_tp_tratruoc
(THANG, GH_ID, MA_TB, THUEBAO_ID,khachhang_id, MANV_THUYETPHUC, MA_TO, MA_PB, THOIGIAN_TH
, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU
, TEN_NGANHANG, TEN_HT_TRA, AVG_THANG, SO_THANGDC, MANV_CN, PHONG_CN, MANV_GT, MANV_THUNGAN, NHOMTB_ID, MANV_QL, MATO_QL, MAPB_QL
, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, HT_TRA_ID, KENHTHU_ID,  goi_old_id, loaitb_id)
with t0 as (select c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                        , c0.ngay_tt, c0.ngay_hd, null ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra, c0.khachhang_id
--                                        , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                        , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                        , c0.nvtuvan_id, c0.nvthu_id NHANVIENGT_ID, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn
                                        ,nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
                       from Tmp3_ts c0
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
                                    where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0 and hieuluc = 1 and ttdc_id = 0
                                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                   union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
                                    select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                                    , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                                    , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                                    from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                    from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                                    where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0 and hieuluc = 1 and ttdc_id = 0
                                                   and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                                          
                )
--, ds as (select a.*, w.ma_tt, rank() over (partition by a.ma_tb ORDER BY a.idno  desc) rnk  
--                      from ttkd_bct.ds_ketqua_capnhat_dai a
--                                            join ttkd_bct.moi_giahan_tratruoc_moi w on w.gh_id = a.gh_id and w.loaitb_id in (58, 59)
--                      where a.thang_kt=0 and to_number(to_char(a.ngay_cn, 'yyyymm')) = 202403---thang n--change anh Huyen, Duc Dung moi thang -------
--                    )
, c as (select t0.thuebao_id, t0.phieutt_id, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nhanviengt_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.khachhang_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang, t0.loaitb_id
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) --select* from c
, dadv as (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
          from tinhcuoc.v_sd_goi_dadv@dataguard 
          where trangthai = 1 and KYHOADON = 20240601
        )
      
select 202407 thang,  -1 gh_id, dbtb.ma_tb, a.thuebao_id, a.khachhang_id
            , nvl(a.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC, nv.ma_to, nv.ma_pb
           , null ngay_cn
           , dbtb.MA_TT, a.MA_GD, a.RKM_ID, a.ngay_BD_MOI, a.TIEN_THANHTOAN, a.VAT, a.NGAY_TT, a.NGAY_NGANHANG, a.SOSERI, a.SERI, a.KENHTHU, a.TEN_NGANHANG
           , a.TEN_HT_TRA, a.AVG_THANG, a.SO_THANGDC, a.manv_cn, a.phong_cn, a.manv_gt, a.manv_thungan
            , (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and a.thuebao_id = thuebao_id and dichvuvt_id = 4
                                                                and goi_id in (select goi_id from css_hcm.goi_dadv_ccbs where nhomgoi_id in (6, 16))
            ) nhomtb_id, dbtb.ma_nv manv_ql
                                        , getxmlagg_table('ttkd_bsc.dm_to', 'ma_to', 'hieuluc = 1 and  tbh_id = ' ||dbtb.tbh_ql_id, '-1') mato_ql
                                    , getxmlagg_table('ttkd_bsc.dm_phongban', 'ma_pb', 'active = 1 and  pbh_id = ' ||dbtb.pbh_ql_id, '-1') mapb_ql
            , a.phieutt_id
           , case when a.rkm_id is null then null
                        when a.ht_tra_id in (1, 7,204,216,214) then 1
                        when a.ht_tra_id in (2,  4, 5) then 0 else null end tien_khop
            , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu 
            , a.ht_tra_id, a.kenhthu_id, dadv.nhomtb_id, a.loaitb_id
 from c a
           --     join css_hcm.db_thuebao dbtb on a.thuebao_id = dbtb.thuebao_id and dbtb.loaitb_id in (58, 59)
            left join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id --and dbtb.loaitb_id in (58, 59)          ---change n -1
--                left join c on a.thuebao_id = c.thuebao_id
            left join ttkd_bsc.nhanvien nv on a.MANV_THUYETPHUC = nv.ma_nv and thang = 202407 and donvi = 'TTKD'
            left join dadv on a.thuebao_id = dadv.thuebao_id and dadv.rnk = 1; 
            select* from tmp3_tp;
            delete from ttkd_bsc.ct_bsc_Trasau_tp_tratruoc where thang = 202408;
            rollback;
            commit;
--- TAO BANG CHOT TAM THU PHI TRA TRUOC
insert into ttkd_Bsc.ct_bsc_trasau_tp_tratruoc
(THANG, GH_ID, MA_TB, THUEBAO_ID,khachhang_id, MANV_THUYETPHUC, MA_TO, MA_PB, THOIGIAN_TH
, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU
, TEN_NGANHANG, TEN_HT_TRA, AVG_THANG, SO_THANGDC, MANV_CN, PHONG_CN, MANV_GT, MANV_THUNGAN, NHOMTB_ID, MANV_QL, MATO_QL, MAPB_QL
, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, HT_TRA_ID, KENHTHU_ID,  goi_old_id, loaitb_id);
with t0 as (select c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                        , c0.ngay_tt, c0.ngay_hd, null ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra, c0.khachhang_id
--                                        , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                        , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                        , c0.nvtuvan_id, c0.nvthu_id NHANVIENGT_ID, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn
                                        ,nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
                       from Tmp3_tp c0
                                    left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                    left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                    left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                                    left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                                    left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                    )
        
select 202408 thang,  -1 gh_id, dbtb.ma_tb, a.thuebao_id, a.khachhang_id
            , nvl(a.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC, nv.ma_to, nv.ma_pb
           , null ngay_cn
           , dbtb.MA_TT, a.MA_GD, a.RKM_ID, a.ngay_BD_MOI, a.TIEN_THANHTOAN, a.VAT, a.NGAY_TT, a.NGAY_NGANHANG, a.SOSERI, a.SERI, a.KENHTHU, a.TEN_NGANHANG
           , a.TEN_HT_TRA, a.manv_cn, a.phong_cn, a.manv_gt, a.manv_thungan
            , (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and a.thuebao_id = thuebao_id and dichvuvt_id = 4
                                                                and goi_id in (select goi_id from css_hcm.goi_dadv_ccbs where nhomgoi_id in (6, 16))
            ) nhomtb_id, dbtb.ma_nv manv_ql
                                        , getxmlagg_table('ttkd_bsc.dm_to', 'ma_to', 'hieuluc = 1 and  tbh_id = ' ||dbtb.tbh_ql_id, '-1') mato_ql
                                    , getxmlagg_table('ttkd_bsc.dm_phongban', 'ma_pb', 'active = 1 and  pbh_id = ' ||dbtb.pbh_ql_id, '-1') mapb_ql
            , a.phieutt_id
           , case when a.rkm_id is null then null
                        when a.ht_tra_id in (1, 7,204) then 1
                        when a.ht_tra_id in (2,  4, 5) then 0 else null end tien_khop
            , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu 
            , a.ht_tra_id, a.kenhthu_id,  a.loaitb_id
 from t0 a
           --     join css_hcm.db_thuebao dbtb on a.thuebao_id = dbtb.thuebao_id and dbtb.loaitb_id in (58, 59)
            left join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id --and dbtb.loaitb_id in (58, 59)          ---change n -1
--                left join c on a.thuebao_id = c.thuebao_id
            left join ttkd_bsc.nhanvien nv on a.MANV_THUYETPHUC = nv.ma_nv and thang = 202408 and donvi =  'TTKD' ;
          
;
            COMMIT;
            select* from TTKD_BSC.CT_DONGIA_TRATRUOC where thang = 202408;
            ---- TINH DON GIA
  insert into TTKD_BSC.CT_DONGIA_TRATRUOC(THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, MA_TB,ma_nv,ma_to,ma_pb,NHOMTB_ID,  SOTHANG_DC, HT_TRA_ONLINE,
                 HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  dongia)
                select THANG, 'DONGIA_TS_TP_TT' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, ma_tb, MANV_THUYETPHUC, MA_TO, MA_PB, NHOMTB_ID,  SOTHANG_DC, HT_TRA_ONLINE,
                 HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP
--   
                   , cast( 18000 as number(5)) dongia
--                    , cast( 5000 as number(5)) dongia
  from (
        with hs as (select thang, khachhang_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc xu
                where  xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
             select     
             a.thang, a.thuebao_id, a.ma_tb,A.MANV_THUYETPHUC,  a.ma_to, a.ma_pb
                                           ,a.nhomtb_id NHOMTB_ID , a.goi_old_id , a.loaitb_id
--                                            , hs.khachhang_id--,sum( cuoc_dc_cu) tien_Dc_Cu
------                          
--------                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
--------                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    , max(a.SO_THANGDC) sothang_dc
                                     , sum(case 
                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
                                            else 0 end) ht_tra_online
                                    
                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
                                                        else 0 end) kenhthu_tainha
                                    
                                    , case
                                            when max(a.SO_THANGDC) >=12 then 1.2
                                            when max(a.SO_THANGDC) < 12 and max(a.SO_THANGDC) >= 6 then 1
                                            when max(a.SO_THANGDC) < 6 and max(a.SO_THANGDC) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
------                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                        from ttkd_bsc.ct_Bsc_trasau_tp_tratruoc a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
--                                            left join goi on a.thuebao_id = goi.thuebao_id
--                                            left join  km on a.rkm_id = km.rkm_id
                        where a.thang = 202409 and --a.rkm_id is not null AND -- don gia TP 

                        a.rkm_id is not null AND -- don gia TP 
                        A.MANV_tHUYETPHUC IS NOT NULL --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, A.MANV_THUYETPHUC,  a.ma_to, a.ma_pb, A.LOAITB_ID
                                           ,a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id, a.ma_Tb
        ) a
        where rnk = 1 and dthu > 0;
        select distinct loai_Tinh 
        delete from dongia_moi where loai_tinh = 'DONGIA_TS_TT';
        commit;
        select  from ttkd_bsc.tl_giahan_tratruoc where thang = 202404;
      select* from  ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202408 and  loai_tinh in ('DONGIA_TS_TP_TT') ;
      and thuebao_id in (select thuebao_id from ttkd_Bsc.ct_bsc_trasau_tp_tratruoc where thang = 202404 group by thuebao_id having count(thuebao_id) > 1);
DELETE FROM dongia_moi WHERE THANG = 202404 AND loai_tinh in ('DONGIA_TS_TP_TT') ;
INSERT INTO ttkd_bsc.tl_giahan_Tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB,  DA_GIAHAN_DUNG_HEN
                                                        , TIEN)
SELECT a.thang, loai_tinh, ma_kpi,  a.ma_Nv, b.MA_tO, b.MA_PB
                      , sum(case when dthu > 0 then 1 else 0 end) da_giahan--, null
                      , round(sum(dongia*heso_chuky*heso_dichvu*TIEN_KHOP)) tien
-- select * from tl_giahan_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRA'
--from TTKD_BSC.ct_dongia_tratruoc  a
from ttkd_Bsc.ct_dongia_Tratruoc  a
    left join ttkd_Bsc.nhanvien b on a.thang = b.thang and a.ma_Nv= b.ma_nv
where ma_kpi = 'DONGIA' --and loaitb_id in (58,59)
            and loai_tinh in ('DONGIA_TS_TP_TT') and a.thang = 202408 and b.ma_pb not in ('VNP0702300','VNP0702400','VNP0702500')
group by a.thang, loai_tinh, ma_kpi,  a.ma_Nv, b.MA_tO, b.MA_PB;
commit;
select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang = 202408;
select sum(tien) from  ttkd_bsc.tl_giahan_Tratruoc where loai_tinh in ('DONGIA_TS_TP_TT') and thang = 202404;
select* from dongia_moi where ma_nv = 'CTV021905' and loai_tinh = 'DONGIA_TS_TP_TT';
SELECT* FROM DONGIA_MOI WHERE  loai_tinh = 'DONGIA_TS_TP_TT';
-- tinh bsc