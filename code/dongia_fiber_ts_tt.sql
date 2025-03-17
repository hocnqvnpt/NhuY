-- tao bang temp
drop table tmp3_ts;
create table tmp3_ts as 
WITH 
db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202411
)
,km as (select rkm_id, thuebao_id, ma_tb, loaitb_id, ngay_bddc, ngay_ktdc, ngay_kt, ngay_huy, ngay_thoai
                            , thang_bddc, thang_ktdc, thang_kt_mg, thang_huy, thang_kt_dc                                                            
                            , case when ngay_huy is not null then ngay_huy
                                        when ngay_huy is null and thang_huy > 0 then last_day(to_date(thang_huy, 'yyyymm'))+1
                          when ngay_huy is null and thang_huy = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_huy_x
                            , case when ngay_thoai is not null then ngay_thoai
                                        when ngay_thoai is null and thang_kt_dc > 0 then last_day(to_date(thang_kt_dc, 'yyyymm'))+1
                          when ngay_huy is null and thang_kt_dc = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_thoai_x
                            , tien_td, cuoc_dc, tyle_sd, tyle_tb, thangdc so_thangdc, thangkm so_thangkm, congvan_id, khuyenmai_id, chitietkm_id, nhom_datcoc_id
                from v_thongtinkm_all
                where  thang_ktdc > 202301 and cuoc_dc > 0 --and  thuebao_id = 11736290
            )
, km1 as    (
                        select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                , to_date(thang_bd_mg, 'yyyymm') ngay_bd_mg
                                , last_day(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm'))  ngay_kt_mg
                                , last_day(to_date(thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai
                       from v_thongtinkm_all
                       where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) and (tyle_sd = 100 or tyle_tb = 100) 
                       and cuoc_dc = 0 and thang_bd_mg > 202201 
                )
, dc as (
        ----------------TT Thang tang tren 1 dong-------------
             select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id
                            , km.ngay_bddc, least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , least(km.ngay_kt, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x ngay_thoai
                            , km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km 
            where (km.tyle_sd = 100 or km.tyle_tb = 100)
                            and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR))  >= ngay_bddc
                            and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                            
                        --    and km.thuebao_id = 9085442
           union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) end ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x NGAY_THOAI, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km left join km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  km1.ngay_bd_mg
            where (km.tyle_sd + km.tyle_tb < 100)
                    and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) >= ngay_bddc
                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                         --   and km.thuebao_id = 8131788
            
             union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_ktdc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg
                            , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km
            where (km.tyle_sd + km.tyle_tb < 100)
                            and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                            and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                          --  and km.thuebao_id = 9085442
    ) 
, tt1 as (
    select thuebao_id, rkm_id ,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202409 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt2 as 
(
    select thuebao_id, rkm_id,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202501 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt3 as (
    select thuebao_id, rkm_id,  ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202411 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
,ts as (
    select  db.thuebao_id
    from db
        left join tt1  on db.thuebao_id = tt1.thuebao_id and tt1.rn = 1
        left join tt2 on db.thuebao_id = tt2.thuebao_id and tt2.rn = 1
        left join tt3 on db.thuebao_id = tt3.thuebao_id and tt3.rn = 1
    where   tt2.rkm_id is  null and tt3.rkm_id is  null -- and tt1.rkm_id is not null
)
,hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc  h on g.thuebao_dc_id = h.thuebao_dc_id
                                            where h.thang_bd > 202307
 ) 
, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                            where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                            and thang_bddc > 202307
) 
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--           join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--        group by bb.phieu_id) 
    , kq_ghtt as (select 
                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai--,  ct.ngay_nganhang
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 111 lan
                     from  css_hcm.hd_khachhang hdkh
                        join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 6
                        join ts  on hdtb.thuebao_id = ts.thuebao_id-- and gh.thang >= 202403
                        join css_hcm.ct_tienhd a on hdtb.hdtb_id = a.hdtb_id
                           join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0 and a.khoanmuctt_id = b.khoanmuctt_id
                           join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
                           left join hddc on hdtb.hdtb_id = hddc.hdtb_id
                           left join kmtb on hdtb.hdtb_id = kmtb.hdtb_id
                           left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
                           left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
                           left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
--													   left join ct on p.phieutt_id = ct.phieu_id

                     where -- a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and  to_number(to_char(p.ngay_tt, 'yyyymm')) = 202501                 ----change--2 thang- ngay 02
--                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
)  

select *
from kq_ghtt a
where ngay_bd_moi is not null ;
select* from tmp3_ts;
delete from ttkd_Bsc.ct_bsc_trasau_tp_tratruoc where thang = 202501;
commit;
-- insert vao bang bsc
insert into ttkd_bsc.ct_bsc_trasau_tp_tratruoc
--insert into ct_bsc_trasau_tp_tratruoc
(THANG, GH_ID, MA_TB, THUEBAO_ID, MANV_THUYETPHUC, MA_TO, MA_PB, THOIGIAN_TH
, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU
, TEN_NGANHANG, TEN_HT_TRA, AVG_THANG, SO_THANGDC, MANV_CN, PHONG_CN, MANV_GT, MANV_THUNGAN, NHOMTB_ID, MANV_QL, MATO_QL, MAPB_QL
, PHIEUTT_ID, TIEN_KHOP,  HT_TRA_ID, KENHTHU_ID, loaitb_id,goi_old_id)
with t0 as (select c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                        , c0.ngay_tt, c0.ngay_hd, null ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
--                                        , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                        , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                        , c0.nvtuvan_id, c0.nvthu_id NHANVIENGT_ID, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn
                                        ,nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
                       from tmp3_ts c0
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

, c as (select t0.thuebao_id, t0.phieutt_id, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nhanviengt_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang, t0.loaitb_id
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                )
, goi_cu as (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
                  from tinhcuoc.v_sd_goi_dadv@dataguard 
                  where  KYHOADON = 20241201
        )
select 202501 thang,  -1 gh_id, dbtb.ma_tb, a.thuebao_id
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
                        when a.ht_tra_id in (1, 7,216,204,214) then 1
                        when a.ht_tra_id in (2,  4) then 0 else null end tien_khop
--            , (
--            PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
            , a.ht_tra_id, a.kenhthu_id,a.loaitb_id, c.nhomtb_id
 from c a
           --     join css_hcm.db_thuebao dbtb on a.thuebao_id = dbtb.thuebao_id and dbtb.loaitb_id in (58, 59)
                left join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id --and dbtb.loaitb_id in (58, 59)          ---change n -1
                left join goi_cu c on a.thuebao_id = c.thuebao_id  and c.rnk = 1
            left join ttkd_bsc.nhanvien nv on a.MANV_THUYETPHUC = nv.ma_nv and thang = 202501 ; 
 commit;
 --- update chung tu
-- 4. TRA SAU THUYET PHUC TRA TRUOC
rollback;
UPDATE TTKD_bSC.ct_bsc_trasau_tp_tratruoc set tien_khop = 1 WHERE THANG = 202501 AND tien_khop = 0 and ma_Gd in (
select ma from ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_Hd_1);

update ttkd_bsc.ct_bsc_trasau_tp_tratruoc a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- phieu con co tien bang
update ttkd_bsc.ct_bsc_trasau_tp_tratruoc a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
select* from css_hcm.hinhthuc_Tra where ht_Tra_id = 207;
-- bang cha du tien
update ttkd_bsc.ct_bsc_trasau_tp_tratruoc a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
       -- and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;
-- quay ve 0

commit;
select * from ttkdhcm_ktnv.ds_chungtu_Nganhang_sub_oneb where ma_gd = 'HCM-TT/03101805';
select tien_Ct,tien_tt from ttkdhcm_ktnv.ds_chungtu_Nganhang_oneb where chungtu_id = 484294;
 delete from  TTKD_BSC.CT_DONGIA_TRATRUOC where thang = 202501 and loai_tinh=  'DONGIA_TS_TP_TT';
 select* from ttkd_Bsc.ct_dongia_tratruoc where thang =202501;
 --- tinh don gia
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
                                            when max(a.SO_THANGDC) < 6 and max(a.SO_THANGDC) >= 3 then 0.9
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
                        where a.thang = 202501-- and --a.rkm_id is not null AND -- don gia TP 

                        and a.rkm_id is not null AND -- don gia TP 
                        A.MANV_tHUYETPHUC IS NOT NULL --and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, A.MANV_THUYETPHUC,  a.ma_to, a.ma_pb, A.LOAITB_ID
                                           ,a.nhomtb_id, a.goi_old_id
                                            , hs.khachhang_id, a.ma_Tb
        ) a
        where rnk = 1 and dthu > 0;

commit;
delete from ttkd_bsc.tl_giahan_Tratruoc where thang = 202501 and loai_tinh=  'DONGIA_TS_TP_TT';
-- insert vao bang ty le
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
            and loai_tinh in ('DONGIA_TS_TP_TT') and a.thang = 202501 and b.ma_pb not in ('VNP0702300','VNP0702400','VNP0702500')
group by a.thang, loai_tinh, ma_kpi,  a.ma_Nv, b.MA_tO, b.MA_PB;

commit;