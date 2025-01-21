    -----10-Fiber ID114 tu van KH tra sau qua tra truoc----
commit;
            drop table tmp3_ts purge;
            select * from tmp3_ts ;--where thang = 202312;
            create table tmp3_ts as

        ---Buoc 1----tao danh sach hop dong tra truoc trong thang T
   -- insert into tmp3_ts
                select *
                from (select  --gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt
                                              gh.thuebao_id, gh.ma_tb
                                            , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                            , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                            , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                            , a.phieutt_id, a.trangthai
                                            , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                            , (select b.kenhthu from css_hcm.kenhthu b where b.kenhthu_id=a.kenhthu_id) kenhthu   
                                            , (select b.ten_nh from css_hcm.nganhang b where b.nganhang_id=a.nganhang_id) ten_nganhang
                                            , (select b.ht_tra from css_hcm.hinhthuc_tra b where b.ht_tra_id=a.ht_tra_id) ten_ht_tra
                                            , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                             from css_hcm.phieutt_hd a, css_hcm.ct_phieutt b
                                        , (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                            ) hddc --, css_hcm.hdtb_datcoc hdttdc
                                        , css_hcm.hd_thuebao hdtb, css_hcm.hd_khachhang hdkh
                                        , (select distinct thuebao_id, ma_tb 
                                              from ttkd_bct.ds_ketqua_capnhat_dai a 
                                              where thang_kt=0 and to_number(to_char(ngay_cn, 'yyyymm')) = 202402   ---change n
                                                            ) gh                                             
                                         , (select * from css_hcm.khuyenmai_dbtb where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc) kmtb
                                        , (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                                                                        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                                        group by bb.phieu_id) ct 
                             where a.kenhthu_id not in (6) and a.trangthai = 1
                                            and a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            and b.hdtb_id = hdtb.hdtb_id and kieuld_id in (550, 551, 24, 13080) and tthd_id <> 7
                                            and hdtb.hdkh_id = hdkh.hdkh_id
                                            and b.hdtb_id = hddc.hdtb_id (+)
                                            and b.hdtb_id = kmtb.hdtb_id (+)
                                             and hdtb.thuebao_id = gh.thuebao_id
                                             and a.phieutt_id = ct.phieu_id (+)
                                   --         and to_number(to_char(a.ngay_tt, 'yyyymm')) = 202312                     ----change thang n---
                                            and ((a.ht_tra_id <> 2 and to_number(to_char(a.ngay_tt, 'yyyymm')) = 202402)                     ----change thang n---
                                                            or (a.ht_tra_id in (2,7,4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202402))                     ----change thang n---
                                           -- and gh.ma_tb in ('fibervanloi')   ----loai taykhi can---
                                        --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                            ) a
                            where ngay_bd_moi is not null 
                                            and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id)
                                            and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where rkm_id = a.rkm_id)
                                      --      and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_tr30day where rkm_id = a.rkm_id)
                                            and not exists (select rkm_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where rkm_id = a.rkm_id)
                                      --  and a.ma_tb = 'hongminh1743051'
;
rollback;
select* from tmp3_Ts;
commit;

        ---Buoc 2: tao danh sach chot Fiber tra sau chuyen tra truoc
          --  create table ttkd_bsc.ct_bsc_trasau_tp_tratruoc as; 173tp 5562493 5231413
           select *   from ttkd_bct.ds_ketqua_capnhat_dai a where thang_kt = 0 and to_number(to_char(ngay_cn, 'yyyymm')) = 202203 ;
                                                            max idno
                                                            ;MA_TT_DHSXKD
select *  from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202401; and rkm_id  in (select rkm_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202111);
alter table ttkd_bsc.ct_bsc_trasau_tp_tratruoc rename column NGAY_HD to ngay_nganhang;
select *  from ct_bsc_trasau_tp_tratruoc where thang = 202312; and rkm_id  in (select rkm_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202111);

insert into ttkd_bsc.ct_bsc_trasau_tp_tratruoc
--insert into ct_bsc_trasau_tp_tratruoc

(THANG, GH_ID, MA_TB, THUEBAO_ID, MANV_THUYETPHUC, MA_TO, MA_PB, THOIGIAN_TH
, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU
, TEN_NGANHANG, TEN_HT_TRA, AVG_THANG, SO_THANGDC, MANV_CN, PHONG_CN, MANV_GT, MANV_THUNGAN, NHOMTB_ID, MANV_QL, MATO_QL, MAPB_QL
, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, HT_TRA_ID, KENHTHU_ID)

with t0 as (select c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                        , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                                    --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                        , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                        , c0.nvtuvan_id, c0.nhanviengt_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
                       from tmp3_ts c0
                                    left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                                    left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                                    left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                                    left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nhanviengt_id
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
, ds as (select a.*, w.ma_tt, rank() over (partition by a.ma_tb ORDER BY a.idno  desc) rnk  
                      from ttkd_bct.ds_ketqua_capnhat_dai a
                                            join ttkd_bct.moi_giahan_tratruoc_moi w on w.gh_id = a.gh_id and w.loaitb_id in (58, 59)
                      where a.thang_kt=0 and to_number(to_char(a.ngay_cn, 'yyyymm')) = 202402---thang n--change anh Huyen, Duc Dung moi thang -------
                    )
, c as (select t0.thuebao_id, t0.phieutt_id, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nhanviengt_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                )
select to_char(a.ngay_cn, 'yyyymm') thang, a.gh_id, a.ma_tb, a.thuebao_id
            , nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC, nv.ma_to, nv.ma_pb
           , a.ngay_cn
           , a.MA_TT, c.MA_GD, c.RKM_ID, c.ngay_BD_MOI, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.NGAY_NGANHANG, c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG
           , c.TEN_HT_TRA, c.AVG_THANG, c.SO_THANGDC, c.manv_cn, c.phong_cn, c.manv_gt, c.manv_thungan
            , (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and a.thuebao_id = thuebao_id and dichvuvt_id = 4
                                                                and goi_id in (select goi_id from css_hcm.goi_dadv_ccbs where nhomgoi_id in (6, 16))
            ) nhomtb_id, dbtb.ma_nv manv_ql
                                        , getxmlagg_table('ttkd_bsc.dm_to', 'ma_to', 'hieuluc = 1 and  tbh_id = ' ||dbtb.tbh_ql_id, '-1') mato_ql
                                    , getxmlagg_table('ttkd_bsc.dm_phongban', 'ma_pb', 'active = 1 and  pbh_id = ' ||dbtb.pbh_ql_id, '-1') mapb_ql
            , c.phieutt_id
           , case when c.rkm_id is null then null
                        when c.ht_tra_id in (1, 7) then 1
                        when c.ht_tra_id in (2,  4) then 0 else null end tien_khop
            , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
            , c.ht_tra_id, c.kenhthu_id
 from ds  a  
           --     join css_hcm.db_thuebao dbtb on a.thuebao_id = dbtb.thuebao_id and dbtb.loaitb_id in (58, 59)
                left join ttkd_bct.db_thuebao_ttkd dbtb on a.thuebao_id = dbtb.thuebao_id and dbtb.loaitb_id in (58, 59)          ---change n -1
                left join c on a.thuebao_id = c.thuebao_id
            left join ttkd_bsc.nhanvien_202402 nv on c.MANV_THUYETPHUC = nv.ma_nv   ---change thang n----
where a.rnk = 1 ;
commit;
select* from ct_bsc_trasau_tp_tratruoc where thang = 202402
        --and a.ma_tb = 'thuylinh194'
         --   and c.rkm_id is not null
-- order by w.thoigian_th
;
delete from ct_bsc_trasau_tp_tratruoc where thang = 202401-- and tien_thanhtoan < 40000
            select * from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202310; and ma_pb = 'VNP0701100'; --fiber40_2019999
            
            select thuebao_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202310 group by thuebao_id having count(*) >1
            ;
            
                         select * from  ct_bsc_trasau_tp_tratruoc a  where thang = 202401 and TIEN_THANHTOAN < 20000
                ;


---check-
---- loai tru  ptm n-1, n
delete from ct_bsc_trasau_tp_tratruoc where thang = 202401 and thuebao_id in (
select a.thuebao_id from ct_bsc_trasau_tp_tratruoc a join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id where thang = 202401 and to_number(to_char(b.ngay_sd, 'yyyymm')) >= 202312);
select b.ngay_sd, a.* from ct_bsc_trasau_tp_tratruoc a join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id where thang = 202401 and to_number(to_char(b.ngay_sd, 'yyyymm')) >= 202312;
commit;
----loai tru tbao co tra truoc n-1
select a.* from ct_bsc_trasau_tp_tratruoc a 
-- delete from ct_bsc_trasau_tp_tratruoc a
where rkm_id is not null and thang = 202401 and exists (select 1 from v_thongtinkm_all where cuoc_dc > 0 
                                                                   -- and 202306 between thang_bddc and least (thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                                              --      and to_date('01/07/2023', 'dd/mm/yyyy') between ngay_bddc and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))
                                                                    and ngay_bddc <= to_date('31/12/2023', 'dd/mm/yyyy')
                                                                    and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))
                                                                                        >= to_date('01/12/2023', 'dd/mm/yyyy')
                                                                    and a.thuebao_id = thuebao_id
                                                                    )
                    --     and a.thuebao_id  in (select thuebao_id from v_thongtinkm_all where cuoc_dc > 0 
                       --                                             and 202208 between thang_bddc and least (thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)))
;
   commit;         
select b.ngay_sd, a.* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id where thang = 202312 and to_number(to_char(b.ngay_sd, 'yyyymm')) >= 202311;
            ------Check and except tra truoc khong bang cuoc PS
select * from ct_bsc_tratruoc_moi_trudan where thang = 202305;
            insert into ct_bsc_tratruoc_moi_trudan
                        select km.CONGVAN_ID, KHUYENMAI_ID, CHITIETKM_ID, TEN_KM, cuoc_dc, round(tien_td/1.1, 0) tien_td, ps cps_T03
                                        , THANG, GH_ID, null PBH_QL_ID, null PBH_GIAO_ID, null TBH_GIAO_ID, null PBH_TH_ID, null PBH_CN_ID, a.MA_TB, MANV_QL, null PHONG_QL, MA_TO, MA_PB, null MANV_GIAO
                                        , null PHONG_GIAO, null MANV_TH, null PHONG_TH, null MA_NV_CN, MANV_THUYETPHUC, null KQ_POPUP, null KQ_DVI_CN, null PHONG_CN, null THANG_KTDC_CU, null TIEN_DC_CU
                                        , MA_TT, MA_GD, a.RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU
                                        , TEN_NGANHANG, TEN_HT_TRA, 1 TRANGTHAI_TB, a.THUEBAO_ID, 58 LOAITB_ID, null MANV_TCTN, null PBH_OA_ID, null MANV_OA, NHOMTB_ID, null KHACHHANG_ID, null GOI_OLD_ID 
                        from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a left join v_thongtinkm_all km on a.rkm_id = km.rkm_id
                             
                                    left join (select ma_tb, sum(tien) ps from bcss_hcm.ctkmtc_int_20230501 where khoanmuctc_id not in (521, 421, 4067, 453)group by ma_tb
                                                        ) ps on a.ma_tb = ps.ma_tb---thang n-1
                        where thang = 202306 and congvan_id = 190 and nhom_datcoc_id in (1)
                                        ---tien tru dan >= tien ps thang n là OK
                                        and TIEN_TD   < COALESCE (ps, 9999999) 
                                          ---import <= 30k
                                        --  and a.ma_tb in ('myngoc9297')
                                        and cuoc_dc < 70000
                                 
                                    order by 6
            ;
             
                update ttkd_bsc.ct_bsc_trasau_tp_tratruoc a set tien_khop = null
                ---select * from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
                where a.thang = 202305 and rkm_id in (select rkm_id from ct_bsc_tratruoc_moi_trudan where thang = a.thang)
                   ;

commit;  

 ---check-
                        ---- loai tru  ptm n-1, n
delete from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thang = 202402 and thuebao_id in (
            select a.thuebao_id from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id 
            where thang = 202402 and to_number(to_char(b.ngay_sd, 'yyyymm')) >= 202401);
            --
select b.ngay_sd, a.* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id where thang = 202402
and to_number(to_char(b.ngay_sd, 'yyyymm')) >= 202401;
        ----loai tru tbao co tra truoc n-1,
select* from v_Thongtinkm_all where thuebao_id in (2937651,8810342,9153901,8056555,9352521,9175386,5966571,11472959)
select a.* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a 
-- delete from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
where rkm_id is not null and thang = 202402 and exists (select 1 from v_thongtinkm_all where cuoc_dc > 0 
                                               -- and 202306 between thang_bddc and least (thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                          --      and to_date('01/07/2023', 'dd/mm/yyyy') between ngay_bddc and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))
                                                and ngay_bddc <= to_date('30/01/2024', 'dd/mm/yyyy')
                                                and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR))
                                                                    >= to_date('01/01/2024', 'dd/mm/yyyy')
                                                and a.thuebao_id = thuebao_id
                                                )
                                        --     and a.thuebao_id  in (select thuebao_id from v_thongtinkm_all where cuoc_dc > 0 
                                           --                                             and 202208 between thang_bddc and least (thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)))
;
commit;
    
insert into ttkd_Bsc.ct_bsc_trasau_tp_tratruoc 
select distinct ma_gd,ma_chungtu from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202402 and tien_khop = 0 and ma_chungtu is not null
select * from nhuy.loaibo-- where loaibo = 1
update nhuy.loaibo
set loaibo = 1 
where so_Thangdc <3
commit;
delete 
select km, loaibo, thang_kt, tratruoc
from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202312 and thuebao_id in (select thuebao_id  from nhuy.loaibo) -- where loaibo = 1)
select *--km, loaibo, thang_kt, tratruoc
from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202312 and thuebao_id not in 
(select thuebao_id  from  ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202312 and tratruoc = 1 and loaibo = 0 and km =1)
delete from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202312 and thuebao_id in (select thuebao_id from loaibo where loaibo = 1)
select tratruoc, loaibo, km from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt= 202312
and thuebao_id in (select thuebao_id from loaibo where loaibo = 1)
commit;