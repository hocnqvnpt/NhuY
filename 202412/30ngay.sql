-- truoc do: import danh sach giao tu onebss ve/ lay trong bang bao ngay schema report
drop table tmp3_60ngay;
--- buoc 1: tao bang tam
create table tmp3_60ngay as
with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--                                                    group by bb.phieu_id)

, kq_ghtt as (select  hdkh.khachhang_id, hdtb.thuebao_id, 0 duan_id, hdtb.ma_tb, 'matt' ma_tt, hdtb.loaitb_id, 22 as lan
                --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , a.phieutt_id, a.trangthai
                , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
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
--                                            join gh on hdtb.thuebao_id = gh.thuebao_id --and rnk = 1
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240801 and 20250101                  ----change--3 thang- ngay 2
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and a.ma_gd = 'HCM-TT/02290172'
                        )
select * from kq_ghtt a
where  a.ngay_bd_moi is NOT null ;
                    and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >=202409);
                    and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                  and not exists (select rkm_id from tmp3_60ngay where rkm_id = a.rkm_id) 
            ;
            select* from TMP3_60NGAY;
            select rkm_id from TMP3_60NGAY group by rkm_id having count(rkm_id) > 1;
            select ngay_tt , ma_gd from css_hcm.phieutt_hd where phieutt_id  = 8278166
;
select* from  ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202412;
-- buoc 2: them vao bang chinh
insert into ttkd_bsc.ct_bsc_tratruoc_moi_30day

-- insert into nhuy.ct_bsc_tratruoc_moi_30day-create table t4 as
    (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT,  SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        );
    with t0 as (select 202412 thang_kt, c0.thuebao_id, c0.phieutt_id, 'matt' ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                , c0.ngay_tt, c0.ngay_hd, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, null nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, null manv_gt, nv3.ma_nv manv_thungan
               from tmp3_60ngay c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
--                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                    where lan = 11
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
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 85
                     )
            , ds as (select 'Phòng Bán Hàng Online' ten_pbh, thuebao_id, thang_kt
                    from bhol_giao 
                    where luonggiao_ttvt is null and (KETQUA_LSOB_DAUTIEN!= 'Không g?p/ không tìm ???c khách hàng ' or
                    KETQUA_LSOB_SAUCUNG !='Không g?p/ không tìm ???c khách hàng ')
                    )
            , c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, null ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) 
                , goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
                from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
    select a.thang_kt thang, 0, 0 pbh_ql_id, 0 pbh_giao_id, 0 tbh_giao_id
                , 0 pbh_id_th, 0  pbh_cn_id
                , dbtb.ma_tb, null manv_cs,a.ten_pbh phong_cs
                 , null ma_to
                 ,null ma_pb
                 ,null manv_giao, a.ten_pbh PHONG_GIAO, null ma_nv_th, a.ten_pbh PHONG_TH
                 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                 , nvtp.ma_pb MAPB_THPHUC
                , c.manv_gt, c.manv_thungan
                 , lkh.khdn, null heso
                , a.THANG_KT, 0 TIEN_DC_CU
                , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT
                                    , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                , tt.trangthai_tb, a.thuebao_id, dbtb.loaitb_id, null pbh_oa_id, null manv_oa
                 , 
                 goi.nhomtb_id
                , dbkh.khachhang_id, null goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                
                , case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204,214,216) then 1
                                when c.ht_tra_id in (2, 4,5,207) then 0 else null end tien_khop
                ,null ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202412 -- change
                                left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
                        
    where  a.thang_kt = 202412 --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
                            and dbkh.khachhang_id  <> 9798610          ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
                  --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb in ('hcmnhp183')
                  -- and a.thuebao_id in (1307757,
                and not exists  (select 1 from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day a where thang = 202412 and rkm_id = c.rkm_id);
;
select count(1) from ds_giahan_Tratruoc_t9;
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202409;
delete from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day a where thang = 202412 and  a.khachhang_id  = 9798610  ;
update  ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day a 
set ma_pb = 'VNP0703000'
where thang = 202412 and ma_pb is null;
commit;
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where ma_ct='VCB_20241202_438486';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_sub_oneb where chungtu_id =438486;
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1 where ma= 'HCM-TT/03057015';
delete from 
UPDATE TTKD_bSC.ct_Bsc_Tratruoc_moi_30day set tien_khop = 1 WHERE THANG = 202412 AND tien_khop = 0 and ma_Gd in (
select ma from ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_Hd_1);
rollback;
select distinct phong_cs , ma_pb from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202412; and ma_pb is null;

-- update ma_pb
-- bhkv
update ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day a 
set ma_pb = (select distinct ma_pb from ttkd_Bsc.nhanvien where thang = 202412 and ten_pb = phong_cs)
where thang = 202412 and ma_pb is null;
-- khdn
update ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day a 
set ma_pb = case when phong_cs like '%Doanh Nghi?p 1' then 'VNP0702300'
                when phong_cs like '%Doanh Nghi?p 2' then 'VNP0702400'
                when phong_cs like '%Doanh Nghi?p 3' then 'VNP0702500' end
where thang = 202412 and ma_pb is null;
commit;
-- update chung tu
update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202412 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
commit;
-- phieu con co tien bang
update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202412 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- bang cha du tien
update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set tien_khop = 1
-- select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202412 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 308)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--        and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;
update ttkd_Bsc.ct_Bsc_tratruoc_moi_30day set tien_khop =  1 
where thang = 202412 and tien_khop = 0 and ht_Tra_id in (207) and kenhthu_id in (25,26);
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202412 and tien_khop = 0;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202412 and tien_khop is not null and rkm_id is   null;
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_SL_COMBO_006';
select* from ttkd_bsc.blkpi_Dm_to_pgd where thang >=202412;

--- gia han truoc ky
--select* from ds_giahan_Tratruoc2 where thuebao_id in (
create table tt as
with km0 as (  
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
, tt as (
    select a.*
    from DS_GIAHAN_TRATRUOC2 a join km0 b on a.rkm_id = b.rkm_id
)  select* from tt;
update ttkd_bsc.ct_Bsc_tratruoc_moi_30day a set tien_khop = 1
--select * from ttkd_bsc.ct_Bsc_tratruoc_moi_30day a 
where thang = 202412 and rkm_id is null and 
and exists (select 1 from tmp3_60ngay where a.thuebao_id = thuebao_id);
update ttkd_bsc.ct_Bsc_tratruoc_moi_30day a set tien_khop = 0, ma_Chungtu = 'x'
where thang = 202412 and rkm_id is not null  
and not exists (select 1 from ds_Giahan_Tratruoc2_v2 where a.thuebao_id = thuebao_id and to_number(to_char(ngay_kt_mg,'yyyymm'))>202412);
update ttkd_bsc.ct_Bsc_tratruoc_moi_30day a
set tien_khop = 1 where thang = 202412 and tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_Hd_1);
commit;
select distinct tien_khop from  ttkd_bsc.ct_Bsc_tratruoc_moi_30day  where thang = 202412;
select distinct ma_kpi, loai_tinh from  ttkd_Bsc.tl_giahan_Tratruoc where thang = 202412;
delete from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202412 and ma_Kpi = 'HCM_TB_GIAHA_022' and ma_pb = 'VNP0703000';

insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE);
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                        where thang = 202412
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
                ) a
;
--- bhol
insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE);
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang,'VNP0703000' ma_pb,'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                            join bhol b on a.thuebao_id = b.thuebao_id
                                        where thang = 202412
                                        group by thang, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang
                order by 2
                ) a
;
commit;
select * from 
            (select GH_ID, a.MA_TB, MANV_CS, PHONG_CS
                     , MANV_GIAO ma_nv, PHONG_GIAO
                     , MA_TO, 'VNP0703000' MA_PB
                     , MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO
                     , THANG_KTDC_CU, TIEN_DC_CU TIEN_DC_CU_VAT, a.MA_GD
                     , RKM_ID, THANG_BD_MOI, so_thangdc, TIEN_THANHTOAN, VAT, THANG, TEN_HT_TRA
                     , to_char(NGAY_TT, 'dd/mm/yyyy') ngay_tt, to_char(NGAY_NGANHANG, 'dd/mm/yyyy') NGAY_NGANHANG, nhomtb_id, ma_chungtu, tien_khop
            from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                    join bhol b on a.thuebao_id = b.thuebao_id
            where thang = 202412
            
);

commit;
-- buoc 3: do bang luong
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_022' )
    , giao =  (select tong from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_022' )
    ,thuchien =  (select da_giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_022' )

where thang = 202412 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where-- giamdoc_phogiamdoc = 1 
 ma_kpi in ('HCM_TB_GIAHA_022') and thang = 202412 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_022' and chitieu_giao is not null and ma_pb = 'VNP0703000';

-- buoc 4: muc do hoan thanh
--- cac phong khac
update ttkd_Bsc.bangluong_kpi  
set mucdo_hoanthanh = case when tyle_Thuchien >= 80 then 120
                        when tyle_thuchien >= 65 and tyle_thuchien < 80 then 100 + (tyle_thuchien - 65)
                        when tyle_thuchien >= 40 and tyle_thuchien < 65 then 100 - 2*(65 - tyle_thuchien)
                        when tyle_thuchien < 40 then 0 end
where ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202412 and ma_pb != 'VNP0703000';
commit;
--- bhol 
update ttkd_Bsc.bangluong_kpi  
set mucdo_hoanthanh = case when tyle_Thuchien >= 80 then 120
                        when tyle_thuchien >= 65 and tyle_thuchien < 80 then 100 + 1.1*(tyle_thuchien - 65)
                        when tyle_thuchien >= 45 and tyle_thuchien < 65 then 50 + 2*(tyle_thuchien-45)
                        when tyle_thuchien >= 30 and tyle_thuchien < 45 then 20 + 1.5*(tyle_thuchien-30)
                        when tyle_thuchien < 30 then 0 end
where ma_kpi ='HCM_TB_GIAHA_022' AND THANG = 202412 and ma_pb = 'VNP0703000';
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi ='HCM_TB_GIAHA_022';
commit;
-------- bo sung them cac giao dich co ngay tt null
create table tmp3_60ngay_Bs as
with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--                                                    group by bb.phieu_id)

, kq_ghtt as (select  hdkh.khachhang_id, hdtb.thuebao_id, 0 duan_id, hdtb.ma_tb, 'matt' ma_tt, hdtb.loaitb_id, 22 as lan
                --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , a.phieutt_id, a.trangthai
                , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
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
--                                            join gh on hdtb.thuebao_id = gh.thuebao_id --and rnk = 1
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and to_number(to_char(hdtb.ngay_ht, 'yyyymmdd')) between 20240901 and 20241201   
                                        and a.ngay_tt is null----change--3 thang- ngay 2
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and a.ma_gd = 'HCM-TT/02290172'
                        )
select * from kq_ghtt a
where  a.ngay_bd_moi is NOT null ;
                    and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi where rkm_id = a.rkm_id and thang >=202409);

insert into ttkd_bsc.ct_bsc_tratruoc_moi_30day

-- insert into nhuy.ct_bsc_tratruoc_moi_30day-create table t4 as
    (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT,  SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        )
    with t0 as (select 202412 thang_kt, c0.thuebao_id, c0.phieutt_id, 'matt' ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                , c0.ngay_tt, c0.ngay_hd, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, null nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, null manv_gt, nv3.ma_nv manv_thungan
               from tmp3_60ngay_bs c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
--                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                    where lan = 11
                )
                 , km0 as (  
                                ----------------TT Thang tang tren 1 dong-------------
                            select  km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                            , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                            , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all_ol km 
                            where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                            --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                            and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                           union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                            select  km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                            , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                            , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all_ol km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                                                                                            from v_thongtinkm_all_ol where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                                                                                        ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                            where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                                           -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                     )
            , ds as (select ten_pbh, thuebao_id, thang_kt
                    from giao_oneb where thang_kt =202412 and thang = 202412
                    )
            , c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, null ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                )-- select* from c;
                , goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
                from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
    select  a.thang_kt thang, 0, 0 pbh_ql_id, 0 pbh_giao_id, 0 tbh_giao_id
                , 0 pbh_id_th, 0  pbh_cn_id
                , dbtb.ma_tb, null manv_cs,a.ten_pbh phong_cs
                 , null ma_to
                 ,null ma_pb
                 ,null manv_giao, a.ten_pbh PHONG_GIAO, null ma_nv_th, a.ten_pbh PHONG_TH
                 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                 , nvtp.ma_pb MAPB_THPHUC
                , c.manv_gt, c.manv_thungan
                 , lkh.khdn, null heso
                , a.THANG_KT, 0 TIEN_DC_CU
                , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT
                                    , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                , tt.trangthai_tb, a.thuebao_id, dbtb.loaitb_id, null pbh_oa_id, null manv_oa
                 , 
                 goi.nhomtb_id
                , dbkh.khachhang_id, null goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                
                , case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204,214,216,208) then 1
                                when c.ht_tra_id in (2, 4,5,207) then 0 else null end tien_khop
                ,null ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                    on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                    on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                join c 
                                    on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien nvtp 
                                    on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202412 -- change
                                left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
                        
    where  a.thang_kt = 202412 and a.thuebao_id in (select thuebao_id from tmp3_60ngay_Bs)
     and not exists (select 1 from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202412 and tien_khop =1 and thuebao_id = a.thuebao_id);   and dbtb.ma_Tb='co29042019' --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
                            and dbkh.khachhang_id  <> 9798610          ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
           ;       --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb in ('hcmnhp183')
                  -- and a.thuebao_id in (1307757,
                and c.rkm_id in (select rkm_id from tmp3_30ngay where lan = 11)
;
commit;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang= 202412 ;and thuebao_id in (
select thuebao_id from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang= 202412 group by thuebao_id having count(distinct nvl(tien_khop,0) )>1);
and rkm_id is null;
select* from tmp3_60ngay_bs where ma_Tb='phuongc1608';
select * from css_hcm.hd_khachhang where ma_gd = 'HCM-TT/03104150';

select ngay_ht from css_Hcm.hd_Thuebao where hdkh_id =25127620;
