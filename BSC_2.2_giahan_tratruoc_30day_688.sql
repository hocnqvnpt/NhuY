--- Ngay quet:
        ---Lan 1: ngay 2 chot het ngay 1 la ngay tt ma GD doi voi hinh thuc tra khac CK
        ---Lan 2: ngay 5 chot het ngay 4 la ngay tt ma GD va ngay ngan hang phai la ngay cuoi thang 30/31 (do con xu ly chung tu nhan cong) doi voi hinh thuc tra la CK
---Ngay thanh toan:
        ---Het ngay cuoi thang doi voi ngay ngan hang (chung tu)
        ---Het ngay 1 doi voi cac hinh thuc con lai (do ngay 1 moi hoan cong cac hop dong doi toc do)
---GHTC: doi voi congvan_id = 190 (tra truoc tru dan) xet tien AVG thang moi >= AVG thang cu
;
s
select * from css_hcm.phieutt_hd  where phieutt_id = 4132116;hdkh_id in (10371518, 10518328);
select * from CSS_HCM.ct_phieutt where hdtb_id = 11426982;
selectc * from css_hcm.hd_khachhang;
rollback;
select * from css_hcm.hd_thuebao where  ma_tb = 'kieu923'; 11110732 11209509
select * from css_hcm.db_datcoc where rkm_id = 3398911;
select hdtb_id,thuebao_dc_id from css_hcm.hdtb_datcoc where rkm_id = 3398911;3104485
3397926
select * from css_hcm.hdtb_datcoc where THUEBAO_THOAITRA_ID = 2591858;2549813;2562561;
select DISTINCT HT_TRA_ID from tmp3_30ngay_A--css_hcm.khuyenmai_dbtb
;
SELECT* FROM CSS_HCM.HINHTHUC_tRA WHERE HT_tRA_ID = 204
-----1-------------Danh sach tham gia han tra truoc--------------PL1(BH, Dai)
drop table tmp3_30ngay-- purge;
select distinct "GH.TRATRUOC+1--,DBDC.THANG_BDB,HDTTDC.THANG_BDC,HDTTDC.*" from tmp3_30ngay;

select rkm_id from tmp3_60ngay group by rkm_id having count(rkm_id) > 1; where rkm_id is null;

            alter table tmp3_30ngay rename column "3--,DBDC.THANG_BDB,HDTTDC.THANG_BDC,HDTTDC.*" to lan;
            
rollback;
commit;
select* from tmp3_30ngay where lan = 22;
update tmp3 set lan = 7;
create table tmp3 as;
--Buoc1--tao danh sach cac hop dong tra truoc
-- 
;
rollback;
insert into tmp3_30ngay (phieutt_id,hdkh_id,rkm_id) values (-1,-1,-1);
insert /*+ ENABLE_PARALLEL_DML  parallel */ into tmp3_30ngay;
create table tmp3_30ngay as 
        WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                    )
        , gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt 
            from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0  and thang_kt = 202408 ) -- change
        , kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
        , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
        group by bb.phieu_id)
--        , ct2 as (select min(aa.ngay_insert) ngay_insert, bb.phieu_id
--         from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb aa
--            join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd bb on aa.chungtu_id = bb.chungtu_id
--            group by bb.phieu_id
--        )   
        , kq_ghtt as (select  gh.khachhang_id, gh.thuebao_id, gh.duan_id, gh.ma_tb, gh.ma_tt, gh.loaitb_id, gh.thang_kt, 22 lan
                --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                            , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                            , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                            , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                            , a.phieutt_id, a.trangthai
                                            , a.ma_gd, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                            , kt.kenhthu  kenhthu   
                                            , nh.ten_nh ten_nganhang
                                            , ht.ht_tra  ten_ht_tra
                                     --       , kmtb.rkm_id a1, hddc.rkm_id a2
                                           , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                             from css_hcm.phieutt_hd a
                                                    join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                                    left join hddc on b.hdtb_id = hddc.hdtb_id
                                                    join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                                    join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                                    join gh on hdtb.thuebao_id = gh.thuebao_id
                                                    left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                                    left join ct on a.phieutt_id = ct.phieu_id
                                                    left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                                    left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                                    left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
--                                                    left join ct2 on a.phieutt_id = ct2.phieu_id
                             where a.kenhthu_id not in (6) and a.trangthai = 1                                                                        
                                            and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240401 and 20240602
                                         --   and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20230427 and 20230701                     ----change--2 thang-
                                      --      and gh.ma_tb in ('ketnoi_thoitrang')   ----loai taykhi can---
                                        --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                         --and hddc.rkm_id is not null
                        )
        select * from kq_ghtt a
        where ngay_bd_moi is not null --and thuebao_id in (9866366,9866341)
                                and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where rkm_id = a.rkm_id and thang >=202404)
                                and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where  rkm_id = a.rkm_id and thang >=202404)
                                and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                    ;        and not exists (select rkm_id from tmp3_30ngay where rkm_id = a.rkm_id) 
--                                and a.ma_gd = 'HCM-TT/02814791'
                       ;
         --       and rkm_id not in (select rkm_id from tmp3)
       --     and a.ma_gd  not like 'HCM-LD%'
        --    and not exists (select thuebao_id from v_thongtinkm_all where cuoc_dc >0 and thuebao_id = a.thuebao_id group by thuebao_id having count(*)=1 )
            --and thuebao_id = 7172624
        --    and ma_tb = 'hcm_thanhv_19'
;
commit;
(select min(aa.ngay_insert) ngay_insert, bb.phieu_id
         from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb aa
            join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd bb on aa.chungtu_id = bb.chungtu_id
            where ma_ct = 'VCB_20240510_39234'
            group by bb.phieu_id
        )   ;39234;
        
select ngay_tt from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02783902';
select* from tmp3_ob where ma_tb = 'lacnuithanh' ;
select* from tmp3_30ngay;
select phieu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd where chungtu_id = 39234 ;ma_Ct = 'VCB_20240510_39234';
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb  where chungtu_id = 39234;where thang = 202405 and ma_gd = 'HCM-TT/02814791'; rkm_id is not null;
-----2------------Chi tiet ket qua tham gia tra truoc theo ky---------------
--drop table bsc_tratruoc_201804 purge;
select* from tmp3_tr30ngay;
update ttkd_bsc.ct_bsc_tratruoc_moi set MANV_GIAO = '01956' where thang = 202011 and ma_tb = 'fibervnn-1138856';
---Kiem tra tien dat coc cu---
             select count(distinct thuebao_id) sluong, round(sum(cuoc_dc/1.1), 2) 
                        from ttkdhcm_ktnv.ghtt_giao_688
                            where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202309 and khachhang_id  <> 9798610 ; 61583	74578733164.55
            
            ;
            select round(sum(tien_dc_cu/1.1), 2) from(
            select distinct thuebao_id, tien_dc_cu from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202203); 62 653 374 916.36
            group by thuebao_id having count(*)>1
            ;
            select count(distinct thuebao_id) from (
             select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202309
             );-- 61583
            group by thuebao_id;
            
            select sum(TIEN_THANHTOAN) from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202112;31743455991
            commit;
            rollback;
--End---
select * from ttkdhcm_ktnv.ghtt_giao_688 where thang_kt = 202310 and tratruoc = 1 and loaibo = 0 and km= 1 and thuebao_id in ('9326578');length(nhanvien_giao) >10;
--create table ttkd_bsc.ct_bsc_tratruoc_moi_30day as CTV057649
select * ;
commit;
delete 
from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202404; and ma_tb = 'vitnam6165925';
        and nvl(rkm_id, 0) not in (select rkm_id from tmp3_30ngay);and ma_tb in ('nutifood59nvt'); and ma_gd is null;, 'ttm781', 'nhudao_70', 'thanhhiencc642', 'tho_200918', 'ztech61')
                    ; 
                    and rkm_id = 3369696; and thuebao_id in (select thuebao_id from ttkd_bct.moi_giahan_tratruoc_moi where kycuoc = 201912 and tratruoc = 1 and loaibo = 1);
commit;
select* from TMP3_30NGAY_44;
----Buoc 2-----tao danh sach chot moi gia han tra truoc thang T
---truncate table hocnq_ttkd.ct_bsc_tratruoc_moi_30day;
        --select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202404;
insert into ttkd_bsc.ct_bsc_tratruoc_moi_30day

        
-- insert into nhuy.ct_bsc_tratruoc_moi_30day-create table t4 as
    (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        )
    with t0 as (select c0.thang_kt, c0.thuebao_id, c0.phieutt_id, c0.ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan
               from TMP3_30NGAY c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                            where lan = 22
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
            , ds as (select min(ghtt_id) gh_id, donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_nv_th, donvi_oa, nhanvien_oa, nvl(duan_id, 0) duan_id, ma_nv, tbh_ql_id, pbh_ql_id
                                , ma_tb, heso, min(to_char(ngay_kt_mg, 'yyyymmdd')) thang_ktdc_cu, max(SO_THANGDC) thangdc, sum(cuoc_dc) cuoc_dc, khachhang_id, thuebao_id, loaitb_id, goi_id, thang_kt
                    from ttkdhcm_ktnv.ghtt_giao_688
                    where tratruoc = 1 and km = 1 and loaibo = 0 
                    group by donvi_giao, tbh_giao_id, nhanvien_giao, pbh_id_th, tbh_id_th, ma_tb, ma_nv_th, donvi_oa, nhanvien_oa, heso, khachhang_id, thuebao_id, loaitb_id, goi_id, duan_id, ma_nv, tbh_ql_id, pbh_ql_id, thang_kt
                    )
            , c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                )
                , goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
                from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
    select a.thang_kt, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
                , a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
                , a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
                 , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
                 , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
                 , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
                 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                 , nvtp.ma_pb MAPB_THPHUC
                , c.manv_gt, c.manv_thungan
                 , lkh.khdn, a.heso
                , a.THANG_KTDC_CU, a.cuoc_dc TIEN_DC_CU
                , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.ngay_nganhang
                                    , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                , tt.trangthai_tb, a.thuebao_id, a.loaitb_id, donvi_oa pbh_oa_id, nhanvien_oa manv_oa
                 , 
                 goi.nhomtb_id
                , a.khachhang_id, a.goi_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                
                , case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204) then 1
                                when c.ht_tra_id in (2, 4,5,207,214) then 0 else null end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                left join ttkd_bsc.dm_phongban pb
                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
                                left join  ttkd_bct.phongbanhang pb_giao
                                    on a.donvi_giao = pb_giao.pbh_id
                                left join  ttkd_bct.phongbanhang pb_th
                                    on a.pbh_id_th = pb_th.pbh_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien_202405 nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv -- change
                                left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
                        
    where  a.thang_kt = 202405 --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
--                            and a.khachhang_id  <> 9798610          ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
                  --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb in ('hcmnhp183')
                  -- and a.thuebao_id in (1307757,
               -- and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202308 and c.rkm_id = rkm_id )
               and exists (select 1 from tmp3_30ngay where thuebao_id = a.thuebao_id and lan = 22)
;select* from  ttkdhcm_ktnv.ghtt_giao_688
                    where tratruoc = 1 and km = 1 and loaibo = 0 and thang_kt = 202405
; 
commit;

rollback;
-- KIEM TRA TIEN THANH TOAN AO
delete 
--select *
from ttkd_bsc.ct_Bsc_tratruoc_moi a
where exists (select 1 from tmp3_60ngay where thuebao_id = a.thuebao_id and lan =22) and thang = 202405 and rkm_id is null;
--
update  ttkd_bsc.ct_bsc_tratruoc_moi_30day set tien_khop = 1 where ma_Tb ='agribank-280tptd' and  thang = 202405 ;
update ttkd_bsc.ct_Bsc_tratruoc_moi set tien_khop = 1 where tien_khop = 0 and rkm_id in  (select rkm_id from tmp3_60ngay where lan =22) and ma_gd in (Select ma_gd from chungtu where ketqua = 'OK');
-- DO VAO BANG CHINH
COMMIT;
(Select DISTINCT ketqua from chungtu );where ketqua = '');
insert into ttkd_Bsc.ct_Bsc_tratruoc_moi_30day;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202405 and tien_Thanhtoan < 100000;
--
set nhomtb_id = 2854435
where thang = 202312 and thuebao_id = 8067986
commit;
select rkm_id from tmp3_tr30ngay  group by rkm_id having count(rkm_id) > 1;

        ----Buoc 3---loc va loai tru thue bao co cuoc ao thap hon cuoc PS
------Check and except tra truoc khong bang cuoc PS
select * from ct_bsc_tratruoc_moi_trudan where thang = 202305;
            insert into ct_bsc_tratruoc_moi_trudan;
                        select km.CONGVAN_ID, KHUYENMAI_ID, CHITIETKM_ID, TEN_KM, cuoc_dc, round(tien_td/1.1, 0) tien_td, COALESCE(ps, ps_m, ps_htv) cps_T03
                                        , THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, a.MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO
                                        , PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, khdn, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU
                                        , MA_TT, MA_GD, a.RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU
                                        , TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, a.THUEBAO_ID, a.LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID 
                        from ttkd_bsc.ct_bsc_tratruoc_moi_30day a left join v_thongtinkm_all km on a.rkm_id = km.rkm_id
                             
                                    left join (select ma_tb, sum(tien) ps from bcss_hcm.ctkmtc_int_20230801@db_tctt where khoanmuctc_id not in (521, 421, 4067, 453)group by ma_tb
                                                        ) ps on a.ma_tb = ps.ma_tb and a.loaitb_id in (58, 59)---thang n -1
                                    left join (select ma_tb, sum(tien) ps_m from bcss_hcm.ctkmtc_mytv_20230801@db_tctt where khoanmuctc_id not in (521, 4639, 1431)group by ma_tb
                                                        ) ps_m on a.ma_tb = ps_m.ma_tb and a.loaitb_id in (61)---thang n -1
                                    left join (select ma_tb, sum(tien) ps_htv from bcss_hcm.ctkmtc_htv_20230801@db_tctt where khoanmuctc_id not in (521)group by ma_tb
                                                        ) ps_htv on a.ma_tb = ps_htv.ma_tb and a.loaitb_id in (18)---thang n -1
                        where thang = 202308 and congvan_id = 190 and nhom_datcoc_id in (1) and TIEN_DC_CU <> TIEN_THANHTOAN + VAT
                                        ---tien tru dan >= tien ps thang n là OK
                                        and TIEN_TD   < COALESCE (ps, ps_m, ps_htv, 9999999) 
                                          ---import <= 30k
                                          and a.ma_tb in ('dqhuan973')
                            ;
                                     --   and cuoc_dc = 30000
                                        and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = a.thang and TIEN_DC_CU = TIEN_THANHTOAN + VAT and thuebao_id = a.thuebao_id)
                                         --    and exists (select 1 from qltn_hcm.ct_no_01042023 where ma_tb = a.ma_tb group by ma_tb, dichvuvt_id having sum(decode(sign(chukyno-20230401), 0, nogoc, 0)) >0)
                                         
                                    order by 6
            ;
             select * from  ttkd_bsc.ct_bsc_tratruoc_moi_30day a  where thang = 202405-- and TIEN_THANHTOAN < 100000;
            ;
                update ttkd_bsc.ct_bsc_tratruoc_moi_30day a set tien_khop = null
                ---select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                where a.thang = 202310 and rkm_id in (6117075,
6111581, 6084937)
                   
                   ;
------Check trung record-----
select * from v_thongtinkm_all 
where cuoc_dc > 0 and khuyenmai_id not in (1977, 9038, 9037, 2056, 2150, 8731) and thang_bddc <= least(nvl(thang_huy, 99999999), nvl(thang_kt_dc, 9999999999))
and 202006 between thang_bddc and thang_ktdc  and thuebao_id in (8150994)
;
-------Update ma_hrm cho mnv_giao
        select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day a where thang = 202309; and rkm_id is not null and tien_khop is null;
 ;
      -- KIEM TRA TIEN KHOP
      select* from
      update ct_bsc_tratruoc_moi_30day 
      set tien_khop = 0
      where thang = 202312 and tien_thanhtoan < 10000
      commit;
    select* from ct_bsc_tratruoc_moi_30day where thang = 202312 and manv_thphuc like 'VNP%' and rkm_id is not  null

     
    --ktra khac ma_to
        select a.manv_giao, a.ma_to to_old, a.ma_pb pbold, b.* from ttkd_bsc.ct_bsc_tratruoc_moi_30day a join ttkd_bsc.nhanvien_202308 b on a.manv_giao = b.ma_nv
                        where a.ma_to <> b.ma_to and thang = 202308; 
   --ktra 1 manv nhieu ma_to
        select manv_giao from ttkd_bsc.ct_bsc_tratruoc_moi_30day 
                        where thang = 202308 group by manv_giao having count(distinct nvl(MA_TO, 'a')) >1
;
        update ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                    set a.ma_to = (select ma_to from ttkd_bsc.nhanvien_202307 where a.manv_giao = ma_nv)
            where thang = 202308 and manv_giao = 'CTV048677';and ma_pb is null
;
    select distinct ma_pb, ma_to--, tbh_giao_id 
    from ttkd_bsc.ct_bsc_tratruoc_moi_30day a where thang = 202308
;
        
--------------Thong ke BSC gia han tra truoc theo Phong--------------
        --*********C.1.1, C.1.2 (DAI, KTTT) Ty le thue bao ghtt khong thanh cong giao Dai thuyet phuc
                select pbh_giao_id, phong_giao
                              , count(*) tong
                              , sum(case when dthu > 0 then 0 else 1 end) khong_giahan
                              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                              , sum(dthu) DTHU_thanhcong, round(sum(case when dthu > 0 then 0 else 1 end)*100/count(thuebao_id), 2) tyle
                from  (select thuebao_id, ma_tb, pbh_oa_id pbh_giao_id, 'Phong Ban Hang Online' phong_giao, tien_dc_cu, sum(tien_thanhtoan) DTHU
                                from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202112 and pbh_oa_id in (29)
                                group by thuebao_id, ma_tb, pbh_oa_id, tien_dc_cu
                                union all
                                            select thuebao_id, ma_tb, pbh_giao_id, bo_dau(phong_giao) phong_giao
                                                        , tien_dc_cu, sum(tien_thanhtoan) DTHU
                                            from ttkd_bsc.ct_bsc_tratruoc_moi_30day 
                                            where thang = 202112 and pbh_giao_id in (29) and nvl(pbh_oa_id, 0) not in (29)
                                            group by thuebao_id, ma_tb, pbh_giao_id, phong_giao, tien_dc_cu
                                ) a
                group by pbh_giao_id, phong_giao
                order by 2
                
;
                --****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao quan ly, cskh---
                                select ma_pb, phong_ql
                                              , count(thuebao_id) tong
                                              , sum(case when dthu > 0 then 0 else 1 end) khong_giahan
                                              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                              , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 then 0 else 1 end)*100/count(thuebao_id), 2) tyle
                                from        (select thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.thuebao_id, a.ma_pb, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                        from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                                        where a.thang = 202112                                                                                          -------change n------------
                                                        group by thang, a.pbh_ql_id, a.phong_ql, a.manv_ql, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                                    )
                                    group by phong_ql, ma_pb
                                order by 2
;            
   
commit;
rollback;
-------Buoc 4---tinh tye le theo nhan vien, to, phong

--------Thong ke BSC gia han tra truoc theo Nhan vien Phong-------------------
                select loai_tinh, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(heso) from ttkd_bsc.tl_giahan_tratruoc where thang = 202308 and ma_kpi  in ( 'HCM_TB_GIAHA_022') group by loai_tinh; 47978	34563
                select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202309 and ma_kpi  in ( 'HCM_TB_GIAHA_022');
                select ma_nv from ttkd_bsc.tl_giahan_tratruoc a where thang = 201910 and not exists (select ma_nv from TTKD_BSC.bangluong_kpi_201910 where ma_nv_hrm = a.ma_nv);
                select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_CL_PTTBB_005, HCM_DT_GIAHA_002 from TTKD_BSC.bangluong_kpi_201910 where HCM_CL_PTTBB_005 is not null or HCM_DT_GIAHA_002 is not null ;
                select * from ttkd_bct.phongbanhang;
                select * from TTKD_BCT.moi_giahan_tratruoc_moi where kycuoc = 201910 and tratruoc = 1;
                select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202308; and ma_tb = 'hcmkieutrangfm'; VNP027554
               delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202309 and ma_kpi in ('HCM_TB_GIAHA_022');
              ;
--------1------C4---Tyle thue bao khong gia han tren Tap khach hang het han tra truoc giao BHKV, BHDN thuyet phuc------------HCM_TB_GIAHA_014, HCM_TB_GIAHA_018
                    ----khong tinh so giao thang_dc <3
                insert into hocnq_ttkd.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
                            select thang, 'KPI_NV' LOAI_TINH
                                         , case when ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_022'
                                                        when ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_022'
                                                    else null
                                            end ma_kpi
                                         , a.ma_nv, a.ma_to, a.ma_pb
                                           , count(thuebao_id) tong
                                          , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
                                          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                          , sum(dthu) DTHU_thanhcong
                                          , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
                            from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, a.heso_giao--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                                                , sum(a.tien_khop)
                                                                from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                                                where a.thang = 202310 and not (khdn = 1 and thang_ktdc_cu >= 20231025 and rkm_id is null)      and manv_giao = 'VNP020227'              ------------n------------
                                                               group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, a.heso_giao--, a.kq_dvi_cn, a.kq_popup
                                                ) a 
                          where ma_pb is not null
                          group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
                            order by 2
;       ----khong tinh T7-----tinh nhan vien quan ly dia ban giao ban dau thuyet phuc thanh cong, khong tinh To truong, LDP----------HCM_TB_GIAHA_016
                ----khong tinh so giao thang_dc <3
                ---- same 60Day, replace 90% --> 85%
                insert into ttkd_bsc.tl_giahan_tratruoc
                        select thang, 'KPI_QLDB' LOAI_TINH
                                        , 'HCM_TB_GIAHA_016' ma_kpi
                                        , a.ma_nv, ma_to, ma_pb
                                      
                                      , case when sum(heso) <= 385 then round(385 * 0.85, 0)
                                                    when sum(heso) > 385 then round(sum(heso) * 0.85, 0) else 0
                                       end tong
                                      
                                      , round(case when sum(heso) > 385 and sum(case when dthu > 0 and tien_khop > 0 then Heso else 0 end) > round(sum(heso) * 0.85, 0) 
                                                                                                    then sum(case when dthu > 0 and tien_khop > 0 then Heso else 0 end) * 1.1
                                                    else sum(case when dthu > 0 and tien_khop > 0 then Heso else 0 end)
                                      end, 0) da_giahan
                                      , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong
                                      , null tyle, null dongia, null
                        from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                                , case ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2) 
                                                                                                        when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                                                                                                                        * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                                                                                                                , 0)

                                                                                                        ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                                                                                        when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from ttkd_bsc.ct_bsc_tratruoc_moi_30day xu
                                                                                                                                                                                                    where xu.loaitb_id in (58, 59)
                                                                                                                                                                                                                    and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                                                                                        ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                                                                                        when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_bsc.ct_bsc_tratruoc_moi_30day xu
                                                                                                                                                                                                    where xu.loaitb_id in (58, 59)
                                                                                                                                                                                                                    and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                                                                                    else 0 
                                                                                        end Heso
                                                                                        , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                                                from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                                                where a.thang = 202306       -- and manv_giao = 'CTV042910'              ------------n------------
                                                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, loaitb_id, khachhang_id, goi_old_id, nhomtb_id
                                                ) a 
                          where ma_pb is not null and ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                      group by a.thang, a.ma_nv, ma_to, ma_pb
                 --       order by 2

;
---------------Chay binh quan To, Phong -----
            select *  from hocnq_ttkd.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_022') ;and loai_tinh = 'KPI_PB';
           -- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
            select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202308 and MA_KPI in ('HCM_TB_GIAHA_022') group by ma_kpi, ma_nv having count(ma_to)>1;
            insert into hocnq_ttkd.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
                            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                , sum(heso_giao) heso
                            from hocnq_ttkd.tl_giahan_tratruoc
                            where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_022')
                            group by THANG, MA_KPI, MA_TO, MA_PB
            ;
             insert into hocnq_ttkd.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
                                select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
                                from hocnq_ttkd.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd@dhsxkd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
                                where a.thang = 202310 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_022')
                                group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
            ;
    commit;
    rollback;
            
            ---Buoc 5---gan BSC ghtt theo vi tri nvien, to truong, LDP
            ----Update nhan vien bang KPI----
            update TTKD_BSC.bangluong_kpi_202309 a set 
                                                                                                                HCM_TB_GIAHA_022 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from ttkd_bsc.tl_giahan_tratruoc@ttkddb where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022')
            where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)
                          
            ;
            ---------------Ty le cua To truong -----
            update TTKD_BSC.bangluong_kpi_202309 a 
                            set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc@ttkddb where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_022')
            where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_022')
                                                                    and thang_kt is null and MA_VTCV = a.MA_VTCV)
                         
            ;
            --------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
            update TTKD_BSC.bangluong_kpi_202309 a 
                                set HCM_TB_GIAHA_022 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc@ttkddb where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_022' )
            where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                                                and ma_kpi in ('HCM_TB_GIAHA_022') and thang_kt is null and MA_VTCV = a.MA_VTCV)
                         
;
                /**Kiem tra
                update TTKD_BSC.bangluong_kpi_202309 a set HCM_TB_GIAHA_022 = null;
                select * from  TTKD_BSC.bangluong_kpi_202309;
                **/
                select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO
                , HCM_TB_GIAHA_022
                             
                from TTKD_BSC.bangluong_kpi_202310@dhsxkd where HCM_TB_GIAHA_022  is not null;
                                                                                                                   
                
commit;
rollback;
        
                    ---chi tiet dongia chi tra gui Nsu---
                    update  ttkd_bsc.ct_bsc_tratruoc_moi_30day set pbh_cn_id = 5, ma_nv_cn = 'VNP017854' where pbh_cn_id is null and pbh_giao_id = 5 and thang = 202110 and ma_tb in ('l31808',
'kayoung123',
);
                    commit;
                    rollback;
                    select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202310;
             --       select * from hocnq_ttkd. ct_dongia_tratruoc_202110 where LOAI_TINH in ('DONGIATRA', 'DONGLUCTT');
                    select *  from ttkd_bsc.ct_dongia_tratruoc where thang = 202310 and ma_kpi like 'DONGIA%'  and LOAI_TINH in ('DONGIATRA30D'); 5084 66593920
                    --delete from ttkd_bsc.ct_dongia_tratruoc where thang = 202309 and ma_kpi like 'DONGIA%'  and LOAI_TINH in ('DONGIATRA30D') ;
                    select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202305 and ma_kpi like 'DONG%'  and LOAI_TINH in ('DONGIATRA30D');
                    --delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202303 and ma_kpi like 'DONGIA%'  and LOAI_TINH in ('DONGIATRA30D');
                    ---chi tiet dongia tru gui Nsu---
                    select ma_pb, manv_ql, manv_tctn, ma_tb, thuebao_id, nvl2(max(rkm_id), 1, 0) ghtt_tc
                    from ttkd_bsc.ct_bsc_tratruoc_moi_30day 
                    where thang = 202110 and  ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                    group by ma_pb, manv_ql, manv_tctn, ma_tb, thuebao_id;

                ----Buoc 6---chot danh sach tinh don gia cho tung thue bao
                -----1--tinh don gia theo ai lam thi duoc huong--------------
                            ---Vb 240 khong tinh don gia QLDB tu van, bat ke tu van cua nguoi giao khac
                            ---VB 411 van tinh don gia cho cac nhan vien kenh khac ngoai QLDB (GDV, KTDB,....)
                            ---NV BHDN la khong tinh
                            ---Heso: heso dongia
                
                    insert into ttkd_bsc.ct_dongia_tratruoc
                            with dc as (select /*+ RESULT_CACHE */ dctb.thuebao_id, 'CanGio' ten_quan from css_hcm.diachi_tb dctb, css_hcm.diachi dc 
                                                                        where dctb.diachild_id = dc.diachi_id and dc.quan_id in (3998))
                                        , hs as (select thang, khachhang_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day xu
                                                        where xu.rkm_id is not null and xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id)
                                        select THANG, cast('DONGIATRA30D' as varchar(30)) LOAI_TINH,  cast('DONGIA' as varchar(30)) ma_kpi, MANV_DONGIA ma_nv, nv.ma_to, nv.ma_pb
                                                             , PHONG_cs, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, null MANV_CN, MANV_THPHUC
                                                             , SOTHANG_DC, HT_TRA_ONLINE, Khuvuc, dongia, DTHU, NGAY_TT, heso_giao, khdn, nhomtb_id, khachhang_id, heso, tien_khop
                                        from (select a.khachhang_id, a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                                                                                    , a.manv_giao, a.manv_thphuc
                                                                             /*       , case
                                                                                                ---Dai TC --> Dai xuly --> QLDB
                                                                                               when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                                ---Dai TC --> Dai xuly -->  manv_thuyetphuc sau cung
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                                ---Dai TC --> Dai xuly --> con lai
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 1 and a.manv_thuyetphuc is null then a.ma_nv_cn

                                                                                                ----Dai KTC, QLDB ko xu ly ---> manv_thuyetphuc sau cung
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.pbh_cn_id is null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                                ----Dai KTC, QLDB xu ly ---> QLDB
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                                ----Dai KTC, QLDB xu ly --> manv_thuyetphuc sau cung
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                                ----Dai KTC, QLDB xu ly --> con lai
                                                                                                when a.pbh_giao_id in (26, 29) and a.kq_dvi_cn = 0 and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                                                        --

                                                                                                ----PBH OB, QLDB ko xu ly (ko Dai) ---> manv_thuyetphuc sau cung
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is null then a.manv_thuyetphuc
                                                                                                ----PBH OB, QLDB xu ly ---> QLDB
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.ma_nv_cn = a.manv_thuyetphuc then a.manv_thuyetphuc
                                                                                                ----PBH OB, QLDB xu ly ---> manv_thuyetphuc sau cung
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc
                                                                                                ----PBH OB, QLDB xu ly ---> con lai
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is not null and a.pbh_cn_id is not null and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                                                                
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is null then a.manv_giao
                                                                                                ----Khong giao ai xu ly ---> nhan vien thuyet phuc bat ky
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is null and manv_giao is null and a.manv_thuyetphuc is not null then a.manv_thuyetphuc                                                                                                
                                                                                                ----Giao VTTP xu ly, PBHOL xy ly, khong ma thuyet phuc ---> manv online
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id in (26, 29) and a.manv_thuyetphuc is null then a.ma_nv_cn
                                                                                                ----Giao VTTP xu ly, co dvi xy ly ---> manv_thuyetphuc sau cung (chi xet PBH Online)
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is not null and a.mapb_thphuc = 'VNP0703000' then a.manv_thuyetphuc
                                                                                                ----Giao VTTP xu ly, co dvi xy ly, manv_thuyetphuc PBH Online hoac khong nhap --> manv_giao VTTP
                                                                                                when a.pbh_giao_id not in (26, 29) and ma_pb is null and a.pbh_cn_id is not null and nvl(a.mapb_thphuc, 'a') <> 'VNP0703000' then a.manv_giao
                                                                                                
                                                                                                else null
                                                                                    end manv_dongia
                                                                                        */
                                                                                        -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                                                                    , case when a.mapb_thphuc = 'VNP0703000' then a.manv_thphuc
                                                                                               -- when a.ma_pb is null then a.manv_giao
                                                                                                when a.ma_pb is not null and to_number(to_char(max(a.ngay_tt), 'yyyymm')) <= 202309 then a.manv_giao
                                                                                             --   manv_thphuc = 'MEDIAPAY'
                                                                                                else a.manv_thphuc
                                                                                        end manv_dongia
                                                                                 
                                                                                    , max(so_thangdc) sothang_dc
                                                                                    
                                                                              --      , sum(decode(bo_dau(ten_ht_tra), 'Tien mat', 0, 1)) ht_tra_online
                                                                                    
                                                                                    , sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                        when a.ht_tra_id in (2) then 1 end) ht_tra_online
                                                                                   -- , (select decode(quan_id, 16, 'CanGio') from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) Khuvuc
                                                                                    , dc.ten_quan khuvuc
                                                                                    , case 
                                                                                               
                                                                                                ----thanh toan nhan cong + ap dung khu vuc Can Gio
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) < 3  
                                                                                                                       -- and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                          and dc.ten_quan is not null
                                                                                                                                    then 8700*0.5
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6  
                                                                                                                        --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                        and dc.ten_quan is not null
                                                                                                                                    then 8700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12  
                                                                                                                        --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                        and dc.ten_quan is not null
                                                                                                                                    then 11700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 12  
                                                                                                                        --and exists (select 1 from css_hcm.diachi_tb dctb, css_hcm.diachi dc where dctb.diachild_id = dc.diachi_id and dc.quan_id in (16) and a.thuebao_id = dctb.thuebao_id) 
                                                                                                                        and dc.ten_quan is not null   
                                                                                                                                    then 16700
                                                                                               ----thanh toan nhan cong
                                                                                               when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) < 3 then 5700 * 0.5
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 5700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 8700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) = 0 and max(so_thangdc) >= 12 then 13700
                                                                                                ----thanh toan online---
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) < 3 then 15700  * 0.5
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 3 and max(so_thangdc) < 6 then 15700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 6 and max(so_thangdc) < 12 then 18700
                                                                                                when sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                                                    when a.ht_tra_id in (2) then 1 end) > 0 and max(so_thangdc) >= 12 then 23700
                                                                                    end 
                                                                                                ------tb thanh toan trong 30 ngay thang T het han
                                                                                                * case when max(ngay_tt) between add_months(trunc(sysdate, 'month'), -1) and trunc(sysdate, 'month') -1 then 1.05
                                                                                                ------tb thanh toan truoc thang T het han
                                                                                                                when max(ngay_tt) < add_months(trunc(sysdate, 'month'), -1) then 1.1
                                                                                                        else 1
                                                                                                    end
                                                                                    dongia
                                                                                    
                                                                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0)
                                                                                                                                                                                                    , 0)
                                                                                                                                                                                    -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                                                                                                                        * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                                                                                                                , 0)

                                                                                                        ----Dich vu Mesh he so 0.2
                                                                                                        when a.loaitb_id = 210 then 0.2  
                                                                                                        ---MyTV he so 0.25 
                                                                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                                                                    else 0 
                                                                                        end Heso
                                                                                    ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.khdn, a.nhomtb_id, a.heso_giao
                                                                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                                                                        from ttkd_bsc.ct_bsc_tratruoc_moi_30day a 
                                                                                                left join dc on a.thuebao_id = dc.thuebao_id 
                                                                                                left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                                                                        where rkm_id is not null and a.thang = 202310 --and (a.manv_thuyetphuc is null or not (a.manv_giao <>  a.manv_thuyetphuc))--and a.ma_tb = 'bxthang19'
                                                                        group by a.thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_thphuc, a.manv_cs, a.ma_to, a.ma_pb, a.mapb_thphuc
                                                                                            , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.nhomtb_id, a.loaitb_id, a.khachhang_id, a.goi_old_id, dc.ten_quan
                                                                                            , a.heso_giao, hs.khachhang_id
                                                        ) a
                                                , ttkd_bsc.nhanvien_202310 nv
                                where a.manv_dongia = nv.ma_nv (+) and a.dthu > 0 and a.rnk = 1
                                                and a.manv_dongia is not null
                                                and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                                         --       and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
                                         --                   and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
                                                and nvl(nv.ma_vtcv, 'khongco') not in ('VNP-HNHCM_BHKV_18',
                                                                                                                                    'VNP-HNHCM_BHKV_2.2',
                                                                                                                                    'VNP-HNHCM_BHKV_14',
                                                                                                                                    'VNP-HNHCM_BHKV_2',
                                                                                                                                    'VNP-HNHCM_BHKV_12',
                                                                                                                                    'VNP-HNHCM_BHKV_18.1')
                                 
                        ;
                                                
                        ---loai tru dongia truoc 30ngay voi don gia > 0 thang n-1 da chi
                      --  and not exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang = to_char(add_months(trunc(sysdate, 'month'), -2), 'yyyymm') and loai_tinh = 'DONGIATRA_TR30D' and DONGIA > 0 and tien_khop = 1 and ma_nv is not null and a.thuebao_id = thuebao_id)
             ;
   commit;
                    --6---Don gia chi tra cho nhan vien thuyet phuc---
                    select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang  = 202309 and ma_kpi = 'DONGIA'  and loai_tinh = 'DONGIATRA30D' and loai_tinh not  like 'KPI%';  292 51758691
               --     delete from ttkd_bsc.tl_giahan_tratruoc where thang  = 202309 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRA30D' and loai_tinh not  like 'KPI%';
                    
          insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI
                                                                                                        , DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
                                select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                                                        , count(thuebao_id) tong
                                                          , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                                                          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                                          , sum(dthu) DTHU_thanhcong
                                                          , round(sum(dthu)*100/sum(tien_dc_cu/1.1), 2) tyle
                                                          , round(sum(dongia*heso*tien_khop), 0) dongia
                                -- select * 
                                from ttkd_bsc.ct_dongia_tratruoc  a
                                where ma_kpi = 'DONGIA' 
                                                and loai_tinh = 'DONGIATRA30D' and thang = 202310
                                  group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                                
;
                ---Buoc 7--- tinh dong luc ghtt 30 ngay
                    ---7--- dong luc ---T3, T4, T5 KHDN, T3 QLDB CC, T3, T4 QLDB Can Gio
                     select * from ttkd_bsc.ct_dongia_tratruoc  where ma_kpi like 'DONG%' and thang = 202310 and LOAI_TINH in ('DONGLUCTT') and thuebao_id in (9025124, 9024655);
                     --delete from ttkd_bsc.ct_dongia_tratruoc  where ma_kpi like 'DONG%' and thang = 202309 and LOAI_TINH in ('DONGLUCTT');
                     
                    insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                                                                                                                              , SOTHANG_DC, HT_TRA_ONLINE, DTHU, NGAY_TT, khdn, TIEN_KHOP, manv_thuyetphuc)
              
                                                    select thang, 'DONGLUCTT' LOAI_TINH,  'DONGLUC' ma_kpi, nv.ma_nv, nv.ma_to, nv.ma_pb
                                                                  , phong_cs, thuebao_id, ma_tb, tien_dc_cu, manv_giao
                                                                  , SOTHANG_DC, decode(ht_tra_online, 0, 0, 1) HT_TRA_ONLINE, DTHU, NGAY_TT, khdn, TIEN_KHOP, a.manv_thphuc
                                                    from  (select thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_cs, a.thuebao_id, a.ma_tb, tien_dc_cu
                                                                                , a.manv_giao, a.manv_thphuc
                                                                                , max(so_thangdc) sothang_dc
                                                                                , sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                                        when a.ht_tra_id in (2) then 1 end) ht_tra_online
                                                                                , 0 dongia
                                                                                ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, a.khdn
                                                                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                                                                                            from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                                                                            where a.thang = 202310                  ------------n------------
                                                                                            group by thang, a.pbh_ql_id, a.phong_cs, a.pbh_giao_id, a.manv_giao, a.manv_cs
                                                                                                                , a.thuebao_id, a.ma_tb, tien_dc_cu, a.khdn, a.manv_thphuc
                                                                            ) a
                                                                    , ttkd_bsc.nhanvien_202310 nv
                                                    where a.manv_giao = nv.ma_nv and a.rnk =1
                                                                    and nvl(nv.MA_PB, 'khongco') not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
                                                                    and not (nvl(nv.ma_to, 'khongco') in ('VNP0701402', 'VNP0702215', 'VNP0702216')
                                                                                    and nvl(nv.ma_vtcv, 'khongco') in ('VNP-HNHCM_BHKV_14', 'VNP-HNHCM_BHKV_18'))
                                                    
                                                    ;
                                select * from ttkd_bsc.tl_giahan_tratruoc where ma_kpi like 'DONG%' and thang = 202310 and LOAI_TINH in ('DONGLUCTT'); 101 38385000
                                delete from ttkd_bsc.tl_giahan_tratruoc where ma_kpi like 'DONG%' and thang = 202309 and LOAI_TINH in ('DONGLUCTT');
                                
                                
                                ---8----thong ke tong tien dong luc
                                ----nhan vien giao trung 1 in 2 (manv thuyetphuc, ma_nvcn) theo chu quan hoac manv thuyetphuc va ma_nvcn is null theo khach quan
                                        -- tyle tinh = 65% from tbao 65% de tinh theo gia dong luc
                               -- select sum(dongia) from (
                                insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                        , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
                                            select thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
                                                        , count(thuebao_id) tong
                                                          , sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1
                                                                                    when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                else 0 end) da_giahan
                                                          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                                          , sum(dthu) DTHU_thanhcong
                                                          , round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                else 0 end)*100/count(thuebao_id), 2) tyle
                                                          , case 
                                                                        when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                            else 0 end)*100/count(thuebao_id), 2) >= 85 
                                                                                then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                                else 0 end) - (count(thuebao_id) * 0.65) + 1, 0) * 35000
                                                                        when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                            else 0 end)*100/count(thuebao_id), 2) >= 80
                                                                                then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                                else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 30000
                                                                        when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                            else 0 end)*100/count(thuebao_id), 2) >= 75
                                                                                then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                                else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 25000
                                                                        when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                            else 0 end)*100/count(thuebao_id), 2) >= 70 
                                                                                then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                                else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 20000
                                                                        when round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                            when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                            else 0 end)*100/count(thuebao_id), 2) >= 65 
                                                                                then round(sum(case when dthu > 0 and tien_khop > 0 and ma_nv = manv_thuyetphuc then 1 
                                                                                                                                when dthu > 0 and tien_khop > 0 and manv_thuyetphuc = 'khongco' then 1 
                                                                                                                                else 0 end) -  (count(thuebao_id) * 0.65) + 1, 0) * 15000
                                                                        else 0
                                                                end dongia
                                  --    select *
                                        from ttkd_bsc.ct_dongia_tratruoc 
                                        where ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTT' --and ma_nv = nvl(manv_thuyetphuc, ma_nv_cn) 
                                                        and thang = 202310 --and ma_pb = 'VNP0701100'-- and ma_nv = 'VNP017802' --and ngay_tt < add_months(trunc(sysdate, 'month'), -1)
                                        group by thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
                                        order by 2
                      --                  )
    ;
    
 commit;

        -----Buoc 8---upload bang luong dong gia, nho chi Tung chay
        
        ---update bang luong don gia TTKD
        update ttkd_bsc.bangluong_dongia_202308 a set dongluc_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc@ttkddb
                                                                                                                                                                        where thang = 202308 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTT' 
                                                                                                                                                                                        and ma_nv = a.MA_NV_HRM
                                                                                                                                                                        group by ma_nv 
                                                                                                                                                                        having  sum(tien)  <> 0)
                                                                                                             , dongluc_ghts = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc@ttkddb
                                                                                                                                                                        where thang = 202308 and ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS' 
                                                                                                                                                                                        and ma_nv = a.MA_NV_HRM
                                                                                                                                                                        group by ma_nv 
                                                                                                                                                                        having  sum(tien)  <> 0)
                                                                                                                 , luong_dongia_ghtt = (select sum(tien) from ttkd_bsc.tl_giahan_tratruoc@ttkddb
                                                                                                                                                                        where thang = 202308 and ma_kpi = 'DONGIA' and ma_nv = a.MA_NV_HRM
                                                                                                                                                                        group by ma_nv 
                                                                                                                                                                        having  sum(tien)  <> 0)
        ;
        select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202305 and ma_kpi  like 'DONGIA%'; and loai_tinh = 'DONGLUCTS';380509000 + 109585000;
        select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang = 202204 and ma_kpi like 'DONGIA%' and loai_tinh = 'DONGIATRU' and ma_pb = 'VNP0702300';898,335,000 320 720 000
        select sum(LUONG_DONGIA_GHTT), sum(DONGLUC_GHTT), sum(DONGLUC_GHTs), sum(LUONG_DONGIA_GHTT) +sum(DONGLUC_GHTT) + sum(DONGLUC_GHTs)
                   -- , sum(THUHOI_GHTT_T123)--,  sum(DONGLUC_GHTS_hoito), sum(LUONG_DONGIA_KHAC_THUHOI)
                    ,  sum(luong_khac)
        from ttkd_bsc.bangluong_dongia_202310@dhsxkd; where  MA_DONVI = 'VNP0702300'; 
      18 547 222
        select *--MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV
                       -- , MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, LUONG_DONGIA_GHTT, DONGLUC_GHTT, DONGLUC_GHTs
        from ttkd_bsc.bangluong_dongia_202309@dhsxkd where ma_nv IN ('VNP017796', 'VNP020613');
        
        select ten_pb, ma_kpi, loai_tinh, sum(tien) from ttkd_bsc.tl_giahan_tratruoc a join ttkd_bsc.nhanvien_202310 b on a.ma_nv = b.ma_nv
        where thang = 202310 and ma_kpi  like 'DONGIA%' group by ma_kpi, loai_tinh, ten_pb;
        
        select a.ma_nv, TEN_NV, ma_kpi, loai_tinh, sum(tien) from ttkd_bsc.tl_giahan_tratruoc a join ttkd_bsc.nhanvien_202310 b on a.ma_nv = b.ma_nv
        where thang = 202310 and ma_kpi  like 'DONG%' group by ma_kpi, loai_tinh, a.ma_nv, TEN_NV;
    
                                                                                                                                                                        
                                                                                                                                               
        
        select * from ttkd_bsc.bangluong_dongia_202303;
      --  update ttkd_bsc.bangluong_dongia_202204 a set dongluc_ghtt = null, luong_dongia_ghtt = null, dongluc_ghts = null
;
commit;

        -- tong luong don gia:
update bangluong_dongia_202204 set tong_luong_dongia= null;
update bangluong_dongia_202204
            set tong_luong_dongia=nvl(luong_dongia_cdbr,0)+nvl(luong_dongia_vnpts,0)+nvl(luong_dongia_cntt,0)+nvl(luong_dongia_vnptt,0)
                                                        +nvl(luong_dongia_ghtt,0)+nvl(dongluc_ghtt,0)+nvl(dongluc_ghts,0)
                                                        +nvl(luong_dongia_shipper,0)+nvl(ctvxhh_qly_ptr_ctv,0)+nvl(dongluc_shop,0);
                                                     --   +nvl(dongluc_vb128_032022,0)+nvl(dongluc_vb128_042022,0);

update bangluong_dongia_202204
            set tong_luong_dongia='' where tong_luong_dongia=0;

select sum(tong_luong_dongia) from bangluong_dongia_202204; 6222860267
commit;
rollback;


select * from ttkd_bsc.DONGIA_VTTP where thang = 202310;
            ----update dongia TTVT
select sum(tien_dongia) from (
  -- insert into ttkd_bsc.dongia_vttp
            select THANG, LOAI_TINH, MA_KPI, a.MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                        , MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA
                        , DTHU, NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, tien_khop, round(DONGIA*HESO*tien_khop, 0) tien_dongia
                            , nv1.ten_nv, dv1.ten_dv dv_cap1, dv2.ten_dv dv_cap2, dv2.donvi_id 
            from ttkd_bsc.ct_dongia_tratruoc a
                         , admin_hcm.nhanvien nv1, admin_hcm.donvi dv1, admin_hcm.donvi dv2
            where thang = 202309 and ma_kpi like 'DONGIA%'  --and LOAI_TINH in ('DONGIATRA30D')
                            and a.ma_nv = nv1.ma_nv (+) and nv1.donvi_id = dv1.donvi_id (+) and dv1.donvi_cha_id = dv2.donvi_id (+)
                                   and not exists (select * from ttkd_bsc.nhanvien_202309 where ma_nv = a.ma_nv)
                                and dv2.donvi_id  not in (284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427)
    
    )
    ;
    commit;
