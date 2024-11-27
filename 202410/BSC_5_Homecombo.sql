--------------------Chi tieu C2, noi dung---------------
--Fiber – tang DT: tu van ket goi Home Combo cho fiber le
--(Fiber hh co ket goi + MyTV PTM (fiberhh) hok trung ds ket goi. + MyTV OTT
---------------------
    ---Buoc 1----------CT1 Fiber Home hmoi
    insert into ttkd_bsc.ct_bsc_homecombo
--    insert into ct_bsc_homecombo

select 202405 thang, 'Fiber_moi' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
--                         ,    null ghichu                                                                                           ----1-change----
               , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
from ct_homecombo_2024 a
--            from ct_homecombo_2023 a

            , (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
                from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
                where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                                and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                                and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
                             --  and hdtb.ma_tb in ('lmh91')--, 'hieuliem92020')
                ) hd
    where a.thang = 202404 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
                    and trangthai_fiber = 'Moi'
                    and not exists (select 1 from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202404)     ---change n-1------
;
select* from css_hcm.kieu_ld
--rollback;
         ---Buoc 2----------CT2 Fiber Home hienhuu
    insert into ttkd_bsc.ct_bsc_homecombo
--    insert into ct_bsc_homecombo
            select 202405 thang, 'Fiber_hh' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
--                         , null ghichu                                                                                           ----1-change----
                            , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
            from ct_homecombo_2024 a
--            from ct_homecombo_2023 a
                        , (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
                            from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
                            where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                                            and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                                            and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
                                         --  and hdtb.ma_tb in ('lmh91')--, 'hieuliem92020')
                            ) hd
                where a.thang = 202405 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
                                and trangthai_fiber != 'Moi'
                                and not exists (select * from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang =202401)---change n-1------
;
commit; 
----stop------MyTV (fiber hh) ko trung ds Fiberhh ket goi
        insert into ttkd_bsc.ct_bsc_homecombo
                            select 202203 thang, 'MyTV_Fhh' loai_kpi, a.ma_tb, g.ma_goi, null, a.ngay_bbbg, a.ma_td, a.ma_tiepthi, a.ma_to, a.ma_pb
                                        , a.hdtb_id, a.thuebao_id, a.ctv_id, a.hdkh_id, a.tocdo_id, a.goi_id, null
                            --select *
                            from ttkd_bct.ptm_codinh_202203 a, css_hcm.goi_dadv g
                            where a.loaitb_id in (61) and a.goi_id = g.goi_id
                                            and exists (select * from css_hcm.db_thuebao x, css_hcm.db_adsl y 
                                                                        where x.thuebao_id= y.thuebao_id and x.loaitb_id = 58 and x.trangthaitb_id not in (7,8,9)
                                                                                        and to_number(to_char(x.ngay_sd, 'yyyymm')) <= 202202 and y.madoicap = a.madoicap)
;

commit;
rollback;
;
select distinct homecombo--a.*--, (getxmlagg_table('ttkd_bsc.nhanvien_202208', 'ten_pb', 'ma_nv =''' || a.ma_nv||'''', 'gg','g')) ten_pb
from ttkd_bsc.ct_bsc_homecombo  a
where thang  = 202209
;
select ma_Tb from ttkd_Bsc.ct_Bsc_homecombo where thang = 202402 group by ma_Tb having count(ma_Tb) > 1
------Thue bao Fiber moi join Homecombo C6.1------
select * from ttkd_bsc.ct_bsc_homecombo where thang = 202312 and LOAI_KPI like 'Fiber%'; ghichu = 'bsung T8 ket goi, hoan cong T9' where thang = 202008 and LOAI_KPI = 'Yeuto_moi'; or ghichu = 'Bo sung BSC T11';ma_tb in ('vanhoai230131.fb.hcm', 'thihoa251256.fb.hcm','thanhhieu252494.fb.hcm','nguyetque231081.fb.hcm','tuongvy262102.fb.hcm','vanlieu253615.fb.hcm');
select thuebao_id from ttkd_bsc.ct_bsc_homecombo where thang = 202304 group by thuebao_id having count(*)>1;
--delete from ttkd_bsc.ct_bsc_homecombo where  thang  = 202108 and loai_kpi = 'MyTV_fhh ';
             
rollback;
commit;

---------------Thong ke BSC theo Nhan vien Phong-------------------
                select sum(SOTHUCHIEN) from ttkd_bsc.tl_homecombo where thang = 202308 and ma_kpi in ('HCM_CT_CLUOC_001') and LOAI_TINH = 'KPI_PB' ;; and ma_nv ='CTV069560';
                select ma_nv, ma_to, ma_pb from ttkd_bsc.tl_homecombo a where thang = 201910 and ma_pb is not null and not exists (select 1 from TTKD_BSC.bangluong_kpi_201910 where ma_nv_hrm = a.ma_nv);
                select ma_nv, ma_to, ma_pb from ttkd_bsc.tl_homecombo a where thang = 201910 and ma_nv is null and not exists (select 1 from TTKD_BSC.bangluong_kpi_201910 where ma_to = a.ma_to);
                
                select * from ttkd_bsc.tl_homecombo where thang = 202307 and ma_kpi in ('HCM_CT_CLUOC_001') and LOAI_TINH = 'KPI_PB' ;
                select * from ttkd_bsc.ct_bsc_homecombo where thang = 202305;
               --delete from ttkd_bsc.tl_homecombo where thang = 202307 and ma_kpi in ('HCM_CT_CLUOC_001') and LOAI_TINH = 'KPI_PB';          
--Buoc 3- thong ke tong hop ma_nv, Ma_to, Ma_pb co Fiber ket co homeCB, T02/23 lay het HomeCB
insert into ttkd_bsc.tl_homecombo
            select thang, 'KPI_NV' LOAI_TINH
                            ,  'HCM_CT_CLUOC_001' ma_kpi
                            , ma_nv, ma_to, ma_pb, null sogiao
                            , count(distinct ma_tb) sothuchien
                            , null tyle2, null
            from ttkd_bsc.ct_bsc_homecombo 
            where loai_kpi like 'Fiber_%' and thang = 202312 and ma_pb is not null
                           ---     and goi_id in (16006, 16008, 16010, 16012, 16014, 16007, 16009, 16011, 16013, 16015)
            group by thang, ma_nv, ma_to, ma_pb
;
------Update to truong----------
select* from ttkd_Bsc.tl_homecombo where thang = 202311 and LOAI_TINH = 'KPI_TO' and  ma_kpi = 'HCM_CT_CLUOC_001' 
insert into ttkd_bsc.tl_homecombo
            select thang, 'KPI_TO' LOAI_TINH, ma_kpi, null ma_nv, ma_to, ma_pb
                            , sum(sogiao) sogiao, sum(sothuchien) sothuchien, round(sum(sothuchien) * 100/sum(sogiao), 2) tyle
                            , null
            from ttkd_bsc.tl_homecombo
            where thang = 202312 and LOAI_TINH = 'KPI_NV' and  ma_kpi = 'HCM_CT_CLUOC_001' 
            group by thang, ma_kpi, ma_to, ma_pb
;
------Update Pho Giam doc----------
insert into ttkd_bsc.tl_homecombo
            select a.thang, 'KPI_PB' LOAI_TINH, a.ma_kpi, B.MA_NV ma_nv, null ma_to, a.ma_pb
                            , sum(sogiao) sogiao, sum(sothuchien) sothuchien, round(sum(sothuchien) * 100/sum(sogiao), 2) tyle, b.ma_nv ldp_phutrach
            from ttkd_bsc.tl_homecombo a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
            where a.thang = 202312 and LOAI_TINH = 'KPI_TO'  and  a.ma_kpi = 'HCM_CT_CLUOC_001' 
            group by a.thang, a.ma_kpi, a.ma_pb, b.ma_nv
;
rollback;
commit;
select* from ttkd_Bsc.nhanvien_Vttp where ma_nv in (
select * from ttkd_bsc.ct_Bsc_homecombo where thang = 202311 and ma_Tb = 'hoangson899'
select * from ttkd_Bsc.ct_bsc_homecombo where thang = 202312 and loai_kpi in ('Fiber_hh','Fiber_moi');-- group by ma_Tb having count(ma_tb) > 1
        ---End---update bang C2
select kieuld_id from css_hcm.hd_Thuebao where hdtb_id = 22549489

selecT* from css_hcm.kieu_ld where kieuld_id = 280