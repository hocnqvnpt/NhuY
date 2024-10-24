--------------------Chi tieu C2, noi dung---------------
--Fiber – tang DT: tu van ket goi Home Combo cho fiber le
--(Fiber hh co ket goi + MyTV PTM (fiberhh) hok trung ds ket goi. + MyTV OTT
---------------------
---Buoc 1----------CT1 Fiber Home hmoi
delete from ct_bsc_homecombo where thang = 202312;
	select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_024' and thang_kt is null;
select * from vietanhvh.PL4_0401@ttkddb where thang = 2024091;

select* from ttkd_bsc.nhanvien where (thang = 202409 and donvi = 'VTTP') OR (THANG = 202409 AND DONVI = 'TTKD');
commit;

select distinct loai_goi from ct_homecombo_2024 where thang = 202409; and ma_kpi = 'HCM_TB_GIAHA_024' AND MA_NV = 'VNP020231';
select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang ;
rollback;
    
update ttkd_bsc.ct_bsc_homecombo set ma_Nv = 'CTV021936',ma_to='VNP0701890', ma_pb ='VNP0701800' where thang = 202409 and ma_Tb = 'ng_minhhoang59';
commit;
select* from ct_homecombo_2024 where thang = 202409 and loai_kpi like 'Fiber_%';-- and account_fiber in ('p105d_ktxdhyd','buivantuan' )
insert into ttkd_bsc.ct_bsc_homecombo
select 202409 thang, 'Fiber_moi' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
       ,    null ghichu                                                                                           ----1-change----
--   , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
--from hocnq_ttkd.ct_homecombo_2023 a
from ct_homecombo_2024 a

, (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
    from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
    where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                    and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                    and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
                 --  and hdtb.ma_tb in ('lmh91')--, 'hieuliem92020')
    ) hd
where a.thang = 202408 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
        and trangthai_fiber = 'Moi' and loai_goi in  ('HOMECHAT4','HOMECHAT2','HOMECHAT6','HOMESANH1','HOMESANH2','HOMESANH3','HOMESANH4')
        and not exists (select 1 from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202408)     ---change n-1------
;
commit;
---Buoc 2----------CT2 Fiber Home hienhuu
select* from ttkd_bsc.ct_bsc_homecombo where thang = 202409 and ma_Tb = 'ng_minhhoang59';
insert into ttkd_bsc.ct_bsc_homecombo
select 202409 thang, 'Fiber_hh' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
--    , null ghichu                                                                                           ----1-change----
    , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
from ct_homecombo_2024 a
, (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
    from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
    where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                    and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                    and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
--                   and hdtb.ma_tb in('p105d_ktxdhyd','buivantuan' ) --, 'hieuliem92020')
    ) hd
where a.thang = 202408 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
        and trangthai_fiber != 'Moi' and loai_goi in  ('HOMECHAT4','HOMECHAT2','HOMECHAT6','HOMESANH1','HOMESANH2','HOMESANH3','HOMESANH4')
        and not exists (select * from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202408)     ---change n-1------
;
commit;
select ma_tb from ttkd_bsc.ct_bsc_homecombo where thang = 202409 and loai_kpi like 'Fiber%' group by ma_tb having count(ma_Tb) > 1; --and ma_tb not in 
(select ma_tb from ct_bsc_homecombo where thang = 202312 and loai_kpi in ( 'Fiber_hh','Fiber_moi'  )) --group by ma_tb having count(ma_tb) >1)
commit;
select ma_Tb from ttkd_Bsc.ct_bsc_homecombo where thang = 202402 group by ma_Tb having count(ma_tb) > 1;-- and mnv_Tt = 'VNP019514' and account_fiber not in ('thliem_fm','dtqa10','pdong307')
select* from ttkd_Bsc.ct_bsc_homecombo where thang = 202409; and ma_nv = 'VNP019514'
thliem_fm, dtqa10, pdong307
select* from ttkd_Bsc.tl_homecombo where thang = 202310 and ma_nv = 'VNP019514'
select* from ttkd_Bsc.nhanvien_202310 where ten_nv = 'Bùi Thanh Tâm'
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
select count(distinct ma_Tb) from ttkd_bsc.ct_Bsc_homecombo where thang = 202310  and ma_pb is not null --='CTV078890'
delete from ttkd_bsc.ct_Bsc_homecombo where thang = 202310
commit;
rollback;
;
select distinct homecombo--a.*--, (getxmlagg_table('ttkd_bsc.nhanvien_202208', 'ten_pb', 'ma_nv =''' || a.ma_nv||'''', 'gg','g')) ten_pb
from ttkd_bsc.ct_bsc_homecombo  a
where  thang  = 202209
;
------Thue bao Fiber moi join Homecombo C6.1------
select * from ttkd_bsc.ct_bsc_homecombo where  thang  = 202402 and LOAI_KPI like 'Fiber%'; ghichu = 'bsung T8 ket goi, hoan cong T9' where thang = 202008 and LOAI_KPI = 'Yeuto_moi'; or ghichu = 'Bo sung BSC T11';ma_tb in ('vanhoai230131.fb.hcm', 'thihoa251256.fb.hcm','thanhhieu252494.fb.hcm','nguyetque231081.fb.hcm','tuongvy262102.fb.hcm','vanlieu253615.fb.hcm');
select thuebao_id from ttkd_bsc.ct_bsc_homecombo where  thang  = 202304 group by thuebao_id having count(*)>1;
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
insert into ttkd_bsc.ct_bsc_homecombo 
select* from ct_bsc_homecombo where thang = 202311
commit;
		select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_SL_COMBO_006' and thang_kt is null;
alter table dongia_Chungtu add thang number(6);
update dongia_Chungtu set thang = 202409;
select* from ttkd_bsc.tonghop_c2 where thang  = 202312 and dichvu_pre = 1;
--Buoc 3- thong ke tong hop ma_nv, Ma_to, Ma_pb co Fiber ket co homeCB, T02/23 lay het HomeCB
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi =  'HCM_SL_COMBO_006';
select count (distinct ma_Tb) from ttkd_Bsc.ct_bsc_homecombo where thang = 202312 and ma_nv = 'CTV079968' and loai_kpi like 'Fiber_%' ;
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select thang, 'KPI_NV' LOAI_TINH
                    ,  'HCM_SL_COMBO_006' ma_kpi
                    , ma_nv, ma_to, ma_pb--, null sogiao
                    , count(distinct ma_tb) sothuchien
                    --, null tyle2, null
--    select *
    from ttkd_bsc.ct_bsc_homecombo 
    where loai_kpi in ('Fiber_hh','Fiber_moi') and thang = 202409 and ma_pb is not null
                   ---     and goi_id in (16006, 16008, 16010, 16012, 16014, 16007, 16009, 16011, 16013, 16015)
    group by thang, ma_nv, ma_to, ma_pb
    
;
commit;
------Update to truong----------
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select thang, 'KPI_TO' LOAI_TINH, ma_kpi, null ma_nv, ma_to, ma_pb,
                     sum(DA_GIAHAN_DUNG_HEN) sothuchien
    from ttkd_bsc.tl_giahan_tratruoc
    where thang = 202409 and LOAI_TINH = 'KPI_NV' and  ma_kpi = 'HCM_SL_COMBO_006' 
    group by thang, ma_kpi, ma_to, ma_pb
;
commit;
select* from TTKD_BSC.blkpi_danhmuc_kpi where thang_kt is null and ten_kpi like '%ome%'
;
select* from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where ma_kpi = 'HCM_SL_COMBO_006'
------Update Pho Giam doc----------
insert into tl_homecombo 
delete from ttkd_Bsc.tl_homecombo where thang = 202311 and loai_tinh = 'KPI_PB';
select  distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409;-- and loai_tinh = 'KPI_PB'
commit;
select SUM(DA_GIAHAN_DUNG_HEN) from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi = 'HCM_SL_COMBO_006'  and loai_tinh = 'KPI_TO';
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202409 and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_SL_COMBO_006' ;
commit;
ROLLBACK;
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select a.thang, 'KPI_PB' LOAI_TINH, a.ma_kpi, b.ma_nv ma_nv, null ma_to, a.ma_pb
                  ,  sum(DA_GIAHAN_DUNG_HEN) sothuchien
    from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_nv, ma_to, ma_kpi, thang, ma_pb , dichvu from ttkd_bsc.blkpi_dm_to_pgd 
    where dichvu ='Mega+Fiber') b on a.thang = b.thang 
    and a.ma_to = b.ma_to and a.ma_pb = b.ma_pb
    where a.thang = 202409 and LOAI_TINH = 'KPI_TO'  and  a.ma_kpi = 'HCM_SL_COMBO_006' 
    group by a.thang, a.ma_kpi, a.ma_pb, b.ma_nv
;
select* from ttkd_Bsc.tonghop_giao_372 where thang = 202409;
select* from ttkd_bsc.nhanvien where thang = 202409 and ma_pb ='VNP0703000';
select* from ttkd_Bsc.blkpi_danhmuc_kpi_vtcv where  ma_kpi = 'HCM_SL_COMBO_006' and thang = 202409 ;
select distinct ht_Tra_id, kenhthu_id from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang= 202409 and tien_khop = 0 ;
update ttkd_Bsc.ct_Bsc_tratruoc_moi_30day
set tien_khop = 1 where kenhthu_id in (25,26,22)
and thang= 202409 and tien_khop = 0 ;
commit;
select* from css_hcm.kenhthu where kenhthu_id in (22,23,29);
select so_Ct, ghichu from css_hcm.phieutt_hd where ma_Gd ='HCM-TT/02945645';
select distinct loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409;
		select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_SL_COMBO_006' and thang_kt is null;
select distinct ma_nv, ma_to, ma_kpi, thang, ma_pb from ttkd_bsc.blkpi_dm_to_pgd where thang = 202409 and ma_kpi = 'HCM_SL_COMBO_006' and ma_to ='VNP07015A0';
select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202409 and ma_kpi = 'HCM_SL_COMBO_006' ;
select* from ttkd_Bsc.nhanvien_vttp where ma_nv = 'CTV085321';
---- update pho giam doc
update ttkd_bsc.tl_homecombo
set ma_nv = ldp_phutrach
where thang = 202401 and ma_kpi = 'HCM_CT_CLUOC_001' and LOAI_TINH = 'KPI_PB' 
rollback;
select* from ttkd_Bsc.bangluong_kpi_202409 where hcm_Tb_giaha_024 is not null; 

create table tonghop_c2 as select* from ttkd_bsc.tonghop_c2
update tonghop_c2 a set sothuchien = nvl((select sothuchien from ttkd_bsc.tl_homecombo where thang = 
to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_kpi = 'HCM_CT_CLUOC_001' and LOAI_TINH = 'KPI_PB' and ma_nv = a.ma_nv), 0)
                                                                , sogiao = round(sogiao, 0)
where thang = 202311 and DICHVU_PRE = 1 and GIAMDOC_PHOGIAMDOC = 1
commit;
select * from ttkd_bsc.tonghop_c2 where thang = 202401 and DICHVU_PRE = 1;
---End---update bang C2
---Nhan vien
    ttkd_bsc.tonghop_c2 where thang = 202401 and dichvu_pre = 1 and giamdoc_phogiamdoc = 1
update ttkd_bsc.tonghop_c2 a 
set sothuchien = nvl((select sothuchien from ttkd_bsc.tl_homecombo 
                where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_kpi = 'HCM_CT_CLUOC_001' and LOAI_TINH = 'KPI_NV' and ma_nv = a.ma_nv),0)
    , sogiao = round(sogiao, 0)
where thang = 202402 and DICHVU_PRE = 1
;commit;
---To truong
update ttkd_bsc.tonghop_c2 a
set sothuchien = nvl((select sothuchien from ttkd_bsc.tl_homecombo 
             where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_kpi = 'HCM_CT_CLUOC_001' and LOAI_TINH = 'KPI_TO' and ma_to = a.ma_to), 0)
     , sogiao = round(sogiao, 0)
where thang = 202402 and DICHVU_PRE = 1 and TO_TRUONG_PHO = 1
;
---PGD
update ttkd_bsc.tonghop_c2 a 
set sothuchien = nvl((select sothuchien from ttkd_bsc.tl_homecombo 
            where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_kpi = 'HCM_CT_CLUOC_001' and LOAI_TINH = 'KPI_PB' and ma_nv = a.ma_nv),0)
     , sogiao = round(sogiao, 0)
where thang = 202402 and DICHVU_PRE = 1 and GIAMDOC_PHOGIAMDOC = 1
;
----end
select sum(sothuchien), sum(sogiao) from  ttkd_bsc.tonghop_c2 a 
            where thang = 202402 and         DICHVU_PRE = 1 ;--and GIAMDOC_PHOGIAMDOC = 1;
select sum(sothuchien) fro
commit;