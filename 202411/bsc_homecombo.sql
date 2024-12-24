delete from cttb_huy_goihome; where chu_nhom is null;
delete from cttb_dk_goi_home where chu_nhom is null;
commit;
select * from cttb_huy_goihome a;
select * from cttb_dk_goi_home a;

---Update Ngay_active_VNP-----               
update cttb_dk_goi_home a set a.ngay_active_vnp = (
select min(ngay_active_vnp) ngay_active_vnp from cttb_dk_goi_home
where chu_nhom in (select chu_nhom from cttb_dk_goi_home
                                group by chu_nhom having count(distinct ngay_active_vnp) >1)
                                and a.chu_nhom = chu_nhom
                    group by chu_nhom)
where chu_nhom in (select distinct chu_nhom from cttb_dk_goi_home
group by chu_nhom having count(distinct ngay_active_vnp) >1)
;

---Buoc 2 thong tin ghep goi, huy goi
create table CT_HOMECOMBO_202411 as ;
select* from  (
with dbtb as (select db.thuebao_id, db.ma_tb, db.ngay_sd ngay_sd_fiber, dbc.tocdo_id, td.ma_td 
                            from CSS_HCM.db_thuebao db, css_hcm.db_adsl dbc, CSS_HCM.tocdo_adsl td
                            where db.thuebao_id=dbc.thuebao_id and dbc.tocdo_id=td.tocdo_id --and db.ma_tb = 'thuytien6'
                            ),
hdmoi as (
        select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.ma_tb, hdkh.ngay_yc, hdtb.ngay_ht, hdkh.ctv_id
                                                , hdtb_adsl.tocdo_id, tocdo_adsl.ma_td
                                                , tthd.trangthai_hd, hdtb.ghichu
        from css_hcm.hd_khachhang hdkh, CSS_HCM.hd_thuebao hdtb, CSS_HCM.hdtb_adsl , CSS_HCM.tocdo_adsl, css_hcm.trangthai_hd tthd
        where hdtb.hdkh_id = hdkh.hdkh_id and hdtb.hdtb_id=hdtb_adsl.hdtb_id and hdtb_adsl.tocdo_id=tocdo_adsl.tocdo_id 
                        --and hdtb.hdtb_id = b.hdtb_id
                        and tthd.tthd_id = hdtb.tthd_id
                        and hdtb.hdtb_id = (select max(hdtb_id)
                                from css_hcm.hd_thuebao     
                                where kieuld_id  in (51, 280) and loaitb_id in (58, 59) and hdtb.ma_tb = ma_tb
                                group by ma_tb
            )
        and trunc(hdkh.ngay_yc) >= to_date('01/11/2024','dd/mm/yyyy') -- CHANGE
        )
select distinct
        a.*,  
        dbtb.ngay_sd_fiber, dbtb.tocdo_id, dbtb.ma_td
        , hdmoi.hdtb_id, hdmoi.ngay_yc ngay_yc_fiber, hdmoi.ngay_ht ngay_ht_fiber, hdmoi.trangthai_hd, hdmoi.ghichu
        , case
        when b.ten_nv is not null then b.ten_nv else c.ten_nv
        end TEN_NV
        , case
        when b.ten_to is not null then b.ten_to else c.ten_to
        end TO_TIEP_THI
        , bo_dau(case
        when b.ten_kenhbh is not null then b.ten_kenhbh else c.ten_kenhbh
        end) TEN_KENHBH
        , initcap(bo_dau(case
        when b.ten_pb is not null then b.ten_pb else c.ten_pb
        end)) DONVI_TIEPTHI
        , initcap(bo_dau(e.ten_pb)) PHONG_DK_GOI
        , nvl(b.ma_to, c.ma_to) ma_to
        , nvl(b.ma_pb, c.ma_pb) ma_pb
from cttb_dk_goi_home a  
            left join  dbtb
                    on a.account_fiber = dbtb.ma_tb
            left join  hdmoi
                    on a.account_fiber = hdmoi.ma_tb
            -- lay thong tin nhan vien
            left join (select a.ma_nv, a.ten_nv, a.ma_to, a.ma_pb, b.ten_to, a.ten_pb, c.ten_kenhbh
                        from TTKD_BSC.NHANVIEN  a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to and a.thang = 202411 -- CHANGE
                        left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                        ) b 
                    on a.mnv_tt = b.ma_nv
            left join (select a.ma_nv, a.manv_nnl, a.ten_nv, a.ma_to, a.ma_pb, b.ten_to, a.ten_pb, c.ten_kenhbh
                        from ttkd_bsc.NHANVIEN a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to and a.thang = 202411-- CHANGE
                        left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                        ) c
                    on a.mnv_tt = c.manv_nnl
            left join ttkd_bsc.NHANVIEN  e -- CHANGE
                    on a.nguoi_th = e.user_ccbs        and e.thang = 202411           
)
;

-- update phong ban TTVT
-- 
update ct_homecombo_202411 a 
set donvi_tiepthi = (select bo_dau(ten_pb)
from ttkd_Bsc.nhanvien_Vttp where thang = 202404 and ma_nv = a.mnv_tt) ,
ten_nv =  (select ten_nv from ttkd_Bsc.nhanvien where thang = 202411 and ma_nv = a.mnv_tt),
to_tiep_thi = (select ten_to from ttkd_Bsc.nhanvien where thang = 202411 and ma_nv = a.mnv_tt),
ten_kenhbh = 'VTTP',
ma_to = (select ma_to from ttkd_Bsc.nhanvien where thang = 202411 and ma_nv = a.mnv_tt),
ma_pb = (select ma_pb from ttkd_Bsc.nhanvien where thang = 202411 and ma_nv = a.mnv_tt)
where  DONVI = 'VTT'       ;
--


insert into ct_homecombo_2024
    (THANG, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
    , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
    , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB)
        (select 202411 thang, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
                        , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
                        , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB
        from ct_homecombo_202411)  ----change---
;
commit;

--- thong tin huy chi tiet

---
CREATE OR REPLACE FUNCTION GET_PTM_YYYYMM_HOMECB
(
  ngay_dk IN DATE,
  MA_TB IN VARCHAR2,
  col_name IN VARCHAR2
) 
RETURN VARCHAR2 IS
  MA_TT VARCHAR2(50);
  v_NAM VARCHAR2(10) := TO_CHAR(ngay_dk, 'yyyy');
  v_curNAM VARCHAR2(10) := TO_CHAR(ngay_dk, 'yyyymm');
BEGIN
  -- Kh?i chính th?c thi câu l?nh SQL ??ng
  BEGIN
    EXECUTE IMMEDIATE
      'SELECT DISTINCT ' || col_name || '
       FROM
           (SELECT ngay_dk, account_fiber, trangthai_fiber, mnv_tt, ten_nv, donvi_tiepthi, ma_to, ma_pb, loai_tb
            FROM hocnq_ttkd.ct_homecombo_' || v_NAM || '
            WHERE thang = ' || v_curNAM || '
            GROUP BY ngay_dk, account_fiber, trangthai_fiber, mnv_tt, ten_nv, donvi_tiepthi, ma_to, ma_pb, loai_tb)
       WHERE rownum = 1
         AND account_fiber = ''' || MA_TB || '''
         AND TO_CHAR(ngay_dk, ''yyyymm'') = ''' || TO_CHAR(ngay_dk, 'yyyymm') || ''''
       INTO MA_TT;
  EXCEPTION
    WHEN OTHERS THEN
      -- N?u có l?i, tr? v? NULL
      MA_TT := NULL;
  END;

  -- Tr? v? giá tr? k?t qu? c?a hàm
  RETURN MA_TT;

END GET_PTM_YYYYMM_HOMECB;

/
---
insert into CT_GDVP_HUY_2024
(select distinct 202411, CHU_NHOM, 'HOME' LOAI, LOAI_GOI
            , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'loai_tb') loai_tb
            , ACCOUNT_FIBER, ACCOUNT_MYTV, NGAY_DK, MNV_TT, NGAY_HUY, NGUOI_TH, LYDOHUY
             , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'ten_nv') ten_nv
            , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'donvi_tiepthi') donvi_tiepthi 
from cttb_huy_goihome)
;
commit;


--- insert vao bang ct bsc
;
insert into ttkd_bsc.ct_bsc_homecombo
select 202411 thang, 'Fiber_moi' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
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
where a.thang = 202410 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
        and trangthai_fiber = 'Moi' and loai_goi in  ('HOMECHAT4','HOMECHAT2','HOMECHAT6','HOMESANH1','HOMESANH2','HOMESANH3','HOMESANH4')
        and not exists (select 1 from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202410)     ---change n-1------
;
commit;
---
insert into ttkd_bsc.ct_bsc_homecombo
select 202411 thang, 'Fiber_hh' loai_KPI, account_fiber, loai_goi, a.ngay_dk ngay_dk_goi, hd.ngay_sd ngay_sd_fiber, hd.thuonghieu, mnv_tt, ma_to, ma_pb, hd.hdtb_id, hd.thuebao_id, hd.ctv_id, hd.hdkh_id, hd.tocdo_id, hd.goi_id
    , null ghichu                                                                                           ----1-change----
--    , 'bsung n-1 ket goi, hoan cong n' ghichu                                                                                           ---or 2-change----
from ct_homecombo_2024 a
, (select hdgoi.ctv_id, hdgoi.goi_id, hdtb.hdtb_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.hdkh_id, td.tocdo_id, td.thuonghieu, db.ngay_sd
    from css_hcm.hd_thuebao hdtb, css_hcm.hdtb_goi_dadv hdgoi, css_hcm.bd_goi_dadv dbgoi, css_hcm.hdtb_adsl hdtd, css_hcm.tocdo_adsl td, css_hcm.db_thuebao db
    where hdgoi.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (51, 280) and hdtb.tthd_id <> 7 and hdtb.loaitb_id = 58
                    and hdgoi.nhomtb_id = dbgoi.nhomtb_id and hdtb.thuebao_id = dbgoi.thuebao_id and dbgoi.trangthai = 1
                    and hdtd.hdtb_id = hdtb.hdtb_id and hdtd.tocdo_id = td.tocdo_id and hdtb.thuebao_id = db.thuebao_id (+)
--                   and hdtb.ma_tb in('p105d_ktxdhyd','buivantuan' ) --, 'hieuliem92020')
    ) hd
where a.thang = 202410 and trangthai_goi <> 'Huy' and a.account_fiber = hd.ma_tb      --change n, n-1------
        and trangthai_fiber != 'Moi' and loai_goi in  ('HOMECHAT4','HOMECHAT2','HOMECHAT6','HOMESANH1','HOMESANH2','HOMESANH3','HOMESANH4')
        and not exists (select * from ttkd_bsc.ct_bsc_homecombo where thuebao_id = hd.thuebao_id and thang >=202410)     ---change n-1------
;
commit;
--- tinh bsc
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select thang, 'KPI_NV' LOAI_TINH
                    ,  'HCM_SL_COMBO_006' ma_kpi
                    , ma_nv, ma_to, ma_pb--, null sogiao
                    , count(distinct ma_tb) sothuchien
                    --, null tyle2, null
--    select *
    from ttkd_bsc.ct_bsc_homecombo 
    where loai_kpi in ('Fiber_hh','Fiber_moi') and thang = 202411 
                   ---     and goi_id in (16006, 16008, 16010, 16012, 16014, 16007, 16009, 16011, 16013, 16015)
    group by thang, ma_nv, ma_to, ma_pb
    
;
rollback;
commit;
-----Update to truong----------
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select thang, 'KPI_TO' LOAI_TINH, ma_kpi, null ma_nv, ma_to, ma_pb,
                     sum(DA_GIAHAN_DUNG_HEN) sothuchien
    from ttkd_bsc.tl_giahan_tratruoc
    where thang = 202411 and LOAI_TINH = 'KPI_NV' and  ma_kpi = 'HCM_SL_COMBO_006' 
    group by thang, ma_kpi, ma_to, ma_pb
;

--- update pho giam doc
select* from ttkd_Bsc.tl_giahan_tratruoc
    where thang = 202411  and  ma_kpi = 'HCM_SL_COMBO_006' ;
insert into ttkd_bsc.tl_giahan_tratruoc (thang, loai_tinh, ma_kpi,  ma_to, ma_pb, DA_GIAHAN_DUNG_HEN)
    select a.thang, 'KPI_PB' LOAI_TINH, a.ma_kpi, null ma_to, a.ma_pb
                  ,  sum(DA_GIAHAN_DUNG_HEN) sothuchien
    from ttkd_bsc.tl_giahan_tratruoc a --left join (select distinct ma_nv, ma_to, ma_kpi, thang, ma_pb , dichvu from ttkd_bsc.blkpi_dm_to_pgd 
--    where dichvu ='Mega+Fiber') b on a.thang = b.thang 
--    and a.ma_to = b.ma_to and a.ma_pb = b.ma_pb
    where a.thang = 202411 and LOAI_TINH = 'KPI_TO'  and  a.ma_kpi = 'HCM_SL_COMBO_006' 
    group by a.thang, a.ma_kpi, a.ma_pb
;
commit;
---- do bang luong
update TTKD_BSC.bangluong_kpi a set 
        thuchien = (select sum(DA_GIAHAN_DUNG_HEN) from ttkd_bsc.tl_giahan_tratruoc where
        thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv and ma_kpi = 'HCM_SL_COMBO_006')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_COMBO_006') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006';

-- to truong
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to
and ma_kpi = 'HCM_SL_COMBO_006')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_SL_COMBO_006')
    and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006';

rollback;
-- pgd
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_pb = a.ma_pb
and ma_kpi = 'HCM_SL_COMBO_006' )
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
and ma_kpi in ('HCM_SL_COMBO_006') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_COMBO_006' and giao is not null;
commit;

select* from  TTKD_BSC.bangluong_kpi a where ma_kpi = 'HCM_SL_COMBO_006' and ten_vtcv like 'Phó%' and giao is null and thang = 202411;
delete  from TTKD_BSC.bangluong_kpi a where ma_kpi = 'HCM_SL_COMBO_006' and ten_vtcv like 'Phó%' and thang = 202411 
and ma_Nv in ('VNP017072','VNP017585','VNP016659','VNP017948','VNP017014','VNP019529','VNP017729','VNP016898','VNP001724','VNP001757','VNP017496',
'VNP016983','VNP017947');