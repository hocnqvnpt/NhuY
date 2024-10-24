 ----Buoc 1 import 2 file excel into 2 bang
 insert into ttkd_bsc.ct_Bsc_tratruoc_moi_tr30day(THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_QL, PHONG_QL, MA_TO, MA_PB, MANV_GIAO, 
PHONG_GIAO, MANV_TH, PHONG_TH, MA_NV_CN, MANV_THUYETPHUC, KQ_POPUP, KQ_DVI_CN, PHONG_CN, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, 
TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_HD, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, MANV_TCTN, PBH_OA_ID, MANV_OA, NHOMTB_ID, 
KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, TIEN_KHOP, MA_CHUNGTU, MAPB_THPHUC, HESO_GIAO, HT_TRA_ID, KENHTHU_ID, KHDN, MANV_GT, MANV_THUNGAN, NGAY_NGANHANG)
select thang, gh_id, pbh_ql_id, pbh_giao_id, tbh_Giao_id, pbh_Th_id, pbh_cn_id, ma_tb, manv_cs, phong_cs, ma_to, ma_pb, manv_giao,
from ct_Bsc_tratruoc_moi_tr30day;
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202404;
update 
create table ;
delete from cttb_huy_goihome; where chu_nhom is null;
select * from cttb_huy_goihome a;
select* from ttkd_bsc.tl_giahan_Tratruoc where thang = 202404;
commit;
---update MANV_HRM---
delete from cttb_dk_goi_home where chu_nhom is null;
select * from cttb_dk_goi_home; where trangthai_goi != 'Huy' ;
commit;
rollback;
           
                                ----Update Ngay_active_VNP-----               
                                        update cttb_dk_goi_home a set a.ngay_active_vnp = (
                                            select min(ngay_active_vnp) ngay_active_vnp from cttb_dk_goi_home
                                            where chu_nhom in (select chu_nhom from cttb_dk_goi_home
                                                                                            group by chu_nhom having count(distinct ngay_active_vnp) >1)
                                                            and a.chu_nhom = chu_nhom
                                            group by chu_nhom)
                                        where chu_nhom in (select distinct chu_nhom from cttb_dk_goi_home
                                                group by chu_nhom having count(distinct ngay_active_vnp) >1)
                                                ;

commit;
--delete from cttb_dk_goi_home;
select* from CT_HOMECOMBO_202312 purge
;
drop table CT_HOMECOMBO_202404;
---Buoc 2 thong tin ghep goi, huy goi
create table CT_HOMECOMBO_202409 as 
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
        and trunc(hdkh.ngay_yc) >= to_date('01/09/2024','dd/mm/yyyy') -- CHANGE
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
                        from TTKD_BSC.NHANVIEN a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to and a.thang = 202409 and A.donvi = 'TTKD'-- CHANGE
                        left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                        ) b 
                    on a.mnv_tt = b.ma_nv
            left join (select a.ma_nv, a.manv_nnl, a.ten_nv, a.ma_to, a.ma_pb, b.ten_to, a.ten_pb, c.ten_kenhbh
                        from ttkd_bsc.NHANVIEN a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to and a.thang = 202409 and A.donvi = 'TTKD' -- CHANGE
                        left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                        ) c
                    on a.mnv_tt = c.manv_nnl
            left join ttkd_bsc.NHANVIEN e -- CHANGE
                    on a.nguoi_th = e.user_ccbs  AND  e.thang = 202409 and E.donvi = 'TTKD'                  
)
;
update CT_HOMECOMBO_202312 set DONVI = 'TTKD';
commit;
---------------Update data VTTP----------------
select* from CT_HOMECOMBO_202401
select account_fiber from hocnq_ttkd.CT_HOMECOMBO_202311 group by account_fiber having count(account_fiber) > 1
--select * from temp;
drop table temp purge;
SELECT* FROM TTKD_BSC.NHANVIEN_VTTP
select* from hocnq_ttkd.v_nhanvien
create table temp as
select  chu_nhom, account_fiber, mnv_tt, donvi_tiepthi, nguoi_th, phong_dk_goi, ten_kenhbh, nv.ten_nv, nv.ten_to, bo_dau(nv.ten_phong) TEN_PHONG, 
bo_dau(nv.TEN_CHUQUAN)TEN_CHUQUAN
from CT_HOMECOMBO_202312 left join hocnq_ttkd.v_nhanvien nv on mnv_tt = nv.ma_nv
;
--update CT_HOMECOMBO_202312 set ten_kenhbh = 'VTTP', DONVI = 'VTT'
--                        where mnv_tt not in (select mnv_tt from temp where ten_nv is null)
--                                        and mnv_tt not in (select mnv_tt from temp where ten_phong = 'Trung Tam Kinh Doanh' or ten_phong = ' ')
--                                        and mnv_tt not in (select mnv_tt from temp where ten_chuquan = 'Trung Tam Kinh Doanh')
-- update dong do VTTP lam
update CT_HOMECOMBO_202406 set ten_kenhbh = 'VTTP', DONVI = 'VTT'
where mnv_tt in (select ma_nv from ttkd_bsc.nhanvien where thang = 202406 and donvi = 'VTTP');
rollback;
commit;

select distinct donvi_tiepthi from hocnq_ttkd.CT_HOMECOMBO_202311 
;
            ----Update Ten_pb TTVT-----
select * from ttkd_bsc.nhanvien_vttp

--update CT_HOMECOMBO_202312 set donvi_tiepthi = (select bo_dau(nv.TEN_PHONG) from ttkd_Bsc nv where mnv_tt = nv.ma_nv)
--                                                                            , ten_nv = (select nv.ten_nv from v_nhanvien nv where mnv_tt = nv.ma_nv)
--                                                                            , to_tiep_thi = (select nv.ten_to from v_nhanvien nv where mnv_tt = nv.ma_nv)
--where DONVI = 'VTT'
--;
--select* from 
--            ----Update Ten_pb TTVT don vi chu quan--------
--update CT_HOMECOMBO_202312 set donvi_tiepthi = (select bo_dau(nv.ten_chuquan) from v_nhanvien nv where mnv_tt = nv.ma_nv)
--where substr(donvi_tiepthi,1,5) not in ('Trung', 'Vien ')  and DONVI = 'VTT'
;
---

-- update phong ban TTVT
-- 
update ct_homecombo_202409 a 
set donvi_tiepthi = (select bo_dau(ten_pb)
from ttkd_Bsc.nhanvien where thang = 202409 AND donvi = 'VTTP' and ma_nv = a.mnv_tt) ,
ten_nv =  (select ten_nv from ttkd_Bsc.nhanvien where thang = 202409 AND donvi = 'VTTP' and ma_nv = a.mnv_tt),
to_tiep_thi = (select ten_to from ttkd_Bsc.nhanvien where thang = 202409 AND donvi = 'VTTP' and ma_nv = a.mnv_tt),
ten_kenhbh = 'VTTP',
ma_to = (select ma_to from ttkd_Bsc.nhanvien where thang = 202409 AND donvi = 'VTTP' and ma_nv = a.mnv_tt),
ma_pb = (select ma_pb from ttkd_Bsc.nhanvien where thang = 202409 AND donvi = 'VTTP' and ma_nv = a.mnv_tt)
where  DONVI = 'VTT'       ;
--
-- kiem tra
select* from hocnq_Ttkd.ct_homecombo_202306;
commit;
rollback;
select* from ct_homecombo_202401 where donvi_tiepthi is null-- and donvi = 'TTKD'
select* from admin_hcm.donvi where donvi_id in (283892,283669,10647,283890)
select * from ct_homecombo_2024
select phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu from CT_HOMECOMBO_202312 where  donvi_tiepthi is null group by phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu;
select distinct donvi_tiepthi, donvi from CT_HOMECOMBO_202102
select phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu from hocnq_ttkd.CT_HOMECOMBO_202311 where  donvi_tiepthi = ' ' group by phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu;

;

-------Append vao table nam----------
--                create table ct_homecombo_2024 as 
create table ct_homecombo_2024 as;
delete from ct_homecombo_2024 where chu_nhom is null ;where thang = 202312;
delete from ct_homecombo_2024 where thang = 202404;
commit;
select chu_nhom, account_fiber, ngayhc_fiber,account_myTV, ngayhc_mytv, ngay_active_vnp, ngay_dk, nguoi_th, trangthai_goi, mnv_tt, trangthai_fiber, trangthai_mytv, goi_moi_hienhuu from ct_homecombo_2020 where thang = 202010;
insert into ct_homecombo_2024
    (THANG, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
    , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
    , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB)
        (select 202409 thang, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
                        , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
                        , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB
        from CT_HOMECOMBO_202409)  ----change---
;
commit;
rollback;

create or replace FUNCTION            GET_PTM_YYYYMM_HOMECB 
(
 ngay_dk IN date
 , MA_TB IN VARCHAR2
, col_name IN VARCHAR2

) RETURN VARCHAR2 is 
    MA_TT VARCHAR2(50);
    v_NAM VARCHAR2(10):= to_char(ngay_dk, 'yyyy');
    v_curNAM VARCHAR2(10):= to_char(ngay_dk, 'yyyymm');

BEGIN
    execute immediate
                'select distinct '||  col_name ||'
                from
                                (
                                select ngay_dk, account_fiber, trangthai_fiber, mnv_tt, ten_nv, donvi_tiepthi, ma_to, ma_pb, loai_tb
                                from ct_homecombo_' || v_NAM || ' 
                                where thang = ' || v_curNAM || ' group by ngay_dk, account_fiber, trangthai_fiber, mnv_tt, ten_nv, donvi_tiepthi, ma_to, ma_pb, loai_tb
                                )
                where rownum = 1 and account_fiber = '''||ma_tb|| ''' and to_char(ngay_dk, ''yyyymm'') = '''||to_char(ngay_dk, 'yyyymm') ||''''

    into MA_TT;
  RETURN MA_TT;
  exception
       when OTHERS then
      return  null;
END GET_PTM_YYYYMM_HOMECB;


commit;
-------Append vao table nam----------
                delete from CT_GDVP_HUY_2023 where thang = 202205 and  LOAI = 'HOME';--and to_number(to_char(NGAY_HUY, 'yyyymm')) = 202008;
               --  create table CT_GDVP_HUY_2023 as
                 select * from CT_GDVP_HUY_2024 where thang = 202406;-- and  LOAI = 'HOME';
            create table CT_GDVP_HUY_2024 as select* from hocnq_ttkd.CT_GDVP_HUY_2023 where 1=2;
                    insert into CT_GDVP_HUY_2024
                                                            (select distinct 202406, CHU_NHOM, 'HOME' LOAI, LOAI_GOI
                                                                        , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'loai_tb') loai_tb
                                                                        , ACCOUNT_FIBER, ACCOUNT_MYTV, NGAY_DK, MNV_TT, NGAY_HUY, NGUOI_TH, LYDOHUY
                                                                         , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'ten_nv') ten_nv
                                                                        , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'donvi_tiepthi') donvi_tiepthi 
                                                            from cttb_huy_goihome)
                    ;
                    delete from CT_GDVP_HUY_2024 where chu_nhom is null;
                    update CT_GDVP_HUY_2024 a set 
                    loai_tb = (select loai_tb from ct_homecombo_2024
                                                where to_char(ngay_dk, 'yyyymm') = to_char(ngay_dk, 'yyyymm') and a.account_fiber = account_fiber and rownum = 1
                    )
                    ,
                    ten_Nv =  (select ten_Nv from ct_homecombo_2024
                                                where to_char(ngay_dk, 'yyyymm') = to_char(ngay_dk, 'yyyymm') and a.account_fiber = account_fiber and rownum = 1),
                    donvi_tiepthi = (select donvi_tiepthi from ct_homecombo_2024
                                                where to_char(ngay_dk, 'yyyymm') = to_char(ngay_dk, 'yyyymm') and a.account_fiber = account_fiber and rownum = 1)
                    where donvi_tiepthi is null or loai_tb is null or ten_Nv is null;
                     commit;
        
commit;
rollback;
