----Buoc 1 import 2 file excel into 2 bang

delete from cttb_huy_goihome;
select * from hocnq_ttkd.cttb_huy_goihome a;
SELECT *
  FROM all_tab_cols
 WHERE owner = 'HOCNQ_TTKD'
   AND table_name = 'CT_HOMECOMB'
   AND column_name = <<name of column>>
---update MANV_HRM---
desc hocnq_ttkd.cttb_dk_goi_home
delete from cttb_dk_goi_home;
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


--delete from cttb_dk_goi_home;
drop table 
select* from CT_HOMECOMBO_202401 
;
            ---Buoc 2 thong tin ghep goi, huy goi
create table CT_HOMECOMBO_202401 as (
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
                left join (select db.thuebao_id, db.ma_tb, db.ngay_sd ngay_sd_fiber, dbc.tocdo_id, td.ma_td 
                                from CSS_HCM.db_thuebao db, css_hcm.db_adsl dbc, CSS_HCM.tocdo_adsl td
                                where db.thuebao_id=dbc.thuebao_id and dbc.tocdo_id=td.tocdo_id --and db.ma_tb = 'thuytien6'
                                ) dbtb
                        on a.account_fiber = dbtb.ma_tb

                left join (select hdkh.hdkh_id, hdtb.hdtb_id, hdtb.ma_tb, hdkh.ngay_yc, hdtb.ngay_ht, hdkh.ctv_id
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
                            and trunc(hdkh.ngay_yc) >= to_date('01/01/2024','dd/mm/yyyy')
                                    ) hdmoi
                        on a.account_fiber = hdmoi.ma_tb
         left join (select a.ma_nv, a.ten_nv, a.ma_to, a.ma_pb, b.ten_to, a.ten_pb, c.ten_kenhbh
                            from    TTKD_BSC.NHANVIEN_202401 a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to
                                                             left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                             ) b
                        on a.mnv_tt = b.ma_nv
            left join (select a.ma_nv, a.manv_nnl, a.ten_nv, a.ma_to, a.ma_pb, b.ten_to, a.ten_pb, c.ten_kenhbh
                                    from    ttkd_bsc.NHANVIEN_202401 a join ttkd_bsc.dm_to b on a.ma_to = b.ma_to
                                                             left join ttkd_bsc.dm_kenhbh c on b.kenhbh_id = c.kenhbh_id
                                    ) c
                        on a.mnv_tt = c.manv_nnl
                left join ttkd_bsc.NHANVIEN_202401 e
                        on a.nguoi_th = e.user_ccbs                   
)
;
update CT_HOMECOMBO_202312 set DONVI = 'TTKD';
---------------Update data VTTP----------------
--select * from temp;
drop table temp purge;
create table temp as
select chu_nhom, account_fiber, mnv_tt, donvi_tiepthi, nguoi_th, phong_dk_goi, ten_kenhbh, nv.ten_nv, nv.ten_to, bo_dau(nv.ten_phong) TEN_PHONG, bo_dau(nv.TEN_CHUQUAN)TEN_CHUQUAN
from CT_HOMECOMBO_202312 left join hocnq_ttkd.v_nhanvien nv on mnv_tt = nv.ma_nv
;
update CT_HOMECOMBO_202312 set ten_kenhbh = 'VTTP', DONVI = 'VTT'
                        where mnv_tt not in (select mnv_tt from temp where ten_nv is null)
                                        and mnv_tt not in (select mnv_tt from temp where ten_phong = 'Trung Tam Kinh Doanh' or ten_phong = ' ')
                                        and mnv_tt not in (select mnv_tt from temp where ten_chuquan = 'Trung Tam Kinh Doanh')
;
            ----Update Ten_pb TTVT-----
update CT_HOMECOMBO_202312 set donvi_tiepthi = (select bo_dau(nv.TEN_PHONG) from v_nhanvien nv where mnv_tt = nv.ma_nv)
                                                                            , ten_nv = (select nv.ten_nv from v_nhanvien nv where mnv_tt = nv.ma_nv)
                                                                            , to_tiep_thi = (select nv.ten_to from v_nhanvien nv where mnv_tt = nv.ma_nv)
                            where DONVI = 'VTT'
;
            ----Update Ten_pb TTVT don vi chu quan--------
update CT_HOMECOMBO_202312 set donvi_tiepthi = (select bo_dau(nv.ten_chuquan) from v_nhanvien nv where mnv_tt = nv.ma_nv)
                        where substr(donvi_tiepthi,1,5) not in ('Trung', 'Vien ')  and DONVI = 'VTT'
;
        
commit;
rollback;

select * from CT_HOMECOMBO_202311;
select phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu from CT_HOMECOMBO_202312 where  donvi_tiepthi = ' ' group by phong_dk_goi, nguoi_th, mnv_tt, donvi_tiepthi, ngay_dk, account_fiber, goi_moi_hienhuu;
select distinct donvi_tiepthi, donvi from CT_HOMECOMBO_202102
;

-------Append vao table nam----------
               -- create table ct_homecombo_2024 as 
                select * from ct_homecombo_2023 where thang = 202311;
                delete from ct_homecombo_2023 where thang = 202205;
          
                select chu_nhom, account_fiber, ngayhc_fiber,account_myTV, ngayhc_mytv, ngay_active_vnp, ngay_dk, nguoi_th, trangthai_goi, mnv_tt, trangthai_fiber, trangthai_mytv, goi_moi_hienhuu from ct_homecombo_2020 where thang = 202010;
                insert into ct_homecombo_2023 (                 THANG, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
                                                                                                        , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
                                                                                                        , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB)
                                                                                         (select 202311 thang, CHU_NHOM, THANHVIEN, MNP, LOAI_GOI, LOAI_TB, ACCOUNT_FIBER, TRANGTHAI_FIBER, NGAYHC_FIBER, ACCOUNT_MYTV, TRANGTHAI_MYTV, NGAYHC_MYTV, NGAY_ACTIVE_VNP, TEN_KH, DIACHI_KH
                                                                                                        , NGAY_DK, TRANGTHAI_GOI, GOI_MOI_HIENHUU, MNV_TT, NGUOI_TH, DONVI, NGAY_SD_FIBER, TOCDO_ID, MA_TD, hdtb_id, NGAY_YC_FIBER, NGAY_HT_FIBER, TRANGTHAI_HD, GHICHU, TEN_NV, TO_TIEP_THI
                                                                                                        , TEN_KENHBH, DONVI_TIEPTHI, PHONG_DK_GOI, MA_TO, MA_PB
                                                                                        from CT_HOMECOMBO_202312)  ----change---
;
commit;
rollback;

-------Append vao table nam----------
                delete from CT_GDVP_HUY_2023 where thang = 202205 and  LOAI = 'HOME';--and to_number(to_char(NGAY_HUY, 'yyyymm')) = 202008;
               --  create table CT_GDVP_HUY_2023 as
             
                 select * from CT_GDVP_HUY_2023 where thang = 202311 and  LOAI = 'HOME';
            
                insert into CT_GDVP_HUY_2023
                                                        (select distinct 202311, CHU_NHOM, 'HOME' LOAI, LOAI_GOI
                                                                    , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'loai_tb') loai_tb
                                                                    , ACCOUNT_FIBER, ACCOUNT_MYTV, NGAY_DK, MNV_TT, NGAY_HUY, NGUOI_TH, LYDOHUY
                                                                     , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'ten_nv') ten_nv
                                                                    , GET_PTM_YYYYMM_homecb(ngay_dk, account_fiber, 'donvi_tiepthi') donvi_tiepthi 
                                                        from cttb_huy_goihome)
                ;
                     
        
commit;
rollback;
