select* from ttkd_Bsc.userld_202404_goc;
select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%Nh? Ý';

create table userld_202405_goc as select* from userld_202403_goc where 1=2;
commit;
create table bscc_import_goi_bris  as 
select* from manpn.bscc_import_goi_bris   where 1= 2;
drop table QR_CODE
update bscc_import_ptm_bris set thang = 202403 where thang is null;
select* from bscc_import_goi_bris;

SELECT * FROM TTKD_BSC.DT_PTM_VNP_202403 --WHERE GOI_LUONGTINH IS NOT NULL
;
select  MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND,
USER_DHSXKD, MANV_HRM,  USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID from nhanvien_202405 
minus 
select  MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, 
USER_DHSXKD, MANV_HRM,  USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID from ttkd_Bsc.nhanvien_202405 ;
--update nhanvien_202405 set ma_vtcv = 'VNP-HNHCM_KDOL_17', ten_Vtcv = 'Nhân Viên Outbound Bán Hàng'
--where  ma_vtcv = 'VNP-HNHCM_KDOL_3.2'
select* from ttkd_Bsc.nhanvien_202405 a
join nhanvien_202405 b on a.ma_nv = b.ma_nv 
where a.ma_nv = 'CTV081986' and nvl(a.MANV_NNL,0) = b.MANV_NNL;
drop table qr_code;
create table qr_code as
WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                            )
--, gh as (select khachhang_id, thuebao_id, duan_id, ma_tb, ma_tt, loaitb_id, thang_kt 
--    from ttkdhcm_ktnv.ghtt_giao_688 where tratruoc = 1 and km = 1 and loaibo = 0  and thang_kt = 202404 ) -- change
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
            where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--                join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--group by bb.phieu_id)

, kq_ghtt as (select  hdkh.khachhang_id, hdtb.thuebao_id,hdtb.ma_tb, hdtb.loaitb_id--, gh.thang_kt, 05 lan
        --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                                    , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                    , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                                    , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                                    , a.phieutt_id, a.trangthai
                                    , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                    ,  kt.kenhthu 
                                    , ht.ht_tra ten_ht_tra
                             --       , kmtb.rkm_id a1, hddc.rkm_id a2
                                   , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, 
                                   hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                     from css_hcm.phieutt_hd a
                                            join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            left join hddc on b.hdtb_id = hddc.hdtb_id
                                            join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                            join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                            join gh on hdtb.thuebao_id = gh.thuebao_id
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
--                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                     where a.kenhthu_id not in (6) and a.trangthai = 1  and to_number(to_char(a.ngay_tt, 'yyyymm')) >= 202404                                                   
--                                    and ((a.ht_tra_id <> 2 and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240301 and 20240331)                     ----change--2 thang- ngay 02
--                                                        or (a.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20240301 and 20240331))                     ----change--2 thang- ngay 02
                                 --   and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20230427 and 20230701                     ----change--2 thang-
                              --      and gh.ma_tb in ('ketnoi_thoitrang')   ----loai taykhi can---
                                --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                 --and hddc.rkm_id is not null
                )
select * from kq_ghtt a
where ngay_bd_moi is not null ;
select a.THUEBAO_ID,a.MA_TB, a.LOAITB_ID, a.PHIEUTT_ID, a.MA_GD,  to_number(to_char(a.ngay_tt,'yyyymm'))thang_tt, a.NGAY_TT, a.TIEN_THANHTOAN, a.VAT, a.KENHTHU, a.TEN_HT_TRA, b.loaihinh_Tb,
c.ten_Dvvt, e.ma_kh, f.ma_Tt
from ghtt a
    left join css_hcm.loaihinh_Tb b on a.loaitb_id =b.loaitb_id
    left join css_hcm.dichvu_vt c on b.dichvuvt_id = c.dichvuvt_id 
    left join css_hcm.db_thuebao d on a.thuebao_id = d.thuebao_id
    left join css_hcm.db_khachhang e on d.khachhang_id = e.khachhang_id
    left join css_hcm.db_thanhtoan f on d.thanhtoan_id = f.thanhtoan_id
where c.dichvuvt_id = 4
;
select* from css_hcm.phieutt_hd;
select a.*, ma_kh from dddd a
left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
left join css_hcm.db_khachhang c on b.khachhang_id = c.khachhang_id

select* from css_hcm.hd_khachhang;
select * from ttkdhcm_ktnv.DM_MVIEW ;
where MVIEW_NAME= upper('duan')
