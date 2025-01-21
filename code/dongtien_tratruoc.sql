    select * from  bcss.v_ct_tratruoc@dataguard where ky_cuoc = 20240101 and thuebao_id in (11921919,11921697,11921723,11921917);

select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%Hân';
create table thang_1 as ;
WITH hddc as (select distinct hdtb_id,h.rkm_id b, g.rkm_id a, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc , g.thang_thoaitra,g.ngay_thoai
                                            from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where h.thang_bd > 202310
                                            )
   , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css.v_khuyenmai_dbtb@dataguard 
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                                and thang_bddc > 202310
                        )
    , kq_ghtt as (select 
    hdtb.thuebao_id,hdtb.ma_tb, hdkh.khachhang_id, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                                                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
                                                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt
                                                        , a.phieutt_id, a.trangthai
                                                        , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                                                       , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                                         from css.v_phieutt_hd@dataguard a
                                                                join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                                                left join hddc on b.hdtb_id = hddc.hdtb_id
                                                                join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id = 6 --and hdtb.kieuld_id  in (551, 550, 24, 13080) 
                                                                join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                                                                left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                         where a.kenhthu_id not in (6) and a.trangthai = 1 and  to_number(to_char(a.ngay_Tt, 'yyyymm')) = 202401  
                                        
--                                                        and to_number(to_char(nvl(a.ngay_tt, hdtb.ngay_ht), 'yyyymm')) = 202401
                                    ) 
, tien_thu as (
        select a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, a.khachhang_id
                , sum(case when tien_thanhtoan >0 then tien_thanhtoan end) tien_thu
        from kq_ghtt a 
        left join css.v_db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id
        left join css.v_db_thanhtoan@dataguard c on b.thanhtoan_id = c.thanhtoan_id
        group by a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, a.khachhang_id
)
,  kq_hdtt as (select 
    hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id
--                                                            nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
--                                                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
--                                                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt
                                                        , a.phieutt_id, a.trangthai
                                                        , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat, hdkh.ngay_yc
                                                       , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                                         from css.v_phieutt_hd@dataguard a
                                                                join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and b.tien < 0 --and b.khoanmuctt_id = 11 --
                                                                left join hddc on b.hdtb_id = hddc.hdtb_id
                                                                join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id = 6  and hdtb.kieuld_id  in (551, 550, 553) 
                                                                join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                                                left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                         where   a.trangthai = 1   
                                                  and  to_number(to_char(a.ngay_Tt, 'yyyymm')) = 202401-- and   thang_thoaitra = 202401 -- and                                            
--                                                     to_number(to_char(nvl(hdkh.ngay_yc,a.ngay_tt), 'yyyymm')) = 202401
) 
, tien_thoai as (
        select a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, b.khachhang_id
                , sum(case when tien_thanhtoan < 0 then tien_thanhtoan end) tien_thoai
        from kq_hdtt a 
        left join css.v_db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id
        left join css.v_db_thanhtoan@dataguard c on b.thanhtoan_id = c.thanhtoan_id
        group by a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, b.khachhang_id)
, tien_phanbo as (
    select a.ma_tb, a.ma_tt, a.thuebao_id , a.loaitb_id, sum(tien) as tien_phanbo
    from bcss.v_ct_tratruoc@dataguard  a
    where ky_cuoc = 20240101
    group by  a.ma_tb, a.ma_tt, a.thuebao_id , a.loaitb_id
    
)
select* from tien_phanbo;

select a.*,d.ma_kh, b.tien_thoai, c.tien_phanbo
    from tien_Thu a
            left join tien_thoai b on a.thuebao_id = b.thuebao_id
            left join tien_phanbo c on a.thuebao_id = c.thuebao_id
            left join css_hcm.db_khachhang d on a.khachhang_id = d.khachhang_id

;
commit;
insert into ttkd_Bsc.ct_dongia_Tratruoc
select 202405,LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, -DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU, 1,'Boi hoan tien don gia tru thang 4 theo noi dung mail ngay 22/5/2024' GHICHU, TYLE_THANHCONG, HESO_CHUKY, NHANVIEN_XUATHD, TIEN_XUATHD, TIEN_THUYETPHUC from ttkd_Bsc.ct_dongia_Tratruoc where ma_Tb= 'hcm_ca_00014592';

update ttkd_Bsc.ct_dongia_Tratruoc set dongia = -dongia where thang = 202405 and ma_Tb in ('hcm_ca_00039115','hcm_ca_ivan_00015635','hcm_ca_ivan_00015722','hcm_ca_ivan_00015680','');
update ttkd_Bsc.ct_dongia_Tratruoc a
set dongia = (Select dongia from ttkd_Bsc.ct_dongia_Tratruoc  
where thang < 202405 and ma_Tb in ('hcm_ca_00039115','hcm_ca_ivan_00015635','hcm_ca_ivan_00015722','hcm_ca_ivan_00015680') and a.ma_Tb = ma_Tb)
 where thang = 202405 and ma_Tb in ('hcm_ca_00039115','hcm_ca_ivan_00015635','hcm_ca_ivan_00015722','hcm_ca_ivan_00015680');;
select* from ttkd_bsc.nhanvien_202404 where sdt = '0911768685'  
;
update TTKD_BSC.CT_DONGIA_TRATRUOC set thang = 202405  WHERE THANG = 202404 AND ghichu = 'bo sung cho 1 thue bao gia han 2 hop dong'-- IN (
select thuebao_id from ttkd_bsc.ct_dongia_tratruoc where thang = 202404 and loai_tinh in ('DONGIA_TS_TP_TT','DONGIATRA_OB') group by thuebao_id having count (Thuebao_id) > 1 )--ao_id not in (select thuebao_id from ct_dongia_tratruoc where thang = 202404)
