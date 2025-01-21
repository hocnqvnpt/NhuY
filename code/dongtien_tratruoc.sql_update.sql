
    select * from  ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202405;--919,11921697,11921723,11921917);
    select * from  ttkd_Bsc.ct_dongia_tratruoc a where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ma_nv  = 'khongco'; and ipcc is null;
    update  ttkd_Bsc.ct_dongia_tratruoc a set ma_nv = 'khongco' where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ma_nv is null; and ipcc is null;
    rollback;
    delete from  ttkd_Bsc.ct_dongia_tratruoc a where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and thuebao_id in (
--    select* from ttkd_Bsc.ct_dongia_tratruoc a where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and thuebao_id in (
    select thuebao_id from  ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB' --and ma_nv is null
    group by thuebao_id , ma_nv having count(thuebao_id) > 1) and tien_khop != 1 and nhanvien_xuathd is null and ma_nv = 'khongco'; 
    delete;
    commit;
  delete from ttkd_bsc.ct_dongia_tratruoc where ma_nv = 'khongco' and thang = 202405 and loai_tinh = 'DONGIATRA_OB' and 
   ghichu != 'HINH THUC TRA: CHUYEN KHOAN nhan vien xuat hd la nhan vien cap nhat chung tu, ty le la ty le don gia cap nhat' ;and nhanvien_xuathd is null;
    update;
update ttkd_Bsc.ct_dongia_tratruoc a
set tien_xuathd = 10000*tyle_thanhcong
where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ma_nv= 'khongco'; exists (select 1 from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt where thuebao_id = a.thuebao_id and a.ma_nv = manv_thuyetphuc
    and thang = 202405 and ipcc =0)
;
flashback table thu_thang1 to before drop rename t;
commit;
select* from ;
drop table thu_t1;
select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%Hân';
create table thu_t1 as ;

WITH hddc as (
        select distinct hdtb_id,h.rkm_id b, g.rkm_id a, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc , g.thang_thoaitra,g.ngay_thoai
        from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where h.thang_bd > 202310
                                            )
   , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css.v_khuyenmai_dbtb@dataguard 
              where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                                and thang_bddc > 202310
                        )
    , kq_ghtt as (select distinct 
                        hdtb.thuebao_id,hdtb.ma_tb, hdkh.khachhang_id, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                        , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd
                        , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt
                        , a.phieutt_id, a.trangthai
                        , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                        , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                  from css.v_phieutt_hd@dataguard a
                        join css.v_ct_phieutt@dataguard b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id in (11,5) and b.tien > 0
                        left join hddc on b.hdtb_id = hddc.hdtb_id
                        join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id =6 --and hdtb.kieuld_id  in (551, 550, 24, 13080) 
                        join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
                        left join kmtb on b.hdtb_id = kmtb.hdtb_id
                  where a.trangthai = 1 and  to_number(to_char(a.ngay_Tt, 'yyyymm')) = 202401  --  a.kenhthu_id not in (6) and 
                                        
--                                                        and to_number(to_char(nvl(a.ngay_tt, hdtb.ngay_ht), 'yyyymm')) = 202401
                                    ) 
, tien_thu as (
        select a.thuebao_id, a.ma_tb, a.loaitb_id, a.khachhang_id,a.ma_gd
                , sum(case when tien_thanhtoan >0 then tien_thanhtoan end) tien_thu
        from kq_ghtt a 
        left join css.v_db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id
        left join css.v_db_thanhtoan@dataguard c on b.thanhtoan_id = c.thanhtoan_id
        group by a.thuebao_id, a.ma_tb, a.loaitb_id, a.khachhang_id, a.ma_Gd
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
                                                                join css.v_hd_thuebao@dataguard hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.tthd_id <> 7  and hdtb.kieuld_id  in  (551, 550, 553)
                                                                join css.v_hd_khachhang@dataguard hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                                                left join kmtb on b.hdtb_id = kmtb.hdtb_id
                                         where   a.trangthai = 1   
                                                  and  to_number(to_char(a.ngay_Tt, 'yyyymm')) = 202401-- and   thang_thoaitra = 202401 -- and                                            
--                                                     to_number(to_char(nvl(hdkh.ngay_yc,a.ngay_tt), 'yyyymm')) = 202401
) 
, tien_thoai as (
        select a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, b.khachhang_id
                , sum(case when tien_thanhtoan < 0 then tien_thanhtoan end) tien
        from kq_hdtt a 
        left join css.v_db_Thuebao@dataguard b on a.thuebao_id = b.thuebao_id
        left join css.v_db_thanhtoan@dataguard c on b.thanhtoan_id = c.thanhtoan_id
        group by a.thuebao_id, a.ma_tb, a.loaitb_id, c.ma_Tt, b.khachhang_id)
, tien_phanbo as (
    select a.ma_tb, a.ma_tt, a.thuebao_id , a.loaitb_id, sum(tien) as tien_phanbo
    from bcss.v_ct_tratruoc@dataguard  a
    where ky_cuoc = 20240101 and tien<0
    group by  a.ma_tb, a.ma_tt, a.thuebao_id , a.loaitb_id
)

SELECT * FROM kq_ghtt where ma_Tb= 'hcmbinhduong2024';
select* from css_hcm.ct_phieutt where phieutt_id = 8046125; like 'Ti?n Tr? Tr??c%';
with t_thu as 
(select ma_Tb , sum(tien) tien from thu  group by ma_Tb)
select* from thu_Thang1 a join t_thu b on a.ma_Tb = b.ma_tb where a.tien_thu=2*b.tien ;and a.tien_thu!=b.tien ; = 'vantai041023';
select* from thu where ma_Tb not in (select ma_tb  from thu_Thang1);
select* from thu_Thang1 where ma_Tb not in (select ma_tb  from thu);

select* from thu;
select a.*,d.ma_kh, b.tien_thoai, c.tien_phanbo
    from tien_Thu a
            left join tien_thoai b on a.thuebao_id = b.thuebao_id
            left join tien_phanbo c on a.thuebao_id = c.thuebao_id
            left join css_hcm.db_khachhang d on a.khachhang_id = d.khachhang_id
;
SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202405 AND DONVI ='TTKD' AND MA_NV IN (
select DISTINCT NHANVIEN_XUATHD from ttkd_bsc.ct_dongia_tratruoc where  thang = 202405 and tien_khop > 0 and loai_tinh in ('DONGIATRA_OB_BS','DONGIATRA_OB') GROUP BY THUEBAO_ID , MA_NV, NHANVIEN_XUATHD HAVING COUNT(THUEBAO_ID) > 1
);
commit;
