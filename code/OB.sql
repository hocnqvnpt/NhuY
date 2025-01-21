 create table fffffff_ as   WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                                from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                                where h.thang_bd > 202403
                                                )
       , kmtb as (select hdtb_id, rkm_id        , ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                                                    and thang_bddc > 202403
                            )
        , ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
    group by bb.phieu_id)
        , kq_ghtt as (select 
        hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                    , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                    , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                    , p.phieutt_id, p.trangthai,  ct.ngay_nganhang
                    , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
                   , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, 
                   p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, 
                   kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra
                         from css_hcm.ct_tienhd a
                                left join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11-- and b.tien < 0
                                left join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id
                                left join hddc on a.hdtb_id = hddc.hdtb_id
                                join css_hcm.hd_thuebao hdtb on a.hdtb_id = hdtb.hdtb_id  and hdtb.kieuld_id in (551, 550, 24, 13080) 
                                join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id  
                                left join kmtb on a.hdtb_id = kmtb.hdtb_id
                                left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
                                left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
                                left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
                                join  ttkd_bsc.nhuy_ct_ipcc_obghtt gh on hdtb.thuebao_id = gh.thuebao_id and thang >= 202403 --and gh.nhanvien_id = hdkh.ctv
                                left join ct on p.phieutt_id = ct.phieu_id
                         where   to_number(to_char(hdkh.ngay_yc,'yyyymm')) = 202404  and a.khoanmuctt_id = 11 and   
                         hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                              and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20240401 and 20240502)                     ----change--2 thang- ngay 02
                                                            or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20240401 and 20240502)) 
                                        ) 
    select *
    from kq_ghtt a
    where ngay_bd_moi is not null ; and not exists (select 1 from tmp_ob where rkm_id = a.rkm_id); 
    select* from css_hcm.ct_tienhd
select rkm_id from FFFFFFF_ group by rkm_id having count(rkm_id) > 1;
insert into tmp_ob 
select * from hocnq_ttkd.x_tempppp where rkm_id not in (select rkm_id from tmp_ob);
create table tmp3_ob as select* from tmp_ob;
commit;
select* from  ttkd_bsc.nhuy_ct_ipcc_obghtt -- tmp3_ob a 
  left  JOIN  ttkd_bsc.nhuy_ct_ipcc_obghtt B ON a.thuebao_id = b.thuebao_id and a.nvtuvan_id = b.nhanvien_id --where a.thuebao_id = 1227269;
  where nhanvien_id is not null
  order by 2;
  rollback;
  drop table tmp_ob;
  create table tmp_ob as select * from (
--and rkm_id not in (select rkm_id from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where rkm_id is not null)
select KHACHHANG_ID, THUEBAO_ID, MA_TB, KIEULD_ID, LOAITB_ID, RKM_ID, NGAY_BD_MOI, NGAY_KT_MOI, PHIEUTT_ID, TRANGTHAI, NGAY_NGANHANG, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, 
TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN, VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, TTHD_ID, 
KENHTHU, TEN_NGANHANG, TEN_HT_TRA , row_number() over (partition by thuebao_id, rkm_id, nvtuvan_id order by thuebao_id ) rkn from tmp3_ob) where rkn = 1  ;
select thuebao_id from ttkd_Bsc.nhuy_ct_ipcc_obghtt group by thuebao_id having count (distinct NGAY_KTDC ) > 1--a where a.thuebao_id = 1261713;
commit;
select* from tmp_ob where ngay_tt is null and ngay_Nganhang is null--to_number(to_char(ngay_tt, 'yyyymmdd')) not between 20240401 and 20240502 and to_number(to_char(ngay_nganhang, 'yyyymmdd')) not between 20240401 and 20240502