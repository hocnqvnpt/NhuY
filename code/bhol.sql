select* from css_hcm.phieutt_Hd where ma_Gd='HCM-TT/03197217';
drop table giao_t12;
create table giao_t10 as 
select * from REPORT.BC_GHTT_BIEU1_CHOT@dataguard 
where to_char(thang_kt,'yyyymm') = 202410 and phanvung_id = 28 and (KQ_LS_OB_DAUTIEN not in(0,14) or  KQ_LS_OB_SAUCUNG not in (0,14))
and (to_number(to_char( ngay_bh,'yyyymmdd'))<20241106 or ngay_bh is null) AND LUONGGIAO_TTVT IS NULL;

create table tmp3_60ngay_t12 as
with hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id)
, kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--                                                    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--                                                                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--                                                    group by bb.phieu_id)

, kq_ghtt as (select  hdkh.khachhang_id, hdtb.thuebao_id, 0 duan_id, hdtb.ma_tb, 'matt' ma_tt, hdtb.loaitb_id, 22 as lan
                --, dbdc.thang_bd b, hdttdc.thang_bd c, hdttdc.*
                , nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , a.phieutt_id, a.trangthai
                , a.ma_gd, a.ngay_hd, a.ngay_tt, a.soseri, a.seri, b.tien tien_thanhtoan,b.vat
                , kt.kenhthu
                , nh.ten_nh ten_nganhang
                , ht.ht_tra ten_ht_tra
                , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id
                ,a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                         from css_hcm.phieutt_hd a
                                            join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id and b.khoanmuctt_id = 11 and b.tien > 0
                                            left join hddc on b.hdtb_id = hddc.hdtb_id
                                            join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                            join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                            join gh on hdtb.thuebao_id = gh.thuebao_id --and rnk = 1
                                            left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                            left join ct on a.phieutt_id = ct.phieu_id
                                            left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
                                            left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
                                            left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
                         where a.kenhthu_id not in (6) and a.trangthai = 1
                                        and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240801 and 20250105                  ----change--3 thang- ngay 2
                           --             and gh.ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')   ----loai taykhi can---
                                  --   and gh.ma_tb like 'nutifood%'
                                    --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                    --and a.ma_gd = 'HCM-TT/02290172'
                        )
select * from kq_ghtt a
where  a.ngay_bd_moi is NOT null ;
alter table TMP3_60NGAY_T10 add (tien_khop number );
update TMP3_60NGAY_T10 set tien_khop = 1 where ma_Gd in (select ma from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1);
commit;
select* from giao_t10 a
    left join TMP3_60NGAY_T10 b on a.thuebao_id =b.thuebao_id;
    
    select 202410 thang, a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select 'VNP0703000' ma_pb,'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select  a.thuebao_id, a.ma_tb,  sum(tien_thanhtoan) DTHU, max(b.ngay_tt) ngay_tt
                                             , decode(sum(tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from giao_t10 a
                                                 left join TMP3_60NGAY_T10 b on a.thuebao_id =b.thuebao_id
                                        group by  a.thuebao_id, a.ma_tb
                               )
                order by 2
                ) a
union all
  
    select 202411 thang,a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select 'VNP0703000' ma_pb,'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select  a.thuebao_id, a.ma_tb,  sum(tien_thanhtoan) DTHU, max(b.ngay_tt) ngay_tt
                                             , decode(sum(tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from giao_t11 a
                                                 left join TMP3_60NGAY_T11 b on a.thuebao_id =b.thuebao_id
                                        group by  a.thuebao_id, a.ma_tb
                               )
                order by 2
                ) a
union all
  
    select 202412 thang, a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select 'VNP0703000' ma_pb,'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select  a.thuebao_id, a.ma_tb,  sum(tien_thanhtoan) DTHU, max(b.ngay_tt) ngay_tt
                                             , decode(sum(tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from giao_t12 a
                                                 left join TMP3_60NGAY_T12 b on a.thuebao_id =b.thuebao_id
                                        group by  a.thuebao_id, a.ma_tb
                               )
                order by 2
                ) a
;
select* from giao_t12;
select* from ttkd_Bsc.bangluong_kpi where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_022';
select* from TMP3_60NGAY_T11;
delete from TMP3_60NGAY_T12 where ngay_bd_moi <20241201;
rollback;
select* from giao_t10 a
left join TMP3_60NGAY_T10 b on a.thuebao_id =b.thuebao_id