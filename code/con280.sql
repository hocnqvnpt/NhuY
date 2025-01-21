select a.*, tt.trangthai_hd from tthai_tp a
    left join css.v_hd_khachhang@dataguard b on a.ma_gd = b.ma_gd
    left join css.v_hd_thuebao@dataguard c on b.hdkh_id =c.hdkh_id and a.thuebao_id = c.thuebao_id
    left join css_hcm.trangthai_Hd tt on c.tthd_id = tt.tthd_id;
   select* from fiber_Hh_t8;
   select* from ttkd_Bsc.nhanvien where ten_Nv like '%Th?y';
   create table nhanvien_202408_bk as select* from ttkd_Bsc.nhanvien where thang  = 202408;
   select a.*, b.hdkh_id 
   from magd a 
    left join css_hcm.hd_khachhang b on a.magd_chuaht = b.ma_Gd
    left join css_hcm.hd_thuebao c on b.hdkh_id = c.hdkh_id
    ;
    select* from tnv;
    ROLLBACK;
    COMMIT;
    update 
     ttkd_Bsc.nhanvien
    set thaydoi_vtcv = 1
    where THAYDOI_vTCV = 0 AND thang = 202408 and donvi = 'TTKD' AND MA_nV = 'VNP001724'; MA_nV IN (SELECT VNP001724 FROM TNV);
    WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                    )

        , kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
        , kq_ghtt as (select 
--                                             nvl(kmt  b.rkm_id, hddc.rkm_id) rkm_id
--                                            , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
--                                            , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
--                                            , a.phieutt_id, a.trangthai
                                             a.ma_gd--, a.ngay_hd, a.ngay_tt, ct.ngay_nganhang, a.soseri, a.seri
                                            ,hdtb.ma_Tb,  b.tien tien_thanhtoan,b.vat--,round(- months_between(nvl(kmtb.ngay_bddc, hddc.ngay_bddc),nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc))) sothang_dc
--                                            , kt.kenhthu  kenhthu   
--                                            , nh.ten_nh ten_nganhang
--                                            , ht.ht_tra  ten_ht_tra
                                     --       , kmtb.rkm_id a1, hddc.rkm_id a2
--                                           , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id, a.thungan_tt_id, a.kenhthu_id, a.ht_tra_id
                             from magd gd  join css_hcm.phieutt_hd a on gd.magd_chuaht = a.ma_Gd
                                                    join css_hcm.ct_phieutt b on a.phieutt_id = b.phieutt_id
                                                    left join hddc on b.hdtb_id = hddc.hdtb_id
                                                    join css_hcm.hd_thuebao hdtb on b.hdtb_id = hdtb.hdtb_id--and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id <> 7
                                                    join css_hcm.hd_khachhang hdkh on hdtb.hdkh_id = hdkh.hdkh_id
--                                                    join gh on hdtb.thuebao_id = gh.thuebao_id
--                                                    left join kmtb on b.hdtb_id = kmtb.hdtb_id
--                                                    left join ct on a.phieutt_id = ct.phieu_id
--                                                    left join css_hcm.kenhthu kt on kt.kenhthu_id = a.kenhthu_id
--                                                    left join css_hcm.nganhang nh on nh.nganhang_id = a.nganhang_id
--                                                    left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = a.ht_tra_id
--                                                    left join ct2 on a.phieutt_id = ct2.phieu_id
--                             where a.kenhthu_id not in (6) and a.trangthai = 1                                                                        
--                                            and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20240401 and 20240602
                                         --   and to_number(to_char(a.ngay_tt, 'yyyymmdd')) between 20230427 and 20230701                     ----change--2 thang-
                                      --      and gh.ma_tb in ('ketnoi_thoitrang')   ----loai taykhi can---
                                        --   and hdttdc.hdtb_id not in (11189895, 11110732)    ----loai taykhi can---
                                         --and hddc.rkm_id is not null
                        )
        select * from kq_ghtt a;
        where ngay_bd_moi is not null; --and thuebao_id in (9866366,9866341)
                                and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi_30day where rkm_id = a.rkm_id and thang >=202404)
                                and not exists (select rkm_id from ttkd_bsc.ct_bsc_tratruoc_moi where  rkm_id = a.rkm_id and thang >=202404)
                                and not exists (select 1 from ttkdhcm_ktnv.ghtt_giao_688 where a.rkm_id = rkm_id and thang_kt = a.thang_kt and tratruoc =1 and loaibo =0)
                                and not exists (select rkm_id from tmp3_30ngay where rkm_id = a.rkm_id) 
--                                and a.ma_gd = 'HCM-TT/02814791'
                 HCM-TT/02894707      ;
  WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
                                    )

        , kmtb as (select * from css_hcm.khuyenmai_dbtb 
                    where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc)
        , aaaa as (
            select distinct magd_chuaht
            from magd
        ),
final as (select a.magd_chuaht ma_Gd,c.ma_tb, c.hdtb_id,sum(d.tien) tien, sum(d.vat) vat,sum(d.tien) + sum(d.vat) tong_tien,round(- months_between(nvl(kmtb.ngay_bddc, hddc.ngay_bddc),nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc))) sothang_dc
from aaaa a 
    join css_hcm.hd_khachhang b on a.magd_chuaht = b.ma_Gd
    join css_hcm.hd_thuebao c on b.hdkh_id = c.hdkh_id
    join css_hcm.ct_phieutt d on c.hdtb_id = d.hdtb_id
    left join kmtb on c.hdtb_id = kmtb.hdtb_id
    left join hddc on c.hdtb_id = hddc.hdtb_id
--where b.ma_gd  ='HCM-TT/02894707'
group by a.magd_chuaht, c.hdtb_id,kmtb.ngay_bddc, hddc.ngay_bddc,kmtb.ngay_ktdc, hddc.ngay_ktdc, c.ma_Tb
order by a.magd_chuaht )
select* from final;
select* from css_hcm.hd_khachhang where ma_Gd ='HCM-LD/01561173';
select* from css_hcm.hd_thuebao where hdkh_id = 20685667;   
with hd as (
    select  b.ma_Tb, a.ma_Gd, c.ma_nv, c.ten_nv, d.tien+d.vat doanhthu_Vat
    from css_hcm.hd_khachhang  a
        left join css_hcm.hd_thuebao b on a.hdkh_id = b.hdkh_id
        left join admin_hcm.nhanvien  c on a.ctv_id = c.nhanvien_id
        left join css_hcm.phieutt_Hd  d on a.ma_Gd = d.ma_gd
    where a.loaihd_id = 1  --and a.ma_Gd = 'HCM-LD/01560971'
)
select a.*, d.tocdo, d.thuonghieu goi_cuoc, hd.*
from smart a left join css_hcm.db_thuebao b on a.mã_Thuê_bao = b.ma_Tb
    left join css_hcm.db_cntt c on b.thuebao_id = c.thuebao_id
    left join css_Hcm.tocdo_adsl d on c.tocdo_id = d.tocdo_id
    left join hd on b.ma_Tb = hd.ma_Tb
   ; where a.mã_Thuê_bao ='hcm_smartca_00105889';
   
select* from css.v_db_Thuebao@dataguard where ma_Tb = 'hcm_smartca_00105889';
select* from css_hcm.muccuoc_Tb where mucuoctb_id = 22504;
select* from css_Hcm.tocdo_adsl where tocdo_id = 17187;