drop table tmp3_ob;
create table tmp3_ob as
--commit;
--insert into tmp3_ob;
WITH db as 
(
    select a.thuebao_id, a.loaitb_id, ma_tb
    from css_hcm.db_Thuebao  a
--        join css_hcm.db_adsl  b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9)  and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202408
) 
,km as (select rkm_id, thuebao_id, ma_tb, loaitb_id, ngay_bddc, ngay_ktdc, ngay_kt, ngay_huy, ngay_thoai
                            , thang_bddc, thang_ktdc, thang_kt_mg, thang_huy, thang_kt_dc                                                            
                            , case when ngay_huy is not null then ngay_huy
                                        when ngay_huy is null and thang_huy > 0 then last_day(to_date(thang_huy, 'yyyymm'))+1
                          when ngay_huy is null and thang_huy = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_huy_x
                            , case when ngay_thoai is not null then ngay_thoai
                                        when ngay_thoai is null and thang_kt_dc > 0 then last_day(to_date(thang_kt_dc, 'yyyymm'))+1
                          when ngay_huy is null and thang_kt_dc = 0 then to_date('01/01/1999', 'dd/mm/yyyy')
                                        else null end ngay_thoai_x
                            , tien_td, cuoc_dc, tyle_sd, tyle_tb, thangdc so_thangdc, thangkm so_thangkm, congvan_id, khuyenmai_id, chitietkm_id, nhom_datcoc_id
                from v_thongtinkm_all
                where  thang_ktdc > 202301 and cuoc_dc > 0 --and  thuebao_id = 11736290
            )
, km1 as (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                , to_date(thang_bd_mg, 'yyyymm') ngay_bd_mg
                                , last_day(to_date(least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm'))  ngay_kt_mg
                                , last_day(to_date(thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai
                       from v_thongtinkm_all
                       where thang_bd_mg <= least(thang_kt_mg, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) and (tyle_sd = 100 or tyle_tb = 100) 
                       and cuoc_dc = 0 and thang_bd_mg > 202201 
                )
, dc as (
        ----------------TT Thang tang tren 1 dong-------------
             select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id
                            , km.ngay_bddc, least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , least(km.ngay_kt, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x ngay_thoai
                            , km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km 
            where (km.tyle_sd = 100 or km.tyle_tb = 100)
                            and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR))  >= ngay_bddc
                            and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)
                            
                        --    and km.thuebao_id = 9085442
           union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, km.ngay_bddc,  least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) ngay_ktdc
                            , case when km1.ngay_kt_mg is not null then km1.ngay_kt_mg else least(ngay_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) end ngay_kt_mg
                            , km.ngay_huy_x ngay_huy, km.ngay_thoai_x NGAY_THOAI, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km left join km1 on km1.thuebao_id = km.thuebao_id and km.ngay_ktdc + 1 =  km1.ngay_bd_mg
            where (km.tyle_sd + km.tyle_tb < 100)
                    and least(NGAY_ktdc, nvl(ngay_thoai_x -1, sysdate +1 + INTERVAL '50' YEAR), nvl(ngay_huy_x -1, sysdate +1 + INTERVAL '50' YEAR)) >= ngay_bddc
                    and km.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                         --   and km.thuebao_id = 8131788
            
             union all
        ----------------TT giam cuoc or thang tang tren 2 dong-------------
            select km.rkm_id, km.thuebao_id, km.ma_tb, km.loaitb_id, trunc(to_date(km.thang_bddc, 'yyyymm')) ngay_bddc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_ktdc
                            , last_day(to_date(least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)), 'yyyymm')) ngay_kt_mg
                            , last_day(to_date(km.thang_huy, 'yyyymm'))+1 ngay_huy, last_day(to_date(km.thang_kt_dc, 'yyyymm'))+ 1 ngay_thoai, km.tien_td, km.cuoc_dc
                            , km.so_thangdc, km.so_thangkm, km.congvan_id, km.khuyenmai_id, km.chitietkm_id, nhom_datcoc_id--, km.hdtb_id
            from km
            where (km.tyle_sd + km.tyle_tb < 100)
                            and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
                            and km.loaitb_id not  in (18, 58, 59, 61, 171, 210, 222, 224)                                                                                                
                          --  and km.thuebao_id = 9085442
    ) --select* from dc ; ma_Tb = 'nhakhoaphuchau';
, tt1 as (
    select thuebao_id, rkm_id ,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202409 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
) 
, tt2 as 
(
    select thuebao_id, rkm_id,ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202408 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
)
, tt3 as (
    select thuebao_id, rkm_id,  ROW_NUMBER() OVER (PARTITION BY dc.thuebao_id ORDER BY dc.rkm_id DESC) AS rn
    from dc 
    where 202410 between to_number(to_char(ngay_bddc, 'yyyymm')) and to_number(to_char(ngay_kt_mg,'yyyymm'))
) 
,tt as (
    select  db.thuebao_id,ma_Tb
    from db
        join tt1  on db.thuebao_id = tt1.thuebao_id and tt1.rn = 1
        join tt2 on db.thuebao_id = tt2.thuebao_id and tt2.rn = 1
        join tt3 on db.thuebao_id = tt3.thuebao_id and tt3.rn = 1
) --select a.* from tmp3_ob a join tt on a.thuebao_id = tt.thuebao_id;

--select* from tt where ma_Tb = 'hcm2nhvu_2021';
, 

 hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc , h.thang_bd
                                            from css_hcm.hdtb_datcoc  g left join css_hcm.db_datcoc  h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where thuebao_id = 9731692
 ) 
, kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb 
                            where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                            and thang_bddc > 202307
) 
--, ct as (select min(aa.NGAY_NGANHANG) NGAY_NGANHANG, bb.phieu_id
--            from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
--           join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
--        group by bb.phieu_id) 
    , kq_ghtt as (select 
                hdkh.khachhang_id, hdtb.thuebao_id, hdtb.ma_tb, hdtb.kieuld_id,hdtb.loaitb_id, nvl(kmtb.rkm_id, hddc.rkm_id) rkm_id
                , to_number(to_char(nvl(kmtb.ngay_bddc, hddc.ngay_bddc), 'yyyymmdd')) ngay_bd_moi
                , to_number(to_char(nvl(kmtb.ngay_ktdc, hddc.ngay_ktdc), 'yyyymmdd')) ngay_kt_moi
                , p.phieutt_id, p.trangthai
                , p.ma_gd, p.ngay_hd, p.ngay_tt, p.soseri, p.seri,a.tien tien_hopdong, a.vat vat_hopdong,  b.tien tien_thanhtoan,b.vat vat_thanhtoan
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 888 lan, hdtb.ngay_ht
                     from  css_hcm.hd_khachhang  hdkh
													join css_hcm.hd_thuebao  hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 6
													join tt  on hdtb.thuebao_id = tt.thuebao_id-- and gh.thang >= 202403
													join css_hcm.ct_tienhd  a on hdtb.hdtb_id = a.hdtb_id
													   join css_hcm.ct_phieutt  b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0
													   join css_hcm.phieutt_hd  p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
													   left join hddc on hdtb.hdtb_id = hddc.hdtb_id
													   left join kmtb on hdtb.hdtb_id = kmtb.hdtb_id
													   left join css_Hcm.kenhthu  kt on kt.kenhthu_id = p.kenhthu_id
													   left join css_hcm.nganhang  nh on nh.nganhang_id = p.nganhang_id
													   left join css_hcm.hinhthuc_tra  ht on ht.ht_tra_id = p.ht_tra_id
--													   left join ct on p.phieutt_id = ct.phieu_id
				    
                     where  a.khoanmuctt_id = 11 and 
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224,136) and a.tien > 0 
                                      and ( ( to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20241101 and 20241131)  --    and p.ma_Gd = 'HCM-TT/02814791'            
                                      or ( to_number(to_char(hdtb.ngay_ht, 'yyyymmdd')) between 20241101 and 20241131) and p.ngay_tt is null ) --    and p.ma_Gd = 'HCM-TT/02814791'            
)  
select *
from kq_ghtt a where
ngay_bd_moi is not null ;and not exists (select 1 from tmp3_ob where rkm_id = a.rkm_id);
 and not exists (select 1 from donhang where rkm_id = a.rkm_id)
;
-- kiem tra
select rkm_id from tmp3_ob group by rkm_id having count(rkm_id)>1;

---- BANG OUTBOUND
Drop table tmp_ipcc;
create table tmp_ipcc as  select a.ob_id, ngay_tao, a.thuebao_id, ma_tb, LOAITB_ID, TOCDO_ID, a.KHACHHANG_ID, THANHTOAN_ID
									, NGAY_BDDC, NGAY_KTDC, CUOC_DC, SO_THANGDC, CHITIETKM_ID, SL_DATCOC, MATB_PHU, TRANGTHAI_OB
									, to_date(a.TD_TH, 'dd/mm/yyyy') td_th, TD_BD, TD_KT, TG_DC, TG_HD, TG_GM, TG_DT, a.ma_nd
									, row_number() over (partition by thuebao_id, ma_nd order by ob_id desc) rnk
								from dhsx.v_ghtt_ipcc@dataguard a
								where a.trangthai_ob = 'SUCCESS' and a.tg_dt > 0 and 
                                to_char(to_date(a.TD_TH,'dd/mm/yyyy'),'yyyymm') in ('202409','202410','202411');
                            
--- THEM VAO BANG CHI TIET
insert into ttkd_bsc.nhuy_ct_ipcc_obghtt
		with nv_ipcc as (select a.nguoidung_id, a.user_ipcc, b.nhanvien_id, c.ma_nv
										from admin.nguoidung_ipcc@dataguard a
													join admin.v_nguoidung@dataguard b on a.nguoidung_id = b.nguoidung_id
													join admin.v_nhanvien@dataguard c on b.nhanvien_id = c.nhanvien_id
								)
        select 202411 thang, a.ob_id, ngay_tao, a.thuebao_id, ma_tb, LOAITB_ID, TOCDO_ID, a.KHACHHANG_ID, THANHTOAN_ID
                    , NGAY_BDDC, NGAY_KTDC, CUOC_DC, SO_THANGDC, CHITIETKM_ID, SL_DATCOC,SUBSTR (MATB_PHU, 0, 500)MATB_PHU  , TRANGTHAI_OB
                    , TD_TH, TD_BD, TD_KT, TG_DC, TG_HD, TG_GM, TG_DT
                    --, HDKH_ID, TT_KETNOI, KQ_OB, NGAY_HEN, GHI_CHU, NHANVIEN_ID
                        , a.ma_nd, c.nhanvien_id, c.ma_nv,null nhomtb_id
        from tmp_ipcc a
                --	left join css.v_ghtt_ob@dataguard b on a.ob_id = b.ob_id
                        left join nv_ipcc c on a.ma_nd = c.user_ipcc
--							left join dadv d on d.thuebao_id = a.thuebao_id and d.rnk = 1
        where a.rnk = 1
; --TG_DT, TD_TH
commit;
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_nv='CTV086150'; 
delete from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202411;
insert into ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt (THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, NGAY_BDDC_CU, NGAY_KTDC_CU, CUOC_DC_CU, SO_THANGDC_CU, MA_GD, RKM_ID,
NGAY_BD, NGAY_KT, TIEN_HOPDONG, TIEN_THANHTOAN, VAT_thanhtoan, NGAY_TT, NGAY_HD, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TD_TH, MA_ND_OB, 
NHANVIEN_ID_ob, MANV_OB, MA_TO, MA_PB,  goi_old_id, HDTB_ID, HDKH_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT,
MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_YC, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NHOMTB_ID, KHACHHANG_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, LOAITB_ID, THANHTOAN_ID,
NHANVIEN_XUATHD, TIEN_KHOP, MA_CHUNGTU)
with t0 as (select  c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi,c0.ngay_kt_moi, c0.tien_hopdong,c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                , c0.ngay_tt, c0.ngay_hd, null ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra, c0.ngay_yc
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, 
                                nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id,c0.thungan_hd_id
               from tmp3_ob c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                       where lan  = 308
                        )
                 , km0 as (     
                                ----------------TT Thang tang tren 1 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, km.thang_kt_mg, km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc
                                            , km.tien_td, km.cuoc_dc, round(km.cuoc_dc/km.thangdc + km.thangkm, 0) avg_thang
                                            , km.thangdc + km.thangkm so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km 
                            where (km.tyle_sd = 100 or km.tyle_tb = 100) and cuoc_dc > 0 and km.thangdc > 0
                                            --and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))  ---cong 2 thang
                                            and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                           union all
----------------TT giam cuoc or thang tang tren 2 dong-------------
                            select km.rkm_id, km.thuebao_id, km.loaitb_id, km.thang_bddc, km.thang_ktdc, case when km1.thang_kt_mg is not null then km1.thang_kt_mg else km.thang_ktdc end thang_kt_mg
                                            , km.hieuluc, km.ttdc_id, km.thang_huy, km.thang_kt_dc, km.tien_td, km.cuoc_dc, round(km.cuoc_dc/(km.thangdc + nvl(km1.thangkm, 0)), 0) avg_thang
                                            , km.thangdc + nvl(km1.thangkm, 0) so_thangdc, km.khuyenmai_id
                            from v_thongtinkm_all km left join (select thuebao_id, thang_bd_mg, thang_kt_mg, rkm_id, thangkm
                                            from v_thongtinkm_all where hieuluc = 1 and ttdc_id = 0 and tyle_sd = 100
                                        ) km1 on km1.thuebao_id = km.thuebao_id and km.thang_ktdc + 1 =  km1.thang_bd_mg
                            where (km.tyle_sd + km.tyle_tb < 100) and cuoc_dc > 0 and km.thangdc > 0
                                           -- and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= to_number(to_char(add_months(to_date(decode(thang_bddc, 0, 210001, thang_bddc), 'yyyymm'),0),'yyyymm'))
                                           and least(NGAY_ktdc, nvl(NGAY_THOAI -1, sysdate + INTERVAL '50' YEAR), nvl(NGAY_huy -1, sysdate + INTERVAL '50' YEAR)) >= ngay_bddc + 90
                     )
                , ds as (select thang,ob_id, ngay_tao ngay_ob, thuebao_id, ma_Tb, ngay_bddc, ngay_ktdc ngay_kt_dc, td_th, ma_nd, nhanvien_id,ma_Nv,so_thangDc,cuoc_dc,
                        to_number(to_char(ngay_ktdc,'yyyymm')) thang_kt,nhomtb_id, thanhtoan_id, khachhang_id, row_number() over (partition by thuebao_id, nhanvien_id order by ob_id desc) rnk
                        from ttkd_bsc.nhuy_ct_ipcc_obghtt
                        where thang = 202411
                        )

            , c as (select  t0.thuebao_id, t0.phieutt_id,  t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.ngay_kt_moi,t0.tien_hopdong,t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang,t0.loaitb_id
                                ,t0.thungan_Hd_id, t0.ngay_yc
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) --select* from c;
        , goi_cu as (select nhomtb_id, thuebao_id, row_number() over (partition by thuebao_id order by nhomtb_id desc) rnk
                  from tinhcuoc.v_sd_goi_dadv@dataguard 
                  where trangthai = 1 and KYHOADON = 20240801
        )
        ,goi_moi as (select nhomtb_id, thuebao_id , row_Number() over (partition by thuebao_id order by nhomtb_id desc) rnk 
                            from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
    select 
            202411 thang, a.ob_id, a.ngay_ob, x.thuebao_id, dbtb.ma_tb,a.ngay_bddc ngay_bddc_cu, 
            a.ngay_kt_dc ngay_ktdc_cu,a.cuoc_dc cuoc_dc_cu, a.so_thangdc so_thangdc_cu, x.ma_gd, x.rkm_id , x.ngay_bd_moi, x.ngay_kt_moi,x.tien_hopdong, x.tien_thanhtoan, 
            x.vat, x.ngay_tt, x.ngay_hd, x.ngay_Nganhang,x.soseri, x.seri, x.kenhthu, x.ten_nganhang, x.ten_ht_Tra
            ,a.td_th, a.ma_nd ma_nd_ob, a.nhanvien_id nhanvien_ob_id,a.ma_nv manv_ob,nvtp.ma_to,nvtp.ma_pb,   g.nhomtb_id nhomtb_id_cu ,
            x.hdtb_id, x.hdkh_id
           , x.NVTUVAN_ID, x.NVTHU_ID, x.THUNGAN_TT_ID, x.MANV_CN, x.PHONG_CN, x.MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, x.SO_THANGDC so_thangdc_moi, x.AVG_THANG,  x.ngay_yc
           , x.NVGIAOPHIEU_ID, x.DVGIAOPHIEU_ID ,   m.nhomtb_id , dbtb.khachhang_id, x.phieutt_id, x.ht_tra_id, x.kenhthu_id, x.loaitb_id, dbtb.thanhtoan_id, nvxp.ma_nv nhanvien_xuathd
                , case when x.rkm_id is null then null
                                when x.ma_gd in (select ma_gd from qrcode where thang = 202411) then 2
                                when x.ht_Tra_id in (216) then 2
                                when x.ht_tra_id in (2,4,5,214) then 0 else 4 end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = x.PHIEUTT_ID) ma_chungtu
                
    from c x
                                left join css_hcm.db_thuebao dbtb
                                    on x.thuebao_id = dbtb.thuebao_id
                                left join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                left join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
--                                join css_hcm.trangthai_tb tt
--                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
--                                left join ttkd_bsc.dm_phongban pb
--                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
--                                left join  ttkd_bct.phongbanhang pb_giao
--                                    on a.donvi_giao = pb_giao.pbh_id
--                                left join  ttkd_bct.phongbanhang pb_th
--                                    on a.pbh_id_th = pb_th.pbh_id
                                left join  ds a
                                    on a.thuebao_id = x.thuebao_id and a.nhanvien_id = x.nvtuvan_id and rnk = 1-- and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien nvtp on x.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202411 and nvtp.donvi = 'TTKD'
                                left join ttkd_bsc.nhanvien nvob on a.ma_nv = nvob.ma_nv and nvob.thang = 202411 and nvob.donvi = 'TTKD'
                                left join goi_cu g on x.thuebao_id = g.thuebao_id and g.rnk =1
                                left join goi_moi m on x.thuebao_id = m.thuebao_id and m.rnk = 1
                                left join admin_hcm.nhanvien_onebss xp on x.thungan_tt_id = xp.nhanvien_id
                                left join ttkd_bsc.nhanvien nvxp on xp.ma_nv = nvxp.ma_nv and nvxp.thang = 202411 and nvxp.donvi = 'TTKD'
where not exists (select rkm_id from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where  nvl(tien_khop,0) > 0 and thang = 202410 and rkm_id = x.rkm_id )
                                ;

commit;
-- phi thu tai nha
insert into phithu_tainha(LOAIDV, PHIEU_ID, MA_TT, MA_TB, DICHVUVT_ID, GHICHU, NGAY_CN, CHUKYNO, MANV_TC, TENNV_TC, MA_TN, TEN_TN, MA_ND, TEN_ND, TENTO, TEN_DV, TRAGOC, 
--TRATHUE, TONGTRA, NGAY_LAYSL, THANG_TAMTHU, TENDV_CS, MANV_CS, TENNV_CS, MANV_TCTN, TENNV_TCTN, THANG)
TRATHUE, TONGTRA, NGAY_LAYSL, THANG_TAMTHU, THANG)
--update phithu_tainha set thang = 202404;
--select* from phithu_tainha;
--alter table phithu_tainha add(thang number(6));
select decode(b.dichvuvt_id,2,'VNP','CDH')loaidv, a.phieu_id, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ghichu, to_char(TRUNC(a.ngay_cn),'dd/mm/yyyy') ngay_cn, b.chukyno
  , a.manv_tc, j.ten_nv tennv_tc
  , a.ma_tn, d.ten_nv ten_tn
  , e.ma_nd, e.ten_nd
  , c.ten_dv tento, h.ten_dv
  , sum(tragoc)tragoc, sum(trathue)trathue, sum(tragoc+trathue)tongtra
  , TO_CHAR(sysdate, 'DD-MM-YYYY HH:MI:SS') as ngay_laysl, to_number(to_char(sysdate,'yyyymm')) thang_tamthu
--  , k.tendv_cs, k.manv_cs, k.tennv_cs, k.manv_tctn, k.tennv_tctn
  , 202411 thang
from qltn.v_bangphieutra@dataguard a, qltn.v_ct_tra@dataguard b
    ,admin_hcm.donvi c
    ,admin_hcm.nhanvien d
    ,admin_hcm.nguoidung e
    ,qltn_hcm.hinhthuc_tt f
    ,admin_hcm.donvi h
    ,admin_hcm.nhanvien j
    ,qlc_hcm.cs_online k
where a.phieu_id=b.phieu_id and a.phanvung_id=28 and a.phanvung_id=b.phanvung_id
    and a.ky_cuoc=20241001 and a.ky_cuoc=b.ky_cuoc
    and (b.tragoc+b.trathue-b.chihoahong)<>0
    and a.quaythu_id=c.donvi_id(+)
    and a.ma_tn=d.ma_nv(+)
    and a.nguoidung_id=e.nguoidung_id(+)
    and a.httt_id=f.httt_id(+)
    and c.donvi_cha_id=h.donvi_id(+)
    and a.manv_tc=j.ma_nv(+)
    and b.khoanmuctt_id=3130 and a.httt_id=127
    and (b.tragoc+b.trathue) in (20000,22000,24000)
    and a.ghichu not like '%KHYCTL%'
    and NOT (b.tragoc<>8000 and b.dichvuvt_id not in (4,11))
    and a.ma_tt=k.ma_tt(+) and b.ma_tb=k.ma_tb(+) and b.dichvuvt_id=k.dichvuvt_id(+)
group by decode(b.dichvuvt_id,2,'VNP','CDH'), a.phieu_id, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ghichu, a.ngay_cn, b.chukyno
  , a.manv_tc, j.ten_nv, a.ma_tn, d.ten_nv, e.ma_nd, e.ten_nd, c.ten_dv, h.ten_dv
--  , k.tendv_cs, k.manv_cs, k.tennv_cs, k.manv_tctn, k.tennv_tctn
order by a.ma_tt,a.ngay_cn;
-- update thue bao da dong tien thu tai nha
update ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt set tien_khop = 3
where thang = 202411 and ma_Tb in (select ma_Tb from phithu_tainha where thang = 202411);

commit;
--update chung tu
update ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a set tien_khop = 1
-- select* from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct, bb.ngay_Ht
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- phieu con co tien bang
update ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a set tien_khop = 1
-- select* from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
    commit;
-- bang cha du tien
update ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a set tien_khop = 1
-- select* from ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--        and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;
select* from  ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt  where thang= 202411;
update ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt set tien_khop = 1
where ma_gd in (select ma from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1) and tien_khop = 0
and thang= 202411;
delete from  ttkd_Bsc.tl_giahan_tratruoc  where thang= 202411 and ma_kpi = 'HCM_SL_ORDER_001';
--- insert vao bang ty le
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DA_GIAHAN_DUNG_HEN, DTHU_THANHCONG_DUNG_HEN)
with a as
(

             select     
             a.thang, a.thuebao_id, a.ma_to, a.ma_pb, ma_Tb
                                        ,a.MANV_THUYETPHUC ma_nv, goi_old_id, nhomtb_id
                                          ,sum( cuoc_dc_cu) tien_Dc_Cu
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) >= 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----   ----Fiber tinh he so 1 (neu khong duy tri goi dadv -0.5), MyTV tinh he so 0.5 (neu co Fiber cung ky 0.4), Mesh tinh he so 0.5 (neu co Fiber cung ky 0.2)                                  
                           , case 
                                    when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                    , 0)

                                    ----Dich vu Mesh he so 0.5 (neu co Fiber cung ky 0.2) 
                                    when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                where xu.loaitb_id in (58, 59)
                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                    ---MyTV he so 0.5 (neu co Fiber cung ky 0.4)
                                    when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                else 0 
                               end Heso_dichvu
                            ,  sum(tien_thanhtoan) DTHU--, max(ngay_tt) ngay_tt, a.nhomtb_id, max(ngay_yc)
                            , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a 
--                              
                        where a.rkm_id is not null and thang = 202411 --and c.ma_pb = 'VNP0701400'--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, loaitb_id, ma_tb
--                   
        ) 
 select thang,
                         'KPI_NV' loai_tinh,
                         'HCM_SL_ORDER_001' ma_kpi,
                         a.ma_nv, a.ma_to, a.ma_pb,
                         round(sum(case when dthu > 0 and tien_khop > 0 then heso_chuky*heso_dichvu else 0 end)) da_giahan
--                        ,   round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                      , sum(dthu) DTHU_thanhcong    
--      select *
        from  a 
        where rnk = 1 and dthu > 0 
        group by a.thang, a.ma_nv, a.ma_to, a.ma_pb;
commit;
---- do bang luong
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_NV'
and ma_nv = a.ma_nv
and ma_kpi = 'HCM_SL_ORDER_001')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi in ('HCM_SL_ORDER_001')
    and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_SL_ORDER_001';
commit;
select* from TTKD_BSC.bangluong_kpi a where thang = 202411 and ma_kpi = 'HCM_SL_ORDER_001'