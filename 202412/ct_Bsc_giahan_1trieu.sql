select a.*, b.ten_pb from giao_khoi_bh a
    left join ttkd_Bsc.nhanvien b on a.ma_nv_am =b.ma_nv and b.thang = 202412;
    select* from ttkd_bct.db_Thuebao_ttkd_202411 where ma_Tb= 'metev';
    delete from giao_khoi_bh where ma_Tb is null;
    commit;
insert into ttkd_bsc.ct_Bsc_tratruoc_moi_1tr

-- insert into nhuy.ct_bsc_tratruoc_moi_30day-create table t4 as
    (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT,  SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        )
    with t0 as (select 202411 thang_kt, c0.thuebao_id, c0.phieutt_id, 'matt' ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
                                , c0.ngay_tt, c0.ngay_hd, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, null nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, null manv_gt, nv3.ma_nv manv_thungan
               from tmp3_60ngay c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
--                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
--                    where lan = 11
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
            , ds as (select b.ten_pb ten_pbh, b.ma_nv, b.ma_to, b.ma_pb, thuebao_id,202411 thang_kt
                    from giao_khoi_Bh a
                        left join ttkd_Bsc.nhanvien b on a.ma_nv_am =b.ma_nv and b.thang = 202412
                    )
            , c as (select t0.thang_kt, t0.thuebao_id, t0.phieutt_id, t0.ma_tt, t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, null ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) 
                , goi as (select thuebao_id, nhomtb_id , row_number() over (partition by thuebao_id order by nhomtb_id desc) rn
                from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                    )
    select a.thang_kt thang, 0, 0 pbh_ql_id, 0 pbh_giao_id, 0 tbh_giao_id
                , 0 pbh_id_th, 0  pbh_cn_id
                , dbtb.ma_tb, null manv_cs,a.ten_pbh phong_cs
                 , a.ma_to
                 ,a.ma_pb
                 ,a.ma_nv manv_giao, a.ten_pbh PHONG_GIAO, null ma_nv_th, a.ten_pbh PHONG_TH
                 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                 , nvtp.ma_pb MAPB_THPHUC
                , c.manv_gt, c.manv_thungan
                 , lkh.khdn, null heso
                , a.THANG_KT, 0 TIEN_DC_CU
                , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT
                                    , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                , tt.trangthai_tb, a.thuebao_id, dbtb.loaitb_id, null pbh_oa_id, null manv_oa
                 , 
                 goi.nhomtb_id
                , dbkh.khachhang_id, null goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                
                , case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204,214,216) then 1
                                when c.ht_tra_id in (2, 4,5,207) then 0 else null end tien_khop
                ,null ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                left join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id
                                left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202412 -- change
                                left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
                        
    where  a.thang_kt = 202411 --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
                             ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
           ;       --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb 
                 
UPDATE TTKD_bSC.ct_Bsc_Tratruoc_moi_1tr set tien_khop = 1 WHERE THANG = 202411 AND tien_khop = 0 and ma_Gd in (
select ma from ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_Hd_1);
commit;
                 
UPDATE TTKD_bSC.ct_Bsc_Tratruoc_moi_1tr set thang = 202412;
select* from TTKD_bSC.ct_Bsc_Tratruoc_moi_1tr;


 insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
                            select thang, 'KPI_NV' LOAI_TINH
                                         , 'HCM_TB_GIAHA_031' ma_kpi
                                         , a.ma_nv, a.ma_to, a.ma_pb
                                           , count(thuebao_id) tong
                                          , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                                          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                                          , sum(dthu) DTHU_thanhcong
                                          , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
                                          
                            from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                                                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                                                                , sum(a.tien_khop), heso_giao
                                                                from ttkd_bsc.ct_bsc_tratruoc_moi_1tr a
                                                                where a.thang = 202412                    ------------n------------
                                                                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, heso_giao
                                                ) a 
                          where ma_pb is not null 
                          group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
                            order by 2
; 
commit;
 insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
                            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                                , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                                                , sum(heso_giao) heso
                            from ttkd_bsc.tl_giahan_tratruoc
                            where thang = 202412 and MA_KPI in ('HCM_TB_GIAHA_031')
                            group by THANG, MA_KPI, MA_TO, MA_PB
            ;
--- PGD BHKV
 insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                        , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao);
with pb as (
    select  ma_Nv, ma_pb
    from  ttkd_bsc.blkpi_dm_to_pgd where thang = 202412 AND MA_KPI = 'HCM_TB_GIAHA_031' and phutrach='Phong'
)
, t_to as (
    select  ma_Nv, ma_pb, ma_to
    from  ttkd_bsc.blkpi_dm_to_pgd where thang = 202412 AND MA_KPI = 'HCM_TB_GIAHA_031' and phutrach='To'
)
select a.THANG, 'KPI_PB', a.MA_KPI, nvl(b.ma_nv, c.ma_nv) ma_Nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a 
    left join pb b on a.ma_pb = b.ma_pb 
    left join t_to c on a.ma_to = c.ma_to and a.ma_pb = c.ma_pb
where a.thang = 202412 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_031') 
group by a.THANG, a.MA_KPI, c.ma_nv, a.MA_PB, b.ma_nv
;
---
 insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                        , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
with t_to as (
    select  ma_Nv, ma_pb, ma_to
    from  ttkd_bsc.blkpi_dm_to_pgd where thang = 202412 AND MA_KPI = 'HCM_TB_GIAHA_030' and phutrach='To'
)
select a.THANG, 'KPI_PB', 'HCM_TB_GIAHA_030' MA_KPI, c.ma_Nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a 
    left join t_to c on a.ma_to = c.ma_to and a.ma_pb = c.ma_pb
where a.thang = 202412 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_031') 
group by a.THANG, a.MA_KPI, c.ma_nv, a.MA_PB
;
---  do bang luong 031
update TTKD_BSC.bangluong_kpi a 
set 
 giao =  (select (sum(tong))
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
  , thuchien = (select (sum(DA_GIAHAN_DUNG_HEN)) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
where thang = 202412 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_031') and thang = 202412 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_031' ;
commit;
--- to truong
update TTKD_BSC.bangluong_kpi a 
set
giao =  (select (sum(tong)) from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_031')
,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN))  from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_031')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_031')
where thang = 202412 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_031') 
                                and thang = 202412 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_031';
--- pho giam doc

update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
        ,thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
        , giao = (select tong from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_031')
where thang = 202412 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_031')  
                                            and thang = 202412 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_031';
commit;
--- 
update ttkd_Bsc.bangluong_kpi a
set diem_cong = case when tyle_thuchien >= 90 then 5 else 0 end
where  thang = 202412 and  exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where  ma_kpi in ('HCM_TB_GIAHA_031')  
                                and thang = 202412 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_031';

update ttkd_Bsc.bangluong_kpi a
set diem_tru = case when tyle_thuchien < 40 then 5 else 0 end
where  thang = 202412 and  exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_031')  
                                and thang = 202412 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_031';

--- do bang luong 030
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_030')
        ,thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_030')
        , giao = (select tong from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_030')
where thang = 202412 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_030')  
                                            and thang = 202412 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_030';
                                        
update ttkd_Bsc.bangluong_kpi a
set mucdo_hoanthanh = case when tyle_Thuchien >= 90 then 120 
                            when tyle_thuchien >= 70 and tyle_thuchien < 90 then 100 + 0.8*(tyle_thuchien-70)
                            when tyle_thuchien >= 65 and tyle_thuchien < 70 then 80 + 3.8*(tyle_thuchien-65)
                            when tyle_thuchien >= 60 and tyle_thuchien < 65 then 50 + 5.8*(tyle_thuchien-60)
                            when tyle_thuchien >= 40 and tyle_thuchien < 60 then 35 + 0.75*(tyle_thuchien-40)
                            when tyle_thuchien < 40 then 0 end
where thang = 202412 and ma_Kpi = 'HCM_TB_GIAHA_030'

;
commit;
select* from ttkd_Bsc.bangluong_kpi a where thang = 202412 and ma_Kpi = 'HCM_TB_GIAHA_030'
