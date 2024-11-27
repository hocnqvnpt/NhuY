create table tmp3_30ngay as;
select thuebao_id from donhang_30ngay   where thuebao_id not in (
select a.thuebao_id-- a.*, b.ngay_ktdc
from tmp3_30ngay a
    join giao_oneb b on a.thuebao_id = b.thuebao_id
    );
where thuebao_id not in (select thuebao_id from tmp3_30ngay);
rollback;
commit;
select* from ttkd_Bsc.nhanvien where thang = 202409 and ten_nv like '%H?ng';
insert into ttkd_bsc.ct_bsc_tratruoc_moi;
select* from ttkd_bsc.bangluong_kpi where thang = 202410;
select* from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202410 and thang_ktdc_Cu = 202410;
insert into ttkd_bsc.ct_bsc_tratruoc_moi

        
--insert into nhuy.ct_bsc_tratruoc_moi_30day--create table t4 as
    (THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO
        , MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU
        , MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT,  SOSERI, SERI, KENHTHU, TEN_NGANHANG
        , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
        )
    with t0 as (select 202409 thang_kt, c0.thuebao_id, c0.phieutt_id, 'matt' ma_tt, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi, c0.tien_thanhtoan, c0.vat
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
            , ds as (select ten_pbh, thuebao_id,202409 thang_kt
                    from giao_oneb where thang_kt = 202409
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
    select 202410 thang, 0, 0 pbh_ql_id, 0 pbh_giao_id, 0 tbh_giao_id
                , 0 pbh_id_th, 0  pbh_cn_id
                , dbtb.ma_tb, null manv_cs,a.ten_pbh phong_cs
                 , null ma_to
                 ,null ma_pb
                 ,null manv_giao, a.ten_pbh PHONG_GIAO, null ma_nv_th, a.ten_pbh PHONG_TH
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
                                when c.ht_tra_id in (1, 7,204,216) then 1
                                when c.ht_tra_id in (2, 4,5,207,214) then 0 else null end tien_khop
                ,null  ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                            
                                left join c 
                                    on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202410 -- change
                                left join goi on a.thuebao_id = goi.thuebao_id and rn = 1
                        
    where  a.thang_kt = 202409 --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
--                            and dbkh.khachhang_id  <> 9798610          ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
                  --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb in ('hcmnhp183')
                  -- and a.thuebao_id in (1307757,
               -- and not exists (select 1 from ttkd_bsc.ct_bsc_tratruoc_moi_30day where thang = 202308 and c.rkm_id = rkm_id )
;
select count(1) from ds_giahan_Tratruoc_t9;
commit;                                     
select distinct diachi from userld_202409_goc;
select count(distinct rkm_id) from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day where thang = 202409; and tien_khop is null and ht_Tra_id is not null;
update ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day set tien_khop = 1 where  thang = 202409 and tien_khop is null and ht_Tra_id is not null;
select* from giao_oneb where thang_kt = 202410;
insert into ttkd_Bsc.tl_giahan_Tratruoc (THANG, MA_PB,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE);
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,'HCM_TB_GIAHA_023', 'KPI_PB', phong_giao
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi a
                                        where thang = 202410
                                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao
                order by 2
                ) a
;
commit;
SELECT* FROM TTKD_BSC.NHANVIEN WHERE THANG = 202410 AND TEN_pb='Phòng Nghi?p V? C??c';
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where manv_thuyetphuc= 'daily_bocau1';
select* from admin_hcm.nhanvien where ma_Nv='daily_bocau1';
select* from tmp3_ob ;