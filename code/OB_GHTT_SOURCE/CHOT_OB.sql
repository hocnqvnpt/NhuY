drop table tmp_ob ;
commit;
select* from tmp_ob ;
---- TAO BANG CHOT
create table tmp_ob as ;
WITH hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
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
                            join  ttkd_bsc.nhuy_ct_ipcc_obghtt gh on hdtb.thuebao_id = gh.thuebao_id and thang = 202404
                            left join ct on p.phieutt_id = ct.phieu_id
    
                     where   to_number(to_char(hdkh.ngay_yc,'yyyymm')) = 202404  and a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                          and ((p.ht_tra_id <> 2 and to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20240401 and 20240502)                     ----change--2 thang- ngay 02
                                                        or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymmdd')) between 20240401 and 20240502)) 
                                    ) 
select *
from kq_ghtt a
where ngay_bd_moi is not null and not exists (select 1 from tmp_ob where rkm_id = a.rkm_id); 
select* from css_hcm.ct_phieutt where phieutt_id = 8281378;
select* from  css_hcm.ct_tienhd where  phieutt_id = 8197009;
select* from css_hcm.phieutt_hd where phieutt_id in (8281378,8197009)
select* from ttkd_bsc.blkpi_dm_to_pgd where thang = 202402 --and ma_To = 'aaa' and ma_pb = 'bbb'
select* from hocnq_ttkd.ct_ipc_tmp where thuebao_id = 8132201 and nhanvien_id = 91209-- where thuebao_id = 1975740; --23950096
select* from ttkd_Bsc.nhuy_ct_ipcc_obghtt-- where thang = 202402;
select* from css_hcm.khuyenmai_Dbtb where hdtB_id = 23950882
select* from css_hcm.hd_khachhang where hdkh_id = 21671642;
select* from v_Thongtinkm_all where rkm_id =6771202;
select b.OB_ID,b.NGAY_TAO ngay_OB,b.THUEBAO_ID, b.MA_TB, b.LOAITB_ID,b.KHACHHANG_ID, b.THANHTOAN_ID,b.NGAY_BDDC, b.NGAY_KTDC, b.CUOC_DC, b.SO_THANGDC, b.CHITIETKM_ID, b.ma_nd ma_nd_ob,
b.NHANVIEN_ID nhanvien_id_ob, b.MA_NV manv_ob, KIEULD_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN, 
VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT,
MANV_THUNGAN, SO_THANGDC, AVG_THANG
from hocnq_ttkd.ct_ipc_tmp b
FLASHBACK TABLE ct_bsc_chot_hopdong TO BEFORE DROP;
select* from hopdong
create table xxx as select* from ct_bsc_chot_hopdong
drop table ct_bsc_chot_hopdong;

-- TAO BANG CHI TIET BSC
    insert into nhuy_ct_bsc_ipcc_obghtt
    with t0 as (select  c0.thuebao_id, c0.phieutt_id, c0.ma_gd, c0.rkm_id, c0.ngay_bd_moi,c0.ngay_kt_moi, c0.tien_hopdong,c0.tien_thanhtoan, c0.vat_thanhtoan vat
                                , c0.ngay_tt, c0.ngay_hd, c0.ngay_nganhang, c0.soseri, c0.seri, c0.kenhthu, c0.ten_nganhang, c0.ten_ht_tra
                            --    , round(cuoc_dc/so_thangdc, 0) avg_thang, so_thangdc
                                , c0.hdkh_id, c0.hdtb_id, c0.nvgiaophieu_id, c0.dvgiaophieu_id
                                , c0.nvtuvan_id, c0.nvthu_id, c0.thungan_tt_id, c0.kenhthu_id, c0.ht_tra_id, nv.ma_nv manv_cn, dv.ten_dv phong_cn, nv1.ma_nv manv_thuyetphuc, 
                                nv2.ma_nv manv_gt, nv3.ma_nv manv_thungan, c0.loaitb_id
               from TMP_ob c0
                            left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
                            left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
                            left join admin_hcm.nhanvien_onebss nv1 on nv1.nhanvien_id = c0.nvtuvan_id
                            left join admin_hcm.nhanvien_onebss nv2 on nv2.nhanvien_id = c0.nvthu_id
                            left join admin_hcm.nhanvien_onebss nv3 on nv3.nhanvien_id = c0.thungan_tt_id
                            where c0.nvtuvan_id is null
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
                        to_number(to_char(ngay_ktdc,'yyyymm')) thang_kt,nhomtb_id, thanhtoan_id, khachhang_id
                        from ttkd_bsc.nhuy_ct_ipcc_obghtt
                        where thang >= 202403
                        )
--                    select* from ttkd_Bsc.nhuy_ct_ipcc_obghtt;
--                    select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt
            , c as (select  t0.thuebao_id, t0.phieutt_id,  t0.ma_gd, t0.rkm_id, t0.ngay_bd_moi, t0.ngay_kt_moi,t0.tien_hopdong,t0.tien_thanhtoan, t0.vat, t0.ngay_tt, t0.ngay_hd, t0.ngay_nganhang, t0.soseri, t0.seri
                                , t0.kenhthu, t0.ten_nganhang, t0.ten_ht_tra, t0.hdkh_id, t0.hdtb_id, t0.nvgiaophieu_id, t0.dvgiaophieu_id, t0.nvtuvan_id, t0.nvthu_id, t0.thungan_tt_id
                                , t0.kenhthu_id, t0.ht_tra_id, t0.manv_cn, t0.phong_cn, t0.manv_thuyetphuc, t0.manv_gt, t0.manv_thungan, km0.so_thangdc, km0.avg_thang,t0.loaitb_id
                        from t0
                                    join km0 on t0.rkm_id = km0.rkm_id
                ) --select* from c;
    select a.thang thang_ob, a.ob_id, a.ngay_ob, a.thuebao_id, a.ma_tb,a.ngay_bddc ngay_bddc_cu, 
            a.ngay_kt_dc ngay_ktdc_cu,a.cuoc_dc cuoc_dc_cu, a.so_thangdc so_thangdc_cu, c.ma_gd, c.rkm_id , c.ngay_bd_moi, c.ngay_kt_moi,c.tien_hopdong, c.tien_thanhtoan, 
            c.vat, c.ngay_tt, c.ngay_hd, c.ngay_Nganhang,c.soseri, c.seri, c.kenhthu, c.ten_nganhang, c.ten_ht_Tra
            ,a.td_th, a.ma_nd ma_nd_ob, a.nhanvien_id nhanvien_ob_id,a.ma_nv manv_ob,nvob.ma_to,nvob.ma_pb, nvtp.ma_pb pb_thphuc,  a.thang_kt, a.nhomtb_id nhomtb_id_cu , c.hdtb_id, c.hdkh_id,
            NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, c.SO_THANGDC so_thangdc_moi, c.AVG_THANG, 
            NVGIAOPHIEU_ID, DVGIAOPHIEU_ID    
               ,  (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 and a.thuebao_id = thuebao_id 
                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
                               and rownum = 1
                    ) 
                    nhomtb_id
                , a.khachhang_id, a.nhomtb_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id, c.loaitb_id, a.thanhtoan_id, 
                
                 case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204) then 1
                                when c.ht_tra_id in (2, 4,5) then 0 else null end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
--                                left join ttkd_bsc.dm_phongban pb
--                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
--                                left join  ttkd_bct.phongbanhang pb_giao
--                                    on a.donvi_giao = pb_giao.pbh_id
--                                left join  ttkd_bct.phongbanhang pb_th
--                                    on a.pbh_id_th = pb_th.pbh_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id and a.nhanvien_id = c.nvtuvan_id-- and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien_202404 nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv
                                left join ttkd_bsc.nhanvien_202404 nvob on a.ma_nv = nvob.ma_nv
                    where a.thuebao_id  in (select thuebao_id from tmp_ob where nvtuvan_id is null) --in (select thuebao_id from xsmax);-- change
--    where  a.thang_kt = 202404 --and rkm_id != 3369696 --a.ma_tb  in ('') -- change
           --      and a.gh_id is null
--                            and a.khachhang_id  <> 9798610          ---khong tinh giao 30 ngay, chi tinh 60 ngay theo vb 3116/TTr-NVC-KHDN2 (folder loai tru ghtt) cho het nam 2023
                  --   and a.thangdc < 3 and c.so_thangdc>= 3 
                 --  and a.ma_tb in ('hcmnhp183')
;









































create table ob as select* from nhuy_ct_bsc_ipcc_obghtt;
commit;
select count(*) from xxx where TIEN_KHOP = 1
update CT_BSC_CHOT_HOPDONG a set TIEN_KHOP = 1
-- select * from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
where thang = 202403 and a.rkm_id is not null and tien_khop = 0 and exists (select 1 from xxx where a.phieutt_id = phieutt_id)
and ht_tra_id in (2, 4,5)
and  exists 
(
    select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
    where  phieu_id = a.phieutt_id  
    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
    );
commit;
select * from nhuy_ct_bsc_ipcc_obghtt where   thuebao_id in ( select thuebao_id from xsmax) and rkm_id is not null-- and ht_Tra_id = 2; 
select rkm_id from tmp_ob where thuebao_id = 11763418;
select* from v_Thongtinkm_all where rkm_id = 6885204;
update nhuy_ct_bsc_ipcc_obghtt set thang = 202404 where thang = 202403
select a.thuebao_id, a.nhanvien_ob_id, b.nvtuvan_id from nhuy_ct_bsc_ipcc_obghtt a
join tmp_ob b on a.thuebao_id = b.thuebao_id 
where a.thuebao_id in (Select thuebao_id from xsmax) and a.rkm_id is null
order by 1;
select* from nhuy.tl_giahan_tratruoc where ma_kpi ='HCM_CL_OBDAI_007'
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
         , 'HCM_CL_OBDAI_007' ma_kpi
         , a.manv_ob ma_Nv, a.ma_to, a.ma_pb
           , count(thuebao_id) tong_sl_ob
          , sum(case when dthu > 0  and tien_khop > 0  then 1 else 0 end) da_giahan
          , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
          , sum(dthu) DTHU_thanhcong
          , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from       (select thang, THUEBAO_ID, MA_TB, MANV_OB, b.ma_To, b.ma_pb, max(NGAY_TT) ngay_Tt, sum(cuoc_d_cu) tien_dc_cu ,sum(TIEN_THANHTOAN) dthu, tthd_id--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                from nhuy.CT_BSC_CHOT_HOPDONG a
                                left join ttkd_Bsc.nhanvien_202403 b on a.manv_ob = b.ma_nv
                                where a.thang = 202403     ------------n------------
                               group by thang,MA_TB,THUEBAO_ID, MANV_OB  , b.ma_To, b.ma_pb ,tthd_id
                ) a 
where ma_pb is not null
group by a.thang, a.manv_ob, a.ma_to, a.ma_pb
order by 2;
SELECT* 
update TTKD_BSC.bangluong_kpi_202403 a set 
        HCM_CL_OBDAI_007 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) from nhuy.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_CL_OBDAI_007')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_CL_OBDAI_007') and thang_kt is null and MA_VTCV = a.MA_VTCV) --AND MA_NV = 'VNP017875'
;
select* from  ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt
select MA_CAPNHAT , phieu_id from ttkdhcm_ktnv.phieutt_hd_dongbo where phieu_id in 
(select phieutt_id from CT_BSC_CHOT_HOPDONG where rkm_id is not null and tien_khop = 0 and ma_Chungtu is null);
SELECT* FROM (
 SELECT THANG, OB_ID, to_char(NGAY_OB,'dd/mm/yyyy') ngay_ob, THUEBAO_ID, MA_TB, LOAITB_ID, KHACHHANG_ID, THANHTOAN_ID, to_char(NGAY_BDDC_CU,'dd/mm/yyyy')  NGAY_BDDC_CU, to_char(NGAY_KTDC_CU,'dd/mm/yyyy')  NGAY_KTDC_CU, CUOC_D_CU, SO_THANGDC_CU, CHITIETKM_ID_CU, MA_ND_OB, NHANVIEN_ID_OB, MANV_OB, KIEULD_ID, RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, to_char(NGAY_HD,'dd/mm/yyyy')  NGAY_HD, to_char(NGAY_TT,'dd/mm/yyyy')  NGAY_TT, SOSERI, SERI, TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN, VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, TTHD_ID, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_NGANHANG, MA_CHUNGTU, TIEN_KHOP
FROM ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt
)
update CT_BSC_CHOT_HOPDONG  a
set ma_Chungtu = (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID)
where a.ht_Tra_id = 2 and ma_chungtu is null
;
commit;

select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_CL_OBDAI_007
            from TTKD_BSC.bangluong_kpi_202403 where (HCM_CL_OBDAI_007  is not null);

select column_name, table_name from all_tab_columns where owner=upper('ttkd_Bsc' )and table_name in (upper('nhuy_ct_Bsc_ipcc_obghtt'),upper('ct_Bsc_tratruoc_moi'))
and column_name not in (select column_name from all_tab_columns where owner=upper('ttkd_Bsc' )and table_name=upper('ct_Bsc_tratruoc_moi')
 )
;
select 





    select a.thang_kt, a.gh_id, a.pbh_ql_id, a.donvi_giao pbh_giao_id, a.tbh_giao_id
                , a.pbh_id_th, c.dvgiaophieu_id pbh_cn_id
                , a.ma_tb, a.ma_nv manv_cs, pb.ten_pb phong_cs
                 , (select ma_to_hrm from ttkd_bct.tobanhang where tbh_id = a.tbh_giao_id and a.donvi_giao = pbh_id and hieuluc = 1) ma_to
                 , (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb
                 , a.nhanvien_giao manv_giao, pb_giao.tenphong PHONG_GIAO, a.ma_nv_th, pb_th.tenphong PHONG_TH
                 , c.manv_cn, c.PHONG_CN, nvl(c.MANV_THUYETPHUC, 'khongco') MANV_THUYETPHUC
                 , nvtp.ma_pb MAPB_THPHUC
                , c.manv_gt, c.manv_thungan
                 , lkh.khdn, a.heso
                , a.THANG_KTDC_CU, a.cuoc_dc TIEN_DC_CU
                , c.MA_TT, c.ma_gd, c.rkm_id, c.ngay_BD_MOI, c.so_thangdc, c.avg_thang, c.TIEN_THANHTOAN, c.VAT, c.NGAY_TT, c.ngay_nganhang
                                    , c.SOSERI, c.SERI, c.KENHTHU, c.TEN_NGANHANG, c.TEN_HT_TRA
                , tt.trangthai_tb, a.thuebao_id, a.loaitb_id, donvi_oa pbh_oa_id, nhanvien_oa manv_oa
                 , 
--                 (select nhomtb_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 and a.thuebao_id = thuebao_id 
--                               and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
--                    ) 
                   null nhomtb_id
                , a.khachhang_id, a.goi_id goi_old_id, c.phieutt_id, c.ht_tra_id, c.kenhthu_id
                
                , case when c.rkm_id is null then null
                                when c.ht_tra_id in (1, 7,204) then 1
                                when c.ht_tra_id in (2, 4,5) then 0 else null end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = c.PHIEUTT_ID) ma_chungtu
                
    from ds a
                                join css_hcm.db_thuebao dbtb
                                    on a.thuebao_id = dbtb.thuebao_id
                                join css_hcm.db_khachhang dbkh
                                        on dbtb.khachhang_id = dbkh.khachhang_id
                                join css_hcm.loai_kh lkh
                                        on dbkh.loaikh_id = lkh.loaikh_id
                                join css_hcm.trangthai_tb tt
                                    on dbtb.trangthaitb_id = tt.trangthaitb_id
                                left join ttkd_bsc.dm_phongban pb
                                    on a.pbh_ql_id = pb.pbh_id and pb.active = 1
                  
                                left join  ttkd_bct.phongbanhang pb_giao
                                    on a.donvi_giao = pb_giao.pbh_id
                                left join  ttkd_bct.phongbanhang pb_th
                                    on a.pbh_id_th = pb_th.pbh_id
                                left join c 
                                    on a.thuebao_id = c.thuebao_id and a.thang_kt = c.thang_kt
                                left join ttkd_bsc.nhanvien_202403 nvtp on c.MANV_THUYETPHUC = nvtp.ma_nv -- change
                        ;
select rkm_id from chot_hopdong group by rkm_id having count(rkm_id) > 1;

select * from css_hcm.hinhthuc_Tra where ht_tra_id in (select distinct ht_tra_id from tmp_ob);
select* from tmp_ob where ht_tra_id = 214;
--select* from ttkdhcm_ktnv.phieutt_hd_dongbo where phieu_id in 
(select * from css_hcm.phieutt_hd where ht_tra_id =214);
select* from css_hcm.ds_ungdung_online where ungdung_id = 13;
select owner , table_name from all_tab_columns where column_name = upper('ungdung_id');
select* from tmp3_ts;

select * from ttkd_bsc.ct_bsc_tratruoc_moi;
select a.THANG, a.NGAY_OB, a.THUEBAO_ID, a.MA_TB,a.NGAY_BDDC_CU, a.NGAY_KTDC_CU, a.CUOC_DC_CU, a.SO_THANGDC_CU, 
b.goi_old_id nhomtb_id_CU,
a.MA_ND_OB nv, a.NHANVIEN_ID_OB, a.MANV_OB, a.RKM_ID, NGAY_BD, NGAY_KT, PHIEUTT_ID, MA_GD, NGAY_HD, NGAY_TT, SOSERI, SERI, TIEN_HOPDONG, VAT_HOPDONG, TIEN_THANHTOAN, 
VAT_THANHTOAN, HDTB_ID, HDKH_ID, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, KENHTHU_ID, HT_TRA_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT, 
TTHD_ID, MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_NGANHANG, MA_CHUNGTU, TIEN_KHOP, MA_TO, MA_PB ,a.OB_ID,  a.LOAITB_ID, a.KHACHHANG_ID, a.THANHTOAN_ID, a.NHOMTB_ID, 
from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
join ttkd_Bsc.ct_bsc_tratruoc_moi b on a.thuebao_id = b.thuebao_id and a.thang = 202404
where 1 = 2; 
select* from admin_hcm.nhanvien where nhanvien_id = 92148;
