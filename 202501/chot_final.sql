-- tao bang chot
select* from bang_gom where ma_Tb = 'vanday5389166';
select distinct ma_pb, ten_pb from ttkd_bsc.nhanvien where thang = 202410;
select* from dongia_ob_final where thuebao_id in (
select thuebao_id from bang_gom where ht_Tra_id in (214,216)) and thuebao_id not in (select thuebao_id from ttkd_bsc.ct_dongia_tratruoc where thang = 202404);
create table temp_tratruoc_chot as;
with db as 
(
    select a.thuebao_id, a.loaitb_id
    from css_hcm.db_Thuebao a
        join css_hcm.db_adsl b on a.thuebao_id =b.thuebao_id
    where a.trangthaitb_id not in (7,8,9) and b.chuquan_id = 145 and a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and to_number(to_char(a.ngay_sd,'yyyymm')) <= 202401
) 

, km2 as (
    select a.thuebao_id, a.ma_tb, a.rkm_id,thang_bddc,thang_ktdc,thang_kt_dc,thang_huy, cuoc_Dc ,ROW_NUMBER() OVER (PARTITION BY a.thuebao_id ORDER BY a.rkm_id DESC) AS rn
    from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
    where loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224)  and  cuoc_dc > 0 and  
    202403 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999)) 
)

, tt as (
        select db.thuebao_id
        from db 
--            left join km1 on db.thuebao_id = km1.thuebao_id and km1.rn = 1
            left join km2 on db.thuebao_id = km2.thuebao_id and km2.rn = 1
--            left join km3 on db.thuebao_id = km3.thuebao_id and km3.rn = 1
        where  km2.rkm_id is not null --and km3.rkm_id is null
        )
        
,hddc as (select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where h.thang_bd > 202307
                                            ) 
   , kmtb as (select hdtb_id, rkm_id, ngay_bddc, ngay_ktdc from css_hcm.khuyenmai_dbtb
                                where datcoc_csd > 0 and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999)) >= thang_bddc
--                                                and thang_bddc > 202307
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
               , b.hdtb_id, hdkh.hdkh_id, hdkh.nhanvien_id nvgiaophieu_id, hdkh.donvi_id dvgiaophieu_id, hdkh.ctv_id nvtuvan_id, hdkh.nhanviengt_id nvthu_id
              , p.thungan_tt_id, p.kenhthu_id, p.ht_tra_id, hdtb.tthd_id, hdkh.ngay_yc, p.thungan_hd_id,
               kt.kenhthu , nh.ten_nh ten_nganhang ,ht.ht_tra ten_ht_tra, 111 lan
                     from  css_hcm.hd_khachhang hdkh
													join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id  and hdtb.kieuld_id in (551, 550, 24, 13080) and hdtb.tthd_id = 6
													join ts  on hdtb.thuebao_id = ts.thuebao_id-- and gh.thang >= 202403
													join css_hcm.ct_tienhd a on hdtb.hdtb_id = a.hdtb_id
													   join css_hcm.ct_phieutt b on a.hdtb_id = b.hdtb_id and b.khoanmuctt_id = 11 and b.tien > 0
													   join css_hcm.phieutt_hd p on b.phieutt_id = p.phieutt_id and p.trangthai= 1 and  p.kenhthu_id not in (6) 
													   left join hddc on hdtb.hdtb_id = hddc.hdtb_id
													   left join kmtb on hdtb.hdtb_id = kmtb.hdtb_id
													   left join css_hcm.kenhthu kt on kt.kenhthu_id = p.kenhthu_id
													   left join css_hcm.nganhang nh on nh.nganhang_id = p.nganhang_id
													   left join css_hcm.hinhthuc_tra ht on ht.ht_tra_id = p.ht_tra_id
													   left join ct on p.phieutt_id = ct.phieu_id
				    
                     where  a.khoanmuctt_id = 11 and   
                     hdtb.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.tien > 0 --and hdtb.thuebao_id = 8811198
                                      and ( to_number(to_char(p.ngay_tt, 'yyyymmdd')) between 20240401 and 20240502)                    ----change--2 thang- ngay 02
--                                     or (p.ht_tra_id in (2,7,5,204, 4) and to_number(to_char(ct.ngay_nganhang, 'yyyymm')) =202404)) 
                                    )  

select *
from kq_ghtt a
where ngay_bd_moi is not null 
);
 commit;
 select* from tmp3_ob;
drop table chot_final;
rollback;
--7: >>> hinh thuc gi?
-- tao bang chi tiet bsc
create table chot_final as;
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt;
alter table ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt add (NHANVIEN_XUATHD VARCHAR2(20), NGAY_YC date);
insert into ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt (THANG, OB_ID, NGAY_OB, THUEBAO_ID, MA_TB, NGAY_BDDC_CU, NGAY_KTDC_CU, CUOC_DC_CU, SO_THANGDC_CU, MA_GD, RKM_ID,
NGAY_BD, NGAY_KT, TIEN_HOPDONG, TIEN_THANHTOAN, VAT_thanhtoan, NGAY_TT, NGAY_HD, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TD_TH, MA_ND_OB, 
NHANVIEN_ID_ob, MANV_OB, MA_TO, MA_PB,  goi_old_id, HDTB_ID, HDKH_ID, NVTUVAN_ID, NVTHU_ID, THUNGAN_TT_ID, MANV_CN, PHONG_CN, MANV_THUYETPHUC, MANV_GT,
MANV_THUNGAN, SO_THANGDC_MOI, AVG_THANG, NGAY_YC, NVGIAOPHIEU_ID, DVGIAOPHIEU_ID, NHOMTB_ID, KHACHHANG_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, LOAITB_ID, THANHTOAN_ID,
NHANVIEN_XUATHD, TIEN_KHOP, MA_CHUNGTU);
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
                        where thang = 202410
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
            202410 thang, a.ob_id, a.ngay_ob, x.thuebao_id, dbtb.ma_tb,a.ngay_bddc ngay_bddc_cu, 
            a.ngay_kt_dc ngay_ktdc_cu,a.cuoc_dc cuoc_dc_cu, a.so_thangdc so_thangdc_cu, x.ma_gd, x.rkm_id , x.ngay_bd_moi, x.ngay_kt_moi,x.tien_hopdong, x.tien_thanhtoan, 
            x.vat, x.ngay_tt, x.ngay_hd, x.ngay_Nganhang,x.soseri, x.seri, x.kenhthu, x.ten_nganhang, x.ten_ht_Tra
            ,a.td_th, a.ma_nd ma_nd_ob, a.nhanvien_id nhanvien_ob_id,a.ma_nv manv_ob,nvtp.ma_to,nvtp.ma_pb,   g.nhomtb_id nhomtb_id_cu ,
            x.hdtb_id, x.hdkh_id
           , x.NVTUVAN_ID, x.NVTHU_ID, x.THUNGAN_TT_ID, x.MANV_CN, x.PHONG_CN, x.MANV_THUYETPHUC, MANV_GT, MANV_THUNGAN, x.SO_THANGDC so_thangdc_moi, x.AVG_THANG,  x.ngay_yc
           , x.NVGIAOPHIEU_ID, x.DVGIAOPHIEU_ID ,   m.nhomtb_id , dbtb.khachhang_id, x.phieutt_id, x.ht_tra_id, x.kenhthu_id, x.loaitb_id, dbtb.thanhtoan_id, nvxp.ma_nv nhanvien_xuathd
                , case when x.rkm_id is null then null
                                when x.ma_gd in (select ma_gd from qrcode where thang = 202410) then 2
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
                                left join ttkd_bsc.nhanvien nvtp on x.MANV_THUYETPHUC = nvtp.ma_nv and nvtp.thang = 202410 and nvtp.donvi = 'TTKD'
                                left join ttkd_bsc.nhanvien nvob on a.ma_nv = nvob.ma_nv and nvob.thang = 202410 and nvob.donvi = 'TTKD'
                                left join goi_cu g on x.thuebao_id = g.thuebao_id and g.rnk =1
                                left join goi_moi m on x.thuebao_id = m.thuebao_id and m.rnk = 1
                                left join admin_hcm.nhanvien_onebss xp on x.thungan_tt_id = xp.nhanvien_id
                                left join ttkd_bsc.nhanvien nvxp on xp.ma_nv = nvxp.ma_nv and nvxp.thang = 202410 and nvxp.donvi = 'TTKD'
where not exists (select rkm_id from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt where  nvl(tien_khop,0) > 0 and thang = 202410 and rkm_id = x.rkm_id )
                                ;
update qrcode set thang = 202410 where thang is null;
drop table ttin_chungtu;
delete from qrcode where thang = 202410;
;
select *  from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410 and tien_khop = 4 and ma_tb in (select ma_Tb from phithu_tainha where thang = 202410);
delete from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410;

select* from ttkd_bsc.blkpi_danhmuc_kpi;
commit;
--- tinh bsc

drop table bsc_ob;
UPDATE ttkd_Bsc.ct_dongia_Tratruoc set loai_tinh = 'DONGIATRA_OB_BS_T9' where thang = 202410 and loai_tinh = 'DONGIATRA_OB';
select* from ffff;
drop table dsnvtp;
rollback;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202501 and ma_kpi = 'HCM_SL_ORDER_001';
select* from ttkd_Bsc.blkpi_Danhmuc_kpi_vtcv where thang = 202407;
create table bsc_ob as;
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DA_GIAHAN_DUNG_HEN, DTHU_THANHCONG_DUNG_HEN)
with a as
(

             select     
             a.thang, a.thuebao_id, a.ma_to, a.ma_pb, ma_Tb
                                        ,a.MANV_THUYETPHUC ma_nv, goi_old_id, nhomtb_id
                                          ,sum( cuoc_dc_cu) tien_Dc_Cu
--                          
----                                      -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
----                                   -----vb 414 NVC ap dung ngaytt <= thang 9 nv_giao toan trinh, Ap dung ngaytt >= thang 10 nv_tuvan thuyet phuc
                                    ,max(a.SO_THANGDC_MOI) sothang_dc
--                                     ,sum(case 
--                                            when a.ht_tra_id in (2,7,204) and nvl(a.kenhthu_id,0) != 21 then 1 
--                                            else 0 end) ht_tra_online
--                                    
--                                    , sum(case when nvl(a.kenhthu_id,0) =21  then 1
--                                                        else 0 end) kenhthu_tainha
--                                    
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
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
                        where a.rkm_id is not null and thang = 202410 --and c.ma_pb = 'VNP0701400'--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
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

;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb  where chungtu_id = 202241; ma_Gd = 'HCM-TT/02869430';
---- tao bang chung tu
drop table ttin_ctu
--- DON GIA
;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1;
drop table ttin_ctu ;
select chungtu_id from  ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where thaotac_id in (1,2,3) 
group by chungtu_id 
having count(distinct nhanvien_cn) > 3;
chungtu_id = 321439;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = 107309 ;
create table ttin_ctu as 
	select a.chungtu_id,a.ma ma_gd
					,(select ma_ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id=a.chungtu_id) ma_ct,a.tongtra tien_tt
--					,(select ma_nv || ' - ' || ten_nv from admin_hcm.nhanvien where nhanvien_id=a.thungan_tt_id) nhanvien_tt
                    ,(select ma_nv from admin_hcm.nhanvien where nhanvien_id=a.thungan_tt_id) nhanvien_tt
					,(select nhanvien_cn from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log where chungtu_id=a.chungtu_id and thaotac_id in (2)
					    and ghilog_id in (select max(ghilog_id) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
									  where chungtu_id=a.chungtu_id and thaotac_id in (2)
									    and timeinsert in (select max(timeinsert) from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb_ghi_log 
													    where chungtu_id=a.chungtu_id and thaotac_id in (2)))) nhanvien_cn
--					,to_char(a.ngay_tt,'dd/mm/yyyy') ngay_tt
					,decode(a.bosung,1,'nhancong','xuatfile') thuchien
from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a ;--4991
where  (EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb where chungtu_id = a.chungtu_id and hoantat=1)
						or EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where chungtu_id = a.chungtu_id and hoantat=1))
--						and a.tratruoc = 1
--			and to_number(to_char(a.ngay_tt,'yyyymm')) = 202406
--			and nvl(a.bosung, 999) <> 1
			--and exists (select 1 from css.v_ghtt_chungtu@dataguard where trangthai = 1 and ND_CT = a.ma)
	;
commit;
drop table ttin_chungtu_t10;
flashback table ttin_chungtu to before drop;
select* from ttin_chungtu_t11;where manv_tt = manv_hd;
create table ttin_chungtu_t1 as 
select a.* , c.ma_nv manv_tt, d.ma_nv manv_hd, b.ngay_tt from ttin_ctu a 
    left join css_hcm.phieutt_hd b on a.ma_Gd = b.ma_gd
    left join admin_hcm.nhanvien_onebss c on b.thungan_tt_id = c.nhanvien_id
    left join admin_hcm.nhanvien_onebss d on b.thungan_hd_id = d.nhanvien_id
where (c.ma_Nv) = nvl(d.ma_nv,c.ma_Nv) ;
commit;
SELECT* FROM TTKD_bSC.DONGIA_VTTP WHERE THANG = 202407 AND MA_tB IN (
select MA_tB from ttkd_Bsc.dongia_Vttp where thang = 202407 GROUP BY MA_TB HAVING COUNT(LOAI_TINH)>1);
select* from ttin_chungtu_t7;
rollback;
select a.ma_gd, b.chungtu_id from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a left join TTIN_cTU b on a.ma_gd =b.ma_gd where tien_khop = 1 and thang = 202406;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd = 'HCM-TT/02826927';
select a.*, b.tien_thuyetphuc, b.tien_xuathd from ttkd_bsc.ct_dongia_Tratruoc a
 join  ttkd_bsc.ct_dongia_Tratruoc b on a.thuebao_id = b.thuebao_id and a.loai_tinh  = b.loai_tinh and a.ma_Nv = b.ma_nv
 where a.thang = 20240601 and b.thang = 202406 and a.loai_tinh = 'DONGIATRA_OB' and (a.tien_xuathd=b.tien_xuathd);--a.tien_thuyetphuc = b.tien_thuyetphuc or 
minus ;
select* from ttkd_bsc.nhu where thang = 202406 and loai_tinh = 'DONGIATRA_OB';
rollback;
delete from ttkd_bsc.ct_dongia_Tratruoc where thang = 202410 and loai_tinh = 'DONGIATRA_OB';
commit;

select kieuld_id from css_hcm.hd_Thuebao;
select* from v_thongtin_tt_hd where ma_gd= 'HCM-TT/02963299';
select* from ttkd_bsc.blkpi_danhmuc_sms;
select* from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202501 and loai_tinh = 'DONGIATRA_OB';
insert into ttkd_Bsc.ct_dongia_Tratruoc (THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_TB, MA_NV, KHACHHANG_ID, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, DTHU, 
NGAY_TT, TIEN_KHOP,  NHANVIEN_XUATHD, DONGIA, TYLE_THANHCONG, GHICHU,  TIEN_XUATHD,ipcc)
--create table tmp_Dg_obghtt as;
      select THANG, LOAI_TINH, MA_KPI, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_TB, MA_NV, KHACHHANG_ID, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT, TIEN_KHOP,  
      manv_Tt NHANVIEN_tt, DONGIA, TYLE_THANHCONG, GHICHU--, manv_Tt, manv_hd, nhanvien_capnhat , thungan_tt_id, thungan_Hd_id
            , case when dongia = 10000 then 10000*nvl(TYLE_THANHCONG,0.4) else 0 end tien_xuathd , ipcc--, tt,ngay_insert
      from 
      (
      select 
       THANG,'DONGIATRA_OB' LOAI_TINH, 'DONGIA' MA_KPI, THUEBAO_ID, tien_Dc_Cu, MA_TO, MA_PB, MA_TB, MANV_THUYETPHUC ma_nv,  KHACHHANG_ID, SOTHANG_DC, 
     HESO_CHUKY, HESO_DICHVU, DTHU, NGAY_TT,  TIEN_KHOP, ngay_yc, nhanvien_capnhat,dongia, tyle tyle_thanhcong ,manv_Tt,manv_Hd,ipcc,thungan_Tt_id, thungan_hd_id,
     '1' ghichu--, 'bo sung cho 1 thue bao gia han 2 hop dong' ghichu,
                 , row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by dongia desc) rnk, tt,ngay_insert
        from (
        with hs as (select thang, khachhang_id from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                where  xu.loaitb_id in (61, 171, 18) group by thang, khachhang_id
                )
             select     
             a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id ,ipcc--, a.ma_nv, a.ma_to, a.ma_pb
            ,   a.ma_tb,a.MANV_THUYETPHUC, a.goi_old_id ,  a.nhomtb_id nhomtb_moi_id, a.ma_to, a.ma_pb--, a.ngay_yc, a.nhanvien_xuathd
                , hs.khachhang_id,sum(cuoc_dc_cu) tien_Dc_Cu, a.ngay_yc, d.ma_nv nhanvien_capnhat, b.nhanvien_cn, b.ngay_tt ngay_gachno,e.thungan_Tt_id, e.thungan_hd_id, 
--                c.ngay_insert,
                b.thuchien, b.manv_Tt, b.manv_hd
        ,case --when tien_khop = 1 and d.ma_Nv is null then 7500
         WHEN tien_khop =1 then 10000
        when tien_khop = 2 then 15000
         when tien_khop = 3 then 12000  
         when tien_khop = 4 then 6000
         else 0
         end dongia 
        ,case 
--         when tien_khop = 1 and to_number(to_char(b.ngay_tt,'yyyymmdd'))  <= to_number(to_char(c.ngay_insert,'yyyymmdd'))+1 then 0.4 
--        when tien_khop = 1 and to_number(to_char(b.ngay_tt,'yyyymmdd'))  > to_number(to_char(c.ngay_insert,'yyyymmdd'))+1 then 0.5 else null end tyle
         when tien_khop = 1 and b.ngay_tt  <= c.ngay_insert+1 then 0.4 
        when tien_khop = 1 and b.ngay_tt  > c.ngay_insert+1 then 0.5 else null end tyle, b.ngay_tt tt, c.ngay_insert
                                    ,max(a.so_Thangdc_MOI) sothang_dc
                                                                  , case
                                            when max(a.so_Thangdc_MOI) >=12 then 1.2
                                            when max(a.so_Thangdc_MOI) < 12 and max(a.so_Thangdc_MOI) >= 6 then 1
                                            when max(a.so_Thangdc_MOI) < 6 and max(a.so_Thangdc_MOI) >= 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
----                                    
                                    , case ----Fiber tinh he so 1, neu co MyTV cung ky + 0.15, Neu khong duy tri goi dadv -0.5
                                                        when a.loaitb_id in (58, 59) then 1  +  nvl(0.15* nvl2(hs.khachhang_id, 1, 0) , 0)
                                                                                        -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  ---co goi giao = 1
                                                                                                            * nvl2(a.nhomtb_id, 0, 1)       --- khong duy tri goi = 1
                                                                                                    , 0)
--
                                                        ----Dich vu Mesh he so 0.2
                                                        when a.loaitb_id = 210 then 0.2  
                                                        ---MyTV he so 0.25 
                                                        when a.loaitb_id in (61, 171, 18) then 0.25  
                                                    else 0 
                                        end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU, max(a.ngay_tt) ngay_tt, a.nhomtb_id
                                    , nvl(tien_khop,0) tien_khop
                        from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a 
                                            left join hs on hs.thang = a.thang and hs.khachhang_id = a.khachhang_id
                                            left join ttin_Chungtu_t1 b on a.ma_Gd = b.ma_gd
                                            left join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c on b.chungtu_id = c.chungtu_id
                                            left join admin_hcm.nhanvien_onebss d on b.nhanvien_cn = d.nhanvien_id
                                            left join css_Hcm.phieutt_hd e on a.phieutt_id = e.phieutt_id
                        where  A.THANG = 202501-- and a.thuebao_id in (select thuebao_id from temp_chot)--and a.thang = 202403 and to_number(to_char(ngay_tt,'yyyymm'))  = 202403---        CHANGE
                        group by a.thang, a.thuebao_id, a.loaitb_id,  a.thanhtoan_id , a.ma_to, a.ma_pb--, a.ma_nv, a.ma_to, a.ma_pb
                                            , a.ma_tb,a.MANV_THUYETPHUC, a.nhomtb_id, a.goi_old_id,  a.ngay_yc, a.nhanvien_xuathd, b.nhanvien_cn
                                            , hs.khachhang_id, a.tien_khop, d.ma_nv, b.ngay_tt , c.ngay_insert,b.thuchien, b.manv_Tt, b.manv_hd,ipcc, e.thungan_Tt_id,
                                            e.thungan_hd_id
        ) a
--            join ttkd_bsc.nhanvien_202404 b on a.MANV_THUYETPHUC = b.ma_nv 
            where  dthu > 0 --and not exists (select 1 from ttkd_Bsc.ct_dongia_tratruoc where thang = 202403 and A.THUEBAO_ID = THUEBAO_ID AND A.MANV_THUYETPHUC = MA_NV)
            and not exists (select 1 from ttkd_Bsc.ct_dongia_Tratruoc where thang in  (202501) and tien_khop >0 and thuebao_id = a.thuebao_id and ma_nv = a.MANV_THUYETPHUC and
            loai_tinh = 'DONGIATRA_OB')
        )   where rnk = 1 -- and nvl(manv_Tt ,'a') != nvl(manv_hd,manv_hd) --and nhanvien_capnhat != manv_tt;
       ;

update ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt y
set ma_Chungtu = (select   listagg(distinct a.ma_Ct, ', ') within group (order by a.chungtu_id) ma_Ct
                    from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a 
                            join ttkdhcm_ktnv.ds_chungtu_Nganhang_phieutt_hd_1 b on a.chungtu_id = b.chungtu_id
                            join ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt x on b.ma = x.ma_Gd and thang = 202501
            where b.ma = y.ma_Gd --and ma_Gd is not null
            group by x.ma_gd

            )
where thang = 202501 and tien_khop = 1;
commit;
select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt x where thang = 202501;
update ttkd_bsc.ct_dongia_tratruoc f 
--select* from  ttkd_bsc.ct_dongia_tratruoc f
set tien_xuathd = 0, ghichu = 'gach no tu dong' , dongia = 7500
where thang = 202501 and tien_khop = 1 and loai_tinh = 'DONGIATRA_OB' and exists (
    select 1-- distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_gd
    from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt a
        join ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc b on a.ma_Chungtu = b.ma_Ct
    where
     a.thang = f.thang and
     a.thang = 202501 
     and a.thuebao_id = f.thuebao_id
     and f.ma_Nv = a.manv_thuyetphuc
        and b.tudong = 1
);

-- update 
update ttkd_Bsc.ct_dongia_tratruoc set tien_thuyetphuc = dongia where dongia!= 10000 and loai_tinh = 'DONGIATRA_OB' AND THANG = 202501;
update ttkd_Bsc.ct_dongia_tratruoc set tien_thuyetphuc = (dongia-tien_xuathd) where dongia= 10000 and loai_tinh = 'DONGIATRA_OB' AND THANG = 202501;
commit;
INSERT INTO  ttkd_Bsc.ct_dongia_Tratruoc
select 202501 THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE,
KHUVUC, DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU,4 TIEN_KHOP, 'bs' GHICHU, TYLE_THANHCONG, HESO_CHUKY, NHANVIEN_XUATHD, TIEN_XUATHD,
6000 TIEN_THUYETPHUC, IPCC
from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202411 and loai_tinh = 'DONGIATRA_OB'
and ma_tb in (select ma_Tb from ttkd_Bsc.nhuy_Ct_bsc_ipcc_obghtt where thang = 202411 and ma_pb = 'VNP0703000' AND TIEN_KHOP = 0 and ht_Tra_id = 214);

select* from ttkd_bsc.nhuy_Ct_Bsc_ipcc_obghtt where ma_Tb in ('mesh0065132','mesh0066902','hcmttkhue-htv3','mesh0065131') ;
delete from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202501
and ma_to in (select distinct ma_to from ttkd_Bsc.nhanvien where thang = 202501 and ten_to = 'T? B�n H�ng Online');
select* from ttkd_bsc.ct_Bsc_tratruoc_moi_30day where ma_Tb ='lacnuithanh' and thang = 202405;
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202501; ma_Tb ='mesh0169362';
select* from  ttkd_Bsc.ct_Dongia_Tratruoc where ma_tb is null; thang >= 202404 and  ma_Tb in ('mesh0065132','mesh0066902','hcmttkhue-htv3','mesh0065131');
select ma_Tb from css_hcm.db_Thuebao where thuebao_id in (4333176,7203313,3019931);
select;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
COMMIT;
select *from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202410;
insert into ttkd_Bsc.tl_giahan_Tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, DTHU_THANHCONG_DUNG_HEN, tien)
select a.thang, 'DOANHTHU','DONGIA', a.MA_NV,a.MA_TO,a.MA_PB, ROUND(SUM(TIEN)/0.8),  ROUND(SUM(TIEN)/0.8) --, b.ma_vtcv, b.ten_vtcv, b.ten_to
from  ttkd_Bsc.tl_giahan_Tratruoc a
    join ttkd_Bsc.nhanvien b on a.thang = b.thang  and a.ma_nv = b.ma_nv
WHERE a.THANG = 202501 AND LOAI_TINH IN ('DONGIATRA_OB','DONGIA_TS_TP_TT') and b.ma_Vtcv = 'VNP-HNHCM_KDOL_17'
GROUP BY a.THANG,a.MA_NV,a.MA_TO,a.MA_PB, b.ma_Vtcv, b.ten_vtcv, b.ten_To;
rollback;
select* from ttkd_Bsc.tl_giahan_Tratruoc where THANG = 202501 AND LOAI_TINH IN ('DONGIATRA_OB','DONGIA_TS_TP_TT'); 
COMMIT;
select* from  ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_nv = 'CTV021917';
delete from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202501 and loai_tinh = 'DONGIATRA_OB' and ma_To in 
(select DISTINCT ma_to from ttkd_Bsc.nhanvien where thang = 202501 and UPPER(ten_to) = 'T? B�N H�NG ONLINE');
COMMIT;
SELECT SUM(TIEN) FROM ttkd_Bsc.tl_giahan_tratruoc where thang = 202412 and loai_tinh = 'DONGIATRA_OB' and ma_pb = 'VNP0700900';
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202501 and loai_tinh = 'DONGIATRA_OB'; and ma_pb = 'VNP0700900'; and ma_pb in (select ma_pb from ttkd_Bsc.nhanvien where thang = 202410 and ten_pb like '%Khu%') and loai_Tinh = 'DONGIATRA_OB';
insert into ttkd_Bsc.tl_giahan_tratruoc(THANG, MA_NV, MA_TO, MA_PB, LOAI_TINH, MA_KPI, TIEN)
--select sum(tien) from (
with tien as 
(
    select thang,ma_Nv,loai_tinh, ma_kpi, ma_Tb, heso_chuky*heso_dichvu*NVL(tien_thuyetphuc,0) tien  , ma_pb
    from ttkd_Bsc.ct_dongia_tratruoc 
    where thang = 202501 and loai_tinh = 'DONGIATRA_OB'  --and ghichu = 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
--    and ma_nv = 'VNP017247'
    union all
    select thang,nhanvien_xuathd,loai_tinh, ma_kpi,ma_tb,  heso_chuky*heso_dichvu*NVL(tien_xuathd,0) TIEN,ma_pb
    from ttkd_Bsc.ct_dongia_tratruoc 
    where thang = 202501 and loai_tinh = 'DONGIATRA_OB' --and ghichu= 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
)
select a.thang,a.ma_nv, b.ma_to, b.ma_pb, a.loai_tinh, a.ma_kpi, sum(tien) tien --, ten_pb
from tien a
    join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and a.thang = b.thang
where b.ma_pb != 'VNP0700700'
group by  a.thang,a.ma_nv, b.ma_to, b.ma_pb, a.loai_tinh, a.ma_kpi,b.ten_to, b.ten_pb, b.ma_Vtcv, b.ten_vtcv, ten_pb;
commit;
select distinct loai_Tinh, ma_kpi from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202501;
delete from  ttkd_Bsc.CT_DONGIA_TRATRUOC where thang = 202501 and loai_tinh in ('DOANHTHU','DONGIATRA_OB');
select a.ma_pb,b.ten_Vtcv ,a.ma_nv,ten_pb, tien from ttkd_Bsc.tl_giahan_Tratruoc a join ttkd_Bsc.nhanvien_202406 b on a.ma_nv =b.ma_nv 
where thang = 202406 and  loai_tinh = 'DONGIATRA_OB' ;and tien> 100000 ;group by a.ma_pb;
select distinct ghichu from  ttkd_Bsc.ct_dongia_tratruoc 
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
    select distinct ma_pb , ten_pb from ttkd_Bsc.nhanvien_202406 where ten_pb like 'Ph�ng B�n H�ng Khu V?c%';
rollback;
select* from ttkd_bsc.ct_dongia_tratruoc 
    where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
    select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202406 and nhanvien_xuathd ='VNP016987';
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select* from dongia_ob_Final where thuebao_id in (select thuebao_id from temp_chot);-- where ma_tb= 'sonha1021601';
       INSERT INTO ct_dongia_temp ;
       drop table dongia_ob_final;
       -- chia tien 
    
insert into ttkd_bsc.tl_giahan_tratruoc (ma_kpi, loai_tinh, ma_nv, ma_to, ma_pb, tien)
with tien as (
    SELECT MA_nV , SUM(TIEN_tHUYETPHUC) TIEN FROM (
    select ma_nv, tien_thuyetphuc
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and  not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)
    and ma_Tb in (select ma_Tb from ma_Tb where nguon = 'nsg')
    union all 
    select NHANVIEN_XUATHD, tien_xuathd
    from dongia_ob_final a where not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) 
    and not exists (select 1 from bang_gom where ngay_bd_moi < 20240301 and thuebao_id = a.thuebao_id)
    and ma_Tb in (select ma_Tb from ma_Tb where nguon = 'nsg')

    )
    GROUP BY MA_NV
)
select  'DONGIA','DONGIATRA_OB_BS',a.ma_nv, b.ma_to, b.ma_pb,  a.tien
from tien a
    join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv
    where ma_pb = 'VNP0701400';
    commit;
    rollback;
    select* from ttkd_Bsc.tl_giahan_Tratruoc;
group by a.ma_nv, b.ma_To, b.ma_pb, b.ten_pb;
select* from ct_dongia_temp;
commit;
delete from ttkd_bsc.tl_giahan_Tratruoc  WHERE  LOAI_tINH  = 'DONGIATRA_OB_BS' and ma_pb = 'VNP0701400';
SELECT DISTINCT MA_KPI FROM ttkd_bsc.tl_giahan_Tratruoc;
select A.DA_GIAHAN, B.DA_GIAHAN_DUNG_HEN,A.DA_GIAHAN -B.DA_GIAHAN_DUNG_HEN  , C.* from bsc_ob a join ttkd_bsc.tl_giahan_Tratruoc b on a.ma_Nv = b.ma_nv AND A.MA_KPI = B.MA_KPI
    JOIN TTKD_bSC.NHANVIEN_202404 C ON A.MA_NV = C.MA_nV 
and a.loai_tinh = 'KPI_NV' AND A.ma_kpi = 'HCM_SL_ORDER_001'
WHERE B.THANG = 202404 and c.ma_vtcv = 'VNP-HNHCM_BHKV_47';
select a.thang, a.ma_tb, e.ten_pb phong_chamsoc ,a.manv_thuyetphuc, c.ten_to tento_thuyetphuc, c.ten_pb tenpb_thuyetphuc, a.ma_Tt, a.ma_gd, a.thang_bd_moi, a.tien_thanhtoan,
a.vat, ngay_tt,kenhthu, ten_Ht_Tra, manv_Gt, manv_thungan, case when ma_gd in (select ma_gd from qrcode) then 'QR Code' else 'VNPT Pay' end HT_Tra
from ttkd_Bsc.ct_bsc_trasau_tp_tratruoc a 
    left join ttkd_bct.db_Thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
    left join ttkd_Bsc.nhanvien_202404 c on a.manv_thuyetphuc = c.ma_nv
where thang = 202404 and (ma_gd in (select ma_gd from qrcode) or (ht_tra_id in (204,7)));
select* from bang_gom where ma_tb in (select ma_tb from ma_tb);
select* from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt;
select a.thang, a.ma_tb, e.ten_pb phong_chamsoc ,a.manv_thuyetphuc, c.ten_to tento_thuyetphuc, c.ten_pb tenpb_thuyetphuc,  a.ma_gd, a.ngay_bd,a.ngay_kt, a.tien_thanhtoan 
,a.vat_thanhtoan vat, ngay_tt,kenhthu, ten_Ht_Tra, manv_Gt, manv_thungan  , tien_khop, case when ma_gd in (select ma_gd from qrcode) then 'QR Code' else 'VNPT Pay' end HT_Tra
from ttkd_bsc.nhuy_ct_bsc_ipcc_obghtt a
    left join ttkd_bct.db_Thuebao_ttkd b on a.thuebao_id = b.thuebao_id
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
    left join ttkd_Bsc.nhanvien_202404 c on a.manv_thuyetphuc = c.ma_nv
where thang = 202404 and rkm_id is not null and (tien_khop = 2 or ht_tra_id in (204, 7));
with tien as (select ma_nv, TIEN_THUYETPHUC, ma_pb
from dongia_ob_final where ma_tb in (select a.ma_tb from ma_tb a where nguon = 'nsg') 
    and ma_tb not in (select ma_tb from ct_dongia_temp where thang = 202404 and tien_khop > 0 and dongia > 0)
union all
select NHANVIEN_XUATHD, TIEN_XUATHD, ma_pb
from dongia_ob_final where ma_tb in (select a.ma_tb from ma_tb a where nguon = 'nsg') 
    and ma_tb not in (select ma_tb from ct_dongia_temp where thang = 202404 and tien_khop > 0 and dongia > 0 )
) 
select * from tien a join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv 
;where ma_pb != 'VNP0701400';
select* from ttkd_Bsc.nhanvien_202404 where ma_nv = 'VNP016796';
delete from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202410 and ma_kpi ='HCM_SL_ORDER_001';
select a.ma_pb, a.ma_pb from dongia_ob_final a 
join ttkd_bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv
where a.ma_pb != b.ma_pb;
COMMIT;
delete from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202410 and loai_tinh in ('HCM_TB_GIAHA_022','HCM_TB_GIAHA_023');
select a.ma_nv, ten_nv,a.ma_to, ten_to, a.ma_pb, ten_pb, ma_vtcv, ten_vtcv ,a.tien from ttkd_Bsc.tl_giahan_tratruoc a join ttkd_Bsc.nhanvien_202404 b on a.ma_nv = b.ma_nv 
where thang = 202404 and ma_kpi = 'DONGIA' and loai_tinh in ('DONGIATRA_OB_BS') ;


select* from ct_dongia_TEMP where thang = 202404 and ma_Tb in (select a.ma_tb from ma_tb a where nguon = 'nsg') --and loai_tinh = 'DONGIATRA_OB'
select* from  ttkd_bsc.nhanvien_202404 ;
select* from v_thongtinkm_all where ma_Tb in ('thh76','meshthh76','kimthao220124','hcm_kimthao_10')
select* from dongia_ob_final where ma_tb in (select a.ma_tb from ma_tb a where nguon = 'nsg');
select a.ma_tb from ma_tb a where nguon = 'nsg' and ma_tb not in (select ma_Tb from dongia_ob_final) and ma_Tb not in (select ma_Tb from ttkd_bsc.ct_dongia_tratruoc where thang in (202402,202403) and tien_khop = 1 and dongia>0);