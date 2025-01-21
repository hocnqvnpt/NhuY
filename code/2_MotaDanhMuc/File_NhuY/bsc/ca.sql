oselect * from css_hcm.hd_thuebao where ma_tb = 'hvluong1952';
select * from css_hcm.kieu_ld where kieuld_id =128;
select * from css_hcm.db_Thuebao;
select rowid, a.* from ttkdhcm_ktnv.ds_ca_chotden1411_1511 a where thuebao_id = 9558064;

select rowid, a.* from ttkd_bsc.ct_bsc_giahan_cntt a where thuebao_id = 9558064;
delete from ttkd_bsc.ct_bsc_giahan_cntt a where thang = 202310 and thuebao_id = 9558064 and rowid in ('AAC4+qABgAAB3sTAAH', 'AAC4+qABgAAB3sTAAI', 'AAC4+qABgAAB3sTAAJ');
commit;

select THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS
            , MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, PHONG_THPHUC
            , MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI
            , SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
            , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, GOI_OLD_ID, KHACHHANG_ID
            , PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU, THUEBAO_KHAC_ID, MA_TB_KHAC, KIEULD_ID
from ttkd_bsc.ct_bsc_giahan_cntt
where thang = 202310;

update ttkd_bsc.ct_bsc_giahan_cntt set THUEBAO_KHAC_ID = null
where thang = 202310 and THUEBAO_KHAC_ID = 0;
commit;
----B1 chot du lieu tu ID 271
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202401 and ma_Tb = 'hcm_smartca_00003867 '
delete from 
select* from ttkdhcm_ktnv.ghtt_chotngay_271 a
where ngay_chot =to_date('03/02/2024','dd/mm/yyyy')
and a.thang_kt in (202402,202403) and a.loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344','hcm_pharmacy_00000134');
select* from ttkd_bsc.ct_bsc_Giahan_cntt where thang = 202402 and loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344','hcm_pharmacy_00000134');
commit;
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_024';
ttkd_bsc.ct_bsc_giahan_cntt where thang = 202311

select* from css_Hcm.hdtb_datcoc where hdtb_id = 427418
select distinct ht_Tra_id from css_hcm.phieutt_hd where phieutt_id in (select phieutt_id from ttkdhcm_ktnv.ghtt_chotngay_271 where trunc(ngay_chot)=to_date('02/02/2024','dd/mm/yyyy'))
insert into ttkd_bsc.ct_bsc_giahan_cntt

select 202402 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
                , a.ma_to, (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb,
                a.nhanvien_giao, ten_donvi_giao, a.MANV_HRM_TH, a.TEN_PBH_TH PHONG_TH, null, null, nv.ma_nv MANV_THPHUC, nv1.ten_pb PHONG_THPHUC
                , nv2.ma_nv MANV_GT, nv3.ma_nv MANV_THUNGAN, null, null, a.thang_kt, a.DATCOC_CSD, null, a.ma_gd, null, a.thang_Bd_moi, null SO_THANGDC, null, ptt.tien, ptt.vat, ptt.ngay_tt, a.NGAY_NGANHANG
                , ptt.SOSERI, ptt.SERI, KENHTHU, ten_nh ten_nganhang, ht_tra ten_ht_tra, tttb.trangthai_tb , a.thuebao_id, a.loaitb_id, null, null, null, null, db.khachhang_id, a.phieutt_id, ptt.ht_tra_id, ptt.kenhthu_id
                , case when a.phieutt_id is null then null
                            when ptt.ht_tra_id in (1,7) then 1
                            when ptt.ht_tra_id in (2,5,4,9) then 0 else null 
                            end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
                , a.THUEBAO_ID_KHAC, a.MATB_KHAC, null KIEULD_ID
from ttkdhcm_ktnv.ghtt_chotngay_271 a
            left join ttkd_bsc.nhanvien_202402 b on a.MANV_HRM = b.ma_nv -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien_202402 nv1 on nv.ma_nv = nv1.ma_nv -- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
            
where trunc(a.ngay_chot)=to_date('02/03/2024','dd/mm/yyyy')
and a.thang_kt in (202402,202403) and a.loaitb_id in (147,148) and a.ma_Tb in  ('hcm_tmvn_00003356','hcm_tmvn_00004344'); -- (55 ,80 ,116 ,117,132,140,154,181,288,318 )  -- a.loaitb_id in (147,148) --
order by a.nhanvien_giao desc
;          
commit;
select from css_hcm.db_thuebao where ma_Tb = 'hcm_ca_00061884';

select * from css_hcm.khuyenmai_dbtb where thuebao_id = 11845931;

select * from css_hcm.ct_phieutt where hdtb_id = 17649679;
select owner, table_name from all_tab_columns where column_name = 'KHOANMUCTT_ID' AND OWNER = 'CSS_HCM';
select* from css_hcm.KHOANMUC_TT where khoanmuctt_id in (11,21)
sele
select *  from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202402 and loaitb_id in (147,148) and ma_tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344');

select* from ttkdhcm_ktnv.ghtt_chotngay_271 where thang_kt in (202312,202401) and ngay_chot =to_date('02/01/2024','dd/mm/yyyy') and loaitb_id in (147,148)
update ttkd_bsc.ct_bsc_giahan_cntt a
set ma_To = (select ma_to
from ttkd_bsc.nhanvien_202311 where ma_nv = a.manv_giao)
where thang = 202311 and ma_to is null
commit; 
rollback;

select* from ttkd_bsc.ct_Bsc_Giahan_cntt where thang = 202312 and loaitb_id in (147,148)
delete from ttkd_bsc.ct_Bsc_Giahan_cntt where thang = 202311
update ttkd_bsc.ct_bsc_giahan_cntt a set TIEN_KHOP = 1
--- select * from ttkd_bsc.ct_bsc_giahan_cntt a
where thang = 202402 and a.phieutt_id is not null and TIEN_KHOP = 0 and  a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,288,318 , 147,148) 
--and ma_Tb in  ('hcm_tmvn_00003356','hcm_tmvn_00004344')--
and ht_tra_id in (2, 4, 5, 9)
and  exists 
(
    select 1 -- bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM, tien_nhapthem
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
         where  phieu_id = a.phieutt_id
    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
)
                                                                        
commit;
select* from ttkd_Bsc.ct_bsc_Giahan_cntt where thang_ktdc_Cu = 202401 and thang = 202312
----B2---

select * from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202311;

--------1------C4---Tyle thue bao gia han TC thang kt thang T v� T+1
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025')
commit;
 -- CA 
 selecT* from ttkd_bsc.tl_giahan_tratruoc where ma_kpi in  ('HCM_TB_GIAHA_024') AND THANG = 202401
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , case when THANG_KTDC_CU = thang then 'HCM_TB_GIAHA_024'
                            when THANG_KTDC_CU > thang then 'HCM_TB_GIAHA_025'
                        else null
                end ma_kpi
             , a.ma_nv, a.ma_to, a.ma_pb
               , count(thuebao_id) tong
              , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
              , sum(dthu) DTHU_thanhcong
              , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from       (select thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                , sum(a.tien_khop)
                from ttkd_bsc.ct_bsc_giahan_cntt a
                where a.thang = 202402 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) --(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb
order by 2
commit;
select distinct ma_kpi, loai_Tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312
;
---------------Chay binh quan To, Phong -----
select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202401 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') and loai_tinh = 'KPI_PB';
-- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_024') group by ma_kpi, ma_nv having count(ma_to)>1;

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;

select* from ttkd_bsc.ct_bsc_giahan_cntt a where a.thang = 202402 and tien_khop = 1 and ht_tra_id in (2,4) and ma_chungtu is null

select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where thang_kt is null and ma_kpi = 'HCM_TB_GIAHA_026' 
select hcm_tb_Giaha_026 from ttkd_Bsc.bangluong_kpi_202312
select hcm_Tb_giaha
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
and a.ma_pb = b.ma_pb
where a.thang = 202402 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
order by ma_kpi
;
commit;

rollback;
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202401 and 'HCM_TB_GIAHA_026' = ma_kpi and loai_tinh = 'KPI_NV'
-- TEN MIEN 
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , 'HCM_TB_GIAHA_026' ma_kpi
             , a.ma_nv, a.ma_to, a.ma_pb
               , count(thuebao_id) tong
              , sum(case when dthu > 0  and tien_khop > 0 then 1 else 0 end) da_giahan
              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
              , sum(dthu) DTHU_thanhcong
              , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from (select thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                                    , sum(a.tien_khop)
                                    from ttkd_bsc.ct_bsc_giahan_cntt a
                                    where a.thang = 202402 and loaitb_id in (147,148) and thang_ktdc_cu = 202402--  AND MANV_GIAO = 'VNP017400'------------n------------ 
                                   group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb 
order by 2
commit;
;
---------------Chay binh quan To, Phong -----
select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') and loai_tinh = 'KPI_TO';
-- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_024') group by ma_kpi, ma_nv having count(ma_to)>1;
DELETE FROM 
 ttkd_bsc.tl_giahan_tratruoc WHERE MA_KPI = 'HCM_TB_GIAHA_026' AND THANG = 202312 AND LOAI_tINH = 'KPI_TO' AND MA_TO = 'VNP07012H0'
 COMMIT;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_026') -- AND MA_TO = 'VNP07012H0'
            group by THANG, MA_KPI, MA_TO, MA_PB
;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202402 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026')
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
commit;
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202312 and loaitb_id in (148,147) and thang_ktdc_Cu = 202312 where 
rollback;
-----------------------------------
  select * from ttkd_bsc.tl_giahan_tratruoc  where ma_kpi =  'HCM_TB_GIAHA_023' and thang = 202402  and loai_tinh = 'KPI_PB'
    -- kiem tra
    select* from tt
    ---Buoc 5---gan BSC ghtt theo vi tri nvien, to truong, LDP
            ----Update nhan vien bang KPI----
update TTKD_BSC.bangluong_kpi_202311 a 
set 
HCM_TB_GIAHA_024 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv = a.ma_nv_hrm )
, HCM_TB_GIAHA_025 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025')
, HCM_TB_GIAHA_026 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_026')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025','HCM_TB_GIAHA_026') and thang_kt is null and MA_VTCV = a.MA_VTCV)
select distinct loai_tinh, ma_kpi from ttkd_Bsc.tl_giahan_tratruoc where thang = 202312
;
select* from TTKD_BSC.bangluong_kpi_202310
---------------Ty le cua To truong -----
select* from 
update TTKD_BSC.bangluong_kpi_202310 a 
set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang = 202310 and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_024')
, HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang = 202310 and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_025')                                                                                
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') 
                                and thang_kt is null and MA_VTCV = a.MA_VTCV)
;
--------------Ty le cua Pho GD ma_kpi = 'HCM_TB_GIAHA_018' -----
update TTKD_BSC.bangluong_kpi_202311 a 
                set HCM_TB_GIAHA_024 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = 202311 and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_024' )
                , HCM_TB_GIAHA_025 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                                                where thang = 202311 and loai_tinh ='KPI_PB' 
                                                                                                and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_025' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025')  and thang_kt is null and MA_VTCV = a.MA_VTCV)
     
;
select * from (
select gh_id, ma_Tb, manv_giao, phong_giao, ma_to, ma_pb, manv_th, phong_th, thang_ktdc_cu, tien_dc_cu tien_dc_cu_vat, manv_Thungan, ma_gd, rkm_id,
        THANG_BD_MOI, so_thangdc, TIEN_THANHTOAN, VAT, THANG, TEN_HT_TRA
        ngay_tt, NGAY_NGANHANG, nhomtb_id, ma_chungtu, tien_khop
from ttkd_bsc.ct_bsc_giahan_cntt 
where loaitb_id in (147,148 ) and thang_ktdc_cu= 202312
) where thang = 202312
commit;
select* from ttkd_bsc.ct_bsc_giahan_cntt 

select distinct ma_vtcv from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025')  and thang_kt is null 
/**Kiem tra
update TTKD_BSC.bangluong_kpi_202309 a set HCM_TB_GIAHA_022 = null;
select * from  TTKD_BSC.bangluong_kpi_202309;
**/
select MA_NV, MA_NV_HRM, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO
, HCM_TB_GIAHA_025, HCM_TB_GIAHA_024--, HCM_TB_GIAHA_023,HCM_TB_GIAHA_022
         
from TTKD_BSC.bangluong_kpi_202311 where HCM_TB_GIAHA_025  is not null or HCM_TB_GIAHA_024  is not null;
                                                                                               
---danh sach CA gia han chung tu
  update ttkd_bsc.ct_bsc_giahan_cntt a set TIEN_KHOP = 1
                ---select * from ttkd_bsc.ct_bsc_giahan_cntt a
                where thang = 202311 and a.phieutt_id is not null and TIEN_KHOP = 0
                                and ht_tra_id in (2, 4, 5, 9)
                                and  exists 
                                                                    (
                                                                        select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                                                        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                                                                                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
                                                                        where  phieu_id = a.phieutt_id
                                                                        group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
                                                                        having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
                                                                        )
commit;
rollback;