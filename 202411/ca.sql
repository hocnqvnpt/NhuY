select* from ttkd_bsc.ct_bsc_giahan_cntt;
----- TAO BANG CHI TIET BSC
insert into ttkd_bsc.ct_bsc_giahan_cntt
select
202411 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
                , a.ma_to, (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb,
                a.nhanvien_giao, ten_donvi_giao, a.MANV_HRM_TH, a.TEN_PBH_TH PHONG_TH, null, null, nv.ma_nv MANV_THPHUC, nv1.ten_pb PHONG_THPHUC
                , nv2.ma_nv MANV_GT, nv3.ma_nv MANV_THUNGAN, null, null, a.thang_kt, a.DATCOC_CSD, null, a.ma_gd, null, a.thang_Bd_moi, null SO_THANGDC, null, ptt.tien, ptt.vat, ptt.ngay_tt, a.NGAY_NGANHANG
                , ptt.SOSERI, ptt.SERI, KENHTHU, ten_nh ten_nganhang, ht_tra ten_ht_tra, tttb.trangthai_tb , a.thuebao_id, a.loaitb_id, null, null, null, null, db.khachhang_id, a.phieutt_id, ptt.ht_tra_id, ptt.kenhthu_id
                , case when a.phieutt_id is null then null
                            when ptt.ht_tra_id in (1,7,204) then 1
                            when ptt.ht_tra_id in (2,5,4,9) then 0 else null 
                            end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
                , a.THUEBAO_ID_KHAC, a.MATB_KHAC, null KIEULD_ID
from ttkdhcm_ktnv.ghtt_chotngay_271 a
            left join ttkd_bsc.nhanvien b on a.MANV_HRM = b.ma_nv and b.thang = 202411 and b.donvi='TTKD' -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien nv1 on nv.ma_nv = nv1.ma_nv AND NV1.THANG = 202411 AND NV1.DONVI = 'TTKD'-- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
where ngay_chot =to_date('20241202','yyyymmdd') and thang_kt = 202411 and a.loaitb_id in (147,148);
and a.thang_kt in (202411)   and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 ) 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) );
                   commit;
                   -- (55 ,80 ,116 ,117,132,140,154,181,288,318 ) -- a.loaitb_id in (147,148) --
order by a.phieutt_id 
;  

-- update chung tu
update ttkd_bsc.ct_bsc_giahan_cntt a set TIEN_KHOP = 1
--- select * from ttkd_bsc.ct_bsc_giahan_cntt a
where thang = 202411 and a.phieutt_id is not null and TIEN_KHOP = 0 and  a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 ) 
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
);
commit;

--- id600

update ttkd_bsc.CT_bSC_GIAHAN_CNTT a set tien_khop = 1
-- select* from ttkd_bsc.CT_bSC_GIAHAN_CNTT a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411-- and loaitb_id in (147,148)--and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- phieu con co tien bang
update ttkd_bsc.CT_bSC_GIAHAN_CNTT a set tien_khop = 1
-- select* from ttkd_bsc.CT_bSC_GIAHAN_CNTT a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411-- and loaitb_id in (147,148)--and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- bang cha du tien
update ttkd_bsc.CT_bSC_GIAHAN_CNTT a set tien_khop = 1
-- select* from ttkd_bsc.CT_bSC_GIAHAN_CNTT a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202411 --and loaitb_id in (147,148) --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
       -- and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;

----B2---;

select * from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202405 and tien_khop = 0;

--------1------C4---Tyle thue bao gia han TC thang kt thang T và T+1
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202411 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
commit;
--- cap nhat tay
update ttkd_Bsc.ct_bsc_giahan_cntt set tien_khop = 1
where thang = 202411 and ma_tb='hcm_tmvn_00002032';
commit;
update ttkd_Bsc.ct_bsc_giahan_cntt set tien_khop = 1
where thang = 202411 and ma_tb in ('hcm_ca_ivan_00019983','hcm_ivan_00029682');
--- kiem tra du lieu
select* from ttkd_bsc.ct_bsc_giahan_cntt x
where thang = 202411 and tien_khop = 0 and  exists (select 1 from ttkdhcm_ktnv.ghtt_chotngay_271 a
                                                where  ngay_chot =to_date('20241202','yyyymmdd') --and thang_kt = 202411 and a.loaitb_id in (147,148)
and a.thang_kt in (202411)   and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 ) 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) )
                   and ma_tb = x.ma_tb and tien_khop=1);
-- vet
update ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop = 1 where thang = 202411 and ma_gd in ('HCM-LD/01669720','HCM-LD/01805418');
commit;
select * from ttkd_bsc.ct_bsc_giahan_cntt x
where thang = 202411 and tien_khop = 0 and  exists (select 1 from ttkdhcm_ktnv.ghtt_chotngay_271 a
                                                where  ngay_chot =to_date('20241202','yyyymmdd') --and thang_kt = 202411 and a.loaitb_id in (147,148)
and a.thang_kt in (202411)   and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 ) 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) )
                   and ma_tb = x.ma_tb and tien_khop=1);
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_phieutt_hd_1 where ma in (select ma_gd from ttkd_bsc.ct_bsc_giahan_cntt x
where thang = 202411 and tien_khop = 0 and  exists (select 1 from ttkdhcm_ktnv.ghtt_chotngay_271 a
                                                where  ngay_chot =to_date('20241202','yyyymmdd') --and thang_kt = 202411 and a.loaitb_id in (147,148)
and a.thang_kt in (202411)   and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 ) 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) )
                   and ma_tb = x.ma_tb and tien_khop=1));
update ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop = 1 where ma_gd in ('00862640','HCM-TT/02814473')  and thang=202411;
   commit; 

 -- CA 
 delete from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_024';
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , 'HCM_TB_GIAHA_024' ma_kpi
             , a.ma_nv, a.ma_to, a.ma_pb
               , count(thuebao_id) tong
              , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
              , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
              , sum(dthu) DTHU_thanhcong
              , round(sum(case when tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle
from       (select thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                , sum(a.tien_khop)
                from ttkd_bsc.ct_bsc_giahan_cntt a
                where a.thang = 202411 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and thang_ktdc_cu in (202411)--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb;
commit;

---------------Chay binh quan To, Phong -----
;

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_024') --and  ma_to ='VNP0702302'  --, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;
rollback;
select* from bk_ca;
SELECT* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh in  ('KPI_PB','KPI_TO') ;
select* from ttkd_bsc.tl_giahan_tratruoc a
    join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and b.thang = 202411 and b.donvi = 'TTKD'
where a.thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB' 
ORDER BY TEN_NV;
ROLLBACK;
select sum(da_giahan_dung_hen) from TTKD_BSC.tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 
select * from TTKD_BSC.tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 

select * FROM  ttkd_bsc.blkpi_dm_to_pgd WHERE MA_KPI = 'HCM_TB_GIAHA_024';
commit;
ROLLBACK;
delete from TTKD_BSC.tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB';
--- 9 PHONG BHKV
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_024' AND DICHVU = 'Phong'    )
            b on a.thang = b.thang 
and a.ma_pb = b.ma_pb
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') AND A.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
--- 3 PHONG DOANH NGHIEP
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_027'    )
            b on a.thang = b.thang and a.ma_pb = b.ma_pb AND A.MA_TO = B.MA_TO
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') AND A.MA_PB IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB
;
commit;
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
                                    where a.thang = 202411 and loaitb_id in (147,148) and thang_ktdc_cu = 202411 --and ma_pb ='VNP0702400'--  AND MANV_GIAO = 'VNP017400'------------n------------ 
                                   group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null 
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb ;
order by 2;
rollback;
commit;
;
---------------Chay binh quan To, Phong -----

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_026') --and ma_pb ='VNP0702400'-- AND MA_TO = 'VNP0701603'
            group by THANG, MA_KPI, MA_TO, MA_PB
;

SELECT DISTINCT MA_PB FROM ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202411 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_TO';
---- PHO GIAM DOC
--- 9 phong bhkv
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_026' AND DICHVU = 'Phong'    )
            b on a.thang = b.thang 
and a.ma_pb = b.ma_pb
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') AND A.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
COMMIT;
--- 3 PHONG DOANH NGHIEP
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_026'    )
            b on a.thang = b.thang and a.ma_pb = b.ma_pb AND A.MA_TO = B.MA_TO
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') AND A.MA_PB IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB
;
;
commit;
--- do bang luong CA
--- nhan vien
update TTKD_BSC.bangluong_kpi a 
set 
 giao =  (select (sum(tong))
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
  , thuchien = (select (sum(DA_GIAHAN_DUNG_HEN)) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_024' ;
commit;
--- to truong
update TTKD_BSC.bangluong_kpi a 
set
giao =  (select (sum(tong)) from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN))  from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_024';
--- pho giam doc

update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        ,thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        , giao = (select tong from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang = 202411 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_024';
commit;
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 5
                        when (tyle_Thuchien) > 75 and tyle_thuchien < 100 then 1
                        when (tyle_Thuchien) = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411 and ma_nv not in (select ma_Nv from sosanh) ;
commit;
update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 65 and tyle_thuchien < 75 then 1  
                        when (tyle_Thuchien) >= 45 and tyle_thuchien < 65 then 3
                        when (tyle_Thuchien) >= 30 and tyle_thuchien < 45 then 5
                        when (tyle_Thuchien) < 30 then 7 end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411  and ma_nv not in (select ma_Nv from sosanh) ;

commit;
--- 027
update TTKD_BSC.bangluong_kpi a 
set thuchien = (select da_Giahan_dung_Hen from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
    ,tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
     , giao = (select tong from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')                                                                 
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_027') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_027';
commit;
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024'),
    thuchien = (select da_Giahan_dung_hen from ttkd_bsc.tl_giahan_tratruoc 
    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
    , giao  = (select tong from ttkd_bsc.tl_giahan_tratruoc 
    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_027')  
                                            and thang = 202411 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_027';
  rollback;
  commit;

-- MDHT
update ttkd_Bsc.bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027' ;
COMMIT;
  
--- QLDL
update TTKD_BSC.bangluong_kpi a 
set 
 giao = (select (sum(tong)) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN)) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_028') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_028' ;

update TTKD_BSC.bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                            and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
   , giao =       (select tong from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                            and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')                                                    
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_028') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_028' and ten_nv like '%Kiên';
--- DIEM CONG DIEM TRU
update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when tyle_thuchien >= 100 then 7
                        when  tyle_thuchien > 75 and tyle_thuchien < 100 then 3
                        when  tyle_thuchien = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202411 ;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when  tyle_thuchien >= 65 and tyle_thuchien < 75 then 3  
                        when  tyle_thuchien >= 45 and tyle_thuchien < 65 then 5
                        when  tyle_thuchien >= 30 and tyle_thuchien < 45 then 7
                        when  tyle_thuchien< 30 then 10 end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202411 ;

COMMIT;
update ttkd_Bsc.bangluong_kpi 
set diem_cong = 0, diem_tru=0
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_028' and ma_vtcv = 'VNP-HNHCM_KHDN_4' and ma_nv != 'VNP017445';
delete from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_028' and ma_vtcv = 'VNP-HNHCM_KHDN_4' and ma_nv != 'VNP017445';
--- TEN MIEN
--- NHAN VIEN
update TTKD_BSC.bangluong_kpi a 
set 
 tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang = 202411 and MA_VTCV = a.MA_VTCV) 
--and ma_pb in ('VNP0702400','VNP0701100')
and ma_kpi = 'HCM_TB_GIAHA_026';
--- TO TRUONG
update TTKD_BSC.bangluong_kpi a 
set  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
 , tyle_thuchien= (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')                                                                               
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' 
;
--- PHO GIAM DOC
update TTKD_BSC.bangluong_kpi a 
set tyle_thuchien = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_026'
                and ma_nv = a.ma_nv  )
    ,  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
        , giao =  (select round(sum(tong))
                   from ttkd_bsc.tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                and ma_kpi in ('HCM_TB_GIAHA_026')  
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' ;
;

update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 2
                           when (tyle_Thuchien) >= 95 and tyle_thuchien < 100 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411; 
;

update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 80 and tyle_thuchien < 95 then 1
                        when (tyle_Thuchien) < 80 then 2 end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411 ;
  ;
COMMIT;
SELECT* FROM TTKD_bSC.BANGLUONG_KPI A WHERE THANG = 202411 AND MA_NV IN (SELECT MA_nV FROM SOSANH WHERE MA_KPI = A.MA_KPI)
AND MA_KPI IN ('HCM_TB_GIAHA_026','HCM_TB_GIAHA_024','HCM_TB_GIAHA_027','HCM_TB_GIAHA_028') AND GIAO IS NOT NULL;