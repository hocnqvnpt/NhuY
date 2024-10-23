select * from css_hcm.hd_thuebao where ma_tb = 'hvluong1952';
select* from ttkd_Bsc.nhanvien where thang = 202409 ;and ma_kpi = 'HCM_SL_ORDER_001';
SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG= 202409 and ten_vtcv like 'Nhân Viên Outbound%' and loai_ld = 'Huy ??ng';
update ttkd_Bsc.nhanvien set LOAI_LD = 'Huy ??ng' where thang=  202409 and ma_nv in ('VNP017685','VNP016653','VNP017327');
COMMIT;
select* from ct_dongia_chungtu where thang = 202409;
select distinct ma_pb, ten_pb from ttkd_Bsc.nhanvien_202405;
select * from css_hcm.kieu_ld where kieuld_id =128;
select * from css_hcm.db_Thuebao;
select rowid, a.* from ttkdhcm_ktnv.ds_ca_chotden1411_1511 a where thuebao_id = 9558064;
select* from nhuy.ct_Chungtu_quydoi;
insert into ttkd_bsc.ct_bsc_giahan_cntt 
select* from motne where loaitb_id not in (147,148)
create table bonne as 
select  a.* from ttkd_bsc.ct_bsc_giahan_cntt a   where thang = 202403 and loaitb_id not in (147,148);-- thuebao_id = 9558064;
delete from ttkd_bsc.ct_bsc_giahan_cntt a where thang = 202405; and loaitb_id not in (147,148)-- and thuebao_id = 9558064 and rowid in ('AAC4+qABgAAB3sTAAH', 'AAC4+qABgAAB3sTAAI', 'AAC4+qABgAAB3sTAAJ');
commit;
select* from ttkd_Bsc.nhanvien where ten_nv like '%S?n' and thang = 202406;
select THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS
            , MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, PHONG_THPHUC
            , MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI
            , SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG
            , TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, GOI_OLD_ID, KHACHHANG_ID
            , PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU, THUEBAO_KHAC_ID, MA_TB_KHAC, KIEULD_ID
from ttkd_bsc.ct_bsc_giahan_cntt
where thang = 202310;
select distinct ma_pb, ten_pb from ttkd_Bsc.nhanvien_202403
select * from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240402','yyyymmdd') and loaitb_id in (147,148) and thang_kt = 202403 --and donvi_giao = 23-- ma_pb ='VNP0701600'
;
select* from 
select ma_Gd from css_hcm.phieutt_hd where phieutt_id  =8144198;
select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202405; and loaitb_id in (147,148)
update ttkd_bsc.ct_bsc_giahan_cntt set THUEBAO_KHAC_ID = null
where thang = 202310 and THUEBAO_KHAC_ID = 0;
commit;
select * from ttkdhcm_ktnv.DM_MVIEW where MVIEW_NAME= upper('phieutt_hd')

----B1 chot du lieu tu ID 271
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403 and ma_Tb = 'hcm_smartca_00003831'
delete from 
select* from ttkdhcm_ktnv.ghtt_chotngay_271 a
where ngay_chot =to_date('03/02/2024','dd/mm/yyyy')
and a.thang_kt in (202402,202403) and a.loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344','hcm_pharmacy_00000134');
select* from ttkd_bsc.ct_bsc_Giahan_cntt where thang = 202402 and loaitb_id in (147,148) and ma_Tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344','hcm_pharmacy_00000134');
commit;
update ttkd_bsc.ct_bsc_giahan_cntt a set ngay_Tt = to_date('03/03/2024','dd/mm/yyyy')
where thang = 202403 and ma_Tb = 'hcm_smartca_00003831';
commit;
select distinct ht_tra_id_hp from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240902','yyyymmdd');
select manv_Hrm from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240503','yyyymmdd') ;
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202409 and loaitb_id not in (147,148);
--- KIEM TRA HINH THUC TRA TRUOC KHI CHOT
select distinct ht_Tra_id from css_hcm.phieutt_hd where phieutt_id in
(select * from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240503','yyyymmdd') and thang_kt in (202404,202405) 
and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318) and phieutt_id is not null);
update ttkd_Bsc.ct_Bsc_giahan_Cntt a 
set tien_dc_Cu = 
    (
    select datcoc_csd
     from ttkdhcm_ktnv.giahan_cntt_theoky where thang_kt=202409 and loaibo=0
    and loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and a.thuebao_id = thuebao_id
    
    )
where thang = 202409 ;
commit;
delete from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202409;
----- TAO BANG CHI TIET BSC
insert into ttkd_bsc.ct_bsc_giahan_cntt
select
202409 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
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
            left join ttkd_bsc.nhanvien b on a.MANV_HRM = b.ma_nv and b.thang = 202409 and b.donvi='TTKD' -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien nv1 on nv.ma_nv = nv1.ma_nv AND NV1.THANG = 202409 AND NV1.DONVI = 'TTKD'-- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
where ngay_chot =to_date('20241002','yyyymmdd') and thang_kt = 202409 and a.loaitb_id in (147,148);
and a.thang_kt in (202409)   and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 ) 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) );
                   commit;
                   -- (55 ,80 ,116 ,117,132,140,154,181,288,318 ) -- a.loaitb_id in (147,148) --
order by a.phieutt_id 
;  
update ttkd_Bsc.ct_bsc_giahan_cntt x
set tien_khop = 1 
--select* from  ttkd_Bsc.ct_bsc_giahan_cntt x
where thang = 202409 and tien_khop = 0 and exists (select 1 from css_hcm.phieutt_hd a  
                                                        join ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb b on a.so_Ct = b.ma_Ct
                                                        join ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb c on b.chungtu_id = c.chungtu_id
                                                    where x.phieutt_id = a.phieutt_id and x.tien_Thanhtoan +  x.vat != b.tien_ct);
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202409 and tien_khop = 0;
commit;
select* from css_hcm.kenhthu; where kenhthu_id in (10,23);
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202409 and ma_Tb in ('hcm_ivan_00018297','hcm_ca_00041674','hcm_ca_ivan_00018410','hcm_ca_00073053','hcm_ca_00082837');
commit;
select distinct a.* from ttkdhcm_ktnv.ghtt_chotngay_271 a where ngay_chot=to_date('20240902','yyyymmdd') and thang_kt in (202405,202409)  and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 )  
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) ) and tien_khop = 1
                   and ma_Tb in (select ma_tb from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202405 and tien_khop = 0)   ;
create table bu_ca_tl as select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202403 ;
delete from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202403 and loaitb_id  not in  (147,148) 
create table bu_202403 as select* from ttkd_Bsc.bangluong_kpi_202403 ;
commit;
update ttkd_Bsc.

select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202404;
select from css_hcm.db_thuebao where ma_Tb = 'hcm_ca_00061884';

select * from css_hcm.khuyenmai_dbtb where thuebao_id = 11845931;

select * from css_hcm.ct_phieutt where hdtb_id = 17649679;
select owner, table_name from all_tab_columns where column_name = 'KHOANMUCTT_ID' AND OWNER = 'CSS_HCM';
select* from css_hcm.KHOANMUC_TT where khoanmuctt_id in (11,21)
sele
select *  from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202402 and loaitb_id in (147,148) and ma_tb in ('hcm_tmvn_00003356','hcm_tmvn_00004344');

select* from ttkdhcm_ktnv.ghtt_chotngay_271 where thang_kt in (202403,202404) and ngay_chot =to_date('02/04/2024','dd/mm/yyyy') and nhanvien_giao = 'VNP016563'
select* from  ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403 and maNv_giao = 'CTV021900';
update ttkd_bsc.ct_bsc_giahan_cntt a set ma_to = (Select ma_to from ttkd_Bsc.nhanvien_202403 where a.manv_giao = ma_nv) where ma_to is null and thang = 202403;
update ttkd_bsc.ct_bsc_giahan_cntt a
set ma_To = (select ma_to
from ttkd_bsc.nhanvien_202311 where ma_nv = a.manv_giao)
where thang = 202311 and ma_to is null
commit; 
rollback;
selec
select* from ttkd_bsc.ct_Bsc_Giahan_cntt where thang = 202403 and ma_tb ='hcm_smartca_00003831'-- and loaitb_id in (147,148);

delete from ttkd_bsc.ct_Bsc_Giahan_cntt where thang = 202311
update ttkd_bsc.ct_bsc_giahan_cntt a set TIEN_KHOP = 1
--- select * from ttkd_bsc.ct_bsc_giahan_cntt a
where thang = 202405 and a.phieutt_id is not null and TIEN_KHOP = 0 and  a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 ) 
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
rollback;
update ttkd_Bsc.ct_bsc_Giahan_cntt 
set 
tien_khop = 1
--ma_chungtu = 'VCB21177_20240319'
where thang = 202405 and ma_gd in (select MA_GD from chungtu WHERE ketqua in ('THANH TOAN QUA QR','OK'));
VNP0701603	VNP0701600	VNP016578
select* from ttkd_Bsc.ct_bsc_Giahan_cntt  where thang = 202405 and TIEN_KHOP = 0 AND LOAITB_ID NOT IN (147,148);
update ttkd_Bsc.ct_bsc_Giahan_cntt set tien_khop = 1 where ma_Tb in ('hcm_ca_00056148','hcm_ca_00077204','hcm_ca_00048047') and thang =202405;
delete from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202403 and ma_Kpi = 'HCM_TB_GIAHA_026' AND MA_TO = 'VNP0701603' AND LOAI_tINH = 'KPI_TO';
commit;
select* from ct_bsc_Giahan_cntt where thang =202409;
----B2---;

select * from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202405 and tien_khop = 0;

--------1------C4---Tyle thue bao gia han TC thang kt thang T và T+1
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
commit;
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN, LDP_PHUTRACH, HESO_GIAO)
select THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN, LDP_PHUTRACH, HESO_GIAO
from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
ROLLBACK;
delete
 -- CA 
 VNP0702509	VNP0702500	VNP027256
 selecT* from ttkd_bsc.tl_giahan_tratruoc where ma_kpi in  ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') AND THANG = 202409;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
select thang, 'KPI_NV' LOAI_TINH
             , case when THANG_KTDC_CU = thang then 'HCM_TB_GIAHA_024'
                            when THANG_KTDC_CU > thang then 'HCM_TB_GIAHA_025'
                        else null
                end ma_kpi
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
                where a.thang = 202409 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and thang_ktdc_cu in (202409)--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb;
commit;
order by 2;
delete from ttkd_Bsc.
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where to_number(to_char(ngay_chot,'yyyymmdd')) = 20240902 and ma_Tb in ('hcm_tmqt_00000690','hcm_ca_00078897');
select* from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202409 and ma_Tb in ('hcm_tmqt_00000690');
select* from admin_hcm.nhanvien where ma_nv = 'MEDIAPAY';
SELECT* FROM TTKD_BSC.NHUY_cT_bSC_IPCC_OBGHTT WHERE THANG = 202409 AND NVTUVAN_ID = 8772 AND MANV_THUYETPHUC != 'MEDIAPAY';
commit;
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202409 ;
DELETE from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and MA_KPI in ('HCM_TB_GIAHA_024') and loai_tinh = 'KPI_NV' ;and ma_pb ='VNP0702500';
update  ttkd_Bsc.tl_giahan_tratruoc a
set DA_GIAHAN_DUNG_HEN = 14 ,tyle = round((14/25)*100,2)
where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024') and loai_tinh = 'KPI_TO' and ma_to ='VNP0702509'; -- ma_to is null;
;
select * from ttkd_Bsc.tl_giahan_tratruoc where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') and loai_tinh = 'KPI_TO'-- and ma_to is null;
---------------Chay binh quan To, Phong -----
;
select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_025') --and loai_tinh = 'KPI_PB';
-- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_024') group by ma_kpi, ma_nv having count(ma_to)>1;
select* from  ttkd_bsc.blkpi_dm_to_pgd  where   nvl(dichvu,'Mega+Fiber')='Mega+Fiber' and thang = 202409 and (ma_to in ('VNP0702510','VNP0702407') or 
ten_to like '%T? Bán Hàng Online');
SELECT* FROM TTKD_bSC.NHANVIEN WHERE MA_NV IN ('VNP017699','VNP017621') AND THANG = 202409;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202409 and MA_KPI in ('HCM_TB_GIAHA_024')--, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;
rollback;
select* from bk_ca;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh in  ('KPI_PB','KPI_TO') ;
select* from ttkd_bsc.tl_giahan_tratruoc a
    join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and b.thang = 202409 and b.donvi = 'TTKD'
where a.thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB' 
ORDER BY TEN_NV;
ROLLBACK;
select sum(da_giahan_dung_hen) from TTKD_BSC.tl_giahan_tratruoc where thang = 202409  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 
select * from TTKD_BSC.tl_giahan_tratruoc where thang = 202409  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 

select hcm_tb_Giaha_026 from ttkd_Bsc.bangluong_kpi_202312
select hcm_Tb_giaha;
commit;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                            where dichvu is null or dichvu = 'Dich vu so doanh nghiep')
b on a.thang = b.thang and a.ma_to = b.ma_to 
and a.ma_pb = b.ma_pb
where a.thang = 202409 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
order by ma_kpi
;
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, b.MA_TO, a.MA_PB--, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
     --   sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd where
                                        dichvu not in ('VNP tra sau','VNP tra truoc','Mega+Fiber','MyTV'))
b on a.thang = b.thang and a.ma_to = b.ma_to 
and a.ma_pb = b.ma_pb
where a.thang = 202409 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024');
commit;
select* from TTKD_BSC.BLKPI_DM_TO_PGD WHERE THANG = 202409; AND MA_KPI in ('HCM_TB_GIAHA_024');
rollback;
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202409 and 'HCM_TB_GIAHA_024' = ma_kpi ;loai_tinh = 'KPI_NV';
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
                                    where a.thang = 202409 and loaitb_id in (147,148) and thang_ktdc_cu = 202409 --and ma_pb ='VNP0702400'--  AND MANV_GIAO = 'VNP017400'------------n------------ 
                                   group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null 
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb ;
order by 2;
commit;
;
---------------Chay binh quan To, Phong -----
select *  from ttkd_bsc.tl_giahan_tratruoc where thang = 202409 and MA_KPI in ('HCM_TB_GIAHA_026', 'HCM_TB_GfIAHA_025') and loai_tinh = 'KPI_TO';
-- delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202202 and MA_KPI not like 'DONG%';
select ma_kpi, ma_nv from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and MA_KPI in ('HCM_TB_GIAHA_024') group by ma_kpi, ma_nv having count(ma_to)>1;
DELETE FROM 
 ttkd_bsc.tl_giahan_tratruoc WHERE MA_KPI = 'HCM_TB_GIAHA_026' AND THANG = 202409 AND LOAI_tINH = 'KPI_TO' ;AND MA_TO = 'VNP07012H0'
 ;
 COMMIT;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202409 and MA_KPI in ('HCM_TB_GIAHA_026') --and ma_pb ='VNP0702400'-- AND MA_TO = 'VNP0701603'
            group by THANG, MA_KPI, MA_TO, MA_PB
;
delete from  ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202409 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_PB';
select sum(DA_GIAHAN_DUNG_HEN) from  ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202409 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_PB';
select sum(DA_GIAHAN_DUNG_HEN) from  ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202409 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_TO';
select* from  ttkd_bsc.blkpi_dm_to_pgd  where  thang = 202409 and nvl(dichvu,'Mega+Fiber')='Mega+Fiber'  and (ma_to in ('VNP0702507','VNP0702407') or 
ten_to like '%T? Bán Hàng Online');                                                                                             
;WHERE DICHVU IS NULL OR 
dichvu = 'Dich vu so doanh nghiep'
COMMIT;
delete from  ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202409 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_PB';
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_to, ma_pb,ma_kpi, thang,ma_nv from ttkd_bsc.blkpi_dm_to_pgd WHERE DICHVU IS NULL OR 
dichvu = 'Dich vu so doanh nghiep') b on a.thang = b.thang and a.ma_to = b.ma_to --and a.ma_kpi = b.ma_kpi
where a.thang = 202409 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('') --and a.ma_pb ='VNP0702400'--AND B.MA_NV ='VNP017305'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB
;
commit;
insert into ttkd_bsc.tl_Giahan_tratruoc(THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, TYLE)
select THANG, LOAI_TINH,'HCM_TB_GIAHA_024' MA_KPI, MA_NV, MA_TO, MA_PB, 59 tong, 2 da_giahan_dung_hen, 3.39 tyle
from ttkd_bsc.tl_Giahan_tratruoc where thang = 202409 and ma_kpi=  'HCM_TB_GIAHA_026' and ma_nv ='VNP017793';
insert into 
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202409 and  manv_Giao ='VNP017793';
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, A.MA_TO, a.MA_PB ,DICHVU--, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
--                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_to, ma_pb,ma_kpi, thang,ma_nv,DICHVU from ttkd_bsc.blkpi_dm_to_pgd where dichvu = 'Dich vu so doanh nghiep') b on a.thang = b.thang and a.ma_to = b.ma_to --and a.ma_kpi = b.ma_kpi
where a.thang = 202409 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') ;--AND B.MA_NV ='VNP017305'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB;
select sum(DA_GIAHAN_DUNG_HEN) from ttkd_bsc.tl_giahan_tratruoc a where a.thang = 202409 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_TO'--AND B.MA_NV ='VNP017305'
;
COMMIT;

SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc WHERE THANG = 202403 AND MA_NV = ''
(select * from ttkd_bsc.blkpi_dm_to_pgd WHERE THANG = 202403 AND MA_KPI in ('HCM_TB_GIAHA_026')  AND MA_NV = 'VNP017305');
commit;
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202312 and loaitb_id in (148,147) and thang_ktdc_Cu = 202312 where 
rollback;
select* from ttkd_Bsc.bangluong_kpi where thang = 202409 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_pb ='VNP0701100';
DELETE FROM  ttkd_bsc.tl_giahan_tratruoc  where ma_kpi =  'HCM_TB_GIAHA_026' and thang = 202403  and loai_tinh = 'KPI_' AND MA_NV = 'VNP017305' AND TYLE = 87.5
-----------------------------------
  select * from ttkd_bsc.tl_giahan_tratruoc  where ma_kpi =  'HCM_TB_GIAHA_026' and thang = 202403  and loai_tinh = 'KPI_PB' AND MA_PB = 'VNP0701600'-- MA_nV ='VNP016578'--MA_tO ='VNP0701603'
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