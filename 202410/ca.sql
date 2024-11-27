oselect * from css_hcm.hd_thuebao where ma_tb = 'hvluong1952';
select * from css_hcm.kieu_ld where kieuld_id =128;
select * from css_hcm.db_Thuebao;
select rowid, a.* from ttkdhcm_ktnv.ds_ca_chotden1411_1511 a where thuebao_id = 9558064;
insert into ttkd_bsc.ct_bsc_giahan_cntt 
select* from motne where loaitb_id not in (147,148)
create table bonne as 
select  a.* from ttkd_bsc.ct_bsc_giahan_cntt a   where thang = 202403 and loaitb_id not in (147,148);-- thuebao_id = 9558064;
delete from ttkd_bsc.ct_bsc_giahan_cntt a where thang = 202405; and loaitb_id not in (147,148)-- and thuebao_id = 9558064 and rowid in ('AAC4+qABgAAB3sTAAH', 'AAC4+qABgAAB3sTAAI', 'AAC4+qABgAAB3sTAAJ');
commit;
sel
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
select distinct ht_tra_id_hp from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240602','yyyymmdd');
select manv_Hrm from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240503','yyyymmdd') 
--- KIEM TRA HINH THUC TRA TRUOC KHI CHOT
select distinct ht_Tra_id from css_hcm.phieutt_hd where phieutt_id in
(select * from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot=to_date('20240503','yyyymmdd') and thang_kt in (202404,202405) 
and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318) and phieutt_id is not null);
----- TAO BANG CHI TIET BSC
insert into ttkd_bsc.ct_bsc_giahan_cntt
select 202407 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
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
            left join ttkd_bsc.nhanvien b on a.MANV_HRM = b.ma_nv and B.thang = 202407 and B.donvi = 'TTKD'-- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien nv1 on nv.ma_nv = nv1.ma_nv and NV1.thang = 202407 and NV1.donvi = 'TTKD'-- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
            
where ngay_chot =to_date('20240802','yyyymmdd') and thang_kt = 202407 and a.loaitb_id in (147,148);
--and a.thang_kt in (202407) and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 )  
--                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) );
                   commit;
                   -- (55 ,80 ,116 ,117,132,140,154,181,288,318 ) -- a.loaitb_id in (147,148) --
order by a.phieutt_id 
;  

select distinct a.* from ttkdhcm_ktnv.ghtt_chotngay_271 a where ngay_chot=to_date('20240602','yyyymmdd') and thang_kt in (202405,202406)  and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318 )  
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) ) and tien_khop = 1
                   and ma_Tb in (select ma_tb from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202405 and tien_khop = 0)   ;
create table bu_ca_tl as select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202403 ;
delete from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202403 and loaitb_id  not in  (147,148) 
create table bu_202403 as select* from ttkd_Bsc.bangluong_kpi_202403 ;
commit;
update ttkd_Bsc.
;
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where ngay_chot =to_date('20240802','yyyymmdd') and ma_Tb in ('hcm_tmqt_00000258');
select*  from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202407 and loaitb_id in (147,148) and ma_Tb in ('hcm_ca_00036941',
'hcm_ca_00036952',
'hcm_ca_00036964',
'hcm_ca_00037000',
'hcm_ca_00037001',
'hcm_ca_00037002',
'hcm_ca_00037144',
'hcm_ca_00037639',
'hcm_ca_00037664',
'hcm_ca_00038006',
'hcm_ca_00038009',
'hcm_ca_00038016',
'hcm_ca_00038018',
'hcm_ca_00042692',
'hcm_ca_00042750',
'hcm_ca_00042796',
'hcm_ca_00042799',
'hcm_ca_00042806',
'hcm_ca_00042869',
'hcm_ca_00042947',
'hcm_ca_00043126',
'hcm_ca_00043291',
'hcm_ca_00043295',
'hcm_ca_00043334',
'hcm_ca_00043362',
'hcm_ca_00043371',
'hcm_ca_00043437',
'hcm_ca_00043456',
'hcm_ca_00043459',
'hcm_ca_00043487',
'hcm_ca_00043509',
'hcm_ca_00060097',
'hcm_ca_00060549',
'hcm_ca_00061361',
'hcm_ca_00061364',
'hcm_ca_00061367',
'hcm_ca_00061368',
'hcm_ca_00061369',
'hcm_ca_00061370',
'hcm_ca_00061928',
'hcm_ca_00061929',
'hcm_ca_00064785',
'hcm_ca_00080944',
'hcm_smartca_00069950',
'hcm_ca_00046454',
'hcm_ca_00046570',
'hcm_ca_00046586',
'hcm_ca_00046584',
'hcm_ca_00046566',
'hcm_ca_00046551',
'hcm_ca_00046573',
'hcm_ca_00046547',
'hcm_ca_00046564',
'hcm_ca_00046538',
'hcm_ca_00046569',
'hcm_ca_00046581',
'hcm_ca_00046593',
'hcm_ca_00046577',
'hcm_ca_00046565',
'hcm_ca_00046576',
'hcm_ca_00046536',
'hcm_ca_00046543',
'hcm_ca_00046592',
'hcm_ca_00046561',
'hcm_ca_00046546',
'hcm_ca_00046537',
'hcm_ca_00046588',
'hcm_ca_00046585',
'hcm_ca_00046562',
'hcm_ca_00046541',
'hcm_ca_00046589',
'hcm_ca_00046596',
'hcm_ca_00046594',
'hcm_smartca_00001455',
'hcm_ca_00081125',
'hcm_ca_00081129',
'hcm_ca_00081135',
'hcm_ca_00081167',
'hcm_ca_00081172',
'hcm_ca_00081178',
'hcm_ca_00081226',
'hcm_ca_00081231',
'hcm_ca_00081232',
'hcm_ca_00081234',
'hcm_ca_00081254',
'hcm_ca_00081279',
'hcm_ca_00082558',
'hcm_ca_00081117',
'hcm_ca_00081126',
'hcm_ca_00081138',
'hcm_ca_00081160',
'hcm_ca_00081162',
'hcm_ca_00081183',
'hcm_ca_00081199',
'hcm_ca_00081203',
'hcm_ca_00081207',
'hcm_ca_00081209',
'hcm_ca_00081225',
'hcm_ca_00081227',
'hcm_ca_00081235',
'hcm_ca_00081236',
'hcm_ca_00081238',
'hcm_ca_00081248',
'hcm_ca_00081267',
'hcm_ca_00081268',
'hcm_ca_00081271',
'hcm_ca_00081276',
'hcm_ca_00081277',
'hcm_ca_00081285',
'hcm_ca_00082545',
'hcm_ca_00040495',
'hcm_ca_00040497',
'hcm_smartca_00026916',
'hcm_smartca_00026986',
'hcm_smartca_00026928',
'hcm_smartca_00027044',
'hcm_smartca_00027028',
'hcm_smartca_00027052',
'hcm_smartca_00027088',
'hcm_smartca_00027065',
'hcm_smartca_00027069',
'hcm_smartca_00027043',
'hcm_smartca_00027092',
'hcm_smartca_00027033',
'hcm_smartca_00026972',
'hcm_smartca_00026949',
'hcm_smartca_00027078',
'hcm_smartca_00026978',
'hcm_smartca_00027019',
'hcm_smartca_00027037',
'hcm_smartca_00026953',
'hcm_smartca_00026983',
'hcm_smartca_00026932',
'hcm_smartca_00026922',
'hcm_smartca_00026925',
'hcm_smartca_00026929',
'hcm_smartca_00026930',
'hcm_smartca_00026931',
'hcm_smartca_00026940',
'hcm_smartca_00026937',
'hcm_smartca_00026938',
'hcm_smartca_00026959',
'hcm_smartca_00026942',
'hcm_smartca_00026943',
'hcm_smartca_00026945',
'hcm_smartca_00026944',
'hcm_smartca_00026946',
'hcm_smartca_00027029',
'hcm_smartca_00026950',
'hcm_smartca_00026971',
'hcm_smartca_00026954',
'hcm_smartca_00026960',
'hcm_smartca_00026961',
'hcm_smartca_00026970',
'hcm_smartca_00027035',
'hcm_smartca_00026973',
'hcm_smartca_00027040',
'hcm_smartca_00027027',
'hcm_smartca_00027010',
'hcm_smartca_00027002',
'hcm_smartca_00027003',
'hcm_smartca_00027008',
'hcm_smartca_00027009',
'hcm_smartca_00027032',
'hcm_smartca_00027011',
'hcm_smartca_00027018',
'hcm_smartca_00027024',
'hcm_smartca_00027034',
'hcm_smartca_00027045',
'hcm_smartca_00027048',
'hcm_smartca_00027049',
'hcm_smartca_00027050',
'hcm_smartca_00027051',
'hcm_smartca_00027059',
'hcm_smartca_00027062',
'hcm_smartca_00027063',
'hcm_smartca_00027064',
'hcm_smartca_00027066',
'hcm_smartca_00027067',
'hcm_smartca_00027076',
'hcm_smartca_00027083',
'hcm_smartca_00026995',
'hcm_smartca_00027094',
'hcm_smartca_00027089',
'hcm_smartca_00027077',
'hcm_smartca_00027079',
'hcm_smartca_00027080',
'hcm_smartca_00027093',
'hcm_smartca_00027075',
'hcm_ca_00059635',
'hcm_ca_00059636',
'hcm_ca_00059639',
'hcm_ca_00059640',
'hcm_ca_00059700',
'hcm_ca_00059646',
'hcm_ca_00059647',
'hcm_ca_00059650',
'hcm_ca_00059651',
'hcm_ca_00059664',
'hcm_ca_00059665',
'hcm_ca_00059666',
'hcm_ca_00059667',
'hcm_ca_00059668',
'hcm_ca_00059669',
'hcm_ca_00059670',
'hcm_ca_00059671',
'hcm_ca_00059682',
'hcm_ca_00059683',
'hcm_ca_00059685',
'hcm_ca_00059686',
'hcm_ca_00059687',
'hcm_ca_00059688',
'hcm_ca_00059689',
'hcm_ca_00059691',
'hcm_ca_00059693',
'hcm_ca_00059694',
'hcm_ca_00059695',
'hcm_ca_00059696',
'hcm_ca_00059697',
'hcm_ca_00059698',
'hcm_ca_00059699',
'hcm_ca_00059701',
'hcm_ca_00059702',
'hcm_ca_00059703',
'hcm_ca_00059704',
'hcm_ca_00059706',
'hcm_ca_00059707',
'hcm_ca_00059708',
'hcm_ca_00059709',
'hcm_ca_00059710',
'hcm_ca_00059711',
'hcm_ca_00059713',
'hcm_ca_00059714',
'hcm_ca_00059715',
'hcm_ca_00059716',
'hcm_ca_00059717',
'hcm_ca_00059720',
'hcm_ca_00059723',
'hcm_ca_00059725',
'hcm_ca_00059726',
'hcm_ca_00059727',
'hcm_ca_00059641',
'hcm_ca_00059729',
'hcm_ca_00059730',
'hcm_ca_00059731',
'hcm_ca_00059732',
'hcm_ca_00059733',
'hcm_ca_00059737',
'hcm_ca_00059738',
'hcm_ca_00059739',
'hcm_ca_00059740',
'hcm_ca_00059741',
'hcm_ca_00059742',
'hcm_ca_00059743',
'hcm_ca_00059744',
'hcm_ca_00059745',
'hcm_ca_00059747',
'hcm_ca_00059748',
'hcm_ca_00059754',
'hcm_ca_00059757',
'hcm_ca_00059758',
'hcm_ca_00059759',
'hcm_ca_00059760',
'hcm_ca_00059761',
'hcm_ca_00059762',
'hcm_ca_00059763',
'hcm_ca_00060555',
'hcm_ca_00060558',
'hcm_ca_00060557',
'hcm_ca_00060541',
'hcm_ca_00060559',
'hcm_ca_00060561',
'hcm_ca_00060556');
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
select* from ttkd_Bsc.ct_bsc_Giahan_cntt where thang_ktdc_Cu = 202401 and thang = 202312;
----B2---;

select * from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202405 and tien_khop = 0;

--------1------C4---Tyle thue bao gia han TC thang kt thang T và T+1
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202410 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
commit;
insert into tl_giahan_tratruoc select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202410 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025');
ROLLBACK;
 -- CA 
 VNP0702509	VNP0702500	VNP027256
 selecT* from ttkd_bsc.tl_giahan_tratruoc where ma_kpi in  ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_025') AND THANG = 202403;
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
                where a.thang = 202405 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and thang_ktdc_cu in (202405)--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb
order by 2;
select* from 

commit;

select * from ttkd_Bsc.tl_giahan_tratruoc where thang = 202403 and MA_KPI in ('HCM_TB_GIAHA_024') and loai_tinh = 'KPI_PB' and ma_pb ='VNP0702500';
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

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202405 and MA_KPI in ('HCM_TB_GIAHA_024')--, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;
select* from bk_ca;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202404 and ma_kpi = 'HCM_TB_GIAHA_025' 
select* from ttkd_bsc.ct_bsc_giahan_cntt a where a.thang = 202404 and ma_Tb = 'hcm_smartca_00000316'
ROLLBACK;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where thang_kt is null and ma_kpi = 'HCM_TB_GIAHA_026' 
select hcm_tb_Giaha_026 from ttkd_Bsc.bangluong_kpi_202312
select hcm_Tb_giaha;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
and a.ma_pb = b.ma_pb
where a.thang = 202405 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024')--, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
order by ma_kpi
;

commit;
select* from 
rollback;
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202401 and 'HCM_TB_GIAHA_026' = ma_kpi and loai_tinh = 'KPI_NV';
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
                                    where a.thang = 202405 and loaitb_id in (147,148) and thang_ktdc_cu = 202405--  AND MANV_GIAO = 'VNP017400'------------n------------ 
                                   group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null 
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb ;
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
            where thang = 202405 and MA_KPI in ('HCM_TB_GIAHA_026') -- AND MA_TO = 'VNP0701603'
            group by THANG, MA_KPI, MA_TO, MA_PB
;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2), b.ma_nv, sum(heso_giao)
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd) b on a.thang = b.thang and a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202405 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') --AND B.MA_NV ='VNP017305'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
SELECT* FROM  ttkd_bsc.tl_giahan_tratruoc WHERE THANG = 202403 AND MA_NV = ''
(select * from ttkd_bsc.blkpi_dm_to_pgd WHERE THANG = 202403 AND MA_KPI in ('HCM_TB_GIAHA_026')  AND MA_NV = 'VNP017305');
commit;
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202312 and loaitb_id in (148,147) and thang_ktdc_Cu = 202312 where 
rollback;
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