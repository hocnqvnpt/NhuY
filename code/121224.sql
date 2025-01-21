update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 100.516 where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_Nv = 'VNP027158';
commit;
select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202411 and ma_Tb='hcm_ca_00051140';
select* from ttkd_Bsc.bangluong_kpi where thang >= 202409 and ma_kpi = 'HCM_TB_GIAHA_022' AND MA_PB = 'VNP0703000';

SELECT A.*, B.TEN_KH, B.MA_KH FROM TEN_KH A
    LEFT JOIN CSS_HCM.DB_KHACHHANG B ON A.KHACHHANG_ID =B.KHACHHANG_ID;
    
    select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where ma_Ct='VCB_20241101_436057';
    select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_Nv ='VNP017938';
    update ttkd_Bsc.ct_Bsc_giahan_cntt set ma_to = 'VNP0702309' where thang = 202411 and ma_Tb in ('hcm_smartca_00100889','hcm_ivan_00011239');
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To in ('VNP0702308','VNP0702309');
--- thai trung kien -> khong thay doi
--- minh thao 3/3 -> 120 -> 100 (vi 2 TB duoc loai tru)
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' ;
--- my ngoc: 8/10 -> 80 -> mucdo_Hoanthanh = 100 (vi 2 TB duoc loai tru)
;
--- ten mien
update ttkd_Bsc.ct_Bsc_giahan_cntt set tien_khop = 1 where thang = 202411 and ma_Tb = 'hcm_tmqt_00000505';
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and ma_Tb = 'hcm_tmqt_00000505';

commit;
--- update nhanvien
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_nv= 'VNP016881';
update ttkd_Bsc.tl_giahan_tratruoc set da_giahan_dung_Hen = 1, tyle = 100
where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_nv= 'VNP016881';

commit;
select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_to= 'VNP07015E0';
update ttkd_Bsc.tl_giahan_tratruoc set da_giahan_dung_Hen = 4, tyle = 100  where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_to= 'VNP07015E0'
and loai_tinh = 'KPI_TO';

SELECT* FROM TTKD_bSC.BANGLUONG_KPI WHERE THANG = 202411 AND MA_KPI  = 'HCM_TB_GIAHA_026' and ma_to= 'VNP07015E0';
UPDATE TTKD_bSC.BANGLUONG_KPI SET DIEM_tRU = 0, DIEM_CONG = 2  , TYLE_tHUCHIEN = 100, THUCHIEN= GIAO
WHERE THANG = 202411 AND MA_KPI  = 'HCM_TB_GIAHA_026' and ma_to= 'VNP07015E0' AND MA_NV IN ('VNP017608','VNP016881');

SELECT* FROM TTKD_bSC.NHANVIEN WHERE THANG = 202411 AND MA_nV IN ('VNP017479','VNP016881','CTV080937');

---- cuc phuong btcn
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt a 
where thang = 202411 and exists (
select 1 from ttkdhcm_ktnv.ghtt_chotngay_271 where to_Char(ngay_chot,'yyyymmdd') = '20241202' and a.thuebao_id = thuebao_id
and nvl(tien_khop,0) != nvl(a.tien_khop,0));
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 and tien_khop = 0; ma_Tb='hcm_ca_00034804';
--- ten mien
update ttkd_Bsc.ct_Bsc_giahan_Cntt set tien_khop = 1 where thang = 202411 and ma_tb = 'hcm_tmvn_00003503'; --VNP0701503	VNP0701500	VNP017479
commit;
select* from ttkd_bsc.bangluong_kpi where thang = 202411 and ma_TO= 'VNP0701503' and ma_kpi = 'HCM_TB_GIAHA_026';
-- NHAN VIEN
update ttkd_bsc.bangluong_kpi set thuchien = 6, tyle_Thuchien = round(6*100/7,2), diem_Tru = 1
where thang = 202411 and ma_nv= 'VNP017479' and ma_kpi = 'HCM_TB_GIAHA_026';
-- TO TRUONG
-- PGD
select* from ttkd_bsc.BLKPI_DM_TO_PGD where thang = 202411 and ma_TO= 'VNP0701503' and ma_kpi = 'HCM_TB_GIAHA_026';
    
COMMIT;
select* from ttkd_bsc.NHANVIEN where thang = 202411 and TEN_TO = 'T? Kinh Doanh D?ch V? Cntt';ma_TO= 'VNP0701503' ;
--- dn2
Update ttkd_bsc.ct_Bsc_Giahan_cntt set tien_khop = 1 where thang = 202411 and ma_tb = 'hcm_ca_00034804'; --- VNP0702412	VNP0702400	VNP027796
select* from  ttkd_bsc.ct_Bsc_Giahan_cntt  where thang = 202411 and ma_tb = 'hcm_ca_00034804';
commit;
Update ttkd_bsc.ct_dongia_Tratruoc set tien_khop = 1 ,dongia = 0 where thang = 202411 and ma_tb = 'hcm_ca_00034804'; --- VNP0702412	VNP0702400	VNP027796
-- 
select* from bangluong_kpi where thang =202411 and ma_to ='VNP0702412'  and ma_kpi = 'HCM_TB_GIAHA_027';
update ttkd_Bsc.bangluong_kpi set thuchien = 15, tyle_thuchien = 93.75 where thang =202411 and ma_nv ='VNP027796' and ma_kpi = 'HCM_TB_GIAHA_024';
update ttkd_Bsc.bangluong_kpi set thuchien = 23, tyle_thuchien =79.31, mucdo_hoanthanh = 102.586
where thang =202411 and ma_to ='VNP0702412' and ma_kpi = 'HCM_TB_GIAHA_027';

commit;
--- ten mien doi ma to
select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202411 and ma_tb='hcm_tmvn_00005300';
select* from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_to in ('VNP0702308','VNP0702311');
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_to in ('VNP0702308','VNP0702311') and ten_vtcv like 'T%'  and ma_kpi = 'HCM_TB_GIAHA_026' ;
rollback;
update ttkd_Bsc.bangluong_kpi set giao = 3, tyle_thuchien = 100, diem_Cong = 2 , diem_tru = 0 
where ma_Nv = 'VNP017639' and ma_kpi = 'HCM_TB_GIAHA_026'  and  thang = 202411 ;
update ttkd_Bsc.bangluong_kpi set giao = 3, tyle_thuchien = 66.67, diem_Cong = 0 , diem_tru = 2 
where ma_Nv = 'VNP020742' and ma_kpi = 'HCM_TB_GIAHA_026'  and  thang = 202411 ;

commit;
--- benh vien tu du
update ttkd_Bsc.ct_dongia_tratruoc set tien_khop = 1, dongia=0 ;
select* from ttkd_Bsc.ct_dongia_tratruoc  
where thang = 202411 and ma_tb in ('hcm_ca_00067184',
'hcm_ca_00067185',
'hcm_ca_00067186',
'hcm_ca_00067187',
'hcm_ca_00087981',
'hcm_ca_00087982',
'hcm_ca_00067195',
'hcm_ca_00087985',
'hcm_ca_00067200',
'hcm_ca_00067201',
'hcm_ca_00067203',
'hcm_ca_00067204',
'hcm_ca_00087978',
'hcm_ca_00067207',
'hcm_ca_00087979',
'hcm_ca_00067209',
'hcm_ca_00087983',
'hcm_ca_00067214',
'hcm_ca_00067215',
'hcm_ca_00087986',
'hcm_ca_00067217',
'hcm_ca_00067218',
'hcm_ca_00067219',
'hcm_ca_00087976',
'hcm_ca_00067222',
'hcm_ca_00067223',
'hcm_ca_00087984',
'hcm_ca_00067225',
'hcm_ca_00067227',
'hcm_ca_00067228',
'hcm_ca_00067230',
'hcm_ca_00067231',
'hcm_ca_00087980',
'hcm_ca_00067233',
'hcm_ca_00072276',
'hcm_ca_00072325',
'hcm_ca_00064759',
'hcm_ca_00087989',
'hcm_ca_00064765',
'hcm_ca_00064767',
'hcm_ca_00087990',
'hcm_ca_00067234',
'hcm_ca_00072322',
'hcm_ca_00087987',
'hcm_ca_00067236',
'hcm_ca_00067210',
'hcm_ca_00067237',
'hcm_ca_00067246',
'hcm_ca_00067238',
'hcm_ca_00095318',
'hcm_ca_00067239',
'hcm_ca_00067240',
'hcm_ca_00067241',
'hcm_ca_00067242',
'hcm_ca_00067244',
'hcm_ca_00067245',
'hcm_ca_00055364',
'hcm_ca_00080955',
'hcm_ca_00056531',
'hcm_ca_00080949');
commit;

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_Nv = 'VNP017748' and ma_Kpi = 'HCM_TB_GIAHA_024';
select* from ttkd_Bsc.blkpi_dm_to_pgd where  thang = 202411 and ma_to = 'VNP0702510' and ma_Kpi = 'HCM_TB_GIAHA_027';
rollback;
select* from ttkd_Bsc.tl_Giahan_tratruoc where  thang = 202411 and ma_nv in ('VNP027927','CTV086419','VNP017699')and ma_Kpi = 'HCM_TB_GIAHA_024';
select* from ttkd_Bsc.tl_Giahan_tratruoc where  thang = 202411 and  ma_to = 'VNP0702510' and loai_tinh  = 'KPI_TO'and ma_Kpi = 'HCM_TB_GIAHA_024';;

 -- CA 
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
                where a.thang = 202411 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and thang_ktdc_cu in (202411) 
                and manv_giao in ('VNP027927','CTV086419')--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
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
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_024') and  ma_to ='VNP0702510'  --, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;
rollback;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from ttkd_bsc.tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_027'    )
            b on a.thang = b.thang and a.ma_pb = b.ma_pb AND A.MA_TO = B.MA_TO
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') AND A.MA_PB IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
        AND B.MA_NV = 'VNP017699'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB
;
COMMIT;

----
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
and ma_kpi = 'HCM_TB_GIAHA_024' AND MA_nV IN ('VNP027927','CTV086419');

update ttkd_Bsc.bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 5
                        when (tyle_Thuchien) > 75 and tyle_thuchien < 100 then 1
                        when (tyle_Thuchien) = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411 AND MA_nV IN ('VNP027927','CTV086419');
commit;
update ttkd_Bsc.bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 65 and tyle_thuchien < 75 then 1  
                        when (tyle_Thuchien) >= 45 and tyle_thuchien < 65 then 3
                        when (tyle_Thuchien) >= 30 and tyle_thuchien < 45 then 5
                        when (tyle_Thuchien) < 30 then 7 end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411 AND MA_nV IN ('VNP027927','CTV086419');  ;
COMMIT;
Select* from  TTKD_BSC.bangluong_kpi a where  THANG = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' AND MA_nV IN ('VNP017699','VNP016977');
-- TT PGD
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
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_027' and ma_nv in ('VNP017699','VNP016977');
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
                                            and thang = 202411 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_027'  and ma_nv in ('VNP017699','');

update ttkd_Bsc.bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027'  and ma_nv in ('VNP017699','VNP016977') ;   
rollback;
commit;

--- ten mien psg
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi='HCM_TB_GIAHA_026'  and ma_pb = 'VNP0701500' AND GIAO IS NOT NULL;
UPDATE  ttkd_Bsc.bangluong_kpi SET GIAO = THUCHIEN, TYLE_THUCHIEN = 100 where thang = 202411 and ma_kpi='HCM_TB_GIAHA_026'  and ma_pb = 'VNP0701500' AND GIAO IS NOT NULL;


-- 25 TB EVN
delete from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202411 and ma_Tb in ('hcm_ca_00037899',
'hcm_ca_00037129',
'hcm_ca_00042845',
'hcm_ca_00046056',
'hcm_ca_00045893',
'hcm_ca_00061370',
'hcm_ca_00061367',
'hcm_ca_00061369',
'hcm_ca_00061368',
'hcm_ca_00061364',
'hcm_ca_00061361',
'hcm_ca_00048249',
'hcm_ca_00045947',
'hcm_ca_00045950',
'hcm_ca_00045952',
'hcm_ca_00045953',
'hcm_ca_00045936',
'hcm_ca_00046490',
'hcm_ca_00064943',
'hcm_ca_00064942',
'hcm_ca_00061928',
'hcm_ca_00061929',
'hcm_ca_00067318',
'hcm_ca_00067592',
'hcm_ca_00068128');

commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027'; and ten_Nv like '%Hi?n';
update ttkd_Bsc.bangluong_kpi set giao = 168, tyle_Thuchien = round(54*100/168,2) where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_Nv ='VNP022082';
commit;

update ttkd_Bsc.bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027' ;
select* from ttkd_Bsc.blkpi_dm_To_pgd where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702302';
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_SL_COMBO_006' and ma_pb = 'VNP0701100'