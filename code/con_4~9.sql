insert into ct_Bsc_giahan_cntt
select* from ct_bsc_giahan_cntt where thang = 202411;
select* from rmp_bsc_phong where thang = 202411;
select* from ttkd_Bsc.nhanvien where thang  = 202411; and ma_kpi = 'HCM_TB_GIAHA_024';
commit;
rollback;
insert into ct_Bsc_giahan_cntt(thang, ma_tb,thuebao_id, manv_giao, ma_to,loaitb_id, thang_ktdc_Cu, ma_pb,tien_khop,tien_dc_Cu);
select distinct 202411, ma_tb, thuebao_id, nhanvien_giao, ma_to, loaitb_id,202411,
(select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1)  ma_pb, 0,-1
from ttkdhcm_ktnv.giahan_cntt_theoky a where a.thuebao_id in ( select b.thuebao_id from (
select distinct a.ma_tb,a.thuebao_id,a.kq_import,a.thang_kt,a.ma_tt
from ttkdhcm_ktnv.kq_loaitru a where user_import=61 and thang_kt=202411 
and trunc(ngay_import) =to_date('09/12/2024','dd/mm/yyyy') ) b  ) and thang_kt=202411
and ghichu_pbh like '%418%' and nhanvien_giao = 'VNP017793';
commit;
create table bangluong_kpi as select* from bangluong_kpi where thang = 
;
insert into ct_Bsc_giahan_cntt
select * from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411;
select* from ct_Bsc_giahan_cntt where thang = 202411 ;and thuebao_id in (select thuebao_id from ttkd_Bsc.ct_Bsc_giahan_cntt where thang = 202411 );
select* from ttkd_bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027';
delete from tl_giahan_tratruoc where thang = 202411 and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_026');
commit;
 -- CA 
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
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
                from ct_bsc_giahan_cntt a
                where a.thang = 202411 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) and thang_ktdc_cu in (202411)--(147,148 )  and manv_GIAO = 'VNP017400' ------------n------------ 
               group by thang, THANG_KTDC_CU, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                    ) a 
where ma_pb is not null
group by a.thang, a.THANG_KTDC_CU, a.ma_nv, a.ma_to, a.ma_pb;
commit;

---------------Chay binh quan To, Phong -----
;

insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from tl_giahan_tratruoc
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_024') --and  ma_to ='VNP0702302'  --, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;
commit;
rollback;
select* from bk_ca;
SELECT* from tl_giahan_tratruoc where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh in  ('KPI_PB','KPI_TO') ;
select* from tl_giahan_tratruoc a
    join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and b.thang = 202411 and b.donvi = 'TTKD'
where a.thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB' 
ORDER BY TEN_NV;
ROLLBACK;
select sum(da_giahan_dung_hen) from tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 
select * from tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB'; 

select * FROM  ttkd_bsc.blkpi_dm_to_pgd WHERE MA_KPI = 'HCM_TB_GIAHA_024';
commit;
ROLLBACK;
delete from tl_giahan_tratruoc where thang = 202411  and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_PB';
--- 9 PHONG BHKV
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_024' AND DICHVU = 'Phong'    )
            b on a.thang = b.thang 
and a.ma_pb = b.ma_pb
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') AND A.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
--- 3 PHONG DOANH NGHIEP
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_027'    )
            b on a.thang = b.thang and a.ma_pb = b.ma_pb AND A.MA_TO = B.MA_TO
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_024') AND A.MA_PB IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
ORDER BY A.MA_PB
;
commit;
-- TEN MIEN 
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE)
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
                                    from ct_bsc_giahan_cntt a
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

insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from tl_giahan_tratruoc
            where thang = 202411 and MA_KPI in ('HCM_TB_GIAHA_026') --and ma_pb ='VNP0702400'-- AND MA_TO = 'VNP0701603'
            group by THANG, MA_KPI, MA_TO, MA_PB
;

SELECT DISTINCT MA_PB FROM tl_giahan_tratruoc a where a.thang = 202411 and a.MA_KPI in ('HCM_TB_GIAHA_026') and loai_tinh = 'KPI_TO';
---- PHO GIAM DOC
--- 9 phong bhkv
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
                       WHERE ma_kpi = 'HCM_TB_GIAHA_026' AND DICHVU = 'Phong'    )
            b on a.thang = b.thang 
and a.ma_pb = b.ma_pb
where a.thang = 202411 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_026') AND A.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500') --, 'HCM_TB_GIAHA_025') --and b.ma_nv = 'VNP017819'
group by a.THANG, a.MA_KPI, a.MA_PB, b.ma_nv
;
COMMIT;
--- 3 PHONG DOANH NGHIEP
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, LDP_PHUTRACH, HESO_giao)
select a.THANG, 'KPI_PB', a.MA_KPI,  b.ma_nv, null MA_TO, a.MA_PB, sum(TONG) tong , sum(DA_GIAHAN_DUNG_HEN) DA_GIAHAN_DUNG_HEN,  sum(DTHU_DUYTRI)DTHU_DUYTRI, 
        sum(DTHU_THANHCONG_DUNG_HEN) DTHU_THANHCONG_DUNG_HEN, round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) tyle, b.ma_nv, sum(heso_giao)heso_giao
from tl_giahan_tratruoc a left join (select distinct ma_kpi, ma_to,ma_pb, ma_nv, thang from ttkd_bsc.blkpi_dm_to_pgd 
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
update bangluong_kpi a 
set 
 giao =  (select (sum(tong))
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
  , thuchien = (select (sum(DA_GIAHAN_DUNG_HEN)) 
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_024') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_024' ;
commit;
--- to truong
update bangluong_kpi a 
set
giao =  (select (sum(tong)) from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN))  from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
, tyle_thuchien  = (select TYLE from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_024') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_024';
--- pho giam doc

update bangluong_kpi a 
set tyle_thuchien = (select TYLE from tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        ,thuchien = (select DA_GIAHAN_DUNG_HEN from tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
        , giao = (select tong from tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_024')  
                                            and thang = 202411 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_024';
commit;

update bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 5
                        when (tyle_Thuchien) > 75 and tyle_thuchien < 100 then 1
                        when (tyle_Thuchien) = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411; and ma_Nv not in (select ma_nv from sosanh_V2);
commit;
select* from sosanh_V2 where ma_nv in ('VNP053484','VNP039527','CTV076270','VNP030414','VNP017798','CTV071063'    );
update bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 65 and tyle_thuchien < 75 then 1  
                        when (tyle_Thuchien) >= 45 and tyle_thuchien < 65 then 3
                        when (tyle_Thuchien) >= 30 and tyle_thuchien < 45 then 5
                        when (tyle_Thuchien) < 30 then 7 end
where ma_kpi ='HCM_TB_GIAHA_024' AND THANG = 202411  ;and ma_Nv not in (select ma_nv from sosanh_V2);
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv in('VNP053484','VNP039527','CTV076270','VNP030414','VNP017798','CTV071063'    );
commit;
--- 027
update bangluong_kpi a 
set thuchien = (select da_Giahan_dung_Hen from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
    ,tyle_thuchien = (select TYLE from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
     , giao = (select tong from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')                                                                 
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_027') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_027';
commit;
update bangluong_kpi a 
set tyle_thuchien = (select TYLE from tl_giahan_tratruoc 
                where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
                and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024'),
    thuchien = (select da_Giahan_dung_hen from tl_giahan_tratruoc 
    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
    , giao  = (select tong from tl_giahan_tratruoc 
    where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_PB' 
    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                            and ma_kpi in ('HCM_TB_GIAHA_027')  
                                            and thang = 202411 and MA_VTCV = a.MA_VTCV)and ma_kpi = 'HCM_TB_GIAHA_027';
  rollback;
  commit;

-- MDHT
update bangluong_kpi --set diem_cong = 5 where thang = 202410 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_To = 'VNP0702416';
set mucdo_hoanthanh = case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027' ;
COMMIT;
  
--- QLDL
update bangluong_kpi a 
set 
 giao = (select (sum(tong)) 
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN)) 
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
 ,tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                                    from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                                                                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_028') and thang = 202411 and MA_VTCV = a.MA_VTCV)
and ma_kpi = 'HCM_TB_GIAHA_028' ;

update bangluong_kpi a 
set thuchien = (select DA_GIAHAN_DUNG_HEN from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                            and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
   , giao =       (select tong from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                            and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')                                                    
, tyle_thuchien  = (select TYLE from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_TB_GIAHA_024')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_028') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_028' and ten_nv like '%Kiên';
                                create table bangluong_kpi_1612 as select* from ttkd_Bsc.bangluong_kpi ;
--- DIEM CONG DIEM TRU
update bangluong_kpi  
set diem_cong = case when tyle_thuchien >= 100 then 7
                        when  tyle_thuchien >= 75 and tyle_thuchien < 100 then 3
                        when  tyle_thuchien = 75 then 0
                        end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202411   ;

update bangluong_kpi  
set diem_tru = case when  tyle_thuchien >= 65 and tyle_thuchien < 75 then 3  
                        when  tyle_thuchien >= 45 and tyle_thuchien < 65 then 5
                        when  tyle_thuchien >= 30 and tyle_thuchien < 45 then 7
                        when  tyle_thuchien< 30 then 10 end
where ma_kpi ='HCM_TB_GIAHA_028' AND THANG = 202411 ;

COMMIT;
select* from final_ctu_tien where thang = 202411;
update bangluong_kpi 
set diem_cong = 0, diem_tru=0
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_028' and ma_vtcv = 'VNP-HNHCM_KHDN_4' and ma_nv != 'VNP017445';
delete from bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_028' and ma_vtcv = 'VNP-HNHCM_KHDN_4' and ma_nv != 'VNP017445';
--- TEN MIEN
--- NHAN VIEN
update bangluong_kpi a 
set 
 tyle_thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_026') and thang = 202411 and MA_VTCV = a.MA_VTCV) 
--and ma_pb in ('VNP0702400','VNP0701100')
and ma_kpi = 'HCM_TB_GIAHA_026';
--- TO TRUONG
update bangluong_kpi a 
set  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
, giao =  (select round(sum(tong))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' 
                    and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')
 , tyle_thuchien= (select TYLE from tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_026')                                                                               
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_TB_GIAHA_026') 
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' 
;
--- PHO GIAM DOC
update bangluong_kpi a 
set tyle_thuchien = (select TYLE from tl_giahan_tratruoc 
                where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_026'
                and ma_nv = a.ma_nv  )
    ,  thuchien = (select round(sum(DA_GIAHAN_DUNG_HEN))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
        , giao =  (select round(sum(tong))
                   from tl_giahan_tratruoc where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_PB' 
                    and ma_nv = a.ma_nv and ma_kpi = 'HCM_TB_GIAHA_026')
where thang = 202411 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where giamdoc_phogiamdoc = 1 
                                and ma_kpi in ('HCM_TB_GIAHA_026')  
                                and thang = 202411 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_TB_GIAHA_026' ;
;

update bangluong_kpi  
set diem_cong = case when (tyle_Thuchien) >= 100 then 2
                           when (tyle_Thuchien) >= 95 and tyle_thuchien < 100 then 1
                        end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411; 
;

update bangluong_kpi  
set diem_tru = case when (tyle_Thuchien) >= 80 and tyle_thuchien < 95 then 1
                        when (tyle_Thuchien) < 80 then 2 end
where ma_kpi ='HCM_TB_GIAHA_026' AND THANG = 202411 ;
  ;
COMMIT;
select*  from  ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027'; and ma_vtcv = 'VNP-HNHCM_KHDN_4' and ma_nv != 'VNP017445';
--- so sanh 
drop table sosanh_v2;
create table sosanh_v2 as
select a.ma_nv, a.ten_vtcv, a.ma_kpi,a.giao, b.giao giao_Cl,  a.diem_cong,b.diem_cong cong_cl, a.diem_tru, b.diem_tru tru_cl, a.mucdo_hoanthanh, b.mucdo_hoanthanh mdht_cl
from ttkd_Bsc.bangluong_kpi a
    join bangluong_kpi b on a.thang =b.thang and a.ma_kpi = b.ma_kpi and a.ma_nv = b.ma_nv
where a.thang = 202411 and a.ma_kpi in ('HCM_TB_GIAHA_026','HCM_TB_GIAHA_024','HCM_TB_GIAHA_027','HCM_TB_GIAHA_028') 
    and (nvl(a.diem_cong,0) <> nvl(b.diem_cong,0)  or nvl(a.diem_tru,0) <> nvl(b.diem_tru,0)  or nvl(a.mucdo_hoanthanh,0)  <> nvl(b.mucdo_hoanthanh,0) )
;

-- th1: cong it thanh cong nhieu
-- 1 nv
select* from sosanh_V2 where (cong_cl)!= (diem_Cong);
select* from bangluong_kpi  where thang  =202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_Nv = 'VNP017658';
commit;
rollback;
-- th2: tu tru thanh cong
-- 13 nv
select* from sosanh_V2 where tru_cl is not null and diem_cong is not null and ma_kpi != 'HCM_TB_GIAHA_027';

update ttkd_Bsc.bangluong_kpi set diem_Cong = 0, diem_tru = 0 
where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv in 
(select ma_nv from sosanh_V2 where tru_cl is not null and diem_cong is not null and ma_kpi = 'HCM_TB_GIAHA_024');

update ttkd_Bsc.bangluong_kpi set diem_Cong = 0, diem_tru = 0 
where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_026' and ma_nv in 
(select ma_nv from sosanh_V2 where tru_cl is not null and diem_cong is not null and ma_kpi = 'HCM_TB_GIAHA_026');

update ttkd_Bsc.bangluong_kpi set diem_Cong = 0, diem_tru = 0 
where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_028' and ma_nv in 
(select ma_nv from sosanh_V2 where tru_cl is not null and diem_cong is not null and ma_kpi = 'HCM_TB_GIAHA_028');
-- tu khong cong thanh cong
-- 3 nv
select * from sosanh_V2 where cong_cl = 0 and diem_Cong is not null;
update ttkd_Bsc.bangluong_kpi set diem_cong = 0 where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_024' and ma_nv in ('CTV021955','VNP017798','VNP039527');
commit;
-- th3: tu tru nhieu thanh tru it
select* from sosanh_V2 where nvl(tru_cl,0)!= nvl(diem_tru,0) and diem_cong is null;
-- 28 nhan vien
select* from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_028';
--- 028 : 2 nv
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_028' ;and ma_nv in (select ma_nv from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_028'); 
-- 024 : 16 nv
select* from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_024';
-- bi tru -> 0
and ma_nv in (select ma_Nv from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_024' and tru_cl is not null);
--- dudoc cong -> giu nguyen 
select * from ttkd_bsc.bangluong_kpi where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_024' 
and ma_nv in (select ma_Nv from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_024' and cong_cl is not null);
update ttkd_Bsc.bangluong_kpi set diem_cong = 0 where ma_nv = 'VNP017793' and ma_kpi = 'VNP017793';
commit;
--- 026: 14 nv
select* from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_026';
update ttkd_Bsc.bangluong_kpi set diem_Cong = 0 where thang = 202411 and  ma_kpi = 'HCM_TB_GIAHA_026' 
and ma_nv in (select ma_Nv from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_026');
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and  ma_kpi = 'HCM_TB_GIAHA_026' 
and ma_nv in (select ma_Nv from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_026');
-- 027: 13 nv
select* from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_027';
--update ttkd_Bsc.bangluong_kpi set mucdo_Hoanthanh = 100
where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' 
;
select* from sosanh_V2 where ma_kpi = 'HCM_TB_GIAHA_027'  and ma_nv not in (select ma_Nv from sosanh_V2 where mdht_cl<100
and mucdo_hoanthanh > 100);
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027';
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' ;
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 102.586 where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_Nv = 'VNP027158';
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh = 108.208 where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_027' and ma_Nv = 'VNP009387';

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_Nv ='VNP022082' and ma_kpi ='HCM_TB_GIAHA_027';
select * from  ttkd_bsc.ct_bsc_ptm where loaitb_id =116 and ma_kh = 'HCM015074704' and loaihd_id = 1 and ma_tiepthi = 'VNP017793'
and thang_ptm >= 202401;
rollback;
commit;
--- luu bang tam
insert into bangluong_kpi ;
select -202411 THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG, TYTRONG, DONVI_TINH, CHITIEU_GIAO, GIAO, 
THUCHIEN, TYLE_THUCHIEN, MUCDO_HOANTHANH, DIEM_CONG, DIEM_TRU, GHICHU, NGAY_PUBLIC, NGAY_DEADLINE, MANV_PUBLIC, MANV_APPLY, NGAY_APPLY
from bangluong_kpi where  ma_Nv in ('VNP017793','VNP017658','VNP022082')
and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_027') and thang = 202411;

update ttkd_bsc.bangluong_kpi set giao = giao +151, thuchien = thuchien + 38 where thang = 202411 and ma_Nv in ('VNP017793','VNP017658','VNP022082')
and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_027');

update ttkd_bsc.bangluong_kpi set tyle_Thuchien = round(thuchien*100/giao,2) where thang = 202411 and ma_Nv in ('VNP017793','VNP017658','VNP022082')
and ma_kpi in ('HCM_TB_GIAHA_024','HCM_TB_GIAHA_027');
update ttkd_Bsc.bangluong_kpi set mucdo_hoanthanh =  case when tyle_thuchien >= 100 then 120 
                        when tyle_Thuchien >= 75 and tyle_Thuchien < 100 then 100+0.6*(tyle_Thuchien-75)
                        when tyle_Thuchien >= 65 and tyle_Thuchien < 75  then 85+1.4*(tyle_Thuchien-65)
                        when tyle_Thuchien >= 45 and tyle_Thuchien < 65 then 50 + 1.6*(tyle_Thuchien-45) 
                        when tyle_Thuchien >= 30 and tyle_Thuchien < 45  then 20 + 1.8*(tyle_Thuchien-30) 
                        else 0 end
where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_027'  and ma_Nv in  ('VNP017658','VNP022082');
commit;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411  and ma_kpi ='HCM_TB_GIAHA_023' and ma_nv in ('VNP017658','VNP022082'); (
select ma_nv from ttkd_Bsc.blkpi_dm_to_pgd where thang = 202411 and ma_kpi ='HCM_TB_GIAHA_023' and ten_to is null);
