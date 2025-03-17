--- bang giao
drop table sldh_bhol;
UPDATE sldh_bhol SET TIEN_KHOP = 1 WHERE HT_tRA_ID = 208;
SELECT* FROM sldh_bhol WHERE TIEN_KHOP = 0;
COMMIT;
select* from sldh_bhol where thang = 202501;where ht_tra_id in(214,216);
create table sldh_bhol as;
select* from giao_oneb c where thang_kt = 202501 and c.thang = 202501;
select* from ttkd_bsc.blkpi_danhmuc_kpi where thang = 202501 and ma_kpi = 'HCM_SL_BHOL_009';
select* from ttkd_bsc.nhanvien where ma_Nv ='CTV088925';
select* from css_Hcm.hd_khachhang where nhanvien_id= 555304;
insert into sldh_bhol
with giao_oneb as (
    select thuebao_id, ngay_ktdc from REPORT.BC_GHTT_BIEU1_CHOT@dataguard where phanvung_id = 28 and to_char(Thang_kt,'yyyymm') = '202501'

)
select
    202501 thang, b.thuebao_id, b.ma_tb, a.ngay_yc, c.ngay_Ktdc, nv.ma_nv, nv.ten_nv, nv.ma_to, nv.ma_pb, p.ngay_tt, a.ma_gd, p.ht_Tra_id, p.kenhthu_id, p.trangthai, p.tien, p.vat, 
    kieuld_id, loaihd_id  , case 
                            when p.ht_tra_id in (1, 7,204,214,216,207) then 1
                            when p.ht_tra_id in (2, 4,5, 6) then 0 else null end tien_khop
from css_Hcm.hd_khachhang a
    join css_hcm.hd_Thuebao b on a.hdkh_id = b.hdkh_id and b.kieuld_id in (551, 550, 24, 13080) --and b.tthd_id <> 7
    join giao_oneb c on b.thuebao_id = c.thuebao_id 
    join admin_hcm.nhanvien_onebss d on a.ctv_id = d.nhanvien_id
    join ttkd_Bsc.nhanvien nv on d.ma_nv = nv.ma_nv and to_number(to_char(a.ngay_yc,'yyyymm'))= nv.thang
    left join css_hcm.ct_phieutt ct on b.hdtb_id = ct.hdtb_id and  khoanmuctt_id = 11 and tien> 0
    left join css_hcm.phieutt_hd p on ct.phieutt_id = p.phieutt_id and p.trangthai = 1 

where to_number(to_char(a.ngay_yc,'yyyymm')) between 202411 and 202501 and 
nv.ma_pb ='VNP0703000' ;
select* from sldh_bhol where thang = 202501;
-- update chung tu

update sldh_bhol set tien_khop = 1-- where ht_Tra_id = 216 and thang = 202501;
where tien_khop = 0 and ma_gd in (select ma from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 ) and thang = 202501;
select* from sldh_bhol where  thang = 202501 and ht_Tra_id is not null and tien_khop is null;
SELECT* FROM giao_oneb where thuebao_id =2350321 ; 8554898TTKD_bSC.BANGLUONG_KPI WHERE  'HCM_SL_BHOL_002' =MA_KPI;
select* from ttkd_bsc.nhanvien where ten_nv like '%H?c';
COMMIT;
delete from sldh_bhol where thang = 202501 and ht_Tra_id is not null and tien_khop is null;;
ROLLBACK;
select* from v_Thongtinkm_all where ma_Tb='hcm_hhha'

delete from sldh_Bhol where thang = 202501 and ht_Tra_id = 208;
commit;

delete from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202501 and ma_Kpi in( 'HCM_SL_BHOL_002','HCM_SL_BHOL_009');

commit;
insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,MA_TO,MA_NV,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen,  DTHU_THANHCONG_DUNG_HEN, TYLE)
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,MA_TO, MA_nV,'HCM_SL_BHOL_002' loai_tinh, 'KPI_NV'
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.ma_to, a.ma_pb,a.ma_nv , sum(tien) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, ma_gd, thuebao_id
                                         from SLDH_BHOL a
                                        where thang = 202501 and ma_nv is not null
                                        group by  thang, a.ma_to, a.ma_pb,ma_gd,  a.ma_nv, thuebao_id
                               )
                group by thang, ma_pb, ma_To, ma_Nv
                order by 2
                ) a
;
commit;

insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                                    , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, HESO_giao)
            select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                                , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2) 
                                , sum(heso_giao) heso
            from ttkd_bsc.tl_giahan_tratruoc
            where thang = 202501 and MA_KPI in ('HCM_SL_BHOL_002') --and  ma_to ='VNP0702302'  --, 'HCM_TB_GIAHA_025') 
            group by THANG, MA_KPI, MA_TO, MA_PB 
;

--- do bang luong
-- nv 
UPDATE TTKD_bSC.BANGLUONG_KPI  a
SET TYLE_THUCHIEN = (select tyle from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv ),
    giao = (select tong from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv),
    thuchien = (select da_giahan_dung_Hen from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv)
WHERE 'HCM_SL_BHOL_002' =MA_KPI AND THANG = 202501 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_BHOL_002')  
                                            and thang = 202501 and MA_VTCV = a.MA_VTCV);

commit;
-- to truong
update TTKD_BSC.bangluong_kpi a 
set
giao =  (select (sum(tong)) from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_SL_BHOL_002')
,thuchien = (select (sum(DA_GIAHAN_DUNG_HEN))  from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_SL_BHOL_002')
, tyle_thuchien  = (select TYLE from ttkd_bsc.tl_giahan_tratruoc 
                                                            where thang =  to_char(trunc(sysdate, 'month')-1, 'yyyymm')  and loai_tinh ='KPI_TO' 
                                                                                and ma_to = a.ma_to and ma_pb = a.ma_pb and ma_kpi = 'HCM_SL_BHOL_002')
where thang = 202501 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ( 'HCM_SL_BHOL_002') 
                                and thang = 202501 and MA_VTCV = a.MA_VTCV) and ma_kpi = 'HCM_SL_BHOL_002';
commit;
----
UPDATE TTKD_bSC.BANGLUONG_KPI  a
SET  MUCDO_HOANTHANH = case when tyle_Thuchien >90 and tyle_Thuchien <= 100 then 120
                        when tyle_Thuchien =90 then round( 100*tyle_Thuchien/90,2)
                        else  ROUND(50*tyle_Thuchien/90,2) end
WHERE 'HCM_SL_BHOL_002' =MA_KPI AND THANG = 202501;
select * from ttkd_Bsc.BANGLUONG_KPI  WHERE 'HCM_SL_BHOL_002' =MA_KPI AND THANG = 202501;
---- 009
insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,MA_TO,MA_NV,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen,  DTHU_THANHCONG_DUNG_HEN, TYLE)
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,MA_TO, MA_nV,'HCM_SL_BHOL_009' loai_tinh, 'KPI_NV'
                         , count(ma_gd) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when tien_khop > 0 then 0 else 1 end)*100/count(ma_gd), 2) tyle
                from        (select thang, a.ma_to, a.ma_pb,a.ma_nv , sum(tien) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, ma_gd
                                         from SLDH_BHOL a
                                        where thang = 202501 
                                        group by  thang, a.ma_to, a.ma_pb,ma_gd, a.ma_nv
                               )
                group by thang, ma_pb, ma_To, ma_Nv
                order by 2
                ) a
;
commit;
-- 
UPDATE TTKD_bSC.BANGLUONG_KPI  a
SET TYLE_THUCHIEN = (select tyle from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv ),
    giao = (select tong from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv),
    thuchien = (select da_giahan_dung_Hen from  ttkd_Bsc.tl_giahan_Tratruoc where a.thang = thang and a.ma_kpi = ma_kpi and ma_Nv = a.ma_nv)
WHERE 'HCM_SL_BHOL_009' =MA_KPI AND THANG = 202501 and exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_SL_BHOL_009')  
                                            and thang = 202501 and MA_VTCV = a.MA_VTCV);

---- mdht
UPDATE TTKD_bSC.BANGLUONG_KPI  a
SET  MUCDO_HOANTHANH = case when tyle_Thuchien < 10 then 120
                        when tyle_Thuchien >= 10 and tyle_Thuchien < 15  then 100- (tyle_Thuchien-15)
                       when tyle_Thuchien >= 15  and tyle_Thuchien <= 25 then 50-2*(tyle_Thuchien-25)
                       when tyle_Thuchien > 25 then 0 end
WHERE 'HCM_SL_BHOL_009' =MA_KPI AND THANG = 202501;
commit;
select* from TTKD_bSC.BANGLUONG_KPI  a WHERE 'HCM_SL_BHOL_009' =MA_KPI AND THANG = 202501;
select* from ttkdhcm_ktnv.ds_Chungtu_nganhang_phieutt_hd_1  where ma ='HCM-TT/03149248';
select* from ttkdhcm_ktnv.ds_Chungtu_nganhang_sub_oneb  where ma_gd ='HCM-TT/03149248';
delete from ttkd_bsc.tl_giahan_Tratruoc where thang = 202501 and ma_kpi in ('HCM_SL_BHOL_009','HCM_SL_BHOL_002');
---- code cu
insert into ttkd_Bsc.tl_giahan_Tratruoc(THANG, MA_PB,MA_TO,MA_NV,ma_kpi, LOAI_TINH, TONG, DA_GIAHAN_dung_hen,  DTHU_THANHCONG_DUNG_HEN, TYLE)
select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, ma_pb,'VNP0703003' MA_TO,'VNP017814' MA_nV,'HCM_SL_BHOL_002' loai_tinh, 'KPI_TO'
                         , count(thuebao_id) tong
                         , sum(case when tien_khop > 0 then 1 else 0 end) da_giahan
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, sum(tien) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from SLDH_BHOL a
                                        where thang = 202501 and ma_To = 'VNP0703003'
                                        group by  thang, a.ma_to, a.ma_pb,ma_gd, a.thuebao_id, a.ma_tb
                               )
                group by thang, ma_pb
                order by 2
                ) a
;
commit;
select * from ttkd_Bsc.nhanvien where  donvi = 'TTKD' and ma_to ='VNP0703003';
select* from ttkd_Bsc.nhanvien_vttp_potmasco where thang = 202409;
update ttkd_Bsc.nhanvien set thang = 202410 , donvi = 'POT' where thang is null;
commit;
UPDATE ttkd_bsc.bangluong_Kpi SET CHITIEU_GIAO = NULL where thang = 202410 and ma_kpi ='HCM_SL_HOTRO_006';
insert into ttkd_Bsc.tl_Giahan_Tratruoc(thang, ma_kpi, loai_tinh, ma_Nv,ma_to, ma_pb)
select* from ttkd_bsc.bangluong_Kpi where thang = 202410 and ma_kpi ='HCM_TB_GIAHA_029';
SELECT* FROM TTKD_BSC.BLKPI_DANHMUC_KPI where thang = 202410 and ma_kpi ='HCM_SL_HOTRO_006';
select* from ttkd_Bsc.nhanvien where thang= 202410 and user_Ccbs is null and TEN_nV in (select * from userld_202410_goc);
UPDATE ttkd_Bsc.nhanvien a set user_ccbs =
--select* from ttkd_Bsc.nhanvien a where exists
(select user_ld from userld_202410_goc where  upper(a.ten_Nv) = upper(ten_Daydu)  )--upper(a.ten_Nv) = upper(ten_Daydu) 
where thang = 202410 and user_ccbs is null and donvi != 'POT' AND exists
(select user_ld from userld_202410_goc where  upper(a.ten_Nv) = upper(ten_Daydu)  );
rollback;
commit  ;
