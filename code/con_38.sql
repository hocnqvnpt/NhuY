-- TSL-INT

select  a.thuebao_id , a.loaitb_id,c.rkm_id, greatest(c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.db_datcoc c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and 
    greatest (c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412
union all
select  a.thuebao_id , a.loaitb_id,c.rkm_id, greatest(c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.khuyenmai_Dbtb c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and
    greatest (c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412)

    
select  a.thuebao_id , a.loaitb_id,c.rkm_id, greatest(c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.db_datcoc c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and 
    greatest (c.thang_kt_mg,c.thang_kt,nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412
union all
select  a.thuebao_id , a.loaitb_id,c.rkm_id, greatest(c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) thang_kt
from css_hcm.db_thuebao a 
    left join css_hcm.db_cntt b on a.thuebao_id = b.thuebao_id
    join css_hcm.khuyenmai_Dbtb c on a.thuebao_id = c.thuebao_id
where c.hieuluc = 1 and c.ttdc_id = 0 and a.dichvuvt_id in (7,8,9,13,14,15,16) and
    greatest (c.thang_ktdc,c.thang_kt, nvl(to_number(to_char(b.ngay_kt,'yyyymm')),0)) between 202401 and 202412

select* from ttkd_Bsc.bangluong_kpi where thang = 202402

SELECT c.ma_nv, COUNT(b.ma_kpi)
FROM TTKD_BSC.nhanvien_202402 a
JOIN TTKD_BSC.blkpi_danhmuc_kpi_vtcv b ON a.ma_Vtcv = b.ma_vtcv 
JOIN TTKD_BSC.nhanvien_202402 c ON b.ma_vtcv = c.ma_vtcv
WHERE b.thang_kt IS NULL 
AND (b.to_truong_pho = 1 OR b.giamdoc_phogiamdoc = 1) 
AND a.ma_nv = 'VNP017445'
GROUP BY c.ma_nv;


