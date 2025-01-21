select ten_pb, count(distinct ma_Nv) sl, ten_vtcv, ma_vtcv, thang
from ttkd_Bsc.nhanvien 
where thang >= 202410 and (upper(ten_vtcv) like '%AM%' or  ma_vtcv in  ('VNP-HNHCM_BHKV_6.1','VNP-HNHCM_BHKV_6'))
group by thang,(ma_Vtcv), (ten_vtcv), ten_pb 
order by ten_pb, thang;
SELECT DISTINCT MA_VTCV, TEN_vTCV FROM  ttkd_Bsc.nhanvien 
where thang >= 202410 AND DONVI = 'TTKD' and ma_vtcv in  ('VNP-HNHCM_BHKV_6.1','VNP-HNHCM_BHKV_6');and ma_pb = 'VNP0701100';
AND  upper(ten_vtcv) like 'KINH DOANH DI ??NG';

select* from ttkd_Bsc.nhanvien where thang = 202411 and ma_vtcv = 'VNP-HNHCM_KHDN_3' and ten_pb = 'Phòng Khách Hàng Doanh Nghi?p 2'