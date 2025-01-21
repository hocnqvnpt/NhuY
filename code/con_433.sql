select A.* from xxxx a
    join css_hcm.db_Thuebao b on a.ma_Tb=  b.ma_Tb and b.loaitb_id = 58
where --THIETBI LIKE '%PON%';
(congnghe = 'XGSPON' AND THIETBI LIKE '%PON%') OR 
(CONGNGHE = 'GPON' AND THIETBI = 'Thi?t b? ??u cu?i XGSPON Nokia.XS-2426G-A.');
select mst from css_hcm.db_khachhang where khachhang_id in (
select  khachhang_id from css_hcm.db_Thuebao where ma_Tb in ('acfcsunnies-kha','acfc-levislottehni','acfc-guessaeonbt'
,'acfcmango-kha','acfcswa-btan','acfc_tommycto','acfccotton-kha','acfc-levisdni','acfcnike-hni','acfcgap-hadong'));

select  a.ma_tb,a.ngay_BdDc, a.ngay_kt_mg,a.cuoc_Dc, ten_kh,a.tien_td, to_char(ngay_kt_mg,'yyyymm')thang_kt, d.ma_kh ,c.mst , ct.ten_ctkm
, pb.*
from ds_Giahan_tratruoc2 a
    left join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id
    left join css_hcm.db_Thanhtoan c on b.thanhtoan_id = c.thanhtoan_id
    left join css_hcm.db_khachhang d on b.khachhang_id = d.khachhang_id
    join css_Hcm.ct_khuyenmai ct on a.chitietkm_id = ct.chitietkm_id
    left join ttkd_bct.db_Thuebao_ttkd e on a.thuebao_id = e.thuebao_id
    left join (select distinct ma_pb, ten_pb from ttkd_Bsc.nhanvien where thang = 202411) pb on e.mapb_ql = pb.ma_pb
where d.khachhang_id = 9769067 or c.mst = '0309300048';
    select* from ds_Giahan_tratruoc2 where ma_Tb='acfcgap-hadong'