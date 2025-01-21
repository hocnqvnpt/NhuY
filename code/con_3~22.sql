select a.ma_to, b.ma_To, b.ten_to
from ttkd_Bsc.ct_bsc_Giahan_cntt a 
    left join ttkd_bsc.nhanvien_202403 b on a.manv_giao = b.ma_nv
where a.ma_to != b.ma_to and thang = 202403;

update ttkd_Bsc.ct_bsc_Giahan_cntt a
set ma_to = (select ma_to from ttkd_bsc.nhanvien_202403 where ma_nv = a.manv_giao)
where  thang = 202403;
rollback;
commit;
select* from ttkd_Bsc.ct_bsc_Giahan_cntt  where thang = 202403