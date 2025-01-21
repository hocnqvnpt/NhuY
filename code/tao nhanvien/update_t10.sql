delete from nhanvien_202409_l3 where ma_pb = 'VNP0703000';
rename nhanvien_202410_l2 to nhanvien_202409_l3;
select* from nhanvien_202409_l4;
commit;
rollback;
update nhanvien_202409_l4 a
set manv_hrm = ma_nv,
user_ccbs = (select user_ccbs from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
tr_thai = (select tr_thai from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
user_ccbs2 = (select user_ccbs2 from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
nhanvien_id = (select nhanvien_id from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
user_dhsxkd = (select user_dhsxkd from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
user_ccos = (select user_ccos from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
trangthai_ccos = (select trangthai_ccos from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
user_ipcc = (select user_ipcc from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv),
nhomld_id = (select nhomld_id from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_nv = a.ma_nv);
select* from 
(select MANV_NNL, MA_NV, TEN_NV, MA_VTCV,  upper(TEN_VTCV) ten_Vtcv, MA_TO, upper(TEN_TO) ten_to, MA_PB, TEN_PB,  USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH,  USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from nhanvien_202409_l4 --where ma_Nv ='CTV021803'
order by ma_Nv)
minus
select* from (
select MANV_NNL, MA_NV, TEN_NV, MA_VTCV, upper(TEN_VTCV) ten_Vtcv, MA_TO,upper( TEN_TO) ten_to, MA_PB, TEN_PB,  USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH,  USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID
from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' --and ma_Nv = 'CTV021803'
order by ma_nv);
commit;
select distinct ma_vtcv from nhanvien_202409_l4 group by ma_vtcv having count(distinct ten_vtcv) > 1;
update nhanvien_202409_l4 a set ten_pb = (select distinct ten_pb from ttkd_Bsc.nhanvien where thang = 202409 and donvi  = 'TTKD' and ma_pb = a.ma_pb);
select* from nhanvien_202409_l4 where ma_vtcv = 'VNP-HNHCM_BHKV_1';
select distinct ma_vtcv, ten_Vtcv from nhanvien_202409_l4 where ma_vtcv in ('VNP-HNHCM_BHKV_33','','VNP-HNHCM_BHKV_52','VNP-HNHCM_BHKV_51','VNP-HNHCM_KDOL_3');
update nhanvien_202409_l4 set ten_Vtcv = 'Nhân viên outbound/Telesale' where ma_vtcv = 'VNP-HNHCM_KDOL_3';