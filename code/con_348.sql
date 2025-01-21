select* from ttkd_Bsc.nhanvien where ten_nv like '%Tùng' and thang = 202410;
DROP TABLE dm_user;
create table dm_user as
select a.ma_nv, a.ten_nv, a.so_dt,b.ten_dv,c.ten_Dv donvi_cha, a.email, a.ngay_login, a.gioitinh, a.ngay_sn , d.ma_nd, d.trangthai , e.hinhthuc_hd, C.DONVI_CHA_ID
from admin.v_nhanvien@dataguard a
    left join admin.v_donvi@dataguard b on a.donvi_id = b.donvi_id
    left join admin.v_donvi@dataguard c on b.donvi_cha_id = c.donvi_id
    left join admin.v_nguoidung@dataguard d on a.nhanvien_id = d.nhanvien_id
    left join hrm.hinhthuc_Hd@dataguard e on a.hthd_id = e.hthd_id
where c.donvi_Cha_id IN (893845,283425,283413) and d.trangthai = 1;
select MA_NV, TEN_NV, SO_DT,  DONVI_CHA, EMAIL, NGAY_LOGIN, GIOITINH, NGAY_SN, MA_ND, TRANGTHAI, HINHTHUC_HD, DONVI_CHA_ID
from dm_user where ma_nv not in  (select ma_nv from ttkd_Bsc.nhanvien where thang = 202411 and donvi= 'TTKD') AND DONVI_CHA_ID = 283425 ;
select* from ttkd_bsc.nhanvien where ma_nv='CTV074301';
select* from admin.v_Nhanvien@dataguard where ten_nv = 'Nguy?n Tr?n Qu?c V?';
select* from admin.v_nguoidung@dataguard ;where donvi_id =283427;
select* from admin.v_donvi@dataguard where DONVI_ID IN (893845,283425,283413);
select* from admin.v_donvi@dataguard where TEN_dV LIKE '%Kinh Doan%';
----Check tytrong
select ma_nv, ten_nv, sum(nvl(tytrong,0)) tytrong from  ttkd_bsc.bangluong_kpi where thang = 202411-- and ma_pb in ('VNP0703000')
group by ma_nv, ten_nv
having sum(tytrong)< 100
;
commit;
update ttkd_bsc.bangluong_kpi set tytrong = null where ma_Nv in ('VNP022082') and thang = 202411 and ma_kpi in ('HCM_TB_GIAHA_023','HCM_TB_GIAHA_022');
select* from ttkd_bsc.bangluong_kpi where ma_Nv in ('VNP022082') and thang = 202411;,'
VNP017763
VNP017819
VNP022082