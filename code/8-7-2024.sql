MERGE INTO ttkd_bsc.nhuy_Ct_Bsc_ipcc_obghtt a
   USING ttkd_bsc.nhuy_Ct_Bsc_ipcc_obghtt b
      ON(a.khachhang_id = b.khachhang_id and a.manv_Thuyetphuc = b.manv_thuyetphuc and a.ma_gd = b.ma_Gd and a.thang = b.thang and a.thang = 202406)
   WHEN MATCHED THEN 
      UPDATE SET a.ma_nd_ob = a.ma_nd_ob, a.manv_ob = b.manv_ob, a.ob_id = 1, 
  
      ;
select* from ttkd_bsc.nhuy_Ct_Bsc_ipcc_obghtt where thang = 202406;

with tien as (
    select thang,ma_Nv,loai_tinh, ma_kpi, nvl(tien_dongia,0) tien, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202405  --and ghichu = 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
--    and ma_nv = 'VNP017247'
    union all
    select thang,MANV_XHDON,loai_tinh, ma_kpi,  NVL(TIEN_XHDON,0) TIEN, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202405 --and loai_tinh = 'DONGIATRA_OB' --and ghichu= 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
),
sl as (
    select ma_tb, ma_nv,dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202405 
    union all
    select ma_tb, manv_xhdon,  dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202405 and manv_xhdon is not null and manv_xhdon != ma_nv
),
abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu from sl group by ma_nv
) 
--select* from sl;
--select ma_tb from sl group by ma_tb having count (distinct ma_nv) = 1 and count(ma_nv) > 1;
select a.thang,a.ma_nv,b.ten_nv,b.ten_to, b.ten_pb, sum(tien) tien, abc.sltb, abc.dthu
from tien a
    join ttkd_bsc.nhanvien_vttp b on a.ma_nv = b.ma_nv and a.thang = b.thang
    left join abc on a.ma_nv = abc.ma_nv
group by a.thang,a.ma_nv,b.ten_nv,b.ten_to, b.ten_pb, abc.sltb, abc.dthu
    ;   
    select* from ttkd_Bsc.dongia_Vttp where thang = 202405  and ma_nv ='HCM011049' ;and manv_xhdon = 'HCM012880';
    ;