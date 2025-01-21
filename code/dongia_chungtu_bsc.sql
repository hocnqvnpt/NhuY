select a.ma_ct,a.nd_ct,a.nhandien_thanhtoan ID600_nhan_dien,b.ma_tt ma_gach_one,a.tien_ct,b.tongtra,decode(a.hoantat,1,'Hoan_tat') trangthai 
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a,nhuy.chungtu_chot b 
where a.ma_ct = b.ma_chungtu
and b.tra_truoc=0
and a.nhandien_thanhtoan = b.ma_tt
and a.hoantat = 1
and a.tien_ct = b.tongtra
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt) ;
with nguoi_Tt as (
    select chungtu_id,a.nhanvien_id, b.ma_nv, ma
    from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 a
        join admin_hcm.nhanvien_onebss b on a.nhanvien_id = b.nhanvien_id
    union all
        select chungtu_id,a.nhanvien_id, b.ma_nv, ma
    from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 a
        join admin_hcm.nhanvien_onebss b on a.nhanvien_id = b.nhanvien_id
)
, tt_chungtu as (
    select a.*, b.chungtu_id
    from chungtu_chot a
       join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.ma_chungtu = b.ma_ct
    where b.hoantat = 1
)
select  count(*)-- a.*, b.ma_nv
from tt_chungtu a 
    join nguoi_tt b on a.chungtu_id = b.chungtu_id and nvl(ma_tt, ma_gd)  = b.ma