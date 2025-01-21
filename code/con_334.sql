select distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.chungtu_chot b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 d
where a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=2  
and b.ma_gd=d.ma
and c.hoantat=1
and b.tra_truoc=1
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null 
and a.fcheck=0
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd=b.ma_gd)
order by c.ma_ct;
update ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt c
set ma_chungtu = (select ma_Ct from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
                    join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 b on a.chungtu_id = b.chungtu_id 
                    where b.ma =  c.ma_gd)
where thang = 202408 and tien_khop = 1;
commit;
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202408 and tien_khop = 1;
select* from  ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 where ma = 'HCM-TT/02931648';

select a.ma_Ct, a.ngay_ct ngay_nh, b.ma_gd, nv.ten_nv nv_thanhtoan, dv5.ten_dv phong_tt, dv2.ten_dv donvi_raphieu, nv.ten_Nv
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
    join ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb b on a.chungtu_id = b.chungtu_id
    join css_hcm.phieutt_hd c on b.ma_Gd = c.ma_gd
    left join admin_hcm.nhanvien nv on c.thungan_tt_id = nv.nhanvien_id
    left join admin_hcm.donvi dv3 on c.DONVI_TT_ID = dv3.donvi_id
    left join admin_hcm.donvi dv5 on dv3.donvi_cha_id = dv5.donvi_id
    left join css_hcm.hd_khachhang hd on c.hdkh_id = hd.hdkh_id
    left join admin_hcm.donvi dv1 on hd.donvi_id = dv1.donvi_id
    left join admin_hcm.donvi dv2 on dv1.donvi_cha_id = dv2.donvi_id
    left join admin_hcm.nhanvien nv on hd.nhanvien_id = nv.nhanvien_id
where to_number(to_char(ngay_ct,'yyyymm')) = 202408;
select* from ttkd_bsc.nhanvien where ten_nv = 'Võ Thanh T?ng';
select* from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202408; and nhanvien_xuathd = 'CTV078251';