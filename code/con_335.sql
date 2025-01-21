select sum(tien_xuathd*heso_chuky*heso_dichvu) from tmp_dg_obghtt where tien_khop = 1 and manv_tt = 'CTV078251';
rollback;
update ttkd_bsc.ct_dongia_tratruoc f 
set tien_xuathd = 0, ghichu = 'gach no tu dong' , dongia = 7500;
--select* from ttkd_bsc.ct_dongia_tratruoC f
update tmp_dg_obghtt f 
set tien_xuathd = 0, ghichu = 'g' , dongia = 7500
where thang = 202408 and tien_khop = 1 and loai_tinh = 'DONGIATRA_OB' and exists (
select 1-- distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_gd
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,ttkd_bsc.nhuy_Ct_bsc_ipcc_obghtt b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 d
where
b.thang = 202408 and a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=2  
and b.ma_gd=d.ma
and c.hoantat=1
--and b.tra_truoc=1
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null 
and a.fcheck=0
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd=b.ma_gd)  
and f.thuebao_id = b.thuebao_id and f.ma_Nv = b.manv_thuyetphuc
);
select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202407 and (nhanvien_xuathd )= 'CTV078251';
select* from ttkd_Bsc.nhanvien where ma_nv = 'CTV078251';
commit;
select* from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang = 202408 and khachhang_id =  9798610 ;
select* from ttkd_Bsc.ct_Bsc_giahan_cntt where manv_Giao = 'VNP017748' and thang = 202407;
select* from
select* from ttkd_Bsc.ct_dongia_Tratruoc where ma_tb ='hcm_smartca_00013016';
