select * from css.v_phieutt_hd@dataguard
where ma_gd='';
select* from ct_Bsc_Chungtu;
alter table ct_Bsc_Chungtu add thang number(6);
update ct_Bsc_Chungtu set thang = 202407;
commit;
select thungan_tt_id, thungan_hd_id from css_hcm.phieutt_hd where ma_Gd in ('HCM-TT/02926616','HCM-TT/02926122');
select * from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410 and ma_Tb in ( 'vanduy54','vnpt290523-1');
select* from css_hcm.phieutt_hd where ma_Gd = 'HCM-TT/02963655';
update ttkd_bsc.ct_dongia_tratruoc a
set nhanvien_xuathd = (select distinct a.nhanvien_xuathd from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410 and a.ma_Tb = ma_Tb and manv_thuyetphuc = a.ma_nv)
where thang = 202410 and tien_khop = 1 and tyle_thanhcong is null and loai_Tinh ='DONGIATRA_OB' and ghichu !='gach no tu dong';
rollback;
COMMIT;
select* from ttkd_bsc.ct_dongia_Tratruoc where thang = 202410 and tien_khop > 0 and tien_thuyetphuc is null and loai_Tinh = 'DONGIATRA_OB';
update ttkd_bsc.ct_dongia_tratruoc c
set tien_thuyetphuc = 6000
--select* from ttkd_bsc.ct_dongia_tratruoc c
where thang = 202410 and tien_khop = 1 and loai_Tinh ='DONGIATRA_OB' 
and TIEN_THUYETPHUC is null --;and ma_tb ='ncc_2018'
--;
--ROLLBACK;
--select* from ttkd_bsc.
and exists (select 1 from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt a join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.ma_Chungtu = b.ma_ct and a.ngay_tt <= b.ngay_insert +1 and 
a.ma_tb = c.ma_Tb and a.manv_thuyetphuc = c.ma_nv) ;
rollback;
select* from ct_Tratruoc_a;
create table ct_Tratruoc_a as
SELECT* FROM xx_chungtu_202410 where thang = 202410 and tratruoc = 1 and ghi_chu is null;
select ma_nv from admin_hcm.nhanvien where nhanvien_id ='507961';
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where thang = 202410 and ma_Gd in ('HCM-TT/02961381','HCM-TT/02960610');
(select ma_Tb from ttkd_Bsc.ct_dongia_tratruoc where thang = 202410 and tien_khop = 1 and tyle_thanhcong is null and loai_Tinh ='DONGIATRA_OB' and ghichu !='gach no tu dong');
update ttkd_Bsc.ct_dongia_tratruoc set nhanvien_xuathd = 'CTV087446'
where thang = 202410 and tien_khop = 1 and tyle_thanhcong is null and loai_Tinh ='DONGIATRA_OB' and ghichu !='gach no tu dong' and ma_Tb ='hcmbpb-02.01';
select* from ttkd_bsc.nhanvien where ma_Nv = 'VNP016778';
select* from ct_Bsc_Chungtu where tra_truoc = 1;
insert into ct_Bsc_Chungtu (NGANHANG, MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB, MA_GD, LOAIHINH_TB, GHICHU, ma_Nv, TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID,
TRA_TRUOC, CHUNGTU_ID, NV_GACH, TINH_DONGIA, TINH_BSC, THANG);
create table ct_Bsc_Ctu_temp as select* from ct_Bsc_Chungtu where 1=2;

delete from ct_Bsc_Ctu_temp;

insert into ct_Bsc_Ctu_temp (NGANHANG, MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB, MA_GD, LOAIHINH_TB, GHICHU, ma_Nv, TENDV_GACH, TRAGOC, TRATHUE, TONGTRA, HTTT_ID,
TRA_TRUOC, CHUNGTU_ID, NV_GACH, TINH_DONGIA, TINH_BSC, THANG)
select nganhang,ma_Chungtu, tien_Chungtu, hinhthuc_tt, ngay_Tt, ma_tt, ma_Tb, null ma_Gd, loaihinh_Tb,ghichu, nguoi_cnt, tendv_gach, tragoc, trathue,tongtra, httt_id, 0 tra_Truoc,
nvl(chungtu_id,0), nguoi_cnt nv_Gach, 1 tinh_dongia, 1 tinh_bsc, 202410 thang
from CT_TRSAU_TEMP   
union all
select null ten_nganhang, ma_chungtu,null tien_ct,null ten_Ht_Tra, ngay_tt, null ma_tt, null ma_Tb, ma_Gd,null loaihinh_tb, ghi_chu, NV_GACH,null phongban_tt,null tien_hopdong,null vat_hopdong,null tongtien, null httt_id
,1 tra_truoc, chungtu_id, NV_GACH, 0 tinh_dongia, 1 tinh_bsc, 202410 thang
from ct_Tratruoc_a where thang = 202410;


commit;
select* from ct_Trtruoc;
select* from ct_Bsc_Chungtu where thang = 202410;and tra_Truoc = 1; and tinh_dongia = 1;
--delete  from ct_Bsc_Chungtu where thang = 202410; and tra_Truoc = 1;
rollback;
update ct_Bsc_Chungtu set tinh_dongia = tinh_Bsc where thang = 202410;
select distinct ma_chungtu from ct_Bsc_Chungtu  where thang = 202410 and TINH_DONGIA= 0 and  TINH_BSC =1;
commit;
update ct_Bsc_Chungtu set tinh_Dongia = 1 where thang = 202410;
select* from x_chungtu_202410;
rollback;
---- bo don gia
update ct_Bsc_Chungtu c
set tinh_dongia = 0
--select* from ct_Bsc_Chungtu c
where thang = 202411 and tra_truoc = 1 and exists (select 1 from ttkd_bsc.ct_dongia_tratruoc a 
    join ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt b on a.thang = b.thang and a.ma_Tb = b.ma_Tb and b.manv_Thuyetphuc = a.ma_nv
    where  loai_tinh = 'DONGIATRA_OB' and (tien_xuathd*heso_Chuky*heso_dichvu) > 0
and c.ma_gd = b.ma_gd and c.nv_gach = a.nhanvien_xuathd) and tinh_Dongia = 1;
commit;
select* from nhuy.xx_chungtu_202410
update ct_Bsc_Chungtu a 
set tinh_dongia = 0
--select* from ct_Bsc_Chungtu a 
where thang = 202410 and tra_truoc = 1 and exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang in ( 202408,202410) and loai_tinh = 'DONGIATRA_OB'
and a.ma_Tb = ma_Tb and a.nv_gach = nhanvien_xuathd and (heso_Chuky*heso_dichvu*tien_xuathd) > 0) and tinh_Dongia = 1;
;
commit;
update ct_Bsc_Chungtu a 
set tinh_dongia = 2
--select* from ct_Bsc_Chungtu a 
where thang = 202410 and tra_truoc = 1 and exists (select 1 from ttkd_bsc.ct_dongia_tratruoc where thang in ( 202410) and loai_tinh = 'DONGIATRA_OB'
and a.ma_Tb = ma_Tb and a.nv_gach = nhanvien_xuathd and tien_xuathd = 0) and tinh_Dongia = 0 and tinh_bsc = 1;
commit;
---- bo bsc tra truoc
update ct_bsc_chungtu ct
set tinh_dongia= 0, tinh_Bsc = 0 
where thang = 202410 and exists (
select distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt,b.ma_gd,b.ma_Tb, b.tra_truoc, b.tinh_bsc,b.tinh_dongia
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.ct_bsc_chungtu b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 d
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
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd=b.ma_gd) and b.thang = 202410
and ct.chungtu_id = b.chungtu_id);
---- bo bsc tra sau
update ct_bsc_chungtu ct
set tinh_dongia= 0, tinh_Bsc = 0 
--select count(distinct chungtu_id) from  ct_bsc_chungtu ct
where thang = 202410 and exists (
select distinct a.chungtu_id, c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.ct_bsc_chungtu b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 d
where a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=3  
and b.ma_tt=d.ma
and c.hoantat=1
and b.tra_truoc=0
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null --1574 989
and a.fcheck=0
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt) and b.thang = 202410
and ct.chungtu_id = b.chungtu_id);


merge into  ct_bsc_chungtu ct
using (
        select distinct a.chungtu_id, b.thang-- c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt
        from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.ct_bsc_chungtu b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 d
        where a.chungtu_id=c.chungtu_id
                and c.ma_ct = b.ma_chungtu
                and c.chungtu_id = d.chungtu_id
                and a.loai_db_id=3  
                and b.ma_tt=d.ma
                and c.hoantat=1
                and b.tra_truoc=0
                and a.nhanvien_id is null 
                and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 group by chungtu_id having count(*) =1)
                and c.nhandien_thanhtoan is not null --1574 989
                and a.fcheck=0
                and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt) and b.thang = 202410 ) tb 
on ( ct.chungtu_id = tb.chungtu_id and ct.thang = tb.thang)
when matched then 
    update set ct.tinh_dongia= 0, ct.tinh_Bsc = 0 ;


order by c.ma_ct
rollback;
commit;
select * from ct_bsc_chungtu where thang = 202410 and tinh_bsc =  0;
select* from admin_hcm.nhanvien where nhanvien_id in (949,13948,451341);
insert into ct_Bsc_Chungtu(thang,chungtu_id) values (-1, -1);
select thungan_tt_id, thungan_hd_id from css_hcm.phieutt_hd where ma_gd in (
select distinct  ma_Gd from ct_Bsc_Chungtu where thang = 202410 and tinh_dongia = 1 and tinh_bsc  = 1 and tra_Truoc = 1
and ma_Tb in (select ma_Tb from ttkd_Bsc.ct_Dongia_tratruoc where tien_khop = 1 and heso_chuky!= 0 and thang = 202410)) ;
select* from ttkd_Bsc.ct_Dongia_tratruoc where tien_khop = 1 and heso_chuky = 0 and thang = 202410;
select* from ttkd_Bsc.nhuy_ct_Bsc_ipcc_obghtt where ma_Gd ='HCM-TT/02982144';
select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202410 and tyle_Thanhcong is null and tien_khop =1 and loai_Tinh = 'DONGIATRA_OB';