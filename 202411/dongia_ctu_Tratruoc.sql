insert into ct_trtruoc(ten_nganhang,MA_CT, TIEN_CT, KHOANMUCTT_ID, MA_GD, dichvu_vt , MA_TT, MA_TB, TEN_KIEULD, LOAIHINH_TB, TEN_KH, NGAY_YC, NGAY_TT, TIEN_HOPDONG, VAT_HOPDONG, TONGTIEN, MANV_TT,
PHONGBAN_TT, KENHTHU, TEN_HT_TRA, CHUNGTU_ID, PHIEUTT_ID, ghichu,THANG)
select 
    a.ma_Nh, a.ma_ct, a.tien_ct, ct.khoanmuctt_id, c.ma_Gd,vt.TEN_DVVT dichvu_vt, null ma_tt, tb.ma_tb, ld.ten_kieuld, lh.loaihinh_Tb, HDkh.ten_kh, to_char(hdkh.ngay_yc,'dd/mm/yyyy'), 
     to_char(c.ngay_tt,'dd/mm/yyyy') ngay_tT, CT.TIEN tien_hopdong,
    CT.VAT vat_hopdong, CT.TIEN+CT.VAT tongtien, nv.ma_nv manv_tt, n.ten_pb phongban_tt, kt.kenhthu, ht.ht_tra ten_ht_Tra, a.chungtu_id, c.phieutt_id,c.ghichu, 202408 thang
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
    join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 b on a.chungtu_id = b.chungtu_id
    join css_hcm.phieutt_hd c on b.ma = c.ma_gd
    join css_hcm.ct_phieutt ct on c.phieutt_id =cT.phieutt_id
    left join css_hcm.hd_thuebao tb on ct.hdtB_id = tb.hdtb_id
    left join css_hcm.dichvu_vt vt on tb.dichvuvt_id = vt.dichvuvt_id
    left join css_hcm.kieu_ld ld on tb.kieuld_id = ld.kieuld_ID
    left join admin_hcm.nhanvien_onebss nv on c.thungan_Tt_id = nv.nhanvien_id
    left join css_hcm.hd_khachhang hdkh on tb.hdkh_id = hdkh.hdkh_id
    left join css_hcm.loaihinh_Tb lh on tb.loaitb_id = lh.loaitb_id 
    left join css_hcm.kenhthu kt on c.kenhthu_id = kt.kenhthu_id
    left join css_hcm.hinhthuc_tra ht on c.ht_Tra_id = ht.ht_tra_id
    join ttkd_Bsc.nhanvien n on nv.ma_nv = n.ma_Nv and n.thang = 202408
where to_number(to_char(A.ngay_ht,'yyyymm'))=202408 and n.ten_pb = 'Phòng Nghi?p V? C??c';
commit;
rollback;
desc ct_trtruoc;
alter table ct_trtruoc add chungtu_id number;
select count(distinct ma_tb) from ttkd_bsc.ct_dongia_tratruoc where thang = 202408 and loai_tinh = 'DONGIATRA_OB' and nhanvien_xuathd in 
(select ma_nv from ttkd_Bsc.nhanvien where thang = 202408 and ten_pb = 'Phòng Nghi?p V? C??c');
select* from ct_trtruoc where thang = 202408;
delete  from ct_trtruoc where thang = 202408;

select thungan_tt_id from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02933680';
select* from  ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 where ma = 'HCM-TT/02933680';
select distinct a.* from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_tt a where ma_Gd = 'HCM-TT/02933680';
union all
select a.*, b.ma, 0 tratruoc , b.nhanvien_id
from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a
    left join ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 b on a.chungtu_id = b.chungtu_id
where to_number(to_char(ngay_ht,'yyyymm'))=202408;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_ts ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 where chungtu_id = 271192;
select* from ttkd_bsc.ct_Bsc_Tratruoc_moi where ma_Tb= 'men_2021'; and loai_tinh in ('DONGIATRA','DONGIATRA30D');
select* from ttkd_bsc.bangluong_dongia_qldb where thang = 202309 and maNv = 'CTV079247';
delete from ttkd_bsc.blkpi_danhmuc_sms where sdt = '0815335308';
commit;
create table temp_ctu as
with  ct as (select distinct  ma_ct, x.ma_Gd
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
    select DONVI_ID, HT_TRA_ID, TRANGTHAI, TEN_DV, PHONG_BH, NGUOI_TT, TEN_KH, DIACHI_KH, MST, NGAY_TT, SERI, HOADON, NGAY_HD, SOPHIEU,  a.MA_GD, TEN_DVVT, LOAIHINH_TB, 
    HTTT, TEN_KMTT, LOAIHD_ID, HDTB_ID, THUEBAO_ID, MA_TB, TIEN_THANHTOAN, TIEN, VAT, KENHTHU, MA_TT, GHICHU, GHICHU_TT, RKM_ID, TEN_CTKM, TEN_GOI, NHOMTB_ID, TRANGTHAI_HD, 
    NV_RAPHIEU, DONVI_RAPHIEU, THUNGAN_TT_ID, MANV_TT, TENNV_TT,  nvl(ct.ma_Ct,a.so_Ct) so_Ct
    from ktkh a 
        left join ct on a.ma_gd = ct.ma_gd;
select a.* from temp_ctu a 
    join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb b on a.so_ct = b.ma_Ct
where to_number(to_char(ngay_ht,'yyyymm'))=202408 ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_tt 
where ma_gd in (select ma_gd from ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_tt group by ma_gd having count(distinct ma_Ct) > 1);
