select * from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb;
select* from css_hcm.kieu_ld where kieuld_id = 136;
select* from ttkdhcm_ktnv.ds_Chungtu_tratruoc;
selec

select  a.thuebao_id, a.ma_tb, a.ma_gd, ngay_Bd_moi, ngay_kt_moi, a.tien_thanhtoan, vat, a.ngay_tt, ngay_hd,a.ngay_nganhang,kenhthu,ten_ht_Tra, manv_thuyetphuc,ma_to, ma_pb, manv_cn, phong_cn,
so_thangdc_moi, avg_thang, a.ngay_Yc, ma_Chungtu,c.ngay_Nhap ngaynhap_chungtu
from bang_gom a
    join ttkdhcm_ktnv.phieutt_hd_dongbo b on a.phieutt_id = b.phieu_id
    join ttkdhcm_ktnv.ds_Chungtu_tratruoc c on b.ma_capnhat = c.ma_capnhat
where a.ngay_yc > c.ngay_nhap;
select* from v_Thongtinkm_all where thuebao_id in (1896856,
2806446,
2937662,
2961514,
4731059,
5856477,
7180033,
8104432,
8187932,
8458200,
8461517,
8552030,
8834841,
9679143,
11840522)
;
select* from v_Thongtinkm_all where thuebao_id in (8592325,
8892029,
7186635,
2737589,
9058618,
8172733,
8279586,
4728065,
7420728,
11952749,
9488761,
8157714,
9155662,
8425721,
9058621,
9148788,
8764201,
2872799,
11311924,
9157119,
9488353,
8071190,
8847298,
9155655,
8698191
);
select* from ttkdhcm_ktnv.ds_Chungtu_tratruoc;