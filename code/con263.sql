select* from ttkd_Bsc.nhanvien where donvi='TTKD' and thang = 202408;
select distinct trangthai_ob from dhsx.v_ghtt_ipcc@dataguard;
select* from css_hcm.hinhthuc_tra where ht_Tra_id = 214;
SELECT* FROM TTKD_BSC.NHANVIEN WHERE MA_nV = 'CTV029024';
select a.thuebao_id, b.hdkh_id, c.ma_tb, b.ma_gd, b.ngay_yc, c.tthd_id, d.trangthai_hd
from ttkdhcm_ktnv.ghtt_giao_688 a
    join css_hcm.hd_khachhang b on a.khachhang_id = b.khachhang_id
    join css_hcm.hd_Thuebao c on  b.hdkh_id = c.hdkh_id and a.thuebao_id = c.thuebao_id 
    join css_hcm.trangthai_hd d on c.tthd_id = d.tthd_id
where a.thang_kt = 202407 and km = 1 and loaibo = 0 and tratruoc = 1 and c.tthd_id <> 6 and not exists (select 1 from tmp3_30ngay where a.thuebao_id = thuebao_id)
    and to_number(to_char(ngay_yc,'yyyymm')) between 202406 and 202407 and c.kieuld_id in (551, 550, 24, 13080)
order by ngay_yc desc;
select* from ttkd_bsc.DM_LOAIHINH_HSQD;
select ma_Tb , ngay_bddc, ngay_ktdc, THANG_BDDC, THANG_KTDC, THANGDC, THANG_BD_MG, THANG_KT_MG, THANGKM from v_Thongtinkm_all where ma_Tb in ('hai_2023thaodien2','hai_2023thaodien1','spx_betrieu','spx_camle','spx_72m','spx_hungvuong','spxdongb.bdg','spxhuucanh.bdg',
'spx_nahang','spx_ninhan','spx_dongquang','spx_phuocthien','spx_thonchi','spx_phongnha','spx_tanhuong','spx_daitu','spx_leductho','spx_ngaba') and hieuluc = 1 and ttdc_id = 0
;
with ct as (select distinct MA_CT_ONEB, ma_ct
					from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb x
								join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb y on x.chungtu_id = y.chungtu_id
)
select a.ma_gd, ct.ma_Ct, tp.ten_pb 
from css_hcm.phieutt_hd a
    join css_hcm.hd_khachhang b on a.hdkh_id = b.hdkh_id
    left join ct on a.so_ct = ct.ma_ct_oneb
    left join admin.v_nhanvien@dataguard nv on b.ctv_id = nv.nhanvien_id
    left join ttkd_Bsc.nhanvien tp on nv.ma_nv =tp.ma_nv and tp.thang = 202407 and donvi = 'TTKD'
    where to_Number(to_char(a.ngay_tt,'yyyymm')) = 202407;