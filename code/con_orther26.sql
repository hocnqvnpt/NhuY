select* from css_hcm.doituong ;
select a.ma_Tb, tb.trangthai_tb , nv.ten_Nv nvtt, dv1.ten_dv dvtt, tt.cuoc_dc, tt.ngay_Bddc, tt.ngay_kt_mg, a.ngay_sd, b.ngay_ht ngay_Hoancong, a.ngay_cat
from css_hcm.db_Thuebao a
    join css_hcm.hd_thuebao b on a.thuebao_id = b.thuebao_id
    join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id and c.loaihd_id = 1
    left join admin_Hcm.nhanvien_onebss nv on c.ctv_id = nv.nhanvien_id
    left join admin_hcm.donvi dv on nv.donvi_id = dv.donvi_id
    left join admin_hcm.donvi dv1 on dv.donvi_cha_id = dv1.donvi_id
    left join DS_GIAHAN_TRATRUOC2_V4 tt on a.thuebao_id = tt.thuebao_id
    left join css_hcm.trangthai_Tb tb on a.trangthaitb_id = tb.trangthaitb_id
where a.doituong_id = 190 and to_char(c.ngay_yc,'yyyy') = '2024' and a.loaitb_id in (61,171) ;
select* from v_Thongtinkm_all where ma_Tb='ltthuy050622';
select* from ttkd_Bsc.ct_Bsc_Giahan_Cntt where thang = 202412 and ma_Tb in ('hcm_ca_ivan_00020508','hcm_ca_ivan_00020637');
select * from ttkd_Bsc.ct_Bsc_homecombo where thang = 202410;