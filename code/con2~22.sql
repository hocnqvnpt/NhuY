select a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.DICHVUVT_ID, a.CUOC_DC, a.TIEN_TD, a.NGAY_BD, a.NGAY_KT, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_HUY, a.NGAY_THOAI, a.RKM_ID, 
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NGUOI_CN, a.TEN_KM, a.NGAY_CN, a.DULIEU from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
            left join css_hcm.db_thuebao c on a.thuebao_id = b.thuebao_id
            left join css_hcm.db_khachhang d on c.khachhang_id = d.khachhang_id
            left join css_hcm.loai_kh e on d.loaikh_id = e.loaikh_id
            left join css_hcm.tocdo_adsl t on b.tocdo_id = t.tocdo_id
where a.dichvuvt_id = 4 and a.hieuluc = 1 and ttdc_id = 0 and thang_ktdc > = 202404 and ngay_ktdc is null and t.iptinh = 1 and e.khdn = 1;-- and a.hieuluc = 1 and a.ttdc_id = 0;

select* from ttkd_bsc.nhanvien_202312 where ma_nv = 'CTV021847'

select khachhang_id from v_thongtinkm_all--  where ma_nv ='CTV021847';

select* from ttkd_Bsc.ct_bsc_tratruoc_moi where thang = 202403 and ma_Tb = 'vannguyen_2022';

select a.thuebao_id, a.ma_Tb, a.loaitb_id, lh.loaihinh_Tb, tt.trangthai_Tb, iptinh, khdn, thuonghieu, ma_kh, ma_tt, ten_kh, ten_tb
from css_hcm.db_Thuebao a
    left join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loai_kh lkh on b.loaikh_id = lkh.loaikh_id
    left join css_hcm.db_adsl db on a.thuebao_id = db.thuebao_id
    left join css_hcm.tocdo_adsl t on db.tocdo_id = t.tocdo_id
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id = lh.loaitb_id
    left join css_hcm.trangthai_tb tt on a.trangthaitb_id = tt.trangthaitb_id
    left join css_hcm.db_thanhtoan t on a.thanhtoan_id = t.thanhtoan_id
where a.loaitb_id in (18, 58, 59, 61, 171, 210, 222, 224) and a.trangthaitb_id not in (7,8,9) and t.iptinh = 1 and lkh.khdn = 0 and chuquan_id = 145;
59082948641+41263731246
select sum(tien_thanhtoan) from ttkd_bsc.ct_bsc_Tratruoc_moi_30day where thang = 202403 and ngay_tt