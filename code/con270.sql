select* from 
(
select a.ma_Tb, a.thuebao_id , a.ma_gd_26, d.ngay_ht ngay_Ht_26,
    case when to_number(to_char(d.ngay_ht,'yyyymmdd')) between 20240821 and 20240826 then d.ngay_ht else null end ngay_ht_dunghan_26,
     a.ma_gd_30 , e.ngay_ht ngay_ht_30, 
    case when to_number(to_char(e.ngay_ht,'yyyymmdd')) between 20240821 and 20240830 then e.ngay_ht else null end ngay_ht_dunghan_30

from ton a
    left join css_hcm.hd_khachhang b on a.ma_Gd_26 =b.ma_gd
    left join css_hcm.hd_khachhang c on a.ma_gd_30 = c.ma_gd
    left join css_hcm.hd_thuebao d on b.hdkh_id = d.hdkh_id and a.thuebao_id = d.thuebao_id
    left join css_hcm.hd_thuebao e on b.hdkh_id = e.hdkh_id and a.thuebao_id = e.thuebao_id
) where ngay_Ht_30 is not null and ngay_ht_dunghan_30 is null;
select* from ttkd_Bsc.bangluong_kpi where ma_kpi ='HCM_TB_GIAHA_028';
insert into ttkd_Bsc.blkpi_danhmuc_kpi(MA_KPI, TEN_KPI, LOAI, THANG, MANV_LEAD, NGUOI_XULY, GIAO, TYLE_TH, MUCDO_HT, DIEMCONG, DIEMTRU, ngay_ins)
select 'HCM_TB_GIAHA_028' MA_KPI, 'T? l? thuy?t ph?c khách hàng d?ch v? VNPT CA-IVAN gia h?n tr? c??c tr??c thành công tháng
T_QL?L' TEN_KPI, 'kpi'LOAI ,202408 thang ,'CTV083743' manv_lead , 'Nh? Ý' nguoi_xuly, 0 GIAO, 0 TYLE_TH,0 MUCDO_HT, 0 DIEMCONG, 0 DIEMTRU,sysdate ngay_ins
from dual;
insert into  ttkd_Bsc.blkpi_danhmuc_kpi_vtcv (MA_KPI, MA_VTCV, LOAI, TO_TRUONG_PHO, TEN_VTCV, THANG, ngay_ins)
select  'HCM_TB_GIAHA_028' MA_KPI, 'VNP-HNHCM_KHDN_4' ma_Vtcv,'kpi' loai, 1 to_truong_pho , 'Tr??ng Line' ten_Vtcv,202408 thang , SYSDATE ngay_ins
from dual;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202408 and 'HCM_TB_GIAHA_028' =MA_KPI;
select khachhang_id from css_Hcm.db_thuebao where ma_Tb in ('','hcm_ca_ivan_00018054','hcm_ca_00102268');