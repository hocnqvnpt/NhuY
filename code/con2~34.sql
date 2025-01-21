select owner , table_name from all_tab_columns where column_name = 'LOAIDV_ID';
select* from admin_hcm.loai_Dvi
delete from nhanvien_202404
commit;
    select* from loi_ngaydatcoc
drop table ds_ccos
select rkm_id from loi_ngaydatcoc group by rkm_id having count(rkm_id) > 1
update dm_ccos set thang = 202404 where thang is null;
create table loi_ngaydatcoc as ;
select a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.DICHVUVT_ID, a.CUOC_DC, a.TIEN_TD,  a.thang_bddc, a.thang_ktdc
,to_char(trunc(to_date(a.thang_bddc,'yyyymm'),'MM'), 'DD/MM/YYYY') NGAY_BDDC, to_char(trunc(last_day(to_date(a.thang_ktdc,'yyyymm'))), 'DD/MM/YYYY') NGAY_KTDC
, a.NGAY_HUY, 
a.NGAY_THOAI, a.RKM_ID, a.CONGVAN_ID, to_char(db.ngay_sd,'dd/mm/yyyy') ngay_sd, ct.huong_dc,
a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NGUOI_CN, a.TEN_KM, a.NGAY_CN, a.DULIEU,
case when to_number(to_char(db.ngay_sd,'yyyymm')) = a.thang_bddc then to_char(add_months(db.ngay_sd, ct.huong_dc) -1,'dd/mm/yyyy') end ngay_Ktdc_tutinh
from v_thongtinkm_all a
            join css_hcm.db_adsl b on a.thuebao_id = b.thuebao_id and chuquan_id in (145)
            left join css_hcm.db_Thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.ct_khuyenmai ct on a.chitietkm_id = ct.chitietkm_id
where a.dichvuvt_id = 4 and a.hieuluc = 1 and a.ttdc_id = 0 and  a.ngay_ktdc is null 
and a.thang_ktdc > 0
order by a.ngay_cn desc;
select * from ttkd_Bsc.ct_dongia_tratruoc where thang = 202403 and ma_nv in ('VNP019513','VNP027831','VNP019952')-- group by ma_Nv--loai_tinh = 'DONGIATRU_CA' AND MA_KPI = 'DONGIA_CA';
select* from ttkd_bsc.
select a.LUONG_DONGIA_GHTT, b.tien,a.ma_Nv from ttkd_Bsc.bangluong_dongia_202403 a 
join ttkd_Bsc.tl_giahan_tratruoc b on a.ma_nv = b.ma_nv
where b.thang = 202403 and loai_tinh = 'DONGIATRU_CA' and a.LUONG_DONGIA_GHTT < 0 and a.LUONG_DONGIA_GHTT != b.tien
commit;
SE
update ttkd_Bsc.ct_dongia_tratruoc set loai_tinh = 'DONGIATRU_CA', MA_KPI = 'DONGIA' where thang = 202403 and loai_tinh = 'DONGIATRU' AND MA_KPI = 'DONGIA_CA';