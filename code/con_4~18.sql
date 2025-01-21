alter table rmp_bsc_phong add tyle_truoc_loaitru number;
select* from ttkd_Bsc.bangluong_kpi where thang =202412 and ma_kpi = 'HCM_SL_BHOL_009';
alter table rmp_bsc_phong set ;
select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_TB_GIAHA_029' AND MA_TO = 'VNP0702308';
select* from rmp_bsc_phong;
alter table rmp_bsc_phong rename column da_giahan to thuchien;
select distinct ten_kpi, thang from rmp_bsc_phong where thang = 202411;
update rmp_bsc_phong set thang = 112024 where thang = 202411 and ten_kpi in  ('Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)',
'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu Tên mien)','','','');
commit;
select* from rmp_bsc_phong where thang = 202411 and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';
update rmp_bsc_phong set giao_truoc_loaitru = ()
where thang = 202411 and ten_kpi = 'Ty le thuyet phuc khách hàng GHTT TC tháng T (Dich vu VNPT CA-IVAN)';
select* from giao_oneb a 
    join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id
    where thang = 202411 and thang_kt = 202411 and b.khachhang_id = 9798610;
    
select* from ttkd_Bsc.nhanvien where thang = 202409 and ten_Nv ='Nguy?n V?n D?ng' and donvi = 'TTKD';

select kyhoadon, ma_Tb, cuoc_tk from qltn.v_db_Datcoc@dataguard where ma_tb in ('thusuongb98','camvanbui2016','pt2612','nhantt88','hcm_tuongnhu','mesh0099927')
and kyhoadon in (20241101,20241001) and phanvung_id = 28;
select* from v_Thongtinkm_all_ol where ma_Tb in ('thusuongb98','camvanbui2016','pt2612','nhantt88','hcm_tuongnhu','mesh0099927')
order by ma_tb, ngay_Bddc, ngay_ktdc;
select* from nhuy.userld_202411_goc;
select* from ttkd_Bsc.ct_Bsc_giahan_Cntt where thang = 202411 and ma_tb='hcm_ca_00034804';
select* from admin_hcm.nguoidung where ma_nd in (select user_ld from userld_202411_goc);

select* from ttkd_Bsc.bangluong_kpi where thang = 202411 and ma_kpi = 'HCM_SL_COMBO_006';