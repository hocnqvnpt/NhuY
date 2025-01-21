select b.ma_tb, khdn from matb a left join css_Hcm.db_Thuebao b on a.ma_Tb=  b.ma_Tb and b.loaitb_id in (58,59)
    left join css_hcm.db_khachhang c on b.khachhang_id = c.khachhang_id
    left join css_hcm.loai_kh d on c.loaikh_id = d.loaikh_id;
delete  t4_khcn;
commit;
update t4_khcn set lydo = 'Không có ngày KTDC' where 
ma_Tb in ('nguyenthu_2020',
'hcmntthu2021',
'hangb16',
'hcm_thyhng_24',
'dvlam072019',
'duc234',
'ntphu160522',
'hcmdvlam072019',
'hvhien130417',
'hcmduc234',
'trang09-06',
'thyhng93758707',
'phuudung_608',
'tranphuong2019',
'hcmoanh100522',
'hanh22d',
'hcmhothingoc2022',
'phuongnga93871776',
'hongphc5083575',
'quchung5636',
'quochung_co0410',
'quandoan11',
'naturalrendezvous5781973',
'doncnt5794958',
'lucgiac2023',
'tanbnh5643533',
'cleansnet');
select * from v_Thongtinkm_all where ma_Tb in ('nguyenthu_2020',
'hcmntthu2021',
'hangb16',
'hcm_thyhng_24',
'dvlam072019',
'duc234',
'ntphu160522',
'hcmdvlam072019',
'hvhien130417',
'hcmduc234',
'trang09-06',
'thyhng93758707',
'phuudung_608',
'tranphuong2019',
'hcmoanh100522',
'hanh22d',
'hcmhothingoc2022',
'phuongnga93871776',
'hongphc5083575',
'quchung5636',
'quochung_co0410',
'quandoan11',
'naturalrendezvous5781973',
'doncnt5794958',
'lucgiac2023',
'tanbnh5643533',
'cleansnet');
select* from ds_giahan_Tratruoc2 where ma_Tb = 'lvtrung621';
select* from tinhcuoc_hcm.dbtb partition for (20240301) where ma_Tb = 'lvtrung621';
alter table t4_khcn add  (lydo varchar2(200));
update t4_khcn -- set lydo = null
set lydo = 'Có giao GHTT T4'
where ma_tb in 
(select ma_tb from ttkdhcm_ktnv.ghtt_Giao_688 where tratruoc=1 and km = 1 and loaibo = 0 and thang_kt = 202404);
update t4_khcn a
set lydo = '?ã Gia h?n' 
where exists (select 1 from ds_giahan_tratruoc2 where to_number(to_char(ngay_kt_mg,'yyyymm'))> 202404 and a.ma_tb = ma_tb)
and lydo is null
commit;
select distinct trangthaitb_id from css_hcm.db_Thuebao where ma_Tb in (select ma_Tb from t4_khcn where lydo is null)
select distinct congvan_id from ds_giahan_tratruoc2 where ma_tb in (select ma_Tb from t4_khcn where lydo is null)
select* from t4_khcn where ma_Tb in (Select ma_Tb from ds_giahan_tratruoc2 );

update  t4_khcn a
set lydo = 'Không có KM'
where not exists (select congvan_id from ds_giahan_tratruoc2 where congvan_id not in (190, 343, 483, 491, 545, 8922) and a.ma_tb = ma_Tb) and lydo is null

update  t4_khcn a
set lydo = 'Không có KM'
where ma_Tb  = 'citysmart-d1'--in (select ma_Tb from ds_giahan_tratruoc2 where to_number(to_char(ngay_kt_mg,'yyyymm')) = 202404 and so_Thangdc < 3) and lydo is null;

update  t4_khcn a
set lydo = 'TB ng?ng ho?t ??ng'
where ma_Tb in (select ma_Tb from css_hcm.db_Thuebao where trangthaitb_id in (7,8,9) and dichvuvt_id = 4) and lydo is null;
commit;
select * from ds_giahan_Tratruoc2 where ma_Tb in ;
select* from tinhcuoc_hcm.dbtb partition for (20240301) where ma_Tb in (select ma_Tb from t4_khcn where lydo is null ) ;
select* from ds_giahan_tratruoc2 where ma_Tb in (select ma_Tb from t4_khcn where lydo ='TB ng?ng ho?t ??ng' )

select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU_CA' and ma_nv in ('VNP027830','CTV065855');

select * from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202403 and loai_tinh = 'DONGIATRU_CA' and ma_nv in ('VNP027830','CTV065855');
select* from ttkd_Bsc.nhanvien_202403 where ma_pb = 'VNP0702400';
select * from css_hcm.db_thuebao where ma_tb in 
select * from hocnq_Ttkd.ds_giahan_tratruoc2 where ma_tb in (select ma_Tb from t4_khcn where lydo is null) and 
select* from css_hcm.SOcongvan where congvan_id in (190, 343, 483, 491, 545, 8922)
select owner , table_name from all_tab_columns where column_name = 'CONGVAN_ID';
select* from ttkd_bsc.ct_bsc_giahan_cntt where ma_Tb in ('hcm_smartca_00000424','hcm_smartca_00000542','hcm_smartca_00000482','hcm_smartca_00000430','hcm_smartca_00000536',
'hcm_smartca_00000541','hcm_smartca_00000455','hcm_smartca_00000489','hcm_smartca_00000540','hcm_smartca_00000494','hcm_smartca_00003652','hcm_smartca_00000525',
'hcm_smartca_00000526','hcm_smartca_00000459','hcm_smartca_00000506','hcm_smartca_00000469') and thang = 202403;
select* from ttkd_bsc.ct_bsc_giahan_cntt where ma_Tb in ('hcm_ca_00039367','hcm_ca_00039362','hcm_ca_00039370','hcm_ca_00039360','hcm_ca_00039361','hcm_ca_00039369','hcm_ca_00039363');