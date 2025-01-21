SELECT* FROM TTKD_bSC.ct_dongia_tratruoc where thang = 202404 and loai_Tinh = 'DONGIATRA_OB' ;
UPdate  TTKD_bSC.ct_dongia_tratruoc a
set NHANVIEN_XUATHD = (select NHANVIEN_XUATHD from ct_Dongia_temp where thang = 202404 and a.thuebao_id = thuebao_id and ma_Nv = a.ma_nv )
,TIEN_XUATHD = (select TIEN_XUATHD from ct_Dongia_temp where thang = 202404 and a.thuebao_id = thuebao_id and ma_Nv = a.ma_nv ), 
TIEN_THUYETPHUC= (select TIEN_THUYETPHUC from ct_Dongia_temp where thang = 202404 and a.thuebao_id = thuebao_id and ma_Nv = a.ma_nv )
where thang = 202404 and loai_Tinh = 'DONGIATRA_OB';

insert into TTKD_bSC.ct_dongia_tratruoc (thang,LOAI_TINH, MA_KPI, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_NV, MA_TB, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, 
DTHU, NGAY_TT, TIEN_KHOP, NHANVIEN_XUATHD, DONGIA,  TIEN_THUYETPHUC, TIEN_XUATHD)
select 202405, 'DONGIATRA_OB_NSG_H',ma_kpi, THUEBAO_ID, TIEN_DC_CU, MA_TO, MA_PB, MA_NV, MA_TB, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, 
DTHU, NGAY_TT, TIEN_KHOP, NHANVIEN_XUATHD, DONGIA,  TIEN_THUYETPHUC, TIEN_XUATHD from dongia_ob_final a
where ma_Tb not in (select ma_tb from ma_Tb where nguon = 'nsg' ) and ma_pb = 'VNP0701400' and  not exists  (select 1 from ct_dongia_temp where thang = 202404 and tien_khop >0 and thuebao_id = a.thuebao_id) ;
commit;

with tien as 
(
    select 202405 thang, loai_tinh, ma_kpi, thuebao_id, tien_dc_cu, ma_to, ma_pb, ma_nv, ma_Tb, sothang_dc, heso_chuky, heso_dichvu, dthu, ngay_tt, tien_khop, nhanvien_xuathd,
dongia, tien, tien_thuyetphuc, tien_xuathd
    from dongia_bosung_t4
    union all 
    select  202505 thang, loai_tinh, ma_kpi, thuebao_id, tien_dc_cu, ma_to, ma_pb, ma_nv, ma_Tb, sothang_dc, heso_chuky, heso_dichvu, dthu, ngay_tt, tien_khop, nhanvien_xuathd,
dongia, tien, tien_thuyetphuc, tien_xuathd
    from dongia_bosung_t4_1 
--    where ma_nv not in (select ma_nv from ttkd_Bsc.nhanvien_202404 where ma_pb = 'VNP0701400' ) and nhanvien_xuathd not in (select ma_nv from ttkd_Bsc.nhanvien_202404 where ma_pb = 'VNP0701400' )
    
)
,chot as (
    select ma_nv, tien_thuyetphuc
    from tien
    union all 
    select nhanvien_xuathd,tien_xuathd
    from tien
)
select sum(tien_Thuyetphuc) 
from chot a
    join ttkd_Bsc.nhanvien_202404 b on a.ma_nv =b.ma_nv
    where ma_pb not in ('VNP0702400','VNP0702500','VNP0702300') ;and ma_pb = 'VNP0701400' --and
;
commit;
select* from admin_hcm.nhanvien where nhanvien_id = 13928;
select* from ttkd_Bsc.nhanvien_202406;

select * from dongia_bosung_t4_1 where NHANVIEN_XUATHD   in (select ma_nv from ttkd_Bsc.nhanvien_202404 where ma_pb = 'VNP0701400') and tien_xuathd >0;
select distinct ma_vtcv, ten_Vtcv from ttkd_Bsc.nhanvien_202404 where ma_nv in 
(select nhanvien_xuathd from dongia_bosung_t4_1 where ma_pb  = 'VNP0701400'  and NHANVIEN_XUATHD not  in (select ma_nv from ttkd_Bsc.nhanvien_202404 where ma_pb = 'VNP0701400') 
and tien_xuathd >0);
delete from dongia_bosung_T4_1 where ma_pb = 'VNP0701400';
delete from dongia_bosung_t4_1 where ;
update dongia_bosung_t4_1 
set nhanvien_xuathd = null, tien_xuathd = null;
select* from dongia_bosung_t4_1 where NHANVIEN_XUATHD   in (select ma_nv from ttkd_Bsc.nhanvien_202404 where ma_pb = 'VNP0701400') ;and tien_xuathd >0;
rollback;
alter table ttkd_Bsc.ct_dongia_Tratruoc add (nhanvien_xuathd VARCHAR2(30), TIEN_XUATHD NUMBER(6), TIEN_THUYETPHUC NUMBER(6));
select* from ttkd_Bsc.ct_Bsc_Tratruoc_moi_30day where thang = 202405 and ma_Tb = 'agribank-280tptd';
delete from ttkd_bsc.ct_Bsc_Tratruoc_moi_30day where ma_Tb in ('ctyadvn648','anhduongvn2013') and thang = 202405;
rollback;
selec
commit;
select* from dongia_ob_final where nhanvien_xuat_hd is not null and thuebao_id in (selec;
update dongia_bosung_t4_1 a 
set nhanvien_xuathd = (select nhanvien_xuathd from dongia_ob_final where a.thuebao_id = thuebao_id and a.ma_nv = ma_nv)
where a.nhanvien_xuathd is null and thuebao_id in (select thuebao_id from dongia_ob_final where thuebao_id = a.thuebao_id and nhanvien_xuathd is not null);
select* from ttkd_bsc.ct_Dongia_tratruoc where thang = 202404 and loai_tinh = 'DONGIATRA_OB';
commit;
desc dongia_ob_Final;