-- GHTT
insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
with ds as (
    select* from ttkd_bsc.ct_dongia_tratruoc a where thang =  202409 and loai_tinh = 'DONGIATRA_OB' and 
        (exists (select 1 from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202409  and ma_nv = a.ma_nv) or 
        exists (select 1 from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202409  and ma_nv = a.nhanvien_xuathd))
)
    select ds.THANG,'DONGIA_POTMASCO' LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            round(nvl(tien_thuyetphuc*heso_chuky*heso_dichvu,0)) tien_tp
            , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2,0 donvi_cha_id, heso_chuky, nhanvien_xuathd,  round(nvl(tien_xuathd*heso_chuky*heso_dichvu,0)) tien_xp
    from ds
        left join ttkd_bsc.nhanvien_vttp_potmasco b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang 
--        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
--        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;
--- TRA SAU CHUYEN TRA TRUOC
insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
with ds as (
    select* from ttkd_bsc.ct_dongia_tratruoc a where thang =  202409 and loai_tinh = 'DONGIA_TS_TP_TT' and 
        (exists (select 1 from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202409  and ma_nv = a.ma_nv) or 
        exists (select 1 from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202409  and ma_nv = a.nhanvien_xuathd))
)
    select ds.THANG,'DONGIA_TS_TP_TT' LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            round(nvl(DONGIA*heso_chuky*heso_dichvu,0)) tien_tp
            , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2,0 donvi_cha_id, heso_chuky, nhanvien_xuathd,  round(nvl(tien_xuathd*heso_chuky*heso_dichvu,0)) tien_xp
    from ds
        left join ttkd_bsc.nhanvien_vttp_potmasco b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang 
--        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
--        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;
COMMIT;
SELECT * FROM  ttkd_Bsc.dongia_Vttp A where loai_Tinh IN ('DONGIA_TS_TP_TT', '' )and ROUND(DONGIA*HESO_CHUKY*HESO)!= TIEN_DONGIA; GROUP BY THUEBAO_ID HAVING  COUNT(MA_nV)>1;
------
DELETE from ttkd_Bsc.dongia_Vttp A where loai_Tinh = 'DONGIA_POTMASCO' and thang = 202409 and exists (select 1 from ttkd_bsc.dongia_vttp where thang = a.thang and ma_nv = a.ma_nv
and ma_Tb = a.ma_tb and loai_tinh =  'DONGIATRA_OB');
insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON);
with ds as (
    select* from ttkd_bsc.ct_dongia_tratruoc a where thang =  202404 and loai_tinh = 'DONGIATRA_OB' and 
        (exists (select 1 from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202404  and ma_nv = a.ma_nv)) 
)
    select ds.THANG,'DONGIA_POTMASCO' LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            tien_thuyetphuc
            , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2,0 donvi_cha_id, heso_chuky, nhanvien_xuathd,tien_xuathd
    from ds
        left join ttkd_bsc.nhanvien_vttp_potmasco b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang 
--        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
--        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;
rollback;
update ttkd_Bsc.dongia_vttp 
set TIEN_DONGIA = 0
where ma_nv not in (Select ma_nv from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202409 ) and thang = 202409 and loai_tinh = 'DONGIA_POTMASCO';
commit;
update ttkd_Bsc.dongia_vttp 
set TIEN_XHDON = 0
where nvl(MANV_XHDON,'a') not in (Select ma_nv from ttkd_bsc.nhanvien_vttp_potmasco where thang =  202409) and thang = 202409 and TIEN_XHDON > 0 and loai_tinh = 'DONGIA_POTMASCO';
select* from ttkd_Bsc.dongia_vttp where nvl(MANV_XHDON,'a') not in (Select ma_nv from ttkd_bsc.nhanvien_vttp where thang = 202404) and thang = 202404 and TIEN_XHDON > 0;
--update  ttkd_Bsc.dongia_vttp set tien_dongia = round(dongia*heso*heso_Chuky), tien_xhdon = 0
--select* from ttkd_Bsc.dongia_vttp
where loai_tinh = 'DONGIA_POTMASCO' and tien_dongia != round(dongia*heso*heso_Chuky);
insert into  ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
select a.thang, a.loai_tinh, a.ma_kpi, a.ma_nv, a.thuebao_id, a.ma_tb, a.ma_nv, a.sothang_dc, a.dongia, a.dthu, a.ngay_Tt, a.khachhang_id, a.heso_dichvu, a.tien_khop ,
round(18000*heso_dichvu*heso_Chuky) tien_dongia, b.ten_nv, b.ten_to, b.ten_pb, 0 , heso_chuky, null , null 
from ttkd_Bsc.ct_dongia_tratruoc A
join ttkd_Bsc.nhanvien_vttp_potmasco b on a.ma_nv = b.ma_Nv and a.thang = b.thang 
where loai_tinh = 'DONGIA_TS_TP_TT' and a.thang = 202404
order by tien_khop;
commit;
select* from  ttkd_Bsc.dongia_Vttp where  loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO') and tien_khop >= 1 ;and tien_dongia != round(dongia*heso*heso_Chuky);
with tien as (
    select thang,ma_Nv, tien_dongia
    from ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  

)
,sl as (
    select ma_tb, ma_nv,dthu, thang
    from  ttkd_Bsc.dongia_vttp 
    where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  and ten_nv is not null
)
,abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu from sl group by ma_nv
) 

select  a.ma_nv,b.ten_nv, b.ten_pb,  abc.sltb, abc.dthu,a.thang
,sum(tien_dongia) tien
from tien a
      join ttkd_Bsc.nhanvien_vttp_potmasco b on a.ma_nv = b.ma_Nv and a.thang = b.thang 
      left join abc on a.ma_nv = abc.ma_nv
group by a.ma_nv, b.ten_nv, b.ten_pb,abc.sltb, abc.dthu, a.thang
having sum(tien_dongia) > 0;
select* from ttkd_Bsc.dongia_vttp where ma_Nv = 'HCM000501'    and loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  and ten_nv is not null;
select* from ttkd_Bsc.dongia_vttp where ( ma_Tb, ma_nv,thang) in (
select ma_Tb, ma_Nv, thang from ttkd_Bsc.dongia_vttp where loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  and ten_nv is  null);
--delete from
--select* from 
ttkd_Bsc.dongia_vttp a 
where thang>=202404 and  loai_tinh in ( 'DONGIA_TS_TP_TT','DONGIA_POTMASCO')  and ma_nv not in (select ma_Nv from ttkd_Bsc.nhanvien_vttp_potmasco);
exists (select 1 from  ttkd_Bsc.dongia_vttp where loai_Tinh = 'DONGIATRA_OB' and thang>=202404 and thang = a.thang and thuebao_id = a.thuebao_id and ma_nv = a.ma_Nv and tien_dongia > 0);
select * from ttkd_Bsc.dongia_vttp where thang = 202409 and donvi = 'TTKD'  ;
COMMIT;
select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202408;
select * from ttkd_Bsc.nhanvien where thang = 202409 and  donvi = 'TTKD' AND TEN_VTCV ='Nhân viên qu?n lý ?i?m bán';
