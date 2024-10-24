select* from ttkd_bsc.ct_dongia_tratruoc where ma_tb ='vhtien1660584';
insert into ttkd_bsc.dongia_vttp 
select 202409 THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, -DONGIA,
DTHU, NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, TIEN_KHOP, -TIEN_DONGIA, TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, -TIEN_XHDON
from ttkd_bsc.dongia_vttp where ma_tb in ('thientran1','hcmthientran1');
(select  sum(tien_dongia) a from ttkd_bsc.dongia_vttp where thang = 202403  ) --where a> 0;
where a > 0-- and ma_nv = 'CTV029115'
; --141233050
select distinct dv_Cap2 from dongia_vttp where thang = 202402-- group by thuebao_id having count(Thuebao_id) >  1;

rollback;
select * from ttkd_bsc.nhanvien_vttp where thang = 202404;-- and donvi_id = 283468;
;
select * from ttkd_Bsc.dongia_vttp where thang = 202409;
commit; 
-- code moi

insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
with ds as (
    select* from ttkd_bsc.ct_dongia_tratruoc a where thang =  202409 and loai_tinh = 'DONGIATRA_OB' and 
        (exists (select 1 from ttkd_bsc.nhanvien where thang = 202409 and donvi = 'VTTP' and ma_nv = a.ma_nv) or 
        exists (select 1 from ttkd_bsc.nhanvien where thang = 202409 and donvi = 'VTTP' and ma_nv = a.nhanvien_xuathd))
)
    select ds.THANG, LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            round(nvl(tien_thuyetphuc*heso_chuky*heso_dichvu,0)) tien_tp
            , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2, dv1.donvi_cha_id, heso_chuky, nhanvien_xuathd,  round(nvl(tien_xuathd*heso_chuky*heso_dichvu,0)) tien_xp
    from ds
        left join ttkd_bsc.nhanvien b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang and donvi = 'VTTP'
        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;

update ttkd_Bsc.dongia_vttp 
set TIEN_DONGIA = 0
where ma_nv not in (Select ma_nv from ttkd_bsc.nhanvien where thang = 202409 and donvi = 'VTTP') and thang = 202409;
commit;
update ttkd_Bsc.dongia_vttp 
set TIEN_XHDON = 0
where nvl(MANV_XHDON,'a') not in (Select ma_nv from ttkd_bsc.nhanvien where thang =  202409 and donvi = 'VTTP') and thang = 202409 and TIEN_XHDON > 0;
select* from ttkd_Bsc.dongia_vttp where nvl(MANV_XHDON,'a') not in (Select ma_nv from ttkd_bsc.nhanvien_vttp where thang = 202405) and thang = 202405 and TIEN_XHDON > 0;
-- tien thu hoi
insert into ttkd_Bsc.dongia_Vttp(THANG, LOAI_TINH, MA_KPI, MA_NV, THUEBAO_ID, MA_TB, MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO, TIEN_KHOP, TIEN_DONGIA, 
TEN_NV, DV_CAP1, DV_CAP2, DONVI_ID, HESO_CHUKY, MANV_XHDON, TIEN_XHDON)
with ds as(
     select b.* from thuhoi_Bsc_dongia a 
            join ttkd_bsc.ct_Dongia_Tratruoc b on a.thang = b.thang and a.ma_Tb = b.ma_Tb and a.manv_Thuyetphuc = b.ma_Nv
            where a.thang = 202408 and a.ma_pb is null
)
  select ds.THANG+1, LOAI_TINH, MA_KPI, ds.MA_NV, THUEBAO_ID, MA_TB,ds.MA_NV MANV_THUYETPHUC, SOTHANG_DC,DONGIA, DTHU, NGAY_TT,KHACHHANG_ID, HESO_dichvu, TIEN_KHOP,
            -round(nvl(tien_thuyetphuc*heso_chuky*heso_dichvu,0)) tien_tp
            , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2, dv1.donvi_cha_id, heso_chuky, nhanvien_xuathd,  round(nvl(tien_xuathd*heso_chuky*heso_dichvu,0)) tien_xp
    from ds
        left join ttkd_bsc.nhanvien b on ds.ma_nv= b.ma_Nv and ds.thang = b.thang and donvi = 'VTTP'
        left join admin_hcm.nhanvien_onebss nv on ds.ma_nv = nv.ma_nv
        left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
;

rollback;
commit;
-- 
select* from ttkd_Bsc.dongia_Vttp where manv_xhdon = 'HCM003361' and thang = 202405;
--
with tien as (
    select thang,ma_Nv,loai_tinh, ma_kpi, nvl(tien_dongia,0) tien, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202405  --and ghichu = 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
--    and ma_nv = 'VNP017247'
    union all
    select thang,MANV_XHDON,loai_tinh, ma_kpi,  NVL(TIEN_XHDON,0) TIEN, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202405 --and loai_tinh = 'DONGIATRA_OB' --and ghichu= 'bo sung TB co chot, chua chi don gia, nhan vien xuat hoa don la nhan vien cap nhat chung tu, ty le thanh cong la ty le tien don gia xuat HD (TTCK)'
)
select a.thang,a.ma_nv,b.ten_nv,b.ten_to, b.ten_pb, sum(tien) tien
from tien a
    join ttkd_bsc.nhanvien_vttp b on a.ma_nv = b.ma_nv and a.thang = b.thang
group by a.thang,a.ma_nv,b.ten_nv,b.ten_to, b.ten_pb

;


select* from ttkd_bsc.dongia_vttp where thang = 202405;

select distinct ma_Nv from ttkd_bsc.dongia_vttp where thang = 202312 and ma_nv not in (select ma_Nv from ttkd_bsc.nhanvien_vttp where thang = 202312)
select distinct loai_tinh from ttkd_bsc.dongia_vttp where thang = 202310
            ----update dongia TTVT
select* from all_tables
where owner = 'CSS_HCM' AND table_name like 'DB_%';
select* from ttkd_Bsc.nhanvien;
insert into ttkd_Bsc.nhanvien (thang) values (-1);
------- kiem tra
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202403 and ma_nv not in (select ma_nv from ttkd_bsc.nhanvien_202403) and loai_tinh like 'DONGIATRA%'
and ma_Tb not in (select ma_Tb from dongia_vttp where thang = 202403) and ma_nv in (select ma_nv from ttkd_Bsc.nhanvien_vttp where thang = 202403)
-- insert bang backup
select sum(tien_dongia) from (
  -- insert into dongia_vttp
            select THANG, LOAI_TINH, MA_KPI, a.MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                        , MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA
                        , DTHU, NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, tien_khop, 
                         case 
                         when ht_tra_online = 0 then 0 
                         WHEN ht_tra_online > 0 then  
                         round(DONGIA*HESO*tien_khop, 0) end tien_dongia
                         , nv1.ten_nv, dv1.ten_dv dv_cap1, dv2.ten_dv dv_cap2, dv2.donvi_id 
            from ttkd_bsc.ct_dongia_tratruoc a
                         , admin_hcm.nhanvien_onebss nv1, admin_hcm.donvi dv1, admin_hcm.donvi dv2
            where thang = 202403 and ma_kpi like 'DONGIATRA%'  --and LOAI_TINH in ('DONGIATRA30D')
                            and a.ma_nv = nv1.ma_nv (+) and nv1.donvi_id = dv1.donvi_id (+) and dv1.donvi_cha_id = dv2.donvi_id (+)
--                                   and  exists (select * from ttkd_bsc.nhanvien_vttp where thang = 202402 and ma_nv = a.ma_nv)
                            and not exists (select * from ttkd_bsc.nhanvien_202403 where  ma_nv = a.ma_nv) 
--                            and ma_Tb not in (select ma_Tb from dongia_vttp where thang = 202403)
                                and dv2.donvi_id  not in 
            (11134, 11051, 284212, 284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427,2942,2943)
             )
             select* from dongia_vttp where thang = 202403;
             
select* from ttkd_bsc.ct_dongia_tratruoc a where thang =  202405 and loai_tinh = 'DONGIATRA_OB' and 
( exists (select 1 from ttkd_bsc.nhanvien_vttp where thang = 202405 and ma_nv = a.ma_nv) or  exists (select 1 from ttkd_bsc.nhanvien_vttp where thang = 202405 and ma_nv = a.nhanvien_xuathd));
-- insert bang chinh
  -- insert into ttkd_Bsc.dongia_vttp
            select a.THANG, LOAI_TINH, MA_KPI, a.MA_NV, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO
                        , MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, KHUVUC, DONGIA
                        , DTHU, NGAY_TT, NHOMTB_ID, KHACHHANG_ID, HESO, tien_khop, 
                         case 
                         when ht_tra_online = 0 then 0 
                         WHEN ht_tra_online > 0 then  
                         round(DONGIA*HESO*tien_khop, 0) end tien_dongia
                         , b.ten_nv, b.ten_to dv_cap1, b.ten_pb dv_cap2, dv1.donvi_cha_id 
            from ttkd_bsc.ct_dongia_tratruoc a
                join ttkd_Bsc.nhanvien_Vttp b on a.ma_nv = b.ma_Nv and a.thang = b.thang
                left join admin_hcm.nhanvien_onebss nv on a.ma_nv = nv.ma_nv
                left join admin_hcm.donvi dv1 on nv.donvi_id = dv1.donvi_id
            where a.thang = 202403 and ma_kpi like 'DONGIA%'  --and LOAI_TINH in ('DONGIATRA30D')
--                            and a.ma_nv = nv1.ma_nv (+) and nv1.donvi_id = dv1.donvi_id (+) and dv1.donvi_cha_id = dv2.donvi_id (+)
--                                   and  exists (select * from ttkd_bsc.nhanvien_vttp where thang = 202402 and ma_nv = a.ma_nv)
--                            and not exists (select * from ttkd_bsc.nhanvien_202403 where  ma_nv = a.ma_nv)
--                                and dv2.donvi_id  not in 
--            (11134, 11051, 284212, 284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427,2942,2943)
             )
      commit;       
select sum(tien_dongia) from ttkd_Bsc.dongia_Vttp a where thang = 202403 and ma_Nv = 'HCM013912';
select* from ttkd_bsc.dongia_Vttp a where thang = 202409; and tien_dongia > 0;
select* from ttkd_Bsc.nhanvien_202405 where ma_nv = 'CTV077619';

    select thuebao_id from ttkd_bsc.dongia_vttp where thang = 202312 group by thuebao_id having count(Thuebao_id) > 1
    ;
 select thuebao_id from css_hcm.bd_goi_dadv where trangthai = 1 and dichvuvt_id = 4 --and a.thuebao_id = thuebao_id
                                                        and goi_id not between 1715 and 1726 and goi_id not in (15414, 16221) and goi_id < 100000
       group by thuebao_id having count(thuebao_id) > 1     ;
       
    select* from css_hcm.bd_goi_dadv where trangthai = 1 and thuebao_id in (9548654,4333295,10806394,9699919,4518541);
    select trangthaitb_id from css_hcm.db_Thuebao
    select ma_tb, ma_Kh from css_hcm.db_thuebao a join css_hcm.db_khachhang b on a.khachhang_id = b.khachhang_id  and thuebao_id in (9548654,4333295,10806394,9699919,4518541);
       --   and nhomtb_id not in (2691065)
                            
                            select* from ttkd_bsc.nhanvien_202402 where ten_nv like '%Ý'
select* from ttkd_bsc.nhanvien_Vttp where ma_nv in ('024359157','CTV080974','CTV029038') and thang = 202309
    select* from  admin_hcm.donvi where donvi_id not  in (11134, 11051, 284212, 284317, 11564, 11563, 11352, 284316, 2941, 2945, 2947, 2946, 2948, 2944, 283413, 283429, 283527, 284199, 283427)
select* from ct_bsc_tratruoc_moi where ma_Tb = 'mesh0080658' and thang = 202311
select avG(sothang_dc) from ttkd_bsc.ct_dongia_Tratruoc where thang = 202310 and loai_tinh LIKE 'DONGIA%'-- and ma_kpi = 'HCM_TB_GIAHA_022'
select * from ttkd_bsc.tl_giahan_tratruoc where thang = 202307 and loai_tinh = 'KPI_PB' and ma_kpi = 'HCM_TB_GIAHA_022'
delete  from ttkd_bsc.dongia_vttp  where thang = 202311 and dv_Cap2 like 'Phòng %'
commit;
select distinct heso from ttkd_bsc.dongia_vttp where thang = 202310
select* from ttkd_bct.ds_ketqua_capnhat_dai where thang_kt=0 and to_number(to_char(ngay_cn, 'yyyymm')) = 202312
;
--select sum(tong_tien) from (
with tien as (
    select thang,ma_nv, tien_dongia
    from ttkd_bsc.dongia_vttp where thang = 202409
--    and tien_dongia > 0
    union all
    select thang,MANV_XHDON, TIEN_XHDON
    from ttkd_bsc.dongia_vttp where thang = 202409
)
select a.ma_nv, b.ten_nv, b.ten_pb, sum(tien_dongia) tong_Tien
from tien a
    join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and a.thang = b.thang and donvi = 'VTTP'
group by a.ma_nv, b.ten_nv, b.ten_pb
having sum(tien_dongia) > 0;
);
---- xuat file
with tien as (
    select thang,ma_Nv,loai_tinh, ma_kpi, tien_dongia, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202409  
    union all
    select thang,MANV_XHDON,loai_tinh, ma_kpi,  TIEN_XHDON, donvi_id
    from ttkd_Bsc.dongia_vttp 
    where thang = 202409 
)
,sl as (
    select ma_tb, ma_nv,dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202409
    union all
    select ma_tb, manv_xhdon,  dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202409 and manv_xhdon is not null and manv_xhdon != ma_nv
)
,abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu from sl group by ma_nv
) 

select a.thang,a.ma_nv,b.ten_nv, b.ten_pb, abc.sltb, abc.dthu,sum(tien_dongia) tong_tien
from tien a
    join ttkd_bsc.nhanvien b on a.ma_nv = b.ma_nv and a.thang = b.thang and b.donvi ='VTTP'
    left join abc on a.ma_nv = abc.ma_nv
group by a.thang,a.ma_nv,b.ten_nv, b.ten_pb, abc.sltb, abc.dthu
having sum(tien_dongia) > 0;
select* from ttkd_bsc.dongia_vttp where thang = 202409

;
select* from ttkd_Bsc.ct_dongia_Tratruoc where ma_Tb = 'vhtien1660584';
--select sum (tien) from (
with tien as (
    select thang,ma_Nv, tien_dongia
    from ttkd_Bsc.dongia_vttp 
    where thang = 202409  --and tien_dongia > 0
    union all
    select thang,MANV_XHDON,  TIEN_XHDON
    from ttkd_Bsc.dongia_vttp 
    where thang = 202409 
)
,sl as (
    select ma_tb, ma_nv,dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202409
    union all
    select ma_tb, manv_xhdon,  dthu
    from  ttkd_Bsc.dongia_vttp 
    where thang = 202409 and manv_xhdon is not null and manv_xhdon != ma_nv
)
,abc as (
    select ma_nv, count(distinct ma_tb) sltb, sum(dthu) dthu from sl group by ma_nv
) 

select a.ma_nv,b.ten_nv, b.ten_pb,  abc.sltb, abc.dthu,sum(tien_dongia) tien
from tien a
      join ttkd_Bsc.nhanvien b on a.ma_nv = b.ma_Nv and a.thang = b.thang and donvi = 'VTTP'
      left join abc on a.ma_nv = abc.ma_nv
group by a.ma_nv, b.ten_nv, b.ten_pb,abc.sltb, abc.dthu
having sum(tien_dongia) > 0
--, abc.sltb, abc.dthu
--)