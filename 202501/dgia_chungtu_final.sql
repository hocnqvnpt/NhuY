SELECT * FROM ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a WHERE ;
update ct_Bsc_Chungtu set thang = 202501 where thang = 202502;
delete from ct_Bsc_Chungtu where thang = 202501;    VCB_20250106_546103, VCB_20250122_580508
select* from    ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202501 and ma_Tb in ('hcm_smartca_00110071',
'hcm_smartca_00110082',
'hcm_smartca_00110092',
'hcm_smartca_00110086',
'hcm_smartca_00110094',
'hcm_smartca_00110085',
'hcm_smartca_00110073',
'hcm_smartca_00110081',
'hcm_smartca_00110070',
'hcm_smartca_00110083',
'hcm_smartca_00109987',
'hcm_smartca_00110093',
'hcm_smartca_00110084',
'hcm_smartca_00110087',
'hcm_smartca_00110074',
'hcm_smartca_00110075',
'hcm_smartca_00110072',
'hcm_smartca_00110096',
'hcm_smartca_00110090',
'hcm_smartca_00110077',
'hcm_smartca_00110097',
'hcm_smartca_00110089',
'hcm_smartca_00110079');
commit;
select* from ct_Bsc_Chungtu where thang = 202502;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202501 and nguoi_cn ='tnh_4' and tudong= 1;
--- insert vao bang chot
rollback;
insert into ct_Bsc_Chungtu (CHUNGTU_ID, MA_CHUNGTU,  tongtra, MA_GD, MA_TT, TRA_TRUOC,  NGAY_TT, TINH_BSC,tinh_dongia, THANG, NV_GACH, GHICHU, sl_matb)
with aaa as (select  distinct nhanvien_id, ma_nd from admin_Hcm.nguoidung),
        bbb as (select distinct ma_gd, thungan_Tt_id from css_hcm.phieutt_Hd where thungan_Tt_id is not null),
        ccc as (select  nhanvien_id, ma_nv from admin_Hcm.nhanvien_onebss)
select  a.CHUNGTU_ID, a.MA_CT ma_Chungtu,  a.tongtra,  case when a.traTruoc = 1 then a.ma  else null end ma_gd,
            case when a.traTruoc = 0 then a.ma else null end ma_tt , a.TRATRUOC 
            ,to_char(a.NGAY_HT,'dd/mm/yyyy') ngay_tt, case when a.TUDONG = 1 then 0 else 1 end tinh_bsc,
            case when a.TUDONG = 1 then 0 else 1 end tinh_dongia, 
            202502 THANG,  
            case when a.tratruoc = 1 then nv.ma_nv 
                when a.tratruoc = 0 and kh_dacbiet = 1 then 'dl_mailinh'
                else nvtt.ma_nv end nv_gach, a.GHI_CHU , a.sl_matb
                
from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a
    left join aaa b on a.nguoi_cn = b.ma_nd
    left join admin_Hcm.nhanvien_onebss nvtt on b.nhanvien_id = nvtt.nhanvien_id
    left join bbb p on a.ma = p.ma_gd
    left join  ccc nv on p.thungan_tt_id = nv.nhanvien_id
where a.thang = 202501;
commit;
---- loai chung tu
update ct_Bsc_Chungtu c
set tinh_dongia = 0
--select* from ct_Bsc_Chungtu c
where thang = 202501 and tra_truoc = 1 and exists (select 1 from ttkd_bsc.ct_dongia_tratruoc a 
    join ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt b on a.thang = b.thang and a.ma_Tb = b.ma_Tb and b.manv_Thuyetphuc = a.ma_nv
    where  loai_tinh = 'DONGIATRA_OB' and (tien_xuathd*heso_Chuky*heso_dichvu) > 0
and c.ma_gd = b.ma_gd and c.nv_gach = a.nhanvien_xuathd) and tinh_Dongia = 1;
commit;
---- tinh toan
select* from ct_Bsc_chungtu where ma_Chungtu='VCB_20250112_457066';

update ct_Bsc_Chungtu set tinh_Bsc = 0, tinh_dongia = 0
--select distinct chungtu_id from ct_Bsc_Chungtu
where tra_truoc = 0 and thang = 202501 and (chungtu_id, ma_tt) in (
-- lay danh sach chung tu id600, bo hinh thuc UNC/UNT theo file
select a.chungtu_id,
                b.ma_tt--,b.ma_tb
                from ttkdhcm_ktnv.ds_chungtu_nganhang_oneb a, ttkdhcm_ktnv.ds_chungtu_nganhang_baocao_pktkh_ts b, qltn_hcm.hinhthuc_tt c
                where a.chungtu_id = b.chungtu_id 
                            and b.httt_id = c.httt_id
                            and to_number(to_char(b.ngay_tt, 'yyyymm')) = 202501 and tongtra> 0 and  c.httt_id  in (170, 171)
                            )
                            
  -- doan lay httt_id = 192 KBNN
;
commit;
insert into final_Ctu;
---- lay thong tin nguoi thanh toan
with 
chot as (select nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia
        , sum(case when tra_truoc = 0 and tinh_dongia = 1 then ct.sl_matb else 0 end) as sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc,
        sum(CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.sl_matb ELSE NULL END) AS sl_matb_bsc
        from ct_Bsc_Chungtu ct
        where thang = 202501
        group by nv_Gach, ma_chungtu
) 
, final as (
    select a.*, case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_dongia > 0 and sl_matb_dongia > 0 then 1.5
                    when sl_matb_dongia >= 5 or sl_magd_dongia > 1 then 1.5
                    when sl_matb_dongia between 1 and 4 or sl_magd_dongia = 1 then 1
                    else null end heso_dongia, 4000 dongia
                ,case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_bsc > 0 and sl_matb_bsc > 0 then 1.5
                    when sl_matb_bsc >= 5 or sl_magd_bsc > 1 then 1.5
                    when sl_matb_bsc between 1 and 4 or sl_magd_bsc = 1 then 1
                    else null end heso_bsc, 202501 thang
    from chot a
)
select* from final;
commit;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where ma_ct in ('VCB_20250122_580508','VCB_20250106_546103');
update final_Ctu_tien set thang= -202501 where thang = 202501;
delete from final_Ctu_tien  where thang = 202501;
select* from final_Ctu_tien where thang = 202501;
insert into final_Ctu_tien
with chot as (select thang,nv_Gach, ma_chungtu, COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_dongia = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_dongia
        , sum(case when tra_truoc = 0 and tinh_dongia = 1 then ct.sl_matb else 0 end) as sl_matb_dongia
        ,COUNT(DISTINCT CASE WHEN tra_truoc = 1 and tinh_Bsc = 1 THEN ct.ma_gd ELSE NULL END) AS sl_magd_bsc,
        sum(CASE WHEN tra_truoc = 0 and tinh_bsc = 1 THEN ct.sl_matb ELSE NULL END) AS sl_matb_bsc
        from ct_Bsc_Chungtu ct
        where thang = 202501
        group by nv_Gach, ma_chungtu,thang
) 
, final as (
    select a.*, case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_dongia > 0 and sl_matb_dongia > 0 then 1.5
                    when sl_matb_dongia >= 5 or sl_magd_dongia > 1 then 1.5
                    when sl_matb_dongia between 1 and 4 or sl_magd_dongia = 1 then 1
                    else null end heso_dongia, 4000 dongia
                ,case when nv_Gach like 'dl_mailinh%' then 2
                    when sl_magd_bsc > 0 and sl_matb_bsc > 0 then 1.5
                    when sl_matb_bsc >= 5 or sl_magd_bsc > 1 then 1.5
                    when sl_matb_bsc between 1 and 4 or sl_magd_bsc = 1 then 1
                    else null end heso_bsc
    from chot a
) 
,tt as (select case when nv_gach like 'dl_mailinh%' then 'VNP016966' else nv_Gach end ma_Nv , heso_Dongia, NVL(heso_Dongia,0)*dongia tien,ma_chungtu,dongia, heso_bsc
            from final
            where heso_bsc is not null
)
        
select  a.thang, TT.MA_NV,TEN_NV, TEN_TO,TEN_PB, MA_PB, MA_TO, SUM(TIEN) TIEN,count(ma_chungtu) so_chungtu_Gach
,sum(heso_bsc) slct_quydoi_bsc
, sum(heso_Dongia) slct_quydoi_dongia, null
from tt 
    join ttkd_Bsc.nhanvien a ON TT.MA_nV = A.MA_nV AND thang = 202501 and donvi = 'TTKD'
where ma_to = 'VNP0700940'
GROUP BY TT.MA_NV, MA_PB, MA_TO,TEN_NV, TEN_TO,TEN_PB, a.thang
having SUM(TIEN) > 0;
rollback;
commit;
select* from ct_Bsc_chungtu where chungtu_id =542409;
select * from final_Ctu_tien where thang =202501;in ( 202501 ,112024);
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202501 and loai_Tinh = 'DONGIA_CHUNG_TU';
insert into ttkd_Bsc.tl_Giahan_Tratruoc (THANG, MA_PB, MA_TO, MA_NV, TIEN, MA_KPI, LOAI_TINH)
select thang, ma_pb, ma_to,ma_nv,tien, 'DONGIA' MA_KPI, 'DONGIA_CHUNG_TU' LOAITINH
FROM final_Ctu_tien WHERE THANG = 202501;
delete from  ttkd_Bsc.tl_Giahan_Tratruoc where thang =202501 and loai_tinh= 'DONGIA_CHUNG_TU';
select * from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where chungtu_id in(542409,
542411,
542326,
580508,
539974,
562200,
12664,
572037,
542407,
542408,
572651,
25900,
562964); (492023,520452,526656,533277,539266,538688,538782,539262,536808);
select distinct chungtu_id from ct_bsc_chungtu where chungtu_id in (492023,520452,526656,533277,539266,538688,538782,539262,536808);
select distinct chungtu_id,ma_chungtu from ct_bsc_chungtu where tinh_Bsc = 0 and thang = 202501 and nv_Gach = 'VNP016966';
commit;
insert into ct_Bsc_chungtu (thang, chungtu_id, sl_matb, nv_Gach) values (202501,526656,9,'VNP016966');
insert into ct_Bsc_chungtu (thang, chungtu_id, sl_matb, nv_Gach) values (202501,492023,9,'VNP016966');
update ct_Bsc_chungtu set  tinh_Bsc= 1, tinh_dongia = 1 , tra_truoc = 0 where thang = 202501 and  chungtu_id in (526656,492023);
update ct_Bsc_chungtu a 
set ma_Chungtu = (select ma_Ct from  ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where chungtu_id = a.chungtu_id)
 where thang = 202501 and  chungtu_id in (526656,492023);