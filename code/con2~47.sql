select MA_TB ,PHONG_CS,MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MA_GD, RKM_ID, THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT,
KENHTHU, TEN_NGANHANG, TEN_HT_TRA, tien_khop
from ttkd_Bsc.ct_Bsc_tratruoc_moi_30day where thang= 202409 and ma_Tb='htlhang2022';
select* from
insert into;
rollback;
drop table tra_sau_ct;
commit;
delete from ct_Bsc_Chungtu where thang = 202410;
insert into ct_Bsc_Chungtu (CHUNGTU_ID, MA_CHUNGTU,  tongtra, MA_GD, MA_TT, TRA_TRUOC,  NGAY_TT, TINH_BSC,tinh_dongia, THANG, NV_GACH, GHICHU,ma_Tb);
DROP TABLE trasau_ct;
create table trasau_ct as
with tsau as (
    select DISTINCT p.chungtu_id, ct.ma_Tb
    from   ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 p 
        join ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a on p.chungtu_id = a.chungtu_id
        join qltn_Hcm.ct_Tra ct on p.phieu_id = ct.phieu_id 
    WHERE CT.KY_CUOC = 20241001
    AND (to_Char(a.ngay_ht,'yyyymm') ='202410' or a.ghi_chu is not null) 

)
select* from tsau;
;
SELECT* FROM trasau_ct;
commit;
drop table tmp_Ctu;
select ma_ct, sum(sl_matb) from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202411 and traTruoc = 0 group by ma_ct, nguoi_cn; having count(distinct sl_matb)>1 ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc where thang = 202411 and traTruoc = 0 and ma_Ct in ('VCB_20241101_435259');,'VCB_20241021_417021',
'VCB_20240726_263344');
DELETE FROM ct_Bsc_Chungtu WHERE THANG = 202410;
create table tmp_Ctu as 
select chungtu_id, count(distinct ma_Tb) sltb
from trasau_ct
group by chungtu_id;
create table ds_Chungtu as ;
create index idx_ctu on nhuy.ct_bsc_chungtu ( thang,chungtu_id, ma_chungtu);

select  nhanvien_id from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a where to_Char(a.ngay_ht,'yyyymm') ='202410' or a.ghi_chu is not null;
select* from ds_Chungtu;
delete from tra_sau_ct b where not exists (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a where
(to_Char(a.ngay_ht,'yyyymm') ='202410' or a.ghi_chu is not null) and a.chungtu_id = b.chungtu_id);
create index inx_temp_ttt on trasau_ct(chungtu_id);
alter table ct_Bsc_Chungtu add (sl_matb number);
insert into ct_Bsc_Chungtu (CHUNGTU_ID, MA_CHUNGTU,  tongtra, MA_GD, MA_TT, TRA_TRUOC,  NGAY_TT, TINH_BSC,tinh_dongia, THANG, NV_GACH, GHICHU, sl_matb)
with aaa as (select  distinct nhanvien_id, ma_nd from admin_Hcm.nguoidung),
        bbb as (select distinct ma_gd, thungan_Tt_id from css_hcm.phieutt_Hd where thungan_Tt_id is not null),
        ccc as (select  nhanvien_id, ma_nv from admin_Hcm.nhanvien_onebss)
select  a.CHUNGTU_ID, a.MA_CT ma_Chungtu,  a.tongtra,  case when a.traTruoc = 1 then a.ma  else null end ma_gd,
            case when a.traTruoc = 0 then a.ma else null end ma_tt , a.TRATRUOC 
            ,to_char(a.NGAY_HT,'dd/mm/yyyy') ngay_tt, case when a.TUDONG = 1 then 0 else 1 end tinh_bsc,
            case when a.TUDONG = 1 then 0 else 1 end tinh_dongia, 
            202411 THANG,  
            case when a.tratruoc = 1 then nv.ma_nv else nvtt.ma_nv end nv_gach, a.GHI_CHU , a.sl_matb
from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a
    left join aaa b on a.nguoi_cn = b.ma_nd
    left join admin_Hcm.nhanvien_onebss nvtt on b.nhanvien_id = nvtt.nhanvien_id
    left join bbb p on a.ma = p.ma_gd
    left join  ccc nv on p.thungan_tt_id = nv.nhanvien_id
where a.thang = 202411;
commit;

select * from  nhuy.x_chungtu_202410;
select count(1) from  ttkd_bct.bangiao_chungtu_tinhbsc a where a.thang =202410 or a.ghi_chu is not null;
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202410 and ma_to = 'VNP0702308' and ma_kpi = 'HCM_TB_GIAHA_024';
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202410 and ma_NV = 'VNP017763' and ma_kpi = 'HCM_TB_GIAHA_024';
UPDATE ttkd_Bsc.tl_Giahan_Tratruoc SET TONG = 6, TYLE = 100 where thang = 202410 and ma_to = 'VNP0702308' and ma_kpi = 'HCM_TB_GIAHA_024' AND LOAI_TINH = 'KPI_TO';

SELECT* FROM TTKD_bSC.BANGLUONG_KPI where thang = 202410 and ma_to = 'VNP0702308' and ma_kpi = 'HCM_TB_GIAHA_027';
UPDATE TTKD_bSC.BANGLUONG_KPI SET TYLE_tHUCHIEN = 100, MUCDO_HOANTHANH = 120 where thang = 202410 and ma_to = 'VNP0702308' and ma_kpi = 'HCM_TB_GIAHA_027';

SELECT* FROM TTKD_bSC.BLKPI_DM_TO_PGD WHERE THANG = 202410 AND  ma_to = 'VNP0702308';
SELECT* FROM  TTKD_bSC.BANGLUONG_KPI where thang = 202410 and ma_NV = 'VNP017763' and ma_kpi = 'HCM_TB_GIAHA_027';
select nhanvien_id from admin_Hcm.nhanvien_onebss group by nhanvien_id ;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_tinh_bsc a