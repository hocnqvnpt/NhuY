drop table cdcn_td;
drop table xgspon;

select* from xgspon;
select* from 
select* from cdcn_td where ma_Tb='167-d13';
flashback table thd to before drop;
select* from thd where ma_Tb='dnds3';
select distinct kieuld_id, loaihd_id from pon;
create table cdcn_td as
select b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id, e.ten_dv dv_Thuyetphuc, dv2.ten_dv dv_rp, b.hdtb_id
--        row_number() over (partition by  b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id order by  b.hdtb_id desc) rn
from css.v_hd_khachhang@dataguard a
    join css.V_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
    left join admin.nhanvien@dataguard c on a.ctv_id = c.nhanvien_id
    left join admin.donvi@dataguard d on c.donvi_id = d.donvi_id
    left join admin.donvi@dataguard e on d.donvi_cha_id = e.donvi_id
    left join admin.donvi@dataguard dv1 on a.donvi_id = dv1.donvi_id
    left join admin.donvi@dataguard dv2 on dv1.donvi_cha_id = dv2.donvi_id
where kieuld_id in (24,743,51,71,72,19581,19582,19584)
;
select ma_Tb from tmp_Th group by ma_Tb having count(ma_tb)>1;
select* from thd; where ma_Tb='yhocsaigon'; 

drop table tmp_th;

create table tmp_th as 
with b as (
    select thuebao_id,ma_tb, row_number() over (partition by   ma_Tb order by  hdtb_id desc) rn , dv_rp
                from cdcn_td where  
                kieuld_id in (24,743,19581,19582,19584)
)
select a.*, case when a.kieuld_id in (71,72) then  b.dv_rp end thaythe
               
from cdcn_td a 
    left join 
                b on a.thuebao_id = b.thuebao_id and rn = 1
    join xgspon c on a.ma_tb = c.ma_Tb;
    drop table hdong;
with hd as 
(;
create table hdong as
    select a.*, 
    row_number() over (partition by   ma_Tb order by  hdtb_id desc) rn 
    from tmp_th a;
);
select* from hdong;
drop table xxxx;
create table xxxx as;
select a.*, 
            case when hd.loaihd_id = 1 then nvl(dv_thuyetphuc ,dv_rp)
                 when hd.kieuld_id in (71,72) then nvl(thaythe, dv_rp)
                else dv_rp end ten_dv
from xgspon a 
    join hdong hd on a.ma_tb = hd.ma_Tb and hd.rn = 1;
where congnghe = 'XGSPON';
    select* from css_hcm.loai_kh where UPPER(ten_loaikh) like '%THÙ%';
where hd.ma_tb='167-d13' ;
select* from xxxx a
    join css_hcm.db_Thuebao b on a.ma_Tb=  b.ma_Tb and b.loaitb_id = 58
where --THIETBI LIKE '%xgspon%';
(congnghe = 'xgspon' AND THIETBI LIKE '%xgspon%') OR 
(CONGNGHE = 'Gxgspon' AND THIETBI = 'Thi?t b? ??u cu?i xgspon Nokia.XS-2426G-A.'); where ma_Tb='167-d13';
where a.ma_Tb='dnds3';
    select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202409 and loai_tinh = 'DONGIATRA_OB' and tien_khop = 1 and ma_pb = 'VNP0703000';
select* from ttkd_Bsc.nhanvien where thang = 202410 and ma_Nv ='CTV033341';
merge into ttkd_bsc.nhanvien a
using nhuy.nhanvien_202411_l2 b
on (a.ma_nv = b.ma_nv)
when matched then
update set a.THAYDOI_VTCV = b.THAYDOI_VTCV
where a.thang = 202411 and donvi = 'TTKD';
rollback;
drop table temp;
create table temp as
--    select  * from v_thongtinkm_all where ma_Tb='ntbaongoc87';
with skm as (
    select thuebao_id, loaitb_id, ma_Tb,tyle_sd, cuoc_sd, congvan_id, rkm_id, nhom_datcoc_id,chitietkm_id from v_thongtinkm_all
    where tyle_sd <> 100 and tyle_sd + cuoc_sd > 0 and nvl(nhom_datcoc_id, 0) not in (11) and ma_Tb in (select ma_Tb from xxxx)
    and 202411 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
    and loaitb_id in (58, 59) --and thang_bddc > 202001
),
cuoc as (
select 
distinct a.thuebao_id,skm.rkm_id,skm.nhom_datcoc_id, a.ma_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) cuoc_tg
 , decode(skm.tyle_sd, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg), 
                        0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg) - skm.cuoc_sd, 
                        (100 - skm.tyle_sd) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tg)/100) cuoc_saukm--, nvl(tt.times, 0) tb_lan_tt
, skm.tyle_sd, goi1.tien_goi, f.cuoc_tg tg, skm.cuoc_sd
from xxxx join css.v_db_thuebao@dataguard a on xxxx.ma_Tb=  a.ma_tb and a.loaitb_id in (58,59)
            join css_hcm.db_adsl d on a.thuebao_id = d.thuebao_id
            join css_hcm.tocdo_adsl e on d.tocdo_id = e.tocdo_id
            left join css_hcm.muccuoc_tb f on a.mucuoctb_id = f.mucuoctb_id
            left join css_hcm.bd_goi_dadv goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi_id not between 1715 and 1726
            left join css_hcm.goi_dadv ten_goi on goi.goi_id = ten_goi.goi_id
            left join css_hcm.goi_dadv_lhtb goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
            left join  skm on a.thuebao_id = skm.thuebao_id 
) 
select a.* ,c.muccuoc, d.cuoc_saukm
from xxxx a
    left join css.v_db_Thuebao@dataguard b on a.ma_Tb=  b.ma_Tb
    left join css_hcm.muccuoc_Tb c on b.mucuoctB_id =c.mucuoctb_id
    left join cuoc d on b.thuebao_id = d.thuebao_id;
    select* from temp where ma_Tb in (
    select ma_Tb from temp group by ma_Tb having count(ma_Tb) > 1);
    delete from temp where ma_Tb ='39803311' and muccuoc ='IMS_Thuê bao';
    select * from css.v_Db_Thuebao@dataguard where ma_Tb='ntbaongoc87';
    commit;
    select  * from v_thongtinkm_all where ma_Tb='ntbaongoc87';
    update temp set cuoc_saukm  = 296364 where  ma_Tb='ntbaongoc87';
    select* from ttkd_Bsc.bangluong_kpi where thang = 202410 and ten_Nv like '%H?i Nhân';
    select* from css_hcm.bd_Goi_Dadv where thuebao_id =7227432;
    select* from ttkd_Bsc.nhanvien where ma_Nv='CTV081382';
    select* from temp;
    select ten_nv, ma_nv, TONG_LUONG_DONGIA from ttkd_Bsc.bangluong_dongia_202307 where  ten_Nv like '%Thanh H?i';;  ten_Nv like '%Ph??ng Th?o';