select* from css_hcm.dichvu_gt;
SELECT* FROM css_hcm.db_Thuebao where ma_Tb in ('hcmvnpt47751','vnpt47751');
select DICHVUGT_ID from css.v_db_cntt@dataguard ;
select* from css_hcm.dangky_dvgt;
select* from css.v_SUDUNG_DVGT@dataguard;
DROP TABLE xgspon;
drop table hd;
select * from (
with hd as
(;
drop table hd;
create table hd as
    select b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id, e.ten_dv, dv2.ten_dv dv_rp,b.hdtb_id
--        row_number() over (partition by  b.thuebao_id, b.ma_Tb, kieuld_id, loaihd_id order by  b.hdtb_id desc) rn
    from css.v_hd_khachhang@dataguard a
        join css.V_hd_thuebao@dataguard b on a.hdkh_id = b.hdkh_id
        left join admin.nhanvien@dataguard c on a.ctv_id = c.nhanvien_id
        left join admin.donvi@dataguard d on c.donvi_id = d.donvi_id
        left join admin.donvi@dataguard e on d.donvi_cha_id = e.donvi_id
        left join admin.donvi@dataguard dv1 on a.donvi_id = dv1.donvi_id
        left join admin.donvi@dataguard dv2 on dv1.donvi_cha_id = dv2.donvi_id
    where b.tthd_id  <> 7;
);

select a.*, nvl(hd.ten_dv, dv_rp) ten_dv
from xgspon a
    left join hd on a.ma_tb = hd.ma_Tb and a.loaihd_id = hd.loaihd_id and a.kieuld_id = hd.kieuld_id and rn = 1
  ;
 select* from ttkd_Bsc.nhanvien where ma_Nv = 'CTV033341' ;
select* from ttkd_bct.khkt_bc_hoahong;

select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202409 and loai_tinh = 'DONGIATRA_OB';
insert into ttkd_bct.khkt_bc_hoahong (thang_ptm) values (0); 
rollback;
s
commit;
select sum(nvl(LUONG_DONGIA_NVPTM,0))+sum(nvl(LUONG_DONGIA_NVHOTRO,0))
--select* 
from ttkd_bct.khkt_bc_hoahong
where thang_ptm = 202406 and nguon in ('ghtt_nhuy','ghtt_nhuy_ctu') and manv_ptm ='VNP020230';
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202406 and ma_Nv ='VNP020230';
update  ttkd_bct.khkt_bc_hoahong LUONG_DONGIA_NVHOTRO set LUONG_DONGIA_NVHOTRO = 0 where thang_ptm = 202406 and nguon = 'ghtt_nhuy';
select DISTINCT MA_NV, TEN_NV, TEN_PB from ttkd_bsc.blkpi_dm_to_pgd where thang = 202410 ;and dichvu = 'CSKH';
select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202410 ;and dichvu = 'CSKH';
select* from ttkd_Bsc.bangluong_kpi where thang in ( 202409) and ma_kpi = 'HCM_TB_GIAHA_027' ;AND MA_VTCV = 'VNP-HNHCM_BHKV_2';
update  ttkd_Bsc.bangluong_kpi  set chitieu_Giao = null where thang = 202410 and  ma_kpi = 'HCM_TB_GIAHA_023'
and ma_nv not in (select  MA_NV  from ttkd_bsc.blkpi_dm_to_pgd where thang = 202410 and dichvu = 'CSKH') 
and ma_nv not in ('VNP017699','VNP017621');
commit;
select* from ttkd_Bsc.blkpi_danhmuc_kpi where thang = 202411 and nguoi_xuly ='Nh? Ý';
select* from  ttkd_Bsc.bangluong_kpi where thang = 202410;
select* from ttkd_bsc.tl_Giahan_tratruoc where thang = 202406 and ma_nv = 'CTV021917';
select sum(tien_Thuyetphuc*heso_chuky*heso_dichvu) from ttkd_Bsc.ct_Dongia_Tratruoc where thang = 202406 and ma_Nv ='VNP020230' and ipcc = 1;
select* from  ttkd_bct.khkt_bc_hoahong  where  thang_ptm = 202406 and nguon ='ghtt_nhuy_ctu';
select  sum(nvl(luong_dongia_nvptm,0)+nvl(luong_dongia_nvhotro,0)) tong,'NY' nguon 
        from ttkd_bct.khkt_bc_hoahong where thang_ptm = 202411 and nguon in ('ghtt_nhuy','ghtt_nhuy_ctu','tstt') and manv_ptm ='CTV086803'
        union all
        select luong_dongia_ghtt ,'aHocj'from ttkd_bsc.bangluong_dongia_202411 where ma_nv='CTV086803';
        select  sum(nvl(luong_dongia_nvptm,0)+nvl(luong_dongia_nvhotro,0)) tong,'NY' nguon from ttkd_bct.khkt_bc_hoahong where thang_ptm = 202406 and nguon in ('ghtt_nhuy','ghtt_nhuy_ctu') and manv_ptm ='CTV021917'
        union all
        select luong_dongia_ghtt ,'aHocj'from ttkd_bsc.bangluong_dongia_202406 where ma_nv='CTV021917';
        select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202406 and nhanvien_xuathd ='VNP020230';
update ttkd_Bsc.bangluong_kpi
set chitieu_Giao = 100
where thang in ( 202410) and ma_kpi = 'HCM_TB_GIAHA_022' and ma_Nv in ('VNP017072','VNP017150');
insert into  ttkd_bct.khkt_bc_hoahong

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'Bang rong co dinh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong',null, tien_xuathd*heso_Chuky*heso_dichvu,
        'ghtt_nhuy_ctu',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.nhanvien_xuathd =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang IN ( 202411) and loai_tinh = 'DONGIATRA_OB' and nv.ma_pb != 'VNP0700600'
    ;
    COMMIT;
insert into  ttkd_bct.khkt_bc_hoahong

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'Bang rong co dinh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong', tien_thuyetphuc*heso_Chuky*heso_dichvu,null,
        'ghtt_nhuy',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang IN ( 202411) and loai_tinh = 'DONGIATRA_OB' and nv.ma_pb != 'VNP0700600'
    ;
    select* from  ttkd_bct.khkt_bc_hoahong where thang_ptm = 202411  and nguon = 'ghtt_nhuy';
    commit;
        