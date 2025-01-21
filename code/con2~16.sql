update vietanhvh.khkt_bc_hoahong a set diachi_ld = (select diachi_ld from css_hcm.db_thuebao_sub where a.thuebao_id = thuebao_id)
where nguon like 'nhuy%' and diachi_ld is null;
commit;
select* from ttkd_bsc.tl_Giahan_tratruoc where thang = 202405 and ma_pb = 'VNP0703000' and loai_tinh = 'DONGIATRA_OB';
select* from ttkd_bsc.ct_Dongia_tratruoc where thang = 202405 and ma_nv = 'CTV076445';
select* from ttkd_Bsc.nhanvien_202405 where ma_nv = 'CTV076445';
rollback;
delete from vietanhvh.khkt_bc_hoahong where nguon = 'ghtt_nhuy' and nvl(to_Number(to_char(ngay_bbbg,'yyyymm')),0) != 202405;
select* from  vietanhvh.khkt_bc_hoahong where nguon = 'ghtt_nhuy' and to_Number(to_char(ngay_bbbg,'yyyymm')) != 202405;
update vietanhvh.khkt_bc_hoahong a set ngay_bbbg = (Select ngay_Tt from ttkd_Bsc.ct_Dongia_Tratruoc 
where thang = 202405 and loai_Tinh = 'DONGIATRA_OB' and thuebao_id = a.thuebao_id AND A.MANV_PTM = MA_NV AND THUEBAO_ID NOT IN ( 8532402,11872909,11941904,11941907,8710270,11941909,8532403) ) WHERE nguon = 'ghtt_nhuy' and ngay_bbbg is null;
SELECT THUEBAO_ID FROM  ttkd_Bsc.ct_Dongia_Tratruoc 
where thang = 202405 and loai_Tinh = 'DONGIATRA_OB' GROUP BY THUEBAO_ID, MA_NV HAVING COUNT (THUEBAO_ID) > 1;
SELECT* FROM ttkd_Bsc.ct_Dongia_Tratruoc 
where thang = 202405 and loai_Tinh = 'DONGIATRA_OB' AND THUEBAO_ID IN ( 8532402,11872909,11941904,11941907,8710270,11941909,8532403);select * from css_hcm.phieutt_hd a where exists (Select ngay_tt from css_hcm.phieutt_hd where a.ma_Gd = ma_gd) and nguon = 'ghtt_nhuy' and ngay_bbbg is null;
rollback;
select kieuld_id from tmp3_ts; where thang = 202405;
select* from vietanhvh.khkt_bc_hoahong a
where nguon like 'ghtt%' and ngay_bbbg is null;
update vietanhvh.khkt_bc_hoahong e set tenkieu_ld  = (Select  LISTAGG(c.ten_kieuld, '; ') WITHIN GROUP (ORDER BY a.rkm_id) kl
from ttkd_Bsc.ct_Bsc_trasau_tp_Tratruoc a
join tmp3_ts b on a.rkm_id = b.rkm_id
join css_hcm.kieu_ld c on b.kieuld_id = c.kieuld_id
where a.thang = 202405  and a.thuebao_id = e.thuebao_id
group by a.thuebao_id)
where nguon = 'ghtt_nhuy' and e.tenkieu_ld is null;
select* from ttkd_bsc.ct_Bsc_trasau_tp_Tratruoc where thang = 202405 and ma_pb is not null; and ma_Tb = 'hungphu-f35';
delete from vietanhvh.khkt_bc_hoahong a where nguon like 'nhuy%';
insert into vietanhvh.khkt_bc_hoahong
with kld as (
    select a.thuebao_id, LISTAGG(a.ma_gd, '; ') WITHIN GROUP (ORDER BY rkm_id) ma_Gd, LISTAGG(b.ten_kieuld, '; ') WITHIN GROUP (ORDER BY rkm_id) ten_kieuld
    from tmp3_ob a
        join css_Hcm.kieu_ld b on a.kieuld_id = b.kieuld_id
    group by a.thuebao_id
)
select a.THANG_PTM, kld.ma_Gd,a.MA_KH, a.THUEBAO_ID, a.MA_TB, 4, kld.ten_kieuld, a.TEN_TB, c.DIACHI_LD, a.NGAY_BBBG, a.MA_PB, a.TEN_PB, a.MA_TO, a.TEN_TO, a.MANV_PTM, a.TENNV_PTM, a.SOTHANG_DC,
a.DATCOC_CSD, a.DTHU_DNHM, a.DTHU_GOI, a.THANG_TLDG_DT, a.SL_MAILING, a.NHOM_TIEPTHI, a.LOAI_THULAO, a.LUONG_DONGIA_NVPTM, a.LUONG_DONGIA_NVHOTRO, 'ghtt_nhuy' nguon
from vietanhvh.khkt_bc_hoahong a
    left join css_hcm.db_thuebao_sub c on a.thuebao_id =c.thuebao_id
    left join kld on a.thuebao_id = kld.thuebao_id
where  nguon like 'nhuy%'
--group by  a.THANG_PTM, a.MA_GD,a.MA_KH, a.THUEBAO_ID, a.MA_TB, a.dichvu_vt_id, a.TEN_TB, c.DIACHI_LD, a.NGAY_BBBG, a.MA_PB, a.TEN_PB, a.MA_TO, a.TEN_TO, a.MANV_PTM, a.TENNV_PTM, a.SOTHANG_DC,
--a.DATCOC_CSD, a.DTHU_DNHM, a.DTHU_GOI, a.THANG_TLDG_DT, a.SL_MAILING, a.NHOM_TIEPTHI, a.LOAI_THULAO, a.LUONG_DONGIA_NVPTM, a.LUONG_DONGIA_NVHOTRO, a.NGUON 
 ;
    commit;
select diachi_ld from css.v_db_thuebao@dataguard ;where ma_tb = 'hcmngngoctruc';

