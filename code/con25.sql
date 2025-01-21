select * from css_hcm.bd_goi_dadv where nhomtb_id = 1780126-- group by rkm_id having count(rkm_id) > 1--css_hcm.goi_dadv_ccbs;
commit;
select* from css_hcm.goi_dadv_lhtb where loaitb_id = 210
select * from mytv_tt_donle where khachhang_id = 9875714
drop table mytv_tt_donle
select * from css_hcm.goi_dadv_Ccbs
create table mesh_tt_donle as 
    select a.* , d.ma_tt from kehoach_2024 a
        left join css_hcm.db_khachhang  b on a.khachhang_id = b.khachhang_id
        left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
        left join css_hcm.db_Thuebao e on a.thuebao_id = e.thuebao_id
        left join css_hcm.db_thanhtoan d on e.thanhtoan_id = d.thanhtoan_id
    where  a.loaitb_id in (210) and c.khdn = 0 and nhomtb_id is null ; 
select thanhtoan_id from 
update ma_tt;
select* from my_Tv_ts  where ma_tt is null
drop table fiber_tt_donle
create table mesh_ts as
select a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, d.ma_tt
from css_hcm.db_Thuebao a 
    left join css_hcm.db_khachhang  b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
    left join css_hcm.db_thanhtoan d on a.thanhtoan_id = d.thanhtoan_id
    left join css_hcm.db_Adsl e on a.thuebao_id = e.thuebao_id
where a.loaitb_id in (210)  and  not exists (select 1 from css_hcm.bd_goi_dadv where a.thuebao_id = thuebao_id and trangthai = 1)
   and e.chuquan_id = 145 and a.trangthaitb_id not in (7,9,8)  and c.khdn = 0 and not exists (select 1 from kehoach_2024 where thuebao_id = a.thuebao_id) ;

create table fiber_ts as
select a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, d.ma_tt
from css_hcm.db_Thuebao a 
    left join css_hcm.db_khachhang  b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
    left join css_hcm.db_thanhtoan d on a.thanhtoan_id = d.thanhtoan_id
    left join css_hcm.db_Adsl e on a.thuebao_id = e.thuebao_id
where a.loaitb_id in (58,59)    --not exists (select 1 from css_hcm.bd_goi_dadv where a.thuebao_id = thuebao_id and trangthai = 1)
   and e.chuquan_id = 145 and a.trangthaitb_id not in (7,9,8)  and c.khdn = 0 and not exists (select 1 from kehoach_2024 where thuebao_id = a.thuebao_id) ;

create table fiber_tt_donle as 
--with tvcb as (
    select distinct goi_id
    from css_hcm.goi_dadv_lhtb
    where loaitb_id in (61,171)
--)
select* from fiber_TT_mTT
select a.*--, d.ma_tt
from kehoach_2024 a
    left join css_hcm.db_khachhang  b on a.khachhang_id = b.khachhang_id
    left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
     left join css_hcm.db_Thuebao e on a.thuebao_id = e.thuebao_id
        left join css_hcm.db_thanhtoan d on e.thanhtoan_id = d.thanhtoan_id
    where a.loaitb_id in (58,59) and c.khdn = 0 and a.fiber_donle = 0 ; 
    drop table fiber_TT
create table fiber_TT_mTT as
select  a.RKM_ID, a.THUEBAO_ID, a.MA_TB,a.ma_tt ,a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG,a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, a.SO_THANGKM, 
    a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.TRATRUOC,
     count(distinct b.thuebao_id)  as SLTB_TT_ChuaGhepGoi, 
    LISTAGG( distinct b.ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TT_ChuaGhepGoi,
    count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end)  as SLTB_TT_ChuaGhepMaTT, 
    LISTAGG( distinct case when b.ma_Tt!= a.ma_tt then b.ma_tb else null end , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TT_ChuaGhepMaTT,
     count(distinct case when to_char(b.ngay_kt_mg,'yyyymmdd')!= to_char(a.ngay_kt_mg,'yyyymmdd') then b.thuebao_id else null end)  as SLTB_TT_LechKY, 
    LISTAGG(distinct case when to_char(b.ngay_kt_mg,'yyyymmdd')!= to_char(a.ngay_kt_mg,'yyyymmdd') then b.ma_tb else null end  , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TT_LechKy
from fiber_tt_donle a 
    left join mesh_tt_donle b on a.khachhang_id = b.khachhang_id 
group by  a.RKM_ID, a.THUEBAO_ID, a.MA_TB,a.ma_tt,a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG,a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, a.SO_THANGKM, 
    a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.TRATRUOC;
select * from mytv_tt_donle  where khachhang_id = 1072199;
select thanhtoan_id from css_hcm.db_thuebao where thuebao_id = 8224171;
select ma_Tt from css_hcm.db_thanhtoan where thanhtoan_id = 8333586
select* 
select
    
    commit;
create table final_fiber_mts as
select a.RKM_ID, a.THUEBAO_ID, a.MA_TB,a.ma_tt,e.ten_pb phong_cs ,a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG,a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, a.SO_THANGKM, 
    a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.TRATRUOC, 
    a.SLTB_TT_ChuaGhepGoi, a.DSTB_TT_ChuaGhepGoi, a.SLTB_TT_ChuaGhepMaTT,a.DSTB_TT_ChuaGhepMaTT, a.SLTB_TT_LechKy, a.DSTB_TT_LechKy,
     count(distinct b.thuebao_id)  as SLTB_TS_ChuaGhepGoi, 
    LISTAGG( distinct b.ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TS_ChuaGhepGoi,
    count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end)  as SLTB_TS_ChuaGhepMaTT, 
    LISTAGG( distinct case when b.ma_Tt!= a.ma_tt then b.ma_tb else null end , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TS_ChuaGhepMaTT,
    SLTB_TT_ChuaGhepGoi + count(distinct b.thuebao_id) SLTB_ChuaGhepGoi , 
    SLTB_TT_ChuaGhepMaTT + count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end) SLTB_ChuaGhepMaTT
from fiber_TT_mTT a 
    left join mesh_ts b on a.khachhang_id = b.khachhang_id
    left join ttkd_bct.db_thuebao_ttkd c on a.thuebao_id = c.thuebao_id
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = c.pbh_ql_id
group by a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.ma_tt,a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG,a.TIEN_TD, a.CUOC_DC, a.SO_THANGDC, a.SO_THANGKM, 
    a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.TRATRUOC
    ,a.SLTB_TT_ChuaGhepGoi, a.DSTB_TT_ChuaGhepGoi, a.SLTB_TT_ChuaGhepMaTT,a.DSTB_TT_ChuaGhepMaTT, a.SLTB_TT_LechKy, a.DSTB_TT_LechKy,e.ten_pb;
    commit;
select a.*, e.ten_pb phong_cs 
from FINAL_FIBER_Tssau a
     left join ttkd_bct.db_thuebao_ttkd c on a.thuebao_id = c.thuebao_id
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = c.pbh_ql_id
where not (SLTB_ChuaGhepMaTT = 0 and SLTB_ChuaGhepGoi = 0 )
select* from my_tv_ts where khachhang_id = 9785353;
select* from css_hcm.db_khachhang where khachhang_id = 10166983
select* from css_hcm.db_Thuebao where khachhang_id = 9750106 and trangthaitb_id not in (7,8,9) and loaitb_id not in (58,59) and loaitb_id in  (11, 18, 58, 59, 61, 171, 210, 222, 224)
select* from css_hcm.goi_dadv_lhtb
select* from css_hcm.goi_dadv-- where ten_goi like '%ome TV%'
select* from css_hcm.loaihinh_tb where loaitb_id in (61,171,18)