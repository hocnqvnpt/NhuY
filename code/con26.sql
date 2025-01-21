create table met as select* from (
select a.RKM_ID,  a.THUEBAO_ID,  a.MA_TB, --a.NGAY_KT_MG, b.NGAY_KT_MG a,b.ma_Tb b,
a.MA_TT,  a.LOAITB_ID,  a.NGAY_BDDC,  a.NGAY_KTDC,  a.NGAY_KT_MG,  a.TIEN_TD,  a.CUOC_DC,  a.SO_THANGDC, 
a.SO_THANGKM,
 a.CONGVAN_ID,  a.KHUYENMAI_ID,  a.CHITIETKM_ID,  a.NHOM_DATCOC_ID,  a.SL_DATCOC,  a.NHOMTB_ID,  a.GOI_ID,  a.TRANGTHAITB_ID,  a.KHACHHANG_ID,  a.TRATRUOC, 
 a.SLTB_TT_CHUAGHEPGOI,  a.DSTB_TT_CHUAGHEPGOI,  a.SLTB_TT_CHUAGHEPMATT,  a.DSTB_TT_CHUAGHEPMATT,
 a.SLTB_TT_LECHKY,  a.DSTB_TT_LECHKY
    
 ,b.ma_tb mesh , b.ngay_kt_mg kt_mesh, round(abs(b.ngay_kt_mg - a.ngay_kt_mg),0) ngay_lech,
    to_char(abs(to_number(to_char(b.ngay_kt_mg,'mm'))-to_number(to_char(a.ngay_kt_mg,'mm')))) || ' tháng ' ||
    to_char(abs(to_number(to_char(b.ngay_kt_mg,'dd'))-to_number(to_char(a.ngay_kt_mg,'dd'))))   || ' ngày' 
      thang_lech
    , case when to_char( a.ngay_kt_mg,'mm')  =  to_char(b.ngay_kt_mg,'mm') then 1 else 0 end as cung_thang
    , case when to_number(to_char(a.ngay_kt_mg,'yyyymmdd')) < to_number(to_char(b.ngay_kt_mg,'yyyymmdd')) then 1 
        else 0 end as mesh_trehon, 
        row_Number()  OVER (PARTITION BY a.RKM_ID,  a.THUEBAO_ID,  a.MA_TB, --a.NGAY_KT_MG, b.NGAY_KT_MG a,b.ma_Tb b,
a.MA_TT,  a.LOAITB_ID,  a.NGAY_BDDC,  a.NGAY_KTDC,  a.NGAY_KT_MG,  a.TIEN_TD,  a.CUOC_DC,  a.SO_THANGDC, 
a.SO_THANGKM,
 a.CONGVAN_ID,  a.KHUYENMAI_ID,  a.CHITIETKM_ID,  a.NHOM_DATCOC_ID,  a.SL_DATCOC,  a.NHOMTB_ID,  a.GOI_ID,  a.TRANGTHAITB_ID,  a.KHACHHANG_ID,  a.TRATRUOC, 
 a.SLTB_TT_CHUAGHEPGOI,  a.DSTB_TT_CHUAGHEPGOI,  a.SLTB_TT_CHUAGHEPMATT,  a.DSTB_TT_CHUAGHEPMATT,
 a.SLTB_TT_LECHKY,  a.DSTB_TT_LECHKY, b.thuebao_id ORDER BY b.ngay_kt_Mg DESC) AS rn
from fiber_tt_mtt  a
    join mesh_tt_donle b on a.khachhang_id = b.khachhang_id
where to_char(b.ngay_kt_mg,'yyyymmdd')!= to_char(a.ngay_kt_mg,'yyyymmdd') -- and rn = 1
--    group by a.RKM_ID,  a.THUEBAO_ID,  a.MA_TB,  a.MA_TT,  a.LOAITB_ID,  a.NGAY_BDDC,  a.NGAY_KTDC,  a.NGAY_KT_MG,  a.TIEN_TD,  a.CUOC_DC,  a.SO_THANGDC, 
--a.SO_THANGKM,
-- a.CONGVAN_ID,  a.KHUYENMAI_ID,  a.CHITIETKM_ID,  a.NHOM_DATCOC_ID,  a.SL_DATCOC,  a.NHOMTB_ID,  a.GOI_ID,  a.TRANGTHAITB_ID,  a.KHACHHANG_ID,  a.TRATRUOC, 
-- a.SLTB_TT_CHUAGHEPGOI,  a.DSTB_TT_CHUAGHEPGOI,  a.SLTB_TT_CHUAGHEPMATT,  a.DSTB_TT_CHUAGHEPMATT,  a.SLTB_TT_LECHKY,  a.DSTB_TT_LECHKy
    ) where rn = 1 --order by thuebao_id 
and  rownum <= 8007 --and khachhang_id = 8873113 ;
select * from fiber_tt_tt where ma_Tb= 'vnpt-43d'
sum(SLTB_TT_LECHKY) from fiber_tt_tt;
drop table mytv_1
create table mytv_1 as 
select sum(SLTB_TT_LECHKY) from fiber_tt_mtt-- where khachhang_id = 8873113
select sum(SLTB_TT_LECHKY) from (
select a.*
 ,row_Number()  OVER (PARTITION BY a.thuebao_id ORDER BY a.ngay_kt_Mg DESC) AS rn 
from fiber_tt_tt a 
where SLTB_TT_LECHKY > 0) where rn = 1;
select* from met where khachhang_id = 9187783;
select* from maitv where ngay_lech = 0
rollback;
update maitv set ngay_lech = 1 where ngay_lech = 0;
commit;
select to_char(ngay_kt_mg,'yyyymm') thang_kt,ten_pb phong_cs, a.* 
from maitv a
  left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
      left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id;
select* from maitv;
select * from mesh_tt_donle where khachhang_id = 9187783;
select* from mytv_tt_donle where khachhang_id =  10167749