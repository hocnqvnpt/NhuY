create table fiber_Ts_mTt as
select a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, a.ma_tt,
     count(distinct b.thuebao_id)  as SLTB_TT_ChuaGhepGoi, 
    LISTAGG( distinct b.ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TT_ChuaGhepGoi,
    count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end)  as SLTB_TT_ChuaGhepMaTT, 
    LISTAGG(distinct case when b.ma_Tt!= a.ma_tt then b.ma_tb else null end , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TT_ChuaGhepMaTT
  
from fiber_tt_donle a 
    left join mesh_tt_donle b on a.khachhang_id = b.khachhang_id 
group by  a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, a.ma_tt;
select * from mytv_tt_donle  where khachhang_id = 5042523;
select thanhtoan_id from css_hcm.db_thuebao where thuebao_id = 8224171;
select ma_Tt from css_hcm.db_thanhtoan where thanhtoan_id = 8333586
select* from fiber_tt_donle
select
    
    commit;
    drop table final_fiber_mtssau
create table final_fiber_mtssau as
select  a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, a.ma_tt,
    a.SLTB_TT_ChuaGhepGoi, a.DSTB_TT_ChuaGhepGoi, a.SLTB_TT_ChuaGhepMaTT,a.DSTB_TT_ChuaGhepMaTT,
     count(distinct b.thuebao_id)  as SLTB_TS_ChuaGhepGoi, 
    LISTAGG( distinct b.ma_Tb , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TS_ChuaGhepGoi,
    count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end)  as SLTB_TS_ChuaGhepMaTT, 
    LISTAGG( distinct case when b.ma_Tt!= a.ma_tt then b.ma_tb else null end , '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DSTB_TS_ChuaGhepMaTT,
    SLTB_TT_ChuaGhepGoi + count(distinct b.thuebao_id) SLTB_ChuaGhepGoi , 
    SLTB_TT_ChuaGhepMaTT + count(distinct case when b.ma_Tt!= a.ma_tt then b.thuebao_id else null end) SLTB_ChuaGhepMaTT
from fiber_Ts_mTT a 
    left join mesh_ts b on a.khachhang_id = b.khachhang_id
group by a.thuebao_id, a. ma_Tb, a.loaitb_id , a.khachhang_id, a.thanhtoan_id, a.ma_tt,
    a.SLTB_TT_ChuaGhepGoi, a.DSTB_TT_ChuaGhepGoi, a.SLTB_TT_ChuaGhepMaTT,a.DSTB_TT_ChuaGhepMaTT;
select a.* --, b.nhomtb_id
from FINAL_FIBER_mTSSAU a
--    left join css_hcm.bd_goi_dadv b on a.thuebao_id = b.thuebao_id  and trangthai = 1
--    left join css_hcm.goi_dadv c on b.goi_id = c.goi_id
where not (SLTB_ChuaGhepMaTT = 0 and SLTB_ChuaGhepGoi = 0);
select * from mesh_tt_donle

select* from FINAL_FIBER_mTs
select a.* ,   case when ngay_lech is not null then trunc(mod(month_diff, 12)) || ' tháng ' || to_char( ngay_lech - 30*month_diff) || ' ngày' 
    else null end as thang_lech
from mytv1 a
from (
create table mytv1 as
select* from (
  select a.*
    , case when ngay_lech is not null then to_char(abs(to_number(to_char(ngay_kt_mesh,'mm'))-to_number(to_char(ngay_kt_fiber,'mm')))) || ' tháng ' ||
    to_char(abs(to_number(to_char(ngay_kt_mesh,'dd'))-to_number(to_char(ngay_kt_fiber,'dd'))))   || ' ngày' 
      else null end as thang_lech
    , case when to_char( ngay_kt_fiber,'mm')  =  to_char(ngay_kt_mesh,'mm') then 1 else 0 end as cung_thang
    , case when ngay_lech is not null and to_number(to_char(ngay_kt_fiber,'yyyymmdd')) < to_number(to_char(ngay_kt_mesh,'yyyymmdd')) then 1 
        else 0 end as mesh_trehon
        , row_Number()  OVER (PARTITION BY a.ma_tb_mesh ORDER BY a.ngay_kt_mesh DESC) AS rn
  from mesh a
) where rn = 1
  select * from mesh
  create table 
);
create table mytv as 
select* from (
select to_number(to_char(a.ngay_kt_mg,'yyyymm')) thang_kt, a.*, b.ma_Tb ma_tb_mesh, a.ngay_kt_mg ngay_kt_fiber,b.ngay_kt_mg ngay_kt_mesh, round(abs(b.ngay_kt_mg - a.ngay_kt_mg),0) ngay_lech 
    , row_Number()  OVER (PARTITION BY b.ma_tb ORDER BY b.ngay_kt_mg DESC) AS rn
from FINAL_FIBER_Ts a
    left join mytv_tt_donle b on a.khachhang_id = b.khachhang_id
--    where  SLTB_TT_LechKy != 0
where b.ngay_kt_Mg is not null) where ngay_lech > 0 and rn = 1
    order by a.thuebao_id;
 ;
 select * from FINAL_FIBER_Ts
 select * from FINAL_FIBER_Ts
 selecT a.*, ten_pb phong_cs from final_fiber_mtssau a 
    left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
      left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
 where not (SLTB_CHUAGHEPGOI = 0 and SLTB_CHUAGHEPMATT = 0) 
select* from my_Tv_Ts where khachhang_id = 10165948
select* from css_Hcm.bd_goi_dadv where thuebao_id = 8597032;
select thuebao_id, thanhtoan_id, khachhang_id from css_hcm.db_Thuebao where ma_tb = 'hcm2thanh-gh2714';
select* from css_Hcm.bd_goi_dadv where thuebao_id = 9260004