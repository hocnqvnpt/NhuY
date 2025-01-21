select * from qltn.v_db_datcoc@dataguard where thuebao_id = 10933620 and kyhoadon = 20231101
union all
select  * from qltn.v_db_datcoc@dataguard where thuebao_id = 10933620 and kyhoadon = 20231201
union all
select  * from qltn.v_db_datcoc@dataguard where thuebao_id = 10933620 and kyhoadon = 20240101
union all
select  * from qltn.v_db_datcoc@dataguard where thuebao_id = 10933620 and kyhoadon = 20240201
union all
select * from qltn.v_db_datcoc@dataguard where thuebao_id = 10933620 and kyhoadon = 20240301;
select hcm_sl_order_001 , a.* from ttkd_bsc.bangluong_kpi_202407 a where hcm_sl_order_001 is not null;
select* from ttkd_bsc.nhanvien where ten_vtcv = 'T? Tr??ng T? Sau Bán Hàng';
select* from ttkd_bsc.blkpi_danhmuc_kpi_Vtcv where ma_kpi = 'HCM_SL_ORDER_001' and thang = 202407;
select NGANHANG, MA_CHUNGTU, TIEN_CHUNGTU, HINHTHUC_TT, NGAY_TT, MA_TT, MA_TB, MA_GD, LOAIHINH_TB, GHICHU, TRAGOC, TRATHUE, TONGTRA, HTTT_ID, TRA_TRUOC, CHUNGTU_ID
,A.NV_GACH , TINH_DONGIA, A.TINH_BSC, 202407 thang, B.MA_TO, B.MA_PB
from nhuy.ct_BSC_chungtu a
   left join ttkd_Bsc.nhanvien b on a.nv_Gach = b.ma_Nv and b.thang = 202407 and b.donvi = 'TTKD';
   select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202407 and ma_Tb ='hcm_ca_ivan_0001749';in ('hcm_ca_ivan_0001749','hcm_smartca_00016899','hcm_ca_ivan_00016898');
select *
from ttkd_Bsc.ct_Bsc_trasau_Tp_tratruoc where thang=202407;
select* from tien_Thu where thang = 202407 and ngay_bd is null; and ma_Tb not in (select ma_tb from phan_bo where thang = 202407);
select* from qltn.v_db_datcoc@dataguard a where loaitb_id = 63;

select a.*,  from phan_bo a
    left join tien_Thu b on a.ma_tb = b.ma_tb;
select a.* from tien_Thu  a
    left join phan_bo b on a.ma_tb = b.ma_tb and a.thang = b.thang
where b.ma_Tb is null and a.thang = 202407 and a.ma_tb in (select ma_Tb from tien_thoai where thang = 202407)
;
--select sum (tien_ton_T6) from (
with t_thu as (
    select ma_tb, thang ,sum(tien_thanhtoan+vat) tien_thanhtoan
    from tien_thu
    where thang = 202407 and khoanmuctt_id = 11--!= 3421
    group by ma_Tb, thang
)
, t_Thoai as (
    select ma_tb, sum(tien_thanhtoan+vat) tien_thanhtoan, thang
    from tien_thoai
    where thang = 202407 --and khoanmuctt_id != 3421
    group by ma_Tb, thang
)
, t_phanbo_moi as (
    select ma_Tb, sum(ton_ck) ton_ck, sum(cuoc_tk) cuoc_tk, thang
    from phan_bo
    where nvl(ton_ck,0) >=0 and cuoc_tk is not null and thang = 202407
    group by ma_Tb, thang
),
 t_phanbo_cu as (
    select ma_Tb, sum(ton_ck) ton_ck, sum(cuoc_tk) cuoc_tk, thang
    from phan_bo
    where nvl(ton_ck,0) >=0  and thang = 202406 and cuoc_tk is not null
    group by ma_Tb, thang
),
ton_th6 as (
    select a.thang, a.ma_tb,a.ton_ck, b.tien_thanhtoan tien_Thu,  c.tien_thanhtoan tien_thoai, a.cuoc_tk tien_phanbo, 
    a.ton_ck + nvl(c.tien_thanhtoan,0) + nvl(a.cuoc_tk,0)- nvl(b.tien_thanhtoan,0)   as tien_ton_T6
--    a.ton_ck + nvl(c.tien_thanhtoan,0) + nvl(a.cuoc_tk,0)  as tien_ton_T6

    from t_phanbo_moi a
        left join t_thu b on a.ma_tb = b.ma_Tb and a.thang = b.thang
        left join t_thoai c on a.ma_Tb = c.ma_tb and a.thang = c.thang
--    left join t_
    where a.thang = 202407 )
select * from ton_th6 a left join t_phanbo_cu b on a.ma_tb = b.ma_tb where b.thang = 202406 
and b.ton_ck != a.tien_Ton_T6  ;and a.ma_tb= 'hcmngthiha_0609';
202407	hcmtnq.tv5	540169	2041600		43685	-1457746	hcmtnq.tv5	0	20654	202406;
select* from phan_bo where ma_Tb = 'hcmtnq.tv5' and thang = 202407
union all
select* from phan_bo where ma_Tb = 'hcmtnq.tv5' and thang = 202406;
select* from tien_Thu where ma_Tb = 'hcmtnq.tv5' and thang = 202407;
1430000 43685 1430000
540169        583854
huyen141311 7191511;
slam_fm
586442886719
;
625584833394
;
select* from phan_bo where ma_Tb = 'huyen141311' and thang = 202406;
select* from tien_thu where ma_Tb =  'huyen141311' ;
 select  sum(ton_ck) ton_ck--, sum(cuoc_tk) cuoc_tk, thang
    from phan_bo
    where thang = 202406
    group by ma_Tb, thang;
    
with ton as (
    select thuebao_id, ma_tb , ma_tt, ten_dvvt, loaihinh_Tb,  sum(case when thang = 202407 then ton_ck else null end) t7
    ,  sum(case when thang = 202406 then ton_ck else null end) t6
    ,  sum(case when thang = 202405 then ton_ck else null end) t5
    ,  sum(case when thang = 202404 then ton_ck else null end) t4
    ,  sum(case when thang = 202403 then ton_ck else null end) t3
    ,  sum(case when thang = 202402 then ton_ck else null end) t2
    ,  sum(case when thang = 202401 then ton_ck else null end) t1
    ,  sum(case when thang = 202312 then ton_ck else null end) t12

    

    from pb
    group by thuebao_id, ma_tb , ma_tt, ten_dvvt, loaihinh_Tb
)
--create  table tt as select* from (
--    select thuebao_id, trangthai_Tb
--    from css.v_db_thuebao@dataguard a
--        join css.trangthai_Tb@dataguard b on a.trangthaitb_id = b.trangthaitb_id
--);
select a.* , tt.trangthai_tb 
from ton a
    join tt on a.thuebao_id = tt.thuebao_id 
    where t7 = t6 and t6= t5 and t5=t4 and t4=t3 and t3=t2 and t2=t1 and t1=t12;
with tttb as (
    select DISTINCT THUEBAO_ID, trangthai_Tb, A.TRANGTHAITB_ID
    from tinhcuoc_hcm.dbtb partition for (20240201) a
        left join css_hcm.trangthai_Tb b on a.trangthaitb_id = b.trangthaitb_id

)
SElect a.thang, A.THUEBAO_ID, a.rkm_id, a.ma_tt,a.MA_TB, a.TEN_DVVT, a.LOAIHINH_TB,  a.THANG_BD, a.THANG_KT, a.TON_CK, a.CUOC_TK, a.NGAY_KTDC, b.trangthaitb_id, B.trangthai_Tb 
from PB a
    left join TTTB b on a.THUEBAO_ID= b.THUEBAO_ID  
where a.thang = 202402;
    select* from tinhcuoc.v_dbtb@dataguard where  ma_Tb ='ngckhuyn5880460' and ky_cuoc = 20231201;
        select* from tinhcuoc_hcm.dbtb where  ma_Tb ='ngckhuyn5880460' and ky_cuoc = 20231201;

    select thuebao_id, ma_tb , ma_tt, ten_dvvt, loaihinh_Tb, sum(case when thang = 202407 then ton_ck else null end) t7
    ,  sum(case when thang = 202406 then ton_ck else null end) t6
    ,  sum(case when thang = 202405 then ton_ck else null end) t5
    ,  sum(case when thang = 202404 then ton_ck else null end) t4
    ,  sum(case when thang = 202403 then ton_ck else null end) t3
    ,  sum(case when thang = 202402 then ton_ck else null end) t2
    ,  sum(case when thang = 202401 then ton_ck else null end) t1
    ,  sum(case when thang = 202312 then ton_ck else null end) t12
    from pb where ma_tb = 'tanphatco-190419' group by  thuebao_id, ma_tb , ma_tt, ten_dvvt, loaihinh_Tb;
    select* from  pb where ma_tb = 'tanphatco-190419' ;
delete from pb where thang = 202312 and cuoc_tk is null and exists (select * from tien_thu where rkm_id = pb.rkm_id and thang = 202401);
rollback;
select* from css_Hcm.db_Thuebao where dichvuvt_id = 1; 
select* from css_hcm.loaihinh_Tb where dichvuvt_id = 1;
commit;
select * from tien_thu where thang = 202401 and thuebao_id not in (select thuebao_id from pb where thang = 202312);
select* from v_Thongtinkm_all where ma_Tb = 'ctydts_fiber';
with 
select * from qltn.v_db_datcoc@dataguard where kyhoadon = 20231201 and  to_Number(to_char(ngay_cn,'yyyymmdd')) = 20240101 ;and ngay_cn = '01/03/2023 00:22:44'; 
--- ghep file
select* from ;
with ton as (
   select thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt, sum(ton_ck) ton_ck
   from pb where thang = 202312
   group by thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt
)
, thu as (
    select thuebao_id, ma_tb, sum(tien_Thanhtoan+vat) tien
    from tien_thu where thang = 202401
    group by thuebao_id, ma_Tb
)
, thoai as (
    select thuebao_id, ma_Tb, sum(tien_Thanhtoan+vat) tien
    from tien_thoai where thang = 202401
    group by thuebao_id,  ma_Tb
)--10933620	DI001027022
----;227941890;
select kyhoadon,ton_dk , ton_ck, cuoc_tk, ton_dk-cuoc_tk from qltn.v_db_datcoc@dataguard where thuebao_id = 8329847 and kyhoadon = 20231101
union all
select  kyhoadon,ton_dk , ton_ck, cuoc_tk, ton_dk-cuoc_tk from qltn.v_db_datcoc@dataguard where thuebao_id = 8329847 and kyhoadon = 20231201
union all
select  kyhoadon,ton_dk , ton_ck, cuoc_tk, ton_dk-cuoc_tk from qltn.v_db_datcoc@dataguard where thuebao_id = 8329847 and kyhoadon = 20240101
union all
select  kyhoadon,ton_dk , ton_ck, cuoc_tk, ton_dk-cuoc_tk from qltn.v_db_datcoc@dataguard where thuebao_id = 8329847 and kyhoadon = 20240201
union all
select kyhoadon,ton_dk , ton_ck, cuoc_tk, ton_dk-cuoc_tk from qltn.v_db_datcoc@dataguard where thuebao_id = 8329847 and kyhoadon = 20240301
----;
--;
--8360108	hcmthanhb520	MyTV	B?ng r?ng c? ??nh	85008			38057	313350	46951
----;
--;
, pbo as (
   select thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt, sum(cuoc_tk) cuoc_tk, sum(ton_ck) ton_t1_file
   from pb --where thang = 202401
   group by thuebao_id, ma_tb, loaihinh_Tb, ten_dvvt
)
--select* from css_hcm.db_Thuebao where ma_Tb = 'truongconghai1';
,tthh as
(
    select a.thuebao_id, a.ma_Tb, a.loaihinh_Tb, a.ten_dvvt, a.ton_ck ton_t12, b.tien thu_t1, c.tien thoai_t1, d.cuoc_tk pbo_t1, d.ton_t1_file, 
    ( a.ton_ck + nvl( b.tien,0) +nvl( c.tien,0) - nvl(d.cuoc_tk,0) ) ton_t1_tinh
    from ton a
        left join thu b on a.ma_tb = b.ma_Tb 
        left join thoai c on a.ma_tb = c.ma_tb
        left join pbo d on a.ma_tb = d.ma_Tb   
)
select* from tthh where TON_T1_FILE > TON_T1_TINH ; 
select * from ttkd_Bsc.bangluong_kpi_202407 a
    join ttkd_Bsc.bangluong_kpi b on a.ma_nv = b.ma_nv and b.thang = 202407
where b.ma_kpi = 'HCM_SL_ORDER_001' AND NVL(A.HCM_SL_ORDER_001,0) != NVL(B.THUCHIEN,0);
SELECT SUM(HCM_SL_COMBO_006) FROM TTKD_bSC.BANGLUONG_KPI_202407;
select* from phan_bo where ma_Tb ='conghai41';
SELECT SUM(THUCHIEN) FROM TTKD_bSC.BANGLUONG_KPI WHERE THANG = 202407 AND MA_KPI = 'HCM_SL_COMBO_006';
select a.MA_NV, a.MA_NV_HRM, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_DONVI, a.TEN_DONVI, a.MA_TO, a.TEN_TO,a.HCM_SL_COMBO_006 ,
b.HCM_SL_COMBO_006
from ttkd_Bsc.bangluong_kpi_202407 a
    join ttkd_Bsc.bangluong_kpi_202407_dot2 b on a.ma_nv = b.ma_Nv 
where nvl(a.HCM_SL_COMBO_006,0) <> nvl(b.HCM_SL_COMBO_006,0);
select distinct ten_Vtcv from ttkd_bsc.bangluong_kpi_202407 where HCM_SL_COMBO_006 is not null;