with th as (
SELECT THANG , MA_nV, TEN_NV , ma_Vtcv, TEN_VTCV, TEN_TO, TEN_PB ,case when ma_vtcv !='VNP-HNHCM_BHKV_27' then  thuchien*1000000 
        else (thuchien-  (select sum(thuchien) from TTKD_bSC.bangluong_kpi  
            where ma_to = a.ma_to and ma_nv !=  a.ma_nv and thang = a.thang and ma_kpi = a.ma_kpi)) *1000000 end thuchien
FROM TTKD_bSC.bangluong_kpi a 
where thang in (202407,202408)  and ma_vtcv in ('VNP-HNHCM_BHKV_22','VNP-HNHCM_BHKV_27','VNP-HNHCM_BHKV_6') and ma_Kpi ='HCM_DT_PTMOI_021'
union all
SELECT THANG , MA_nV, TEN_NV , ma_Vtcv, TEN_VTCV, TEN_TO, TEN_PB , thuchien*1000000
FROM TTKD_bSC.bangluong_kpi a 
where thang in (202407,202408)  and ma_vtcv in ('VNP-HNHCM_BHKV_15') and ma_Kpi ='HCM_DT_PTMOI_053'
)
, final as (
select th.*,  p1.dinhmuc_2 giao
from th
    join ttkd_bsc.bldg_danhmuc_vtcv_p1 p1 on th.ma_vtcv = p1.ma_Vtcv and p1.thang = 202408)
select * from  final;
select* from ttkd_bsc.bangluong_kpi where thang = 202408 and ma_kpi ='HCM_DT_PTMOI_021';
drop table dgia_p1;
create table dgia_p1 as
with t9 as (
select 202409 thang, ma_nv, sum(dthu_kpi) dthu_kpi from ttkd_bsc.temp_trasau_canhan 
group by ma_nv
union all
select 202409 thang ,MANV_PTM, round(sum(DOANHTHU_KPI_NVPTM*HESO_KPI)) as DOANHTHU_KPI_NVPTM
from manpn.manpn_GOI_TONGHOP_202409_BSCquy
where thoadk_bsc = 1
group by MANV_PTM
)
, ds as (select thang, ma_nv,  sum(dthu_kpi) dthu_kpi 
from t9
group by ma_nv, thang
union all
select 202408 thang, ma_nv, sum(dthu_kpi) dthu_kpi from ttkd_bsc.temp_trasau_canhan_T8 
group by ma_nv
union all
select 202407 thang, ma_nv, sum(dthu_kpi) dthu_kpi from ttkd_bsc.temp_trasau_canhan_T7 
group by ma_nv)
select ds.*, nv.ten_nv, nv.ten_to, nv.ten_pb, nv.ten_vtcv, p1.dinhmuc_2 dinhmuc_giao
from ds 
    join ttkd_Bsc.nhanvien nv on ds.ma_nv = nv.ma_nv and ds.thang = nv.thang
    join ttkd_bsc.bldg_danhmuc_vtcv_p1 p1 on nv.ma_vtcv = p1.ma_Vtcv and p1.thang = 202408
where nv.ma_vtcv in ('VNP-HNHCM_BHKV_22','VNP-HNHCM_BHKV_27','VNP-HNHCM_BHKV_6','VNP-HNHCM_BHKV_15');
select ma_Nv from dgia_p1 group by ma_Nv having count(distinct ten_Vtcv) > 1;
select* from dgia_p1;
update dgia_p1 set ten_vtcv = initcap(ten_vtcv), ten_to = initcap(ten_to), ten_pb = initcap (ten_pb);
create table danhgia as
--drop table danhgia;
select ma_nv,ten_Nv, ten_to, ten_pb, ten_vtcv, round(avg(dthu_kpi)) dthu, dinhmuc_giao,
    case when ten_vtcv in ('Nhân Viên Kinh Doanh ??a Bàn' ,'Nhân Viên Qu?n Lý ?i?m Bán') and avg(dthu_kpi) < 10000000 then 'H? P1'
         when ten_vtcv in ('Nhân Viên Kinh Doanh ??a Bàn' ,'Nhân Viên Qu?n Lý ?i?m Bán') and avg(dthu_kpi) >= 16000000 then 'Nâng P1'
         when ten_vtcv in ('Nhân Viên Kinh Doanh ??a Bàn' ,'Nhân Viên Qu?n Lý ?i?m Bán') and avg(dthu_kpi) >= 10000000 and avg(dthu_kpi) < 16000000 then 'Gi? P1'
         when ten_Vtcv = 'Nhân Viên Giao D?ch' and avg(dthu_kpi) >= 10000000 then 'Nâng P1'
         when ten_Vtcv = 'Nhân Viên Giao D?ch' and avg(dthu_kpi) < 6000000 then 'H? P1'
         when ten_Vtcv = 'Nhân Viên Giao D?ch' and avg(dthu_kpi) >= 6000000  and  avg(dthu_kpi) < 10000000 then 'Gi? P1'
         when ten_Vtcv =  'C?a Hàng Tr??ng Kiêm Gdv' and avg(dthu_kpi) >= 6000000 then 'Nâng P1'
         when ten_Vtcv =  'C?a Hàng Tr??ng Kiêm Gdv' and avg(dthu_kpi) < 4000000 then 'H? P1'
         when ten_Vtcv =  'C?a Hàng Tr??ng Kiêm Gdv' and avg(dthu_kpi) < 6000000 and avg(dthu_kpi) >= 4000000 then 'Gi? P1' end danhgia
         ,  listagg(thang, ',') within group (order by thang) as thang
from dgia_p1
group by ma_nv,ten_Nv, ten_to, ten_pb, ten_vtcv,dinhmuc_giao
;
select * from dgia_p1;
select* from danhgia;
select* from ttkd_Bsc.nhanvien where ten_Nv like '%Vân'