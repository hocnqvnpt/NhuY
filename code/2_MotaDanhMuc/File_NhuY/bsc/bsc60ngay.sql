-- 
select * from hocnq_ttkd.v_thongtinkm_all where thang_bddc = 202310-- ma_tb = 'hiepem'
select * from hocnq_ttkd.v_thongtinkm_all where thang_bddc = 202310 and ma_tb = 'hiepem'
 select *   from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310 and ma_nv = 'CTV021852'-- and ma_tb =  'hiepem'
 select* from tl_giahan_tratruoc
-- BSC 60 NGAY: NHAN VIEN
-- SELECT * FROM tl_giahan_tratruoc WHERE LOAI_TINH = 'KPI_NV'
delete from ttkd_bsc.tl_giahan_tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023' --and loai_Tinh = 'KPI_NV'
create table thang10 as select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202310 and ma_kpi = 'HCM_TB_GIAHA_023' and loai_Tinh = 'KPI_NV'
select* from thang10 where ma_nv = 'CTV021803'
commit;
update 
select * from ttkd_bsc.ct_ (select ma_tb from ttkd_bsc.ct_bsc_tratruoc_moi where tien_khop = 0 and thang = 202311)
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202311 and tien_khop = 0 and ma_kpi = 'HCM_TB_GIAHA_023' 
and ma_tb not in (
select * from ttkd_bsc.ct_dongia_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU' and tien_khop = 0 and ma_Tb 
not in (
select ma_tb from ttkd_Bsc.ct_dongia_tratruoc where thang = 202311 and tien_khop = 0 and ma_kpi = 'HCM_TB_GIAHA_023'
) 
select count(b.ma_kpi) sl_makpi
from TTKD_BSC.nhanvien_202401 a
    join TTKD_BSC.blkpi_danhmuc_kpi_vtcv b on a.ma_Vtcv=  b.ma_vtcv 
where  (thang_kt is null or thang_kt = 202401) and a.ma_nv = 'CTV079490';

select* from ttkd_bsc.bangluong_kpi where thang = 202402


select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and ma_kpi = 'HCM_TB_GIAHA_022' and loai_Tinh = 'KPI_NV'
rollback;
drop table backup_tl_giahan_tratruoc
create table backup_tl_giahan_tratruoc as select* from ttkd_Bsc.tl_giahan_tratruoc where thang = 202311 
insert into ttkd_bsc.tl_giahan_tratruoc 
delete from ttkd_bsc.tl_giahan_tratruoc  where thang = 202311 and ma_kpi in ('HCM_TB_GIAHA_023', 'HCM_TB_GIAHA_022','DONGIATRA','DONGIATRU')
select* from tl_giahan_tratruoc where thang = 202311 and ma_kpi in ('HCM_TB_GIAHA_023', 'HCM_TB_GIAHA_022','DONGIATRA','DONGIATRU')
insert into TTKD_BSC.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                            , DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                            
select thang, 'KPI_NV' LOAI_TINH
 , case when ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
                when ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500') then 'HCM_TB_GIAHA_023'
            else null
    end ma_kpi
 , a.ma_nv, a.ma_to, a.ma_pb
   , count(thuebao_id) tong
  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
  , sum(dthu) DTHU_thanhcong
  , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) *100/count(thuebao_id), 2) tyle, sum(heso_giao) heso
  
from       (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                        , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop--, a.kq_dvi_cn KQ_TH_Dai, a.kq_popup
                                        , sum(a.tien_khop), heso_giao
                        from ttkd_bsc.ct_bsc_tratruoc_moi a
                        where a.thang = 202402                  ------------n------------
                        group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, heso_giao
        ) a 
where ma_pb is not null 
group by a.thang, a.ma_nv, a.ma_to, a.ma_pb
order by 2
commit;
update ttkd_bsc.ct_bsc_tratruoc_moi
select* from ttkd_bsc
delete ttkd_Bsc.tl_giahan_Tratruoc where thang = 202402 and ma_kpi = 'HCM_TB_GIAHA_023'
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_023'  --and ma_nv = 'VNP020227' and loai_tinh = 'KPI_NV' -- ma_to = 'VNP0702510'  and 
-- BSC 60 NGAY: TO
-- select* 
delete from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'KPI_TO' and thang = 202402 and ma_to = 'VNP0702407'  and ma_kpi = 'HCM_TB_GIAHA_023'
delete
-- insert into thang10 select*
from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202310 and loai_tinh = 'KPI_TO' and ma_kpi = 'HCM_TB_GIAHA_023'
commit;
DELETE FROM  ttkd_Bsc.tl_giahan_tratruoc WHERE THANG = 202402 AND  MA_KPI in ('HCM_TB_GIAHA_023') AND  loai_tinh = 'KPI_TO'
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)                                                    
select THANG, 'KPI_TO', MA_KPI, null, MA_TO, MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round( sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc
where thang = 202402 and MA_KPI in ('HCM_TB_GIAHA_023')  and loai_tinh = 'KPI_NV' --and ma_TO = 'VNP0702219'
group by THANG, MA_KPI, MA_TO, MA_PB        
select distinct ma_to from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310
-- BSC 60 NGAY: PHONG BAN
commit;
select *  from ttkd_bsc.tl_giahan_tratruoc a where loai_tinh = 'KPI_PB' and a.MA_KPI in ('HCM_TB_GIAHA_023')and  thang = 202401
commit;
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN
                                                	, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, heso_giao)
select a.THANG, 'KPI_PB', a.MA_KPI, b.ma_nv, null, a.MA_PB, sum(TONG), sum(DA_GIAHAN_DUNG_HEN), sum(DTHU_DUYTRI), sum(DTHU_THANHCONG_DUNG_HEN)
                    , round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
                    , sum(heso_giao) heso
from ttkd_bsc.tl_giahan_tratruoc a left join (select * from ttkd_bsc.blkpi_dm_to_pgd where thang = 202401) b on a.ma_to = b.ma_to and a.ma_kpi = b.ma_kpi
where a.thang = 202401 and loai_tinh = 'KPI_TO' and a.MA_KPI in ('HCM_TB_GIAHA_023') 
group by a.THANG, a.MA_KPI, b.ma_nv, a.MA_PB 
commit;
select distinct ma_pb from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202310
--delete from tl_giahan_tratruoc
select* from tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'KPI_PB' and MA_KPI in ('HCM_TB_GIAHA_023')
select* from bangluong_kpi_202310
-- DO DU LIEU VAO BANG LUONG
--  NHAN VIEN
select count(distinct thuebao_id) from tl_giahan_tratruoc where loai_tinh = 'KPI_NV' and MA_KPI in ('HCM_TB_GIAHA_022') and ma_pb  = 'VNP0701100'
create table bangluong_kpi_202310 as select ma_nv, ma_nv_hrm, TEN_NV,MA_VTCV,TEN_VTCV,MA_DONVI,TEN_DONVI,MA_TO,TEN_TO, 'HCM_TB_GIAHA_023' as HCM_TB_GIAHA_023,'HCM_TB_GIAHA_022' as HCM_TB_GIAHA_022
from TTKD_BSC.bangluong_kpi_202108_L5 where 1 = 2
-- update nhan vien bang KPI
update TTKD_BSC.bangluong_kpi_202310 a set 
HCM_TB_GIAHA_023 = (select round(sum(DA_GIAHAN_DUNG_HEN)*100/sum(TONG), 2)
from ttkd_bsc.tl_giahan_tratruoc@ttkddb 
where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and ma_nv = a.ma_nv_hrm and ma_kpi = 'HCM_TB_GIAHA_023' )
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi in ('HCM_TB_GIAHA_023') and thang_kt is null and MA_VTCV = a.MA_VTCV)
 ---------------Ty le cua To truong -----
update TTKD_BSC.bangluong_kpi_202310 a set 
HCM_TB_GIAHA_023 = (select TYLE from ttkd_bsc.tl_giahan_tratruoc@ttkddb 
where thang = to_char(trunc(sysdate, 'month')-1, 'yyyymm') and loai_tinh ='KPI_TO' and ma_to = a.ma_to and ma_kpi = 'HCM_TB_GIAHA_023')
where exists (select * from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where to_truong_pho = 1 and ma_kpi in ('HCM_TB_GIAHA_023')
and thang_kt is null and MA_VTCV = a.MA_VTCV)
select* from tl_giahan_tratruoc where loai_tinh = 'KPI_PB'