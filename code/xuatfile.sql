with 
ds as (   select distinct a.thuebao_id, a.loaitb_id,a.ma_tb, to_number(to_char(ngay_kt_mg,'yyyymm'))thang_kt, e.ten_pb,b.pbh_ql_id, a.sl_datcoc, a.cuoc_dc cuoc_datcoc,a.tien_td tien_trudan, a.so_thangdc
      ,dd.muccuoc, nvl(dd.cuoc_tg,dd.cuoc_tb) muc_cuoc
         from ds_giahan_tratruoc2 a
            left join ttkd_Bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
            left join fiber_hh_t5 c on a.thuebao_id = c.thuebao_id
            left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
            left join css_hcm.db_adsl bb on a.thuebao_id = bb.thuebao_id
            left join css_hcm.tocdo_adsl cc on bb.tocdo_id = cc.tocdo_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.muccuoc_tb dd on db.mucuoctb_id = dd.mucuoctb_id
        where b.pbh_ql_id =3
        ) select* from ds;
        select thang_kt, count(distinct thuebao_id) sltb,sum(case when loaitb_id in (58,59,23,11,30,26,25) then 1 else 0 end) sl_Fiber_mgw,
        sum(case when loaitb_id in  (61,171,18) then 1 else 0 end) sl_mytv_fiberVNN,
            sum(case when  loaitb_id not in (61,171,18,58,59,23,11,30,26,25) then 1 else 0 end) sl_mesh_camera
--            sum (case when loaitb_id not in (18, 58, 59, 61, 171, 210, 222, 224) then 1 else 0 end)
               , ten_pb from  ds
        where thang_kt between 202406 and 202412
        group by thang_kt, ten_pb;
        
        select mucuoctb_id from ds_giahan_tratruoc2;
        select* from ttkd_Bsc.nhanvien_202405 where ngaySinh like '11/02%';
select A.THANG, A.LOAI_TINH, A.MA_NV, A.MA_TO, A.MA_PB, A.MA_TB, A.SOTHANG_DC, A.NGAY_TT,  A.TIEN_KHOP, A.HESO_CHUKY,A.HESO_DICHVU,A.NHANVIEN_XUATHD, A.DONGIA,tyle_thanhcong,
NVL(TIEN_XUATHD,0)*HESO_CHUKY*HESO_DICHVU  TIEN_XUATHD, NVL(TIEN_THUYETPHUC,0)*HESO_CHUKY*HESO_DICHVU  TIEN_THUYETPHUC, CASE WHEN TIEN_KHOP= 1 THEN 1 ELSE 0 END VB_119,IPCC
from ttkd_Bsc.ct_dongia_Tratruoc A where thang = 202405 and loai_tinh = 'DONGIATRA_OB';
SELECT* FROM TTKD_BSC.nhuy_ct_bsc_ipcc_obghtt WHERE THANG = 202405 AND MA_TB= 'mesh0193778';
select *  FROM TTKD_BSC.CT_DONGIA_tRATRUOC WHERE THANG = 202405  and tien_khop =1 and nvl(tien_xuathd,0)+nvl(TIEN_THUYETPHUC,0)>10000; 
SELECT* FROM TTKD_bSC.CT_DONGIA_tRATRUOC A WHERE THANG = 202405 AND HESO_CHUKY > 0 AND NVL(TIEN_THUYETPHUC,0) = 0 and loai_tinh = 'DONGIATRA_OB';
UPDATE TTKD_bSC.CT_DONGIA_tRATRUOC A 
SET TIEN_THUYETPHUC  = 10000-tien_xuathd
WHERE THANG = 202405 AND TIEN_KHOP = 1 and tien_khop =1 and nvl(tien_xuathd,0)+nvl(TIEN_THUYETPHUC,0)>10000 and loai_tinh = 'DONGIATRA_OB';
rollback;
update  TTKD_BSC.CT_DONGIA_tRATRUOC WHERE THANG = 202405 and loai_tinh = 'DONGIATRA_OB' and tien_khop =1;
commit;
select* from  TTKD_bSC.CT_DONGIA_tRATRUOC A  where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and HESO_CHUKY > 0 and tien_khop >0   AND NVL(TIEN_THUYETPHUC,0) = 0  and nhanvien_xuathd is not  null;
update  TTKD_bSC.CT_DONGIA_tRATRUOC A 
set tien_thuyetphuc = 6000, tien_xuathd = 4000 
where thang = 202405 and loai_tinh = 'DONGIATRA_OB' and ma_tb = 'hoangminh_84'; HESO_CHUKY > 0 and tien_khop = 1 AND NVL(TIEN_THUYETPHUC,0) = 0  and nhanvien_xuathd is not null;
update ttkd_Bsc.CT_DONGIA_tRATRUOC a set tien_thuyetphuc = 6000 , tien_xuathd = 4000 where thuebao_id in (
select * from  TTKD_bSC.CT_DONGIA_tRATRUOC A where tien_khop = 1 and   NVL(TIEN_THUYETPHUC,0) != 7500 and THANG = 202405 and loai_tinh = 'DONGIATRA_OB' and nhanvien_xuathd is  null
)
and thang = 202405  and loai_tinh = 'DONGIATRA_OB';