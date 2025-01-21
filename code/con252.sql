insert into  ttkd_bct.hocnq_cp_nhancong_hoahong  ;
select a.thang, a.thang, a.ma_tb, b.ten_tb, null, null, null, c.loaihinh_tb, a.ma_pb, a.ma_nv, d.ten_nv, null, f.ma_pb, a.dthu, a.sothang_Dc,null, null, 0, 
    round(DONGIA*HESO_CHUKY*HESO_DICHVU) tien, 0, 'TS_TP_TT_Y' 
    from ttkd_Bsc.ct_dongia_Tratruoc a
        left join css_Hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
        left join css_hcm.loaihinh_Tb c on b.loaitb_id = c.loaitb_id
        join ttkd_Bsc.nhanvien_202404 d on a.ma_Nv = d.ma_nv
        left join ttkd_bct.db_thuebao_ttkd_202404 e on a.thuebao_id = e.thuebao_id
        left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) f on f.pbh_id = e.pbh_ql_id
where thang = 202404 and tien_khop > 0 and loai_tinh = 'DONGIA_TS_TP_TT' AND MA_KPI = 'DONGIA'-- AND NVL(TIEN_XUATHD,0) > 0 
--        AND D.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500')
order by tien_khop;
ROLLBACK;
insert into  ttkd_bct.hocnq_cp_nhancong_hoahong  
select 
    a.thang, a.thang, a.ma_tb, b.ten_tb, null, null, null, c.loaihinh_tb, a.ma_pb, a.MA_nV, d.ten_nv, null, f.ma_pb, a.dthu, a.sothang_Dc,null, null, 0, 
    round(TIEN_THUYETPHUC) tien, 0, 'GHTT-TP-Y' 
    from ttkd_Bsc.ct_dongia_Tratruoc a
        left join css_Hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
        left join css_hcm.loaihinh_Tb c on b.loaitb_id = c.loaitb_id
        join ttkd_Bsc.nhanvien_202404 d on a.MA_NV = d.ma_nv
        left join ttkd_bct.db_thuebao_ttkd_202404 e on a.thuebao_id = e.thuebao_id
        left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) f on f.pbh_id = e.pbh_ql_id
where thang = 202404 and tien_khop > 0 and loai_tinh = 'DONGIATRA_OB' AND MA_KPI = 'DONGIA'  AND NVL(TIEN_THUYETPHUC,0) > 0  --and ipcc = 1
--        AND D.MA_PB NOT IN ('VNP0702300','VNP0702400','VNP0702500')
order by tien_khop;
delete from ttkd_bct.hocnq_cp_nhancong_hoahong   where thang = 202404 and nguon in( 'GHTT-XHD-Y','GHTT-TP-Y');
rollback;
select* from  ttkd_bct.hocnq_cp_nhancong_hoahong   where thang = 202404 and nguon in ('TS_TP_TT_Y','TS-TP-Y');
commit;
select* from ttkd_bct.db_thuebao_ttkd_202404;
ROLLBACK;
insert into  ttkd_bct.hocnq_cp_nhancong_hoahong (thang, thang_tldg, manv_ptm, phong_ptm,tienluong_pbh_ptm)
select thang,thang,manv,mapb ,HCM_LUONG_QLDB_071 from ttkd_Bsc.bangluong_dongia_qldb a where thang = 202403;
select distinct nguon, thang from ttkd_bct.hocnq_cp_nhancong_hoahong WHERE NGUON like '%Y%'
    ;
    DELETE  FROM ttkd_bct.hocnq_cp_nhancong_hoahong WHERE NGUON ='TS_TP_TT_Y' and thang = 202404;
select SUM(DONGIA*HESO_DICHVU*HESO_CHUKY) from ttkd_Bsc.ct_dongia_tratruoc where thang = 202405 AND LOAI_tINH = 'DONGIA_TS_TP_TT' AND MA_NV = 'CTV021851';
select * from ttkd_bct.hocnq_cp_nhancong_hoahong where phong_ql is not null and nguon not like '%Y%';
SELECT* FROM TTKD_bSC.TL_gIAHAN_tRATRUOC WHERE THANG = 202404 AND  LOAI_tINH = 'DONGIA_TS_TP_TT' ;AND  MA_NV = 'CTV021851'
;
delete FROM TL_gIAHAN_tRATRUOC WHERE THANG = 202404 AND  LOAI_tINH = 'DONGIATRA_OB';
insert into TL_gIAHAN_tRATRUOC;
update TTKD_bSC.TL_gIAHAN_tRATRUOC set tien = 0 WHERE THANG = 202404 AND  LOAI_tINH = 'DONGIA_TS_TP_TT' and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');
SELECT* FROM ttkd_Bsc.TL_gIAHAN_tRATRUOC  WHERE THANG = 202405 AND ma_kpi ='DONGIA' and ma_pb in ('VNP0702300','VNP0702400','VNP0702500');MA_tB='vantam6238848';

select sum(LUONG_DONGIA_GHTT_BST4) from ttkd_bsc.bangluong_dongia_202404;
update ttkd_Bsc.ct_dongia_tratruoc A set nhanvien_xuathd = (Select nhanvien_xuathd from dongia_ob_final where thang = 202404 AND thuebao_id = A.thuebao_id and ma_Nv = a.ma_Nv
AND NHANVIEN_XUATHD IS NOT NULL)
--SELECT* FROM ttkd_Bsc.ct_dongia_tratruoc 
where thang = 202404 and loai_tinh = 'DONGIATRA_OB';
commit;
select* from ds_giahan_tratruoc2;
with ds as (   select distinct a.thuebao_id,a.khachhang_id ,chitietkm_id, a.loaitb_id, to_number(to_char(a.ngay_bddc,'yyyymm'))thang_bd, to_number(to_char(ngay_kt_mg,'yyyymm'))thang_kt, e.ten_pb, a.ma_tb, a.so_thangdc, a.cuoc_dc, a.tien_td
         from ds_giahan_tratruoc2 a
            left join ttkd_Bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
            left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
        ) 
        select THUEBAO_ID, a.LOAITB_ID, ds.thang_bd,ds.THANG_KT, TEN_PB, MA_TB, ds.SO_THANGDC, ds.CUOC_DC, ds.TIEN_TD, loaihinh_Tb, ma_kh , khdn , ten_ctkm
        from  ds
            left join css_hcm.loaihinh_Tb a on ds.loaitb_id = a.loaitb_id
            left join css_hcm.db_khachhang b on ds.khachhang_id = b.khachhang_id
            left join css_hcm.loai_kh c on b.loaikh_id = c.loaikh_id
            left join css_hcm.ct_khuyenmai d on ds.chitietkm_id = d.chitietkm_id
        where thang_kt between 202406 and 202412
--        group by thang_kt, ten_pb
;
select* from ttkd_bsc.nhanvien_202404 where ngaysinh like '30/08/%';