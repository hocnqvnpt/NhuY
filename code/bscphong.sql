select* from ttkd_Bsc.nhanvien where mail_Vnpt = 'htkphuong.hcm@vnpt.vn';
select* from rmp_bsc_phong;
update rmp_bsc_phong set tyle = 0 where ma_pb = 'VNP0702300' and ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
commit;
select* from ttkd_bsc
drop table tlt;
select* from rmp_bsc_phong  where ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh';
update rmp_bsc_phong  
set phong_giao = 'Phòng Bán Hàng Khu V?c Th? ??c'
where ten_kpi = 'T? l? phi?u t?n d?ch v? B?ng r?ng ch?a x? lý cu?i k? thu?c trách nhi?m c?a kinh doanh' and phong_giao = 'Phòng Bán Hàng Khu V?c Tân Bình' and ma_pb = 'VNP0701800';
commit;
rollback;
select ghichu, ma_gd, so_Ct , tien+vat from css_Hcm.phieutt_hd where ma_gd in (select ma_gd from ttkd_Bsc.ct_bsc_tratruoc_moi x
where thang = 202408 and tien_khop = 0 and ma_pb = 'VNP0701100');
select* from ttkd_Bsc.nhanvien; 
update ttkd_Bsc.ct_bsc_tratruoc_moi x
set tien_khop = 1 where ma_gd in ('HCM-TT/02950176' ,'HCM-TT/02861089'  ,'HCM-TT/02862969') and thang = 202408;
commit;
--select* from ttkd_Bsc.ct_bsc_tratruoc_moi_30day x
where thang = 202408 and tien_khop = 0 and exists (select b.ma_ct, a.ma_gd ,phieutt_id from css_hcm.phieutt_hd a 
                join ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb b on  REGEXP_REPLACE (a.so_ct, '[^\-a-zA-Z0-9]', '') = b.ma_Ct 
                where b.tien_ct-b.tien_tt = (a.tien+a.vat)
                and x.ma_Gd = a.ma_gd)
;
rollback;
commit;
select distinct ma_pb,ten_pb from ttkd_Bsc.nhanvien_202405 where ten_pb like 'Phòng Bán Hàng%';
select;
update ttkd_bsc.ct_bsc_tratruoc_moi_30day x
set tien_khop = 0 where thang = 202408 and  exists(
select 1 from ct_bsc_tratruoc_moi_30day a where  thuebao_id = x.thuebao_id and ma_Gd = x.ma_Gd and thang = 202408 and tien_khop = 0 and exists
(select 1 from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where thang = 202408 and tien_khop = 1
and thuebao_id = a.thuebao_id and ma_Gd = a.ma_Gd));
select  REGEXP_REPLACE (ghichu, '[^\-a-zA-Z0-9]', '')  from  css_hcm.phieutt_hd where ma_Gd ='HCM-TT/02937288';
select b.ma_ct, a.ma_gd, b.tien_ct, a.tien from css_hcm.phieutt_hd a 
                join ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb b on  REGEXP_REPLACE (so_ct, '[^\-a-zA-Z0-9]', '') = b.ma_Ct
                where a.ma_gd = 'HCM-TT/02897364';
                select* from  ttkdhcm_ktnv.ds_Chungtu_nganhang_oneb where ma_ct = 'VCB_20240829_320672' ;"VCB_20240829_320672"
select* from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where ma_gd = 'HCM-TT/02897364';;
SELECT REGEXP_REPLACE (so_ct, '[^\-a-zA-Z0-9]', ''), ghichu from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02958309';
SELECT trim(ghichu), ghichu from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02958309';
SELECT trim(ghichu), ghichu from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02897364';
FROM dual;VCB_20240827_317363 VCB_20240827_317326
with abs as (select ghichu from css_hcm.phieutt_Hd where ma_gd  = 'HCM-TT/02897364')
SELECT REPLACE (SUBSTR(t.ghichu, 1, INSTR(t.ghichu, ' ')-1),'[^\-a-zA-Z0-9]', '') AS col_one,
       REPLACE (SUBSTR(t.ghichu, 1, INSTR(t.ghichu, ' ')+1),'[^\-a-zA-Z0-9]', '')  AS col_two
  FROM abs t;VCB_20240812_293940
  with abs as (select ghichu, a.ma_gd from css_hcm.phieutt_Hd  a join ttkd_Bsc.ct_Bsc_tratruoc_moi b on a.phieutt_id = b.phieutt_id
  where b.thang = 202408 and tien_khop = 0)
, ctu as (
  SELECT SUBSTR(t.ghichu, 1, INSTR(t.ghichu, ', ')-1) AS col_one,
       SUBSTR(t.ghichu, INSTR(t.ghichu, ' ')+1) AS col_two, ma_gd
       from abs t
)
select* from ctu;
SELECT REPLACE (SUBSTR(REPLACE (t.ghichu, '[^\-a-zA-Z0-9]', ''), 1, INSTR(t.ghichu, ' ')-1),'[^\-a-zA-Z0-9]', '') AS col_one,
       REPLACE (SUBSTR(REPLACE (t.ghichu, '[^\-a-zA-Z0-9]', ''), 1, INSTR(t.ghichu, ' ')+1),'[^\-a-zA-Z0-9]', '')  AS col_two
  FROM abs t;
select* from TMP_T8_OB;"VCB_20240829_320672	"
SELECT
REGEXP_SUBSTR (
  'The quick brown fox', '[^ ]+', 1, level
) AS string_parts
FROM dual
CONNECT BY REGEXP_SUBSTR (
  'The quick brown fox', '[^ ]+', 1, level
) IS NOT NULL;
COMMIT;
select* from ct_Bsc_chungtu where ma_Chungtu='VCB_20240913_348415';
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_oneb where ma_ct ='VCB_20240913_348415' ;
select* from ct_Bsc_Ghtt_bhol;
update ttkd_Bsc.nhanvien a set THAYDOI_vTCV = (select THAYDOI_vTCV from tinhbsc  where ma_Nv = a.ma_nv) where thang = 202408 and donvi = 'TTKD';
create table tmp_t9_ob as 
select * from (
select THANG, KHACHHANG_ID, THUEBAO_ID, MA_TO, MA_PB, MA_TB, LOAIHINH_TB, MA_NV, NHOMTB_ID_OLD, NHOMTB_ID_NEW, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, TIEN_KHOP
from (
             select     
             a.thang,  a.khachhang_id, a.thuebao_id, a.ma_to, a.ma_pb, ma_Tb, b.loaihinh_tb
                                        ,a.MANV_THUYETPHUC ma_nv, goi_old_id nhomtb_id_old, nhomtb_id nhomtb_id_new, sum( cuoc_dc_cu) tien_Dc_Cu , max(a.SO_THANGDC_MOI) sothang_dc
                                    , case
                                            when max(a.SO_THANGDC_MOI) >=12 then 1.2
                                            when max(a.SO_THANGDC_MOI) < 12 and max(a.SO_THANGDC_MOI) >= 6 then 1
                                            when max(a.SO_THANGDC_MOI) < 6 and max(a.SO_THANGDC_MOI) > 3 then 0.9
                                            else 0
                                                    end
                                    heso_chuky
                                   , case 
                                            when a.loaitb_id in (58, 59) then 1  -  nvl(0.5 * nvl2(a.goi_old_id, 1, 0)  
                                                                                                    * nvl2(a.nhomtb_id, 0, 1)      
                                                                                            , 0)
                                            when a.loaitb_id = 210 then 0.5 - nvl(0.3* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                        where xu.loaitb_id in (58, 59)
                                                                                                        and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                            when a.loaitb_id in (61, 171, 18) then 0.5 - nvl(0.1* (select distinct 1 from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt xu
                                                                                                where xu.loaitb_id in (58, 59)
                                                                                                                and xu.khachhang_id = a.khachhang_id and xu.thang = a.thang), 0)
                                        else 0 
                                       end Heso_dichvu
                                    ,  sum(tien_thanhtoan) DTHU
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a 
									left join css_hcm.loaihinh_tb b on a.loaitb_id = b.loaitb_id
                        where a.rkm_id is not null and thang = 202409
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, a.loaitb_id, b.loaihinh_tb, ma_tb
        ) where rnk = 1 and dthu > 0
);
select* from rmp_bsc_phong;
insert into rmp_bsc_phong 
select 202409 thang , ma_pb, ma_kpi, 