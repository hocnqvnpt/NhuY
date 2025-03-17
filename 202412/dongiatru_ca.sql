insert into TTKD_bSC.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, tien_dc_cu,MA_TO, MA_PB,DONGIA, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, 
khachhang_id, tien_khop, ghichu, tyle_Thanhcong)
select a.THANG, 'DONGIATRU_CA' LOAI_TINH,  'DONGIA' ma_kpi, a.manv_giao,  tien_dc_cu, a.MA_TO, a.MA_PB, dongia
                                                , phong_cs, a.thuebao_id, a.ma_tb, DTHU, ngay_tt, khachhang_id, tien_khop, ghichu, tyle
from (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.tien_dc_cu,a.phong_cs, a.ma_to, a.ma_pb,
     a.thuebao_id, a.ma_tb 
                ,case 
                    when decode(sum(a.tien_khop), 0, 0, null, 0, 1) = 0 
                        then round((11*tien_dc_Cu/1.1)/100,0) 
                        else 0 
                end dongia
                   , khachhang_id , sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, null ghichu,
                   round((select sum(da_giahan_dung_hen)*100/sum(tong) 
                            from ttkd_Bsc.tl_giahan_tratruoc where thang = 202412 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV'  and ma_nv = a.manv_giao),2) tyle
              from ttkd_bsc.ct_bsc_giahan_cntt a
                    where a.thang = 202412  and thang_ktdc_cu = 202412  and loaitb_id  in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) -- and ma_Tb in ('hcm_ca_00039226','hcm_ca_00055400')     ------------n------------
                    group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, phong_cs,a.tien_dc_cu,
                    a.thuebao_id, a.ma_tb, khachhang_id
             ) a
                 left   join ttkd_bsc.nhanvien nv on a.manv_giao = nv.ma_nv and nv.thang = 202412 and nv.donvi = 'TTKD';

select * from ttkd_bsc.blkpi_danhmuc_kpi where thang_bd = 202404;  

commit;
rollback;
--- update lai don gia bang 0 cho cac nhan vien co vi tri cong viec khong bi tru don gia

update TTKD_BSC.ct_dongia_tratruoc a set ghichu = to_char(dongia) || '; VI TRI CONG VIEC KHONG BI TRU DON GIA'
-- select distinct b.ma_vtcv, b.ten_vtcv from ct_dongia_tratruoc a left join ttkd_bsc.nhanvien_202403 b on a.ma_nv = b.ma_nv  
--select* from TTKD_BSC.ct_dongia_tratruoc a
where thang = 202412 AND LOAI_TINH = 'DONGIATRU_CA' AND MA_KPI = 'DONGIA' and a.ma_nv in 
(Select ma_nv from ttkd_bsc.nhanvien where ma_vtcv not in ('VNP-HNHCM_BHKV_6','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_18', 'VNP-HNHCM_BHKV_41') and thang=202412 and donvi = 'TTKD' )
    and ghichu is  null;

update TTKD_bSC.ct_dongia_tratruoc set dongia = 0 where ghichu is not null and thang = 202412 and loai_tinh = 'DONGIATRU_CA';
rollback;
select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202412 and loai_Tinh='DONGIATRU_CA';
commit;
--- boi hoan

insert into ttkd_Bsc.ct_dongia_tratruoc
select 202412 THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, 
KHUVUC, -DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU,1 TIEN_KHOP,'boi hoan don gia theo VB 418' GHICHU, TYLE_THANHCONG, HESO_CHUKY, NHANVIEN_XUATHD,
TIEN_XUATHD, TIEN_THUYETPHUC, IPCC from ttkd_Bsc.ct_dongia_tratruoc
where ma_Tb in ('hcm_ca_00065276',
'hcm_ca_00075387',
'hcm_ivan_00029059',
'hcm_ca_ivan_00017493'
);
commit;
select* from ttkd_bsc.ct_dongia_tratruoc where tien_Dc_Cu is  null and loai_tinh ='DONGIATRU_CA' AND thang = 202412;
delete from ttkd_Bsc.tl_giahan_tratruoc where thang = 202412 and loai_tinh = 'DONGIATRU_CA';
commit;

-- INSERT VAO BANG TILE
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI
                                                                                                        , DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN) ;
                                                                                                        select sum(dongia) from (
--insert into TTKD_BSC.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)                                                                                                             
select thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb
                , count(thuebao_id) tong
                  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                  , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                  , sum(dthu) DTHU_thanhcong
                  , round(sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) *100/count(thuebao_id), 2)  tyle
                  , round(-1*sum(dongia), 0) dongia
-- select * 
from ttkd_bsc.ct_dongia_tratruoc  a
where ma_kpi = 'DONGIA' 
        and loai_tinh = 'DONGIATRU_CA' and thang = 202412
group by thang, loai_tinh, ma_kpi, ma_nv, ma_to, ma_pb ) ;
commit;
rollback;
select* from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202411 and LOAI_tINH = 'DOANHTHU';
select* from ttkd_Bsc.nhanvien where thang = 202411 and ma_Nv in ('VNP017436','VNP016578');
commit;
select * from dc_cu;
update  ttkd_Bsc.ct_dongia_Tratruoc a
set tien_dc_cu = (select tien from dc_cu where a.thuebao_id = thuebao_id)
where thang = 202412 and  loai_tinh = 'DONGIATRU_CA'  and tien_khop = 0 and ghichu is null and tien_Dc_cu is null;
update  ttkd_Bsc.ct_dongia_Tratruoc a
set dongia=tien_Dc_Cu /10
where thang = 202412 and  loai_tinh = 'DONGIATRU_CA'  and tien_khop = 0 and ghichu is null and dongia is null;

select* from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202412 and  loai_tinh = 'DONGIATRU_CA'  ; and ma_Nv= 'VNP017796';
select* from ttkd_Bsc.ct_Bsc_Giahan_cntt where thang = 202412 and manv_Giao= 'VNP017796';
delete from ttkd_bsc.tl_giahan_tratruoc a
where loai_tinh = 'DONGIATRU_CA' and a.thang = 202412 ;
select * from ttkd_bsc.tl_giahan_tratruoc a
where loai_tinh = 'DONGIATRU_CA' and a.thang = 202412 ;and tien != 0;
select* from ct_Bsc_chungtu where thang = 202412 and tra_truoc  =1 and ma_gd like 'HCM-LD%';
update  ttkd_bsc.tl_giahan_tratruoc set tien = 0  where ma_nv ='VNP029065' and loai_tinh = 'DONGIATRU_CA' and thang = 202412;
select * from ttkd_Bsc.nhanvien where ten_nv like '%Nghi?p';
insert into ttkd_bsc.ct_dongia_tratruoc;
select* from ttkdhcm_ktnv.ghtt_Chotngay_271 where ma_Tb in ('hcm_ca_plugin_00008754','hcm_signsv_00008727');
select 202412 THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, TIEN_DC_CU, MANV_GIAO, MA_NV_CN, MANV_THUYETPHUC, SOTHANG_DC, HT_TRA_ONLINE, 
KHUVUC, -DONGIA, DTHU, NGAY_TT, HESO_GIAO, KHDN, NHOMTB_ID, KHACHHANG_ID, HESO_DICHVU, TIEN_KHOP,'boi hoan' GHICHU, TYLE_THANHCONG, HESO_CHUKY, NHANVIEN_XUATHD, 
TIEN_XUATHD, TIEN_THUYETPHUC, IPCC 
from ttkd_bsc.ct_dongia_tratruoc where thang = 202407 and loai_tinh = 'DONGIATRU_CA'  and ma_nv = 'VNP029065' and ma_tb in('hcm_smartca_00017765',
'hcm_smartca_00017772',
'hcm_smartca_00017763',
'hcm_smartca_00017764',
'hcm_smartca_00017775',
'hcm_smartca_00017770',
'hcm_smartca_00017754',
'hcm_smartca_00017776',
'hcm_smartca_00017766',
'hcm_smartca_00017769');
delete;
update  ttkd_Bsc.tl_giahan_tratruoc set tien = 0 where thang = 202412  and loai_tinh = 'DONGIATRU_CA' and ma_Nv = 'VNP029065';;
commit;
select distinct loai_tinh, ma_kpi from ttkd_Bsc.tl_giahan_tratruoc where thang = 202412  and loai_tinh = 'DONGIATRU_CA' ;
select* from  ttkd_Bsc.tl_giahan_tratruoc where thang = 202412 and ma_kpi = 'THUHOI_BSC_DONGIA';
select * from css_hcm.nhom_datcoc where nhom_datcoc_id = 4;
select* from ttkd_bsc.tl_giahan_tratruoc where  ma_kpi = 'DONGIA' 
        and loai_tinh = 'DONGIATRU_CA' and thang = 202404 and ma_nv not in (select ma_Nv from ttkd_Bsc.nhanvien_202404 where ma_Vtcv in 
        ('VNP-HNHCM_BHKV_6','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_18', 'VNP-HNHCM_BHKV_41') );
select* from ttkd_Bsc.ct_Dongia_tratruoc where ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA' and ma_Nv = 'VNP017672';
commit;
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202405 and ma_Tb ='hcm_ca_00078209';
select* from ttkd_bsc.nhanvien_202403 where ma_nv in ('CTV021986','VNP016777');
delete  from ttkd_bsc.tl_giahan_tratruoc where  ma_kpi = 'DONGIA' 
        and loai_tinh = 'DONGIATRU_CA' and thang = 202405;
        rollback;
select* from ct_Dongia_tratruoc where ma_kpi = 'DONGIA_CA' 
select DISTINCT b.ten_vtcv from tl_giahan_tratruoc a
    join ttkd_bsc.nhanvien_202401 b on a.ma_nv = b.ma_nv
    where ma_kpi = 'DONGIA_CA'; --AND A.MA_NV = 'VNP017576'
    commit;
----------------------
------------------------------------------------------------------------------------------
DROP TABLE DONGIATRU
select a.ma_Nv, a.tien, b.tien from ttkd_Bsc.tl_giahan_tratruoc a left join tl_giahan_Tratruoc b on a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi and a.loai_tinh = b.loai_tinh and a.thang = b.thang
where  a.ma_kpi = 'DONGIA' 
        and a.loai_tinh = 'DONGIATRU_CA' and a.thang = 202403
        and a.tien != b.tien
CREATE TABLE DONGIATRU AS select* from (
select THANG, a.manv_giao, a.MA_TO, a.MA_PB, tien_dc_cu dthu_goi, KQTH, cast(11  as number) tyle_dgia_tru
      ,  Case when tien_khop = 1 then 0
            when kqth >= 85 then 0 
            when kqth <50 then 100
            else  50 end tyle_tru
        , case when 
            tien_khop = 1 then 0 
            when kqth >= 85 then 0 when kqth <= 50 and tien_khop = 0 then round(11*tien_dc_cu/100)
            when tien_khop = 0 and kqth > 50 and kqth < 85 then
        round(11*0.5*tien_dc_cu/100) 
        end dongia_tru, l.loaihinh_Tb
      , a.ma_tb, DTHU dthu_giahan, ngay_tt, tien_khop
from ( select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.tien_dc_cu,a.phong_cs, a.ma_to, a.ma_pb,a.loaitb_id,
     a.thuebao_id, a.ma_tb  , round((select sum(da_giahan_dung_hen)*100/sum(tong) 
                                from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV' and ma_nv = a.manv_giao),2) KQTH, 
--                case 
--                    when decode(sum(a.tien_khop), 0, 0, null, 0, 1) = 0 and (select sum(da_giahan_dung_hen)*100/sum(tong) 
--                                            from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV' and ma_nv = a.manv_giao) >= 85 
--                        then 0 
--                     when decode(sum(a.tien_khop), 0, 0, null, 0, 1) = 0 and  (select sum(da_giahan_dung_hen)*100/sum(tong) 
--                                            from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV'  and ma_nv = a.manv_giao) <= 50 
--                        then round((11*tien_dc_Cu)/100,0) 
--                    when decode(sum(a.tien_khop), 0, 0, null, 0, 1) = 0 and  (select sum(da_giahan_dung_hen)*100/sum(tong) 
--                                            from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV'  and ma_nv = a.manv_giao) >50 
--                                      and (select sum(da_giahan_dung_hen)*100/sum(tong) 
--                                            from ttkd_Bsc.tl_giahan_tratruoc where thang = 202401 and ma_kpi = 'HCM_TB_GIAHA_024' and loai_tinh = 'KPI_NV'  and ma_nv = a.manv_giao) <85
--                        then round(11*0.5*tien_dc_cu)/100
--                        else 0 
--                end dongia
                    khachhang_id , sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
              from ttkd_bsc.ct_bsc_giahan_cntt a
                    where a.thang = 202401  and thang_ktdc_cu = 202401  and a.loaitb_id not in (147,148)           ------------n------------
                    group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, phong_cs,a.tien_dc_cu,
                    a.thuebao_id, a.ma_tb, khachhang_id, a.loaitb_id
             ) a
                    join ttkd_bsc.nhanvien_202401 nv on a.manv_giao = nv.ma_nv
                    left join css_hcm.loaihinh_tb l on a.loaitb_id = l.loaitb_id
                    
) 

select THANG, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN 
from tl_giahan_Tratruoc where loai_tinh = 'DONGIATRU' AND MA_KPI = 'DONGIA_CA';

select * from ttkd_bct.db_thuebao_ttkd where NHOM_NOCUOC = 4 and pbh_ql_id = 3   ;
select* from ttkd_Bsc.tl_Giahan_tratruoc where loai_tinh = 'DONGIA_CHUNG_TU' AND THANG = 202406;
select distinct ma_kpi, loai_tinh from ttkd_Bsc.tl_giahan_tratruoc where thang = 202412;
SELECT * FROM DONGIATRU-- WHERE MANV_GIAO = 'VNP017576';
select* from css_hcm.khoanmuc_tt where khoanmuctt_id in (
select distinct khoanmuctt_id from css_hcm.ct_tienhd where tien< 0);
select* from css_hcm.khoanmuc_tt where khoanmuctt_id = 36
select SUM( TIEN )
from tl_giahan_Tratruoc where loai_tinh = 'DONGIATRU' AND MA_KPI = 'DONGIA_CA';    
SELECT DISTINCT TEN_vTCV FROM NHANVIEN_202412 WHERE MA_nV IN (SELECT MA_nV FROM TTKD_BSC.TL_GIAHAN_TRATRUOC WHERE THANG = 202412 AND loai_Tinh ='DONGIATRU_CA' AND TIEN!=0);