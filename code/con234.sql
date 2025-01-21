select* from v_thongtinkm_all where ma_tb = 'ctyvietsing2018';

;select* from ttkd_bsc.nhanvien_202404 where ten_nv like '%H?c'; 
select* from ttkd_Bsc.ct_bsc_Giahan_cntt where thang = 202404 and ma_Tb in ('hcm_ivan_00010175','hcm_ivan_00016063','hcm_ca_00066675','hcm_ca_00074023',
'hcm_ca_ivan_00015606','hcm_ivan_00015640','hcm_ca_ivan_00015607','hcm_ca_00054196','hcm_ca_ivan_00015527','hcm_ca_00039510','hcm_ca_00039251','hcm_ca_00039509',
'hcm_ivan_00010086','hcm_ca_00039522','hcm_ca_00013622');
create table bangluong_dongia_202404 as select* from  ttkd_bsc.bangluong_dongia_202404;
select a.ma_Nv_hrm, a.luong_dongia_ghtt, a.luong_dongia_ghtt  from ttkd_bsc.bangluong_dongia_202404 a
join bangluong_dongia_202404 b on a.ma_nv_Hrm = b.ma_nv_Hrm
where nvl(a.luong_dongia_ghtt,0) <> nvl(a.luong_dongia_ghtt,0) ; --and b.loai_Tinh in ('DONGIA_TS_TP_TT' ,'DONGIATRA_OB') and b.thang = 202404
group by ma_Nv_hrm,luong_dongia_ghtt;
select* from ttkd_Bsc.tl_Giahan_Tratruoc where thang = 202404;
group by ma_nv_hrm, giamtru_ghtt_cntt;-- where thang =202404 and loai_tinh in ('DONGIATRA_OB' ,'DONGIA_TS_TP_TT','DONGIATRU_CA')-- where thang = 202404;
VNP017937
VNP019952
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202403 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA';
select* from ttkd_Bsc.ct_Dongia_tratruoc where thang = 202404 and tien_khop = 0 and dongia != tien_Dc_cu/10 and ma_kpi = 'DONGIA' and loai_tinh = 'DONGIATRU_CA';
select* from ttkd_bsc.bangluong_kpi where thang = 202404;
select HCM_TB_GIAHA_024,HCM_TB_GIAHA_025,HCM_TB_GIAHA_026, ma_nv,ten_Vtcv  from ttkd_Bsc.bangluong_kpi_202404 where ma_to = 'VNP0701803'--HCM_TB_GIAHA_026 is not null;
select* from TTKD_BSC.blkpi_danhmuc_kpi_vtcv where ma_kpi ='HCM_TB_GIAHA_026' and thang_kt is null;

select a.ma_to, b.ma_to,a.*, b.* from ttkd_Bsc.tl_Giahan_tratruoc a 
join ttkd_Bsc.ct_Bsc_giahan_cntt b on a.ma_nv = b.manv_giao 
where a.ma_to != b.ma_to and a.thang = b.thang and a.thang = 202404 and a.ma_kpi='HCM_TB_GIAHA_024'
;
select* from ttkd_Bsc.tl_Giahan_tratruoc where thang = 202404 and ma_kpi = 'HCM_SL_COMBO_005' and ma_nv;
select sum(soluong_Tb) from (
select a.thang, a.ma_pb, pb.ten_pb, count(distinct a.ma_Tb) soluong_Tb from ttkd_Bsc.ct_Bsc_homecombo a
    join ttkd_bsc.dm_phongban pb on a.ma_pb = pb.ma_pb and pb.active = 1
  where thang = 202404 and loai_kpi in ('Fiber_hh','Fiber_moi','HomeTV') 
  group by a.thang, a.ma_pb , pb.ten_pb);
select* from ttkd_bsc.nhanvien_202404 where ma_nv in 
select * from ttkd_Bsc.nhanvien_202404 where ma_nv in (select ma_nv from ttkd_Bsc.ct_Bsc_homecombo where thang = 202404 and loai_kpi in ('Fiber_hh','Fiber_moi','HomeTV')
and ma_pb is null)
select * from( SELECT ma_TB, CHUONG_TRINH, DTV_OB, to_char(NGAY_OB, 'dd/mm/yyyy') as NGAY_OB, MA_GOI_DV, DOANHTHU_GOI_DV, CHU_KY, to_char(NGAY_MO_DV,'dd/mm/yyyy') as NGAY_MO_DV,
MA_NV, MA_TO, MA_PB,
TEN_NV, TEN_TO, TEN_PB, THANG, LOAI_OB FROM manpn.Z_HCM_CL_OB_CKN_CKD where LOAI_OB = 'CKD' );

select* from css_hcm.hd_thuebao where hdkh_id = 21174780;
select* from css_hcm.hd_thuebao where hdtb_id = 23264905;
select* from css_hcm.ct_phieutt where hdtb_id = 23264905; phieutt_id= 7989921;-- = 'HCM-TT/02582947'
select* from css_hcm.khoanmuc_tt where KHOANMUCTT_ID = 36;
select* from css_hcm.phieutt_hd where phieutt_id = 7989921;

select ngay_hd from css_hcm.phieutt_hd where ma_gd = 'HCM-TT/02547712' ;

select* from v_thongtinkm_all where ma_Tb ='nmtuan_homed';
6374660
5653172;
(select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
                                            from css.v_hdtb_datcoc@dataguard g left join css.v_db_datcoc@dataguard h on g.thuebao_dc_id = h.thuebao_dc_id
--                                            where h.thang_bd > 202310
                                            );
select * from css.v_db_datcoc@dataguard where THUEBAO_DC_ID = 4757248;
