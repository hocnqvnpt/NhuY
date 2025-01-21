SELECT distinct ten_vtcv, ma_vtcv , ten_pb, ma_pb FROM ttkd_bsc.bangluong_dongia_202409 a where donvi = 'TTKD' and LUONG_DONGIA_GHTT >0 ;
update ttkd_bsc.tl_giahan_tratruoc  set tien = 0 where thang = 202409 and ma_pb ='VNP0700600';
commit;
UPDATE TTKD_bSC.BLKPI_DANHMUC_KPI SET CHITIEU_GIAO = 0 where thang = 202409 AND NGUOI_XULY ='Nh? Ý';
select* from ttkd_Bsc.BLKPI_DANHMUC_KPI where thang = 202409 AND NGUOI_XULY ='Nh? Ý';
UPDATE ttkd_Bsc.bangluong_kpi SET DIEM_TRU = -MUCDO_HOANTHANH WHERE thang = 202409 and ma_kpi = 'HCM_CL_TONDV_003' AND MUCDO_HOANTHANH < 0;
UPDATE TTKD_bSC.BLKPI_DANHMUC_KPI SET DIEM_cONG = 1, DIEM_TRU = 1, MUCDO_HOANTHANH = 0, CHITIEU_GIAO = 0 where thang = 202409 and ma_kpi = 'HCM_CL_TONDV_003';
SELECT* FROM RMP_BSC_PHONG;
SELECT DISTINCT MA_KPI, LOAI_TINH FROM TTKD_BSC.TL_GIAHAN_TRATRUOC WHERE THANG = 202409;
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where  ma_Tb= 'hcm_ca_ivan_00018729';
select * from (
select THANG, KHACHHANG_ID, THUEBAO_ID, MA_TO, MA_PB, MA_TB, LOAIHINH_TB, MA_NV, NHOMTB_ID_OLD, NHOMTB_ID_NEW, SOTHANG_DC, HESO_CHUKY, HESO_DICHVU, TIEN_KHOP, ten_nv
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
                                    ,  sum(tien_thanhtoan) DTHU, nv.ten_nv
                                    , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id, a.MANV_THUYETPHUC order by max(a.rkm_id)) rnk
                        from ttkd_bsc.nhuy_ct_Bsc_ipcc_obghtt a 
									left join css_hcm.loaihinh_tb b on a.loaitb_id = b.loaitb_id
                                    left join ttkd_Bsc.nhanvien nv on a.manv_thuyetphuc = nv.ma_nv and a.thang = nv.thang
                        where a.rkm_id is not null and A.thang = 202409 and a.ma_pb = 'VNP0703000'
                        group by a.thang, a.thuebao_id,  a.ma_to, a.ma_pb
                                          ,a.MANV_THUYETPHUC,  a.khachhang_id, goi_old_id, nhomtb_id, a.loaitb_id, b.loaihinh_tb, ma_tb, nv.ten_nv
        ) where rnk = 1 and dthu > 0
)