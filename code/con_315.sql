select* from ttkd_Bsc.ct_bsc_tratruoc_Moi where rownum = 1;
select DISTINCT MA_VTCV , TEN_vTCV from ttkd_Bsc.tl_giahan_Tratruoc a 
left join ttkd_bsc.nhanvien_202403 b on a.ma_Nv = b.ma_nv
where a.loai_tinh ='DONGIATRU_CA' AND TIEN != 0;
update ct_bsc_tratruoc_moi_30day a set TIEN_KHOP = 1
-- select * from ct_bsc_tratruoc_moi_30day a
where thang = 202404 and a.rkm_id is not null and tien_khop = 0
and ht_tra_id in (2, 4,5)
and  exists 
(
    select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                    join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
    where  phieu_id = a.phieutt_id  
    group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
    having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM - 10
    );
commit;
select a.*, a.ma_nv manv_giaophieu, a.ten_Nv tenv_giaophieu, from chi_thao a
    left join css_hcm.phieutt_hd b on a.phieutt_hd =b.phieutt_id
    left join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id
    left join admin_hcm.nhanvien_onebss nv on nv.nhanvien_id = c0.nvgiaophieu_id
    left join admin_hcm.donvi dv on dv.donvi_id = c0.dvgiaophieu_id
 ;
 select a.* , c.ngay_yc 
 from t_222222 a
    left join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
    left join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id
  where a.ma_pb in ('VNP0701500','VNP0701800','VNP0701200','VNP0701300','VNP0701600','VNP0701400','VNP0701100','VNP0702200','VNP0702100');
;
select distinct TEN_GOI, GOI_DA_DV, PHAM_TRAM from guiy
 alter table t_222222 rename column PHONG_GIAOPHIEU to donvi_giaophieu;
 create table t_222222 as
 select THANG, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, MANV_TH, 
 PHONG_TH, MANV_CN manv_giaophieu, PHONG_CN phong_giaophieu, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, THANG_BD_MOI, 
 SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID, LOAITB_ID, PBH_OA_ID, 
 MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU 
 from chi_thao
 where ma_pb in ('VNP0701500','VNP0701800','VNP0701200','VNP0701300','VNP0701600','VNP0701400','VNP0701100','VNP0702200','VNP0702100');
 delete  from t_222222 where rkm_id in (select rkm_id from t_222222  group by rkm_id having count( rkm_id) > 1) and thang = 202404;
 select distinct ma_pb, ten_pb from ttkd_Bsc.nhanvien_202403 where ten_pb like 'Phòng Bán Hàng Khu V?c%'
create table chi_thao as 
select * from ct_bsc_tratruoc_moi_30day where thang = 202404 and to_Number(to_char(ngay_Tt,'yyyymm')) = 202403 and tien_khop = 1
union all 
select * from ttkd_Bsc.ct_bsc_tratruoc_moi_30day where thang = 202403 and to_Number(to_char(ngay_Tt,'yyyymm')) = 202403 and tien_khop = 1
union all 
select THANG-1, GH_ID, PBH_QL_ID, PBH_GIAO_ID, TBH_GIAO_ID, PBH_TH_ID, PBH_CN_ID, MA_TB, MANV_CS, PHONG_CS, MA_TO, MA_PB, MANV_GIAO, PHONG_GIAO, 
MANV_TH, PHONG_TH, MANV_CN, PHONG_CN, MANV_THPHUC, MAPB_THPHUC, MANV_GT, MANV_THUNGAN, KHDN, HESO_GIAO, THANG_KTDC_CU, TIEN_DC_CU, MA_TT, MA_GD, RKM_ID, 
THANG_BD_MOI, SO_THANGDC, AVG_THANG, TIEN_THANHTOAN, VAT, NGAY_TT, NGAY_NGANHANG, SOSERI, SERI, KENHTHU, TEN_NGANHANG, TEN_HT_TRA, TRANGTHAI_TB, THUEBAO_ID,
LOAITB_ID, PBH_OA_ID, MANV_OA, NHOMTB_ID, KHACHHANG_ID, GOI_OLD_ID, PHIEUTT_ID, HT_TRA_ID, KENHTHU_ID, TIEN_KHOP, MA_CHUNGTU
from ttkd_Bsc.ct_bsc_tratruoc_moi where thang = 202403 and to_Number(to_char(ngay_Tt,'yyyymm')) = 202403 and tien_khop = 1