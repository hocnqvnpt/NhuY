insert into finallll
select to_char(a.ngay_kt_mg,'yyyymm') thang_kt,a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID,lh.loaihinh_Tb, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC
,a.SO_THANGDC, a.SO_THANGKM, a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn,
count (distinct case when nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) and b.nhomtb_id is null 
    then b.thuebao_id else null end ) SLTB_ChuaGhep_GoiDadv, null dsdadv,
--dadv.ds,
count(distinct case when a.ma_Tt != b.ma_Tt and nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0)
then b.thuebao_id else null end)  as SLTB_Khac_MaTT, null dsmatt
--, matt.ds_matb
,count(distinct case when nvl(a.tinh_id ,0) != nvl(b.tinh_id,0) or nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) then b.thuebao_id else null end ) 
as SLTB_Khac_DiaChi, null dsdc
--,diachi.ds_matb
, count (distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.thuebao_id else null end) 
SLTB_LechKy, null dslk
--,lk.ds
, count(distinct b.thuebao_id) + 1 as SLTB
from khdn a
    left join final_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id 
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id =lh.loaitb_id
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id 
--        left join matt on a.thuebao_id = matt.thuebao_id
--        left join diachi on a.thuebao_id = diachi.thuebao_id
--        left join lk on a.thuebao_id = lk.thuebao_id
--        left join dadv on a.thuebao_id = dadv.thuebao_id
where  a.khachhang_id  in (9798610,9769564, 9759328,9726101,9795170) --and a.khachhang_id = kh.khachhang_id 

group by a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC,
a.SO_THANGDC, a.SO_THANGKM, --matt.ds_matb,lk.ds,diachi.ds_matb,dadv.ds,
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, lh.loaihinh_Tb,a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn;
commit;
select* from matt
union all
select to_char(a.ngay_kt_mg,'yyyymm') thang_kt,a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID,lh.loaihinh_Tb, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC
,a.SO_THANGDC, a.SO_THANGKM, a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn
,count (distinct case when nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) and b.nhomtb_id is null 
    then b.thuebao_id else null end ) SLTB_ChuaGhep_GoiDadv -- + count(case when a.nhomtb_id is null then 1 else null end) 
--,
,LISTAGG( distinct case when  nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) 
and b.nhomtb_id is null then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) 
 DS_ChuaGhep_Goi
,count(distinct case when a.ma_Tt != b.ma_Tt and nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0)
then b.thuebao_id else null end)  as SLTB_Khac_MaTT
--,
,LISTAGG( distinct case when  a.tinh_id = b.tinh_id and a.quan_id = b.quan_id and a.phuong_id = b.phuong_id  and b.ma_tt != a.ma_tt then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
 DS_ChuaGhepMaTT
,count(distinct case when nvl(a.tinh_id ,0) != nvl(b.tinh_id,0) or nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) then b.thuebao_id else null end )  as SLTB_Khac_DiaChi
--,
,LISTAGG( distinct case when nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) and nvl(a.tinh_id,0) != nvl(b.tinh_id,0) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id)
 DS_KhacDiaChi
, count (distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.thuebao_id else null end) SLTB_LechKy
--,
,LISTAGG( distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) 
 DS_LechKY
, count(distinct b.thuebao_id) + 1 as SLTB

from final_kh a
    left join final_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id 
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id =lh.loaitb_id
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id 
where lkh.khdn= 1 and a.khachhang_id not in (9798610,9769564, 9759328,9726101,9795170) --and a.khachhang_id = kh.khachhang_id 
group by a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC,
a.SO_THANGDC, a.SO_THANGKM, 
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, lh.loaihinh_Tb,a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn;

-- diachi
select* from diachi
