update final_kh a set fiber_donle = null
--- select * from ds_giahan_tratruoc a
where fiber_donle = 0
commit;
    and exists (select 1 from css_hcm.db_Thuebao xj
    where xj.loaitb_id in (61, 171, 210, 222, 224)
                        and xj.khachhang_id = a.khachhang_id)
                        
select distinct fiber_donle from final_kh --where khachhang_id=4899949

select* from css_hcm.diachi_tb where thuebao_id =11824831 --in (11753480,8695097,11841735,10466594)
select * from final_kh where khachhang_id=9755168-- and tinh_id = 33 and quan_id  = 3985 and phuong_id = 53960-- in (50480230,60210420,61780633,62744416)
\;
select 
to_char(a.ngay_kt_mg,'yyyymm') thang_kt,a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID,lh.loaihinh_Tb, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC
,a.SO_THANGDC, a.SO_THANGKM, a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn
,count (distinct case when nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) and b.nhomtb_id is null 
    then b.thuebao_id else null end ) SLTB_ChuaGhep_GoiDadv -- + count(case when a.nhomtb_id is null then 1 else null end) 
--,RTRIM(XMLAGG(XMLELEMENT(E,B.MA_TB,',').EXTRACT('//text()') ORDER BY B.THUEBAO_ID).GetClobVal(),',') AS DS_MATB
--
,null
,count(distinct case when a.ma_Tt != b.ma_Tt and nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0)
then b.thuebao_id else null end)  as SLTB_Khac_MaTT, null
--, null
----,LISTAGG( distinct case when  a.tinh_id = b.tinh_id and a.quan_id = b.quan_id and a.phuong_id = b.phuong_id  and b.ma_tt != a.ma_tt then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DS_ChuaGhepMaTT
,count(distinct case when nvl(a.tinh_id ,0) != nvl(b.tinh_id,0) or nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) then b.thuebao_id else null end )  as SLTB_Khac_DiaChi
----,LISTAGG( distinct case when nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) and nvl(a.tinh_id,0) != nvl(b.tinh_id,0) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DS_KhacDiaChi
, null
, count (distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.thuebao_id else null end) SLTB_LechKy
----,LISTAGG( distinct case when to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm')) then b.ma_Tb end, '; ') WITHIN GROUP (ORDER BY b.thuebao_id) DS_LechKY
,null

, count(distinct b.thuebao_id) + 1 as SLTB

from final_kh a
    left join final_kh b on a.khachhang_id = b.khachhang_id and a.thuebao_id != b.thuebao_id 
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id =lh.loaitb_id
    left join css_hcm.db_khachhang kh on a.khachhang_id = kh.khachhang_id
        left join css_hcm.loai_kh lkh on lkh.loaikh_id = kh.loaikh_id 
where a.khachhang_id in (9798610,9769564, 9759328,9726101,9795170) 
--and  nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0) and b.nhomtb_id is null 
--and  a.ma_Tt != b.ma_Tt and nvl(a.tinh_id,0) = nvl(b.tinh_id,0) and nvl(a.quan_id,0) = nvl(b.quan_id,0) and nvl(a.phuong_id,0) = nvl(b.phuong_id,0)
--and ( nvl(a.tinh_id ,0) != nvl(b.tinh_id,0) or nvl(a.quan_id,0) != nvl(b.quan_id ,0) or nvl(a.phuong_id,0) != nvl(b.phuong_id ,0) )
--and to_number(to_char(a.ngay_kt_mg,'yyyymm')) != to_number(to_char(b.ngay_kt_mg,'yyyymm'))
group by a.RKM_ID, a.THUEBAO_ID, a.MA_TB, a.LOAITB_ID, a.NGAY_BDDC, a.NGAY_KTDC, a.NGAY_KT_MG, a.NGAY_HUY, a.NGAY_THOAI, a.TIEN_TD, a.CUOC_DC,
a.SO_THANGDC, a.SO_THANGKM, 
a.CONGVAN_ID, a.KHUYENMAI_ID, a.CHITIETKM_ID, lh.loaihinh_Tb,a.NHOM_DATCOC_ID, a.SL_DATCOC, a.NHOMTB_ID, a.GOI_ID, a.TRANGTHAITB_ID, a.KHACHHANG_ID, a.FIBER_DONLE,
a.TRATRUOC,a.thanhtoan_id, a.ma_Tt,a.ma_nv, a.tento,a.ten_pb, a.tinh_id, a.quan_id, a.phuong_id, lkh.khdn
order by a.thuebao_id;