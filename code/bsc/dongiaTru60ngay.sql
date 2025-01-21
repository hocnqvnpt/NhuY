select  * from ttkd_Bsc.tl_giahan_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU' and ma_nv = 'VNP020227'
delete  from ttkd_Bsc.tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRU' and ma_nv = 'VNP020227'
   select* from ttkd_Bsc.ct_dongia_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU'  --and ma_nv = 'VNP020227'
insert into ttkd_Bsc.ct_dongia_tratruoc(thang) values (-1000)
select distinct loai_tinh, ma_kpi from ttkd_bsc.tl_giahan_tratruoc where thang = 202312
rollback;
commit;
select distinct ten_Vtcv, ma_vtcv --A.*
from ttkd_Bsc.ct_dongia_tratruoc a
    join ttkd_bsc.nhanvien_202401 b on a.ma_nv = b.ma_Nv 
    where a.thang = 202401 and a.loai_tinh =  'DONGIATRU' and b.ten_vtcv  = 'Nhân Viên Qu?n Lý Thu C??c, Thu N?';
 select* --from  ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRA'
  from ttkd_Bsc.tl_giahan_Tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU';
select*
from ttkd_Bsc.ct_dongia_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU';
commit;
insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
--insert into ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
       select THANG, 'DONGIATRU' LOAI_TINH,  'DONGIA' ma_kpi, a.manv_giao, a.MA_TO, a.MA_PB
                                                , phong_cs, a.thuebao_id, a.ma_tb, DTHU, ngay_tt, khachhang_id, tien_khop
       from (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb
                           , khachhang_id , sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                      from ttkd_bsc.ct_bsc_tratruoc_moi a
                            where a.thang = 202401                    ------------n------------
                            group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, phong_cs, a.thuebao_id, a.ma_tb, khachhang_id
                    ) a
                    join ttkd_bsc.nhanvien_202401 nv on a.manv_giao = nv.ma_nv
        where a.ma_pb in ('VNP0702300')  --and nv.ma_vtcv = 'VNP-HNHCM_KHDN_6'
select distinct thang from ttkd_bsc.ct_dongia_tratruoc where loai_tinh = 'DONGIATRU'
;
--3---Don gia tru AS2 cskh KHDN2-3---tinh tren tap giao ghtt--
insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
--insert into ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, PHONG_QL, THUEBAO_ID, MA_TB, DTHU, NGAY_TT, khachhang_id, tien_khop)
select THANG, 'DONGIATRU' LOAI_TINH,  'DONGIA' ma_kpi, a.manv_giao, a.MA_TO, a.MA_PB
        , phong_cs, a.thuebao_id, a.ma_tb, DTHU, ngay_tt, khachhang_id, tien_khop
from (select thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb
            , khachhang_id, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt, decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
          from ttkd_bsc.ct_bsc_tratruoc_moi a
                where a.thang = 202401                    ------------n------------
                group by thang, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.phong_cs, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, khachhang_id
        ) a
        join ttkd_bsc.nhanvien_202401 nv on a.manv_giao = nv.ma_nv
where nv.ma_pb in ('VNP0702400', 'VNP0702500') 
commit;
ROLLBACK;
select * from ttkd_Bsc.ct_dongia_Tratruoc where thang = 202312 and loai_tinh = 'DONGIATRU'
---7–Tinh tong don giá tr?
--vb 411 eO 834390 tyle thay doi 80 --> 90 ds 60 ngay
select* from ttkd_bsc.tl_giahan_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU'
insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
--insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
select THANG, LOAI_TINH,  ma_kpi, MA_NV, MA_TO, MA_PB
         , count(thuebao_id) tong
        , sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) da_giahan
        , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
        , sum(dthu) DTHU_thanhcong
        , round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) tyle
        , -1* case when round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) > 10 
                            then round((count(thuebao_id) * 0.9)- sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end), 0) * 100000 
                    else 0
            end dongia
--    select *
from ttkd_bsc.ct_dongia_tratruoc 
where ma_kpi = 'DONGIA' and ma_pb in ('VNP0702300')
                        and loai_tinh = 'DONGIATRU' and thang = 202401
group by THANG, LOAI_TINH,  ma_kpi, MA_NV, MA_TO, MA_PB
;
select* from ttkd_Bsc.TL_GIAHAN_TRATRUOC where ma_nv = 'VNP020613' and loai_tinh = 'DONGIATRU'
commit;
--8--- ---7–Tinh tong don giá tr?
--Don gia tru AS2 cskh KHDN2-3 ---tinh tren tap giao ghtt–

insert into ttkd_Bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
--insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)

select THANG, LOAI_TINH, ma_kpi, MA_NV, MA_TO, MA_PB
                     , count(thuebao_id) tong
                    , sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end) da_giahan_giao
                    , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI_giao
                    , sum(dthu) DTHU_thanhcong_giao
                    , round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) tyle_ko_giahan
                    , -1 * case when round(sum(case when dthu > 0 and tien_khop = 1 then 0 else 1 end) *100/count(thuebao_id), 2) > 10 
                                        then round((count(thuebao_id) * 0.9)- sum(case when dthu > 0 and tien_khop = 1 then 1 else 0 end), 0) * 60000 
                                else 0
                        end dongia
--    select *
from ttkd_bsc.ct_dongia_tratruoc 
where ma_kpi = 'DONGIA' and ma_pb in ('VNP0702400', 'VNP0702500')
                        and loai_tinh = 'DONGIATRU' and thang = 202401
group by THANG, LOAI_TINH, ma_kpi, MA_NV, MA_TO, MA_PB
;
SELECT COUNT(DISTINCT THUEBAO_ID) FROM TTKD_bSC.CT_bSC_TRATRUOC_MOI WHERE THANG = 202312 AND MANV_gIAO = 'VNP027159' AND TIEN_KHOP = 1
select* from ttkd_bsc.ct_dongia_tratruoc where thang = 202312 and loai_tinh = 'DONGIATRU'
commit;
--select sum(tien) from tl_giahan_tratruoc where thang = 202310 and loai_tinh = 'DONGIATRU'
--select sum(tien) from ttkd_bsc.tl_giahan_tratruoc where thang = 202309 and loai_tinh = 'DONGIATRU'

--select* from ttkd_bsc.ct_dongia_tratruoc where loai_tinh = 'DONGIATRU' and thang = 202310
--
--select* from ttkd_bsc.tl_giahan_tratruoc where loai_tinh = 'DONGIATRU' and thang = 202310
--rollback;
--select* from ttkd_bsc.ct_bsc_Tratruoc_moi where thang = 202310 and manv_Giao = 'VNP024923'

select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202311 AND MA_KPI = 'DONGIATRU'

select* from tl_giahan_tratruoc where thang = 202311 and loai_tinh = 'DONGIATRU' and ma_nv = 'VNP019492'
Select* from ttkd_bsc.ct_bsc_tratruoc_moi where thang = 202311 and manv_giao = 'VNP019492'
select* from css_hcm.db_Thuebao where thuebao_id = 9385939