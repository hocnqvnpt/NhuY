select* from ct_dongia_tratruoc where thang = 202311 and 'DONGLUCTS' = LOAI_TINH
insert into ttkd_bsc.ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, THUEBAO_ID, MA_TB, ma_nv_cn, manv_thuyetphuc
                                                                                    , SOTHANG_DC, HT_TRA_ONLINE, dongia, DTHU, NGAY_TT, tien_khop)
--insert into ct_dongia_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, THUEBAO_ID, MA_TB, ma_nv_cn, manv_thuyetphuc, SOTHANG_DC, HT_TRA_ONLINE, dongia, DTHU, NGAY_TT, tien_khop)
        select thang, 'DONGLUCTS' LOAI_TINH,  'DONGLUC' ma_kpi, manv_thuyetphuc ma_nv, ma_to, ma_pb
                            , thuebao_id, ma_tb, manv_cn ma_nv_cn, manv_thuyetphuc
                              , SOTHANG_DC, HT_TRA_ONLINE, dongia, DTHU, NGAY_TT, tien_khop
                                  
        from (select thang, manv_thuyetphuc, ma_to, ma_pb, thuebao_id, ma_tb, manv_cn
                                , max(so_thangdc) sothang_dc
                                --, sum(decode(bo_dau(ten_ht_tra), 'Tien mat', 0, 1)) ht_tra_online
                                , sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                when a.ht_tra_id in (2,4) then 1 end) ht_tra_online
                                , case
                                            when max(so_thangdc) >= 3 and max(so_thangdc) < 6 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) > 0 then 35000
                                            when max(so_thangdc) >= 6 and max(so_thangdc) < 12 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) > 0 then 45000
                                            when max(so_thangdc) >=12 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) > 0 then 60000
                                            ----thanh toan tien mat
                                            when max(so_thangdc) >= 3 and max(so_thangdc) < 6 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) = 0 then 25000
                                            when max(so_thangdc) >= 6 and max(so_thangdc) < 12 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) = 0 then 35000
                                            when max(so_thangdc) >=12 
                                                            and sum(case when a.ht_tra_id in (1, 7) then 0
                                                                                            when a.ht_tra_id in (2,4) then 1 end) = 0 then 50000
                                end dongia
                                ,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop, row_number() over (partition by a.thuebao_id order by max(ngay_tt)) rnk
                from ttkd_bsc.ct_bsc_trasau_tp_tratruoc a
                where rkm_id is not null and thang = 202402
                group by thang, manv_thuyetphuc, ma_to, ma_pb, thuebao_id, ma_tb, manv_cn
                ) 
        where ma_pb is not null and dthu > 0 and rnk = 1
;

delete 
--SELECT* 
from ttkd_Bsc.ct_dongia_tratruoc where thang = 202312 and loai_tinh = 'DONGLUCTS' AND THUEBAO_ID IN (SELECT * FROM ABCDEF)
commit;
SELECT* from ttkd_Bsc.nhanvien_202312 where ten_nv like '%Ph??ng'
--DELETE  
select* FROM ttkd_bsc.tl_giahan_tratruoc WHERE THANG = 202312 AND LOAI_TINH = 'DONGLUCTS'
COMMIT;
insert into tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
insert into ttkd_bsc.tl_giahan_tratruoc (THANG, LOAI_TINH, MA_KPI, MA_NV, MA_TO, MA_PB, TONG, DA_GIAHAN_DUNG_HEN, DTHU_DUYTRI, DTHU_THANHCONG_DUNG_HEN, TYLE, TIEN)
        select thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
                                , count(thuebao_id) tong
                                  , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                                  , 0 DTHU_DUYTRI
                                  , sum(dthu) DTHU_thanhcong
                                  , round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                                  , sum(dongia*tien_khop) dongia
            --    select *
                from ttkd_bsc.ct_dongia_tratruoc 
                where ma_kpi = 'DONGLUC' and loai_tinh = 'DONGLUCTS' and thang = 202402-- AND MA_NV IN (SELECT DISTINCT MANV_THUYETPHUC FROM ABCDEF) --and ma_nv = manv_thuyetphuc 
                 group by thang, LOAI_TINH, ma_kpi, ma_nv, ma_to, ma_pb
commit;

select* from ttkd_bsc.bangluong_dongia_202312
