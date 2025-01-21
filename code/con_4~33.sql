select*
from  ttkd_Bsc.ct_Bsc_tratruoc_moi_30day b 
    join bhol a on a.thuebao_id = b.thuebao_id and b.thang = 202411;
where tien_khop=1 and tien_thanhtoan is null;
    select a.*
from (
--****C.4(BHKV), C.4(KHDN) Ty le thue bao ghtt khong thanh cong tren tap KH thuoc BHKV, BHDN giao ds het han----
                select thang, 'HCM_TB_GIAHA_022' , 'KPI_PB'
                         , count(thuebao_id) tong
                         , sum(case when  tien_khop > 0 then 1 else 0 end) da_giahan
--                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen,
                         round(sum(case when tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang, a.thuebao_id, a.ma_tb,  sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ttkd_bsc.ct_bsc_tratruoc_moi_30day a
                                            join bhol b on a.thuebao_id = b.thuebao_id and a.thang = 202411
                                        where thang = 202411
                                        group by thang, a.thuebao_id, a.ma_tb
                               )
                group by thang
                order by 2
                ) a
;
    select* from ttkd_Bsc.ct_Bsc_tratruoc_moi where thang = 202411 and ma_Tb='ctydautuptyb';
    select* from v_Thongtinkm_All where  ma_Tb='ctydautuptyb';
    select * from qltn_hcm.ct_tra 
where  ky_cuoc = 20241101;
select* from ttkdhcm_ktnv.ds_chungtu_nganhang_pqt_sudung where ngay_insert >'03/11/2024 16:26:08';
select* from ttkd_bsc.ct_Bsc_tratruoc_moi where thang = 202411 and ma_Tb='mynhien8124552'; ma_gd='HCM-TT/03081143';
select* from v_Thongtinkm_all where  ma_Tb='mynhien8124552';
select* from tmp3_60ngay where  ma_Tb='mynhien8124552';
select* from ttkd_Bsc.ct_Bsc_Tratruoc_moi where thang = 202411 and ma_Tb in ('xuanvu250122','mesh0090945','luuanap12023','caoanh117','trunghiu93874990');