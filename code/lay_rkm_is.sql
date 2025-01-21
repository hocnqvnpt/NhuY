with hddc as 
(
    select distinct hdtb_id, nvl(h.rkm_id, g.rkm_id) rkm_id--, nvl(h.ngay_bddc, g.ngay_bddc) ngay_bddc, nvl(h.ngay_ktdc, g.ngay_ktdc) ngay_ktdc 
    from css_hcm.hdtb_datcoc g left join css_hcm.db_datcoc h on g.thuebao_dc_id = h.thuebao_dc_id
    
    union all 
    select hdtb_id , rkm_id
    from css_hcm.khuyenmai_dbtb
)-- select * from hddc-- group by hdtb_id having count (distinct rkm_id) > 1

select 
    202402 thang, 0 GH_ID, a.DONVI_GIAO PBH_GIAO_ID,  a.PBH_ID_TH PBH_TH_ID,  a.ma_tb
                , a.ma_to, (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb,
                a.nhanvien_giao, ten_donvi_giao, a.MANV_HRM_TH, a.TEN_PBH_TH PHONG_TH, nv.ma_nv MANV_THPHUC, nv1.ten_pb PHONG_THPHUC
                , nv2.ma_nv MANV_GT, nv3.ma_nv MANV_THUNGAN,  a.thang_kt, a.DATCOC_CSD,  a.ma_gd,  a.thang_Bd_moi,  ptt.tien, ptt.vat, ptt.ngay_tt, a.NGAY_NGANHANG
                , ptt.SOSERI, ptt.SERI, KENHTHU, ten_nh ten_nganhang, ht_tra ten_ht_tra, tttb.trangthai_tb , a.thuebao_id, a.loaitb_id, db.khachhang_id, a.phieutt_id, ptt.ht_tra_id, ptt.kenhthu_id
                , case when a.phieutt_id is null then null
                            when ptt.ht_tra_id in (1,7) then 1
                            when ptt.ht_tra_id in (2,5,4,9) then 0 else null 
                            end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
                , a.THUEBAO_ID_KHAC, a.MATB_KHAC, rkm_id, hd.hdtb_id--, p.hdtb_id, hddc.rkm_id
from ttkdhcm_ktnv.ghtt_chotngay_271 a
            left join ttkd_bsc.nhanvien_202402 b on a.MANV_HRM = b.ma_nv -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien_202402 nv1 on nv.ma_nv = nv1.ma_nv -- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
            left join css_hcm.hd_thuebao hd on ptt.hdkh_id = hd.hdkh_id and nvl(a.thuebao_id_khac, a.thuebao_id) = hd.thuebao_id
            left join hddc on hd.hdtb_id = hddc.hdtb_id
where trunc(a.ngay_chot)=to_date('02/03/2024','dd/mm/yyyy')
and a.thang_kt in (202402,202403) and a.loaitb_id in (147,148)
select * from ttkd_bsc.nhanvien_202402 where ten_nv like '%Khuê'