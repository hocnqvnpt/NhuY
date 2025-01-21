select* from Ttkdhcm_ktnv.ghtt_chotthang_271 where to_char(ngay_insert,'yyyymmdd')='20240806' ;and thang_kt between 202401 and 202406
;
insert into ttkd_Bsc.dm_ccos select* from dm_ccos where thang = 202407;
create table ct_Bsc_quy_giahan_cntt as select* from ttkd_bsc.ct_bsc_giahan_cntt where 1=2;
alter table ct_Bsc_quy_giahan_cntt add ngay_insert date;
commit;
insert into ct_Bsc_quy_giahan_cntt;
select distinct 202406 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
                , a.ma_to, (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb,
                a.nhanvien_giao, ten_donvi_giao, a.MANV_HRM_TH, a.TEN_PBH_TH PHONG_TH, null, null, nv.ma_nv MANV_THPHUC, nv1.ten_pb PHONG_THPHUC
                , nv2.ma_nv MANV_GT, nv3.ma_nv MANV_THUNGAN, null, null, a.thang_kt, a.DATCOC_CSD, null, a.ma_gd, null, a.thang_Bd_moi, null SO_THANGDC, null, ptt.tien, ptt.vat, ptt.ngay_tt, a.NGAY_NGANHANG
                , ptt.SOSERI, ptt.SERI, KENHTHU, ten_nh ten_nganhang, ht_tra ten_ht_tra, tttb.trangthai_tb , a.thuebao_id, a.loaitb_id, null, null, null, null, db.khachhang_id, a.phieutt_id, ptt.ht_tra_id, ptt.kenhthu_id
                , case when a.phieutt_id is null then null
                            when ptt.ht_tra_id in (1,7,204) then 1
                            when ptt.ht_tra_id in (2,5,4,9) then 0 else null 
                            end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
                , a.THUEBAO_ID_KHAC, a.MATB_KHAC, null KIEULD_ID, ngay_insert
from ttkdhcm_ktnv.ghtt_chotthang_271 a
            left join ttkd_bsc.nhanvien_202401 b on a.MANV_HRM = b.ma_nv -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien_202401 nv1 on nv.ma_nv = nv1.ma_nv -- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
where thang_kt = 202401 and to_char(ngay_insert,'yyyymmdd')='20240806' and (a.loaitb_id in  (55 ,80 ,116 ,117,132,140,154,181,318,288 ) ); 
                   or (a.loaitb_id in (288) and tocdo_id not in (17187) ) )
order by  ngay_insert;
select distinct thang_kt from ct_Bsc_quy_giahan_cntt;
select a.*, b.ghichu from ct_Bsc_quy_giahan_cntt  a
    join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
where tien_khop = 0 and exists (select 1 from ttkdhcm_ktnv.ghtt_chotthang_271 c where a.thuebao_id = c.thuebao_id and a.thang_ktdc_cu = c.thang_kt and c.tien_khop = 1)
;
update ct_Bsc_quy_giahan_cntt set tien_khop = 1 where tien_khop = 0 and thuebao_id in (
select thuebao_id from ct_Bsc_quy_giahan_cntt  a
    join css_hcm.phieutt_hd b on a.phieutt_id = b.phieutt_id
where tien_khop = 0 and exists (select 1 from ttkdhcm_ktnv.ghtt_chotthang_271 c where a.thuebao_id = c.thuebao_id and a.thang_ktdc_cu = c.thang_kt and c.tien_khop = 1)
and ghichu like '%BBBTCN%')
;
commit;
update ct_Bsc_quy_giahan_cntt a set TIEN_KHOP = 1

-- select phieutt_id from ct_Bsc_quy_giahan_cntt a
where thang = 202406 and tien_khop = 0 -- and thuebao_id in (select thuebao_id from xsmax)
    and ht_tra_id in (2,4,5)-- and a.phieutt_id = 7675246    
    -- and ma_tb in ('ghtk_binhtrong','ghtk_baucat','ghtk_bclythuongkiet')
    and  exists 
    (
        select 1--bb.phieu_id, aa.so_tien_ghico, aa.tienthoai_ghino, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        from ttkdhcm_ktnv.ds_chungtu_tratruoc aa
                        join ttkdhcm_ktnv.phieutt_hd_dongbo bb on aa.MA_CAPNHAT = bb.MA_CAPNHAT
        where  phieu_id = a.phieutt_id
        group by bb.phieu_id, TONGTIENCT_NHOM, TONGTIENHD_NHOM
        having TONGTIENCT_NHOM + sum(tienthoai_ghino) + sum(tien_nhapthem) >= TONGTIENHD_NHOM  -10
        );
commit;

            --1. phieu cha hoan tat
update ct_Bsc_quy_giahan_cntt a set tien_khop = 1
-- select* from ct_Bsc_quy_giahan_cntt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202406 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
        where   bb.hoantat = 1 and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- phieu con co tien bang
update ct_Bsc_quy_giahan_cntt a set tien_khop = 1
-- select* from ct_Bsc_quy_giahan_cntt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202406 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                        join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   aa.tien_sub_ct >=  (cc.tien+cc.vat) and aa.ma_gd = a.ma_gd 
--                                    group by aa.chungtu_id 
    );
-- bang cha du tien
update ct_Bsc_quy_giahan_cntt a set tien_khop = 1
-- select* from ct_Bsc_quy_giahan_cntt a
where  ht_tra_id in (2,4,5,207) and tien_khop = 0  and thang = 202406 --and rkm_id in (select rkm_id from tmp3_30ngay where lan = 22)
and exists 
    (
        select aa.ma_gd,bb.ma_Ct, bb.chungtu_id, bb.tien_ct,  bb.tien_tt, aa.hoantat, bb.hoantat, aa.tien_sub_ct
        from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  aa
                        join ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB bb on aa.chungtu_id = bb.chungtu_id
                         join css_hcm.phieutt_hd cc on aa.ma_gd = cc.ma_gd
        where   bb.tien_ct > bb.tien_tt and aa.ma_gd = a.ma_gd
--        and   aa.tien_sub_ct <  (cc.tien+cc.vat)--and aa.hoantat = 0
--                                    group by aa.chungtu_id 
    );
commit;
select* from ct_Bsc_quy_giahan_cntt where thuebao_id in (
select thuebao_id from ct_Bsc_quy_giahan_cntt where  loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ) group by thuebao_id having count (thuebao_id) > 1);
select * from (
select thang, ma_pb, phong_giao, thang_ktdc_cu
                         , count(thuebao_id) tong
                         , sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end) da_giahan
                         , round(sum(tien_dc_cu/1.1), 0) DTHU_DUYTRI
                         , sum(dthu) DTHU_thanhcong_dung_hen, round(sum(case when dthu > 0 and tien_khop > 0 then 1 else 0 end)*100/count(thuebao_id), 2) tyle
                from        (select thang,thang_ktdc_Cu, a.pbh_giao_id, a.phong_giao, a.manv_giao ma_nv, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu, sum(tien_thanhtoan) DTHU, max(ngay_tt) ngay_tt
                                             , decode(sum(a.tien_khop), 0, 0, null, 0, 1) tien_khop
                                         from ct_Bsc_quy_giahan_cntt a
                                          where thang = 202406 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 )
                                        group by thang,thang_ktdc_Cu, a.pbh_giao_id, a.phong_giao, a.manv_giao, a.ma_to, a.ma_pb, a.thuebao_id, a.ma_tb, tien_dc_cu
                               )
                group by thang, ma_pb, phong_giao, thang_ktdc_Cu
                order by 2
);
select* from ct_Bsc_quy_giahan_cntt;