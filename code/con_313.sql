select* from ttkd_bsc.ct_bsc_trasau_tp_tratruoc where thuebao_id =1730769;

select * from css_hcm.hd_Thuebao where ma_Tb = 'hcm_smartca_00004413';
select* from css_hcm.db_cntt where thuebao_id=11921496--  '54010101575f1b5179f6834f7f140964'--thuebao_id =12190256
select* from css_hcm.hd_thuebao where ma_Tb = 'hcm_ca_00094398'
;
create table thuebao_id as
select* from ttkd_bsc.ct_bsc_giahan_cntt where thang = 202403 and phieutt_id is not null-- and tien_khop is not null
and thuebao_id in (select thuebao_id from ttkdhcm_ktnv.ghtt_chotngay_271 a where   ngay_chot =to_date('20240402','yyyymmdd') 
     and  sl_da_thanhtoanht=1 and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 )  and thang_kt in (202403,202404)
     union all
     select thuebao_id from ttkdhcm_ktnv.ghtt_chotngay_271_luu_04022024 a where sl_da_thanhtoanht=1 and thang_kt=202404
     and    a.thuebao_id  in  (select b.thuebao_id from ghtt_chotngay_271 b  where ngay_chot =to_date('20240402','yyyymmdd') and loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 )  
     and  sl_da_thanhtoanht=0 ));
     create table ct_bsc_giahan_cntt as select* from ttkd_bsc.ct_bsc_giahan_cntt where 1=2
insert into ttkd_bsc.ct_bsc_giahan_cntt
 --and a.thuebao_id in (select x.thuebao_id from thuebao_id x)  -- a.loaitb_id in (147,148) --
--order by a.nhanvien_giao desc
union all
;commit;
delete from ct_Bsc_giahan_Cntt
insert into ttkd_bsc.ct_Bsc_giahan_Cntt 
select 202403 thang, 0 GH_ID, null PBH_QL_ID, a.DONVI_GIAO PBH_GIAO_ID, null, a.PBH_ID_TH PBH_TH_ID, null, a.ma_tb, null, null
                , a.ma_to, (select ma_pb from ttkd_bsc.dm_phongban pb where a.donvi_giao = pb.pbh_id and pb.active = 1) ma_pb,
                a.nhanvien_giao, ten_donvi_giao, a.MANV_HRM_TH, a.TEN_PBH_TH PHONG_TH, null, null, nv.ma_nv MANV_THPHUC, nv1.ten_pb PHONG_THPHUC
                , nv2.ma_nv MANV_GT, nv3.ma_nv MANV_THUNGAN, null, null, a.thang_kt, a.DATCOC_CSD, null, a.ma_gd, null, a.thang_Bd_moi, null SO_THANGDC, null, ptt.tien, ptt.vat, ptt.ngay_tt, a.NGAY_NGANHANG
                , ptt.SOSERI, ptt.SERI, KENHTHU, ten_nh ten_nganhang, ht_tra ten_ht_tra, tttb.trangthai_tb , a.thuebao_id, a.loaitb_id, null, null, null, null, db.khachhang_id, a.phieutt_id, ptt.ht_tra_id, ptt.kenhthu_id
                , case when a.phieutt_id is null then null
                            when ptt.ht_tra_id in (1,7) then 1
                            when ptt.ht_tra_id in (2,5,4,9) then 0 else null 
                            end tien_khop
                , (select listagg(MA_CAPNHAT, ', ') within group (order by PHIEU_ID) from ttkdhcm_ktnv.phieutt_hd_dongbo where PHIEU_ID = a.PHIEUTT_ID) ma_chungtu
                , a.THUEBAO_ID_KHAC, a.MATB_KHAC, null KIEULD_ID
from ttkdhcm_ktnv.ghtt_chotngay_271 a
            left join ttkd_bsc.nhanvien_202403 b on a.MANV_HRM = b.ma_nv -- CHANGE
            left join css_hcm.phieutt_hd ptt on a.phieutt_id = ptt.phieutt_id
            left join css_hcm.hd_khachhang hdkh on ptt.hdkh_id = hdkh.hdkh_id
            left join admin_hcm.nhanvien_onebss nv on hdkh.ctv_id = nv.nhanvien_id
            left join ttkd_bsc.nhanvien_202403 nv1 on nv.ma_nv = nv1.ma_nv -- CHANGE
            left join admin_hcm.nhanvien_onebss nv2 on hdkh.nhanviengt_id = nv2.nhanvien_id
            left join admin_hcm.nhanvien_onebss nv3 on ptt.thungan_tt_id = nv3.nhanvien_id
            left join css_hcm.kenhthu kt on ptt.kenhthu_id = kt.kenhthu_id
            left join css_hcm.nganhang nh on ptt.nganhang_id= nh.nganhang_id
            left join css_hcm.hinhthuc_tra ht on ptt.ht_tra_id= ht.ht_tra_id
            left join css_hcm.db_thuebao db on a.thuebao_id = db.thuebao_id
            left join css_hcm.trangthai_tb tttb on db.trangthaitb_id = tttb.trangthaitb_id
            
where  ngay_chot =to_date('20240402','yyyymmdd') 
     and  sl_da_thanhtoanht=1 and a.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 )  and thang_kt in (202403,202404) --and a.thuebao_id in (select x.thuebao_id from thuebao_id x) -- a.loaitb_id in (147,148) --
and a.thuebao_id in (Select x.thuebao_id from thuebao_id x) 
order by a.nhanvien_giao desc
;          
;         
commit;

insert into ttkd_bsc.ct_bsc_giahan_cntt 
select* from ct_bsc_giahan_Cntt a 
where exists (select 1 from ttkd_Bsc.ct_bsc_giahan_Cntt where thang = 202403 and phieutt_id is  null and a.thuebao_id = thuebao_id)
and a.phieutt_id is not null;
delete
select* from  ttkd_bsc.ct_bsc_giahan_cntt  where thang = 202403 and thuebao_id in (select x.thuebao_id from thuebao_id x) --and phieutt_id is null
select * from ct_bsc_giahan_Cntt where loaitb_id  in (55 ,80 ,116 ,117,132,140,154,181,288,318,147,148 ) --group by thuebao_id having count(thuebao_id) > 1
and thuebao_id not in (select thuebao_id from  ttkd_Bsc.ct_bsc_giahan_Cntt where thang = 202403 )