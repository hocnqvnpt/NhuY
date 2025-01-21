select * from qltn_hcm.ct_tra partition for(20240101)
select distinct httt_id from qltn_hcm.bangphieutra  partition for(20240101)-- —> 1 phi?u tr? dành cho bao nhiêu thuê bao
select* from qltn_hcm.nhom_httt where nhom_httt_id in (
select distinct nhom_httt_id from qltn_hcm.hinhthuc_tt where httt_id in (select distinct httt_id from qltn_hcm.bangphieutra  partition for(20240101))
)       
    

select thuebao_id from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271_2803 where thang_kt between 202310 and 202424 and ngay_tt_hp is not null and phieutt_id is not null group by thuebao_id having count(distinct phieutt_id) > 1
;
create table ca_ghtt as 
select distinct THANG_KT, TRATRUOC, MA_TB, THANG_BDDC_HP, THANG_KTDC_HP, NGAY_TT_HP, SOSERI, SERI, TRANGTHAI_HP,
TONGTIEN_HP, HT_TRA_ID_HP, HT_TRA_HP, RKM_ID, CHITIETKM_ID, NGAY_UPDATE, KENHTHU, TEN_HT_TRA, THUNGAN_HD_ID, THUNGAN_TT_ID, NGAY_HD, 
SO_THANGDC, AVG_THANG, MA_GD, HDTB_ID, HDKH_ID, PHIEUTT_ID, HOANTAT, MA_CAPNHAT, NGAYCN_MACAPNHAT, NGAY_UPDATE_HT, MA_CHUNGTU_GOP, TONGTIEN_CHUNGTU,
TIEN_KHOP, TONGTIEN_HOADON, KQ_NGAY_HT_ID44, NGAY_NGANHANG, THUEBAO_ID, KENHTHU_ID, THUEBAO_ID_KHAC, MATB_KHAC, MST, MST_KHAC, MA_TT, TRANGTHAI_THUTIEN, 
BS_FILE, LOAITB_ID, NGAYKT_MOI_ONE, TTHD_ID, NGAY_HT_HD, NGAYKT_CU, GIAHAN, CHOT, NGAY_CHOT, DOMAIN, KIEULD_ID, GHINHAN, KHACHHANG_ID, SERIAL_MATB_CU, 
SERIAL_MATB_KHAC, SERIAL_BAN
from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271_2803 a 
where thang_kt between 202310 and 202424  and  ngay_tt_hp is not null and a.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 ); --and thuebao_id in(
8670884);
;
PROCEDURE nhuy.PRO_GHTT_BSC_271
drop table ca_ghtt
select  ma_capnhat from  ttkdhcm_ktnv.phieutt_hd_dongbo where phieu_id  = 8205539
select phieutt_id from ca_ghtt where ht_tra_id_hp = 2 and  ma_capnhat is null;
rollback;
update ca_ghtt a set ma_capnhat = (select  distinct ma_capnhat from  ttkdhcm_ktnv.phieutt_hd_dongbo where phieu_id = a.phieutt_id)
where ma_capnhat is null and ht_Tra_id_hp = 2;
commit;
update ca_ghtt set tien_khop = 0 where ht_Tra_id_hp = 2;
commit;
update ca_ghtt a set TIEN_KHOP = 1
-- select * from nhuy.ct_bsc_tratruoc_moi_30day a
where tien_khop = 0
and ht_tra_id_hp in (2)
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
with nvcs as (
select b.thuebao_id , d.tento, d.ma_to_hrm, e.ma_pb, e.ten_pb
from ttkd_bct.db_thuebao_ttkd b 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = b.tbh_ql_id and b.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.pbh_ql_id
)
select a.thuebao_id,a.thang_kt, a.ngaykt_cu, a.thuebao_id, b.dichvuvt_id, c.ten_dvvt, a.ma_tb, a.ma_tt, b.ten_tb,tt.ten_tt,tt.diachi_tt,b.diachi_ld,kh.email email_kh,
b.email email_Tb, b.loaitb_id, lh.loaihinh_Tb,a.tongtien_hp datcoc_csd, kh.so_dt sdt_lh, a.mst, nvcs.tento, nvcs.ma_to_hrm, nvcs.ma_pb, nvcs.ten_pb

from ca_ghtt a
    left join css_hcm.db_thuebao b on a.thuebao_id = b.thuebao_id 
    left join css_hcm.dichvu_vt c on b.dichvuvt_id = c.dichvuvt_id
    left join css_hcm.db_thanhtoan tt on b.thanhtoan_id = tt.thanhtoan_id 
    left join css_hcm.db_khachhang kh on b.khachhang_id = kh.khachhang_id
    left join css_hcm.loaihinh_Tb lh on a.loaitb_id = lh.loaitb_id
    left join nvcs on a.thuebao_id = nvcs.thuebao_id
    left join ttkd_bsc.nhanvien_202402 nv on nvcs.ma_nv = nv.ma_nv 