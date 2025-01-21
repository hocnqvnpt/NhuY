select * from css_hcm.phieutt_hd where hdkh_id in (19139710,19177965);
truncate table ghtt_chotngay_271_temp
select* from css_hcm.ct_phieutt where phieutt_id in (7415212,7402163);
select * from css_hcm.db_datcoc where rkm_id = 6334558
select * from ttkdhcm_ktnv.ghtt_chotngay_271-- where mst = '0315478657'
create table ghtt_kq_thanhtoan_271_2803 as select* from ttkdhcm_ktnv.ghtt_kq_thanhtoan_271_2803;
desc ttkdhcm_ktnv.ghtt_kq_thanhtoan_271_2803 a ;
commit;

select * from ghtt_chotngay_271 where  trunc(ngay_insert)=to_date('29/03/2024','dd/mm/yyyy');

update ghtt_chotngay_271 a set TIEN_KHOP = 1
-- select * from nhuy.ct_bsc_tratruoc_moi_30day a
where tien_khop = 0
and ht_tra_id_hp in (2, 4)
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
select a.*
--a.thang_kt, a.ngay_kt, a.thuebao_id, a.dichvuvt_id, a.ten_dvvt, a.ma_Tb, a.ma_tt, a.ten_tb, a.ten_tt, a.diachi_tt,
--a.diachi_ld, a.email_kh, a.email_tb, a.loaitb_id, a.ten_loaihinh_Tb, a.datcoc_csd, a.sdt_lh, a.ms_thue, a.ten_pbh, a.ma_To, a.ten_to, a.ma_nv, a.ten_nv,a.ten_vtcv,
--a.donvi_giao, a.ten_donvi_giao, a.nhanvien_giao,nv.ten_Nv, a.ten_to_th,a.pbh_id_th,a.ten_pbh_Th,a.ma_Nv_th,a.ten_nv_th,a.sdt_nv_th, a.mail_nv_th,
--a.ten_nv_truongline tennv_totruong, a.sdt_nv_truongline sdt_totruong, a.mail_nv_truongline mail_totruong, a.khachhang_id, a.manv_tiepthi,a.ten_Nv_ptm, a.pbh_id_tiepthi,
--a.ten_pbh_ptm, a.ma_nv_cn,a.ten_nv_cn,ngaycn_kq, ghichu,sothang_gh,sotien_gh,ten_kq, 

from ghtt_chotngay_271 a 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = a.donvi_giao
    left join ttkd_Bsc.nhanvien_202402 nv on a.manv_tiepthi = nv.ma_nv
--    left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id 
--    left join css_hcm.db_Thanhtoan tt on b.thanhtoan_id = tt.thanhtoan_id 
where trunc(ngay_insert)=to_date('29/03/2024','dd/mm/yyyy')
and thang_kt between 202310 and 202404 and a.loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 );




update ghtt_chotngay_271 a
set tien_khop = 1
where ht_Tra_id_hp in  (1,7) and  trunc(ngay_insert)=to_date('29/03/2024','dd/mm/yyyy');
drop table ghtt_kq_thanhtoan_271_2803;
select
   insert into ghtt_chotngay_271 a
                (a.thang_kt,a.ngay_kt, a.thuebao_id , a.domain,a.dichvuvt_id ,a.ten_dvvt, a.ten_ld
                 ,a.ma_tb, a.ma_tt,a.ten_tb, a.ten_tt,a.diachi_tt,a.diachi_ld,a.email_kh,a.email_tb
                 ,a.loaitb_id, a.ten_loaihinh_tb , a.datcoc_csd, a.sdt_lh,a.email,a.ms_thue
                 ,a.ten_pbh,a.ma_to, a.ten_to, a.manv_hrm, a.ten_nv,a.ten_vtcv
                 , a.donvi_giao,a.ten_donvi_giao,a.nhanvien_giao,a.ten_nv_giao ,a.ten_to_th
                 ,a.pbh_id_th, a.ten_pbh_th, a.manv_hrm_th, a.ten_nv_th,a.sdt_nv_th,a.mail_nv_th
                 ,a.ten_nv_truongline,a.sdt_nv_truongline,a.mail_nv_truongline
                 ,a.khachhang_id,a.manv_tiepthi ,a.ten_nv_ptm ,a.pbh_id_tiepthi, a.ten_pbh_ptm
                 ,a.manv_hrm_cn ,a.ten_nv_cn ,a.ngaycn_kq,a.ghichu,a.sothang_gh,a.sotien_gh,a.ten_kq,a.kq_id,a.ld_id
                 ,a.trangthai_hp,a.thang_bd_moi,a.thang_kt_moi,a.tongtien_hp,a.ngay_tt_hp
                 ,a.ngay_kt_moi ,a.seri,a.soseri
                 ,a.thuebao_id_khac,a.matb_khac,a.mst,a.mst_khac,a.tien_khop,a.tongtien_chungtu,a.tongtien_hoadon
                 ,a.hoantat, a.ma_capnhat, a.ngaycn_macapnhat
                 , a.kq_ngay_ht_id44, a.ngay_nganhang ,  a.ngay_update_ht
                 ,a.bs_file,a.ht_tra_id_hp,a.kenhthu_id,a.trangthai_thutien,a.ma_gd,a.phieutt_id,a.ptm
                 ,a.sl_da_thanhtoan,a.sl_da_thanhtoanht,a.daily,a.giahan,a.tt_1lan,a.ngay_insert,a.user_insert )

          select   distinct  a.thang_kt,a.ngay_kt, a.thuebao_id , a.domain,a.dichvuvt_id ,a.ten_dvvt, a.ten_ld
                 ,a.ma_tb, a.ma_tt,a.ten_tb, a.ten_tt,a.diachi_tt,a.diachi_ld,a.email_kh,a.email_tb
                 ,a.loaitb_id, a.ten_loaihinh_tb , a.datcoc_csd, a.sdt_lh,a.email,a.ms_thue
                 ,a.ten_pbh,a.ma_to, a.ten_to, a.manv_hrm, a.ten_nv,a.ten_vtcv
                 , a.donvi_giao,a.ten_donvi_giao,a.nhanvien_giao,a.ten_nv_giao ,a.ten_to_th
                 ,a.pbh_id_th, a.ten_pbh_th, a.manv_hrm_th, a.ten_nv_th,a.sdt_nv_th,a.mail_nv_th
                 ,a.ten_nv_truongline,a.sdt_nv_truongline,a.mail_nv_truongline
                 ,a.khachhang_id,a.manv_tiepthi ,a.ten_nv_ptm ,a.pbh_id_tiepthi, a.ten_pbh_ptm
                 ,a.manv_hrm_cn ,a.ten_nv_cn ,a.ngaycn_kq,a.ghichu,a.sothang_gh,a.sotien_gh,a.ten_kq,a.kq_id,a.ld_id
                 ,a.trangthai_hp,a.thang_bd_moi,a.thang_kt_moi,a.tongtien_hp,a.ngay_tt_hp
                 ,a.ngay_kt_moi ,a.seri,a.soseri
                 ,a.thuebao_id_khac,a.matb_khac,a.mst,a.mst_khac,a.tien_khop,a.tongtien_chungtu,a.tongtien_hoadon
                 ,a.hoantat, a.ma_capnhat, a.ngaycn_macapnhat
                 , a.kq_ngay_ht_id44, a.ngay_nganhang ,  a.ngay_update_ht
                 ,a.bs_file,a.ht_tra_id_hp,a.kenhthu_id,a.trangthai_thutien,a.ma_gd,a.phieutt_id,a.ptm
                 ,a.sl_da_thanhtoan,a.sl_da_thanhtoanht,a.daily,a.giahan,a.tt_1lan
                 , sysdate ngay_insert,1016 user_insert
        from ghtt_chotngay_271_temp a;
 commit;
select* from ghtt_chotngay_271 where trunc(ngay_chot)=to_date('29/03/2024','dd/mm/yyyy')  
loaitb_id in (55 ,80 ,116 ,117,132,140,154,181,288,318 )