update t4_khcn set lydo = 'Không thu?c ch? qu?n TTKD'
where ma_Tb in (select a.ma_tb from t4_khcn a join css_hcm.db_thuebao b on a.ma_Tb= b.ma_Tb and dichvuvt_id = 4
left join css_hcm.db_adsl c on b.thuebao_id = c.thuebao_id
where lydo is null and chuquan_id > 145) and lydo is null;
commit;
update t4_khcn set lydo = 'TB t?m ng?ng' where ma_tb in (
select a.* from ttkdhcm_ktnv.ghtt_giao_688  a
 join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
join css_hcm.db_thanhtoan c on b.thanhtoan_id = c.thanhtoan_id
join ttkd_bct.db_thuebao_ttkd d on a.thuebao_id = d.thuebao_id
where a.ma_tb in (select * from t4_khcn where lydo is null ) and a.trangthaitb_id = 6) and lydo is null;
update t4_khcn set  lydo = 'Thuê bao ch?a h?t tr? tr??c' where lydo in ('TB còn tr? tr??c','?ã Gia h?n')
update 
rollback;
select* from v_Thongtinkm_all where ma_Tb in ('nthongnhung888','phuhung789','hcmtrlb')--nthongnhung888')--,'mydung165')
update t4_khcn set lydo = 'TB còn tr? tr??c' where ma_Tb in (
select ma_Tb   from t4_khcn x where exists (select 1 from ds_giahan_Tratruoc2 a where  rkm_id in (select rkm_id from v_Thongtinkm_all where ttdc_id = 0 and hieuluc = 1)
and x.ma_tb = ma_tb and to_number(to_char(ngay_kt_mg,'yyyymm')) > 202404))
and lydo = '?ã Gia h?n'
select thang_kt,km,tratruoc,loaibo from ttkdhcm_Ktnv.ghtt_giao_688 where ma_Tb = 'hcm_ledo';
commit;
update t4_khcn set lydo = 'Thuê bao t?m d?ng ' where  ma_Tb = 'hcmtrlb';
select * from css_hcm.hd_Thuebao where 'hcm_kieutrang_13' = ma_tb;
select* from css_hcm.trangthai_Tb ;
select a.*, e.ten_pb phong_giao from t4_khcn a left join ttkdhcm_ktnv.ghtt_giao_688 b on a.ma_tb = b.ma_Tb and b.thang_kt = 202404 and km = 1 and tratruoc = 1 and loaibo = 0
left join  (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = b.donvi_giao

