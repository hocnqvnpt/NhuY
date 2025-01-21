select* from ttkd_Bsc.nhuy_ct_bsc_ipcc_obghtt where thang = 202405 and ma_Tb = 'yummyf50';
select* from tmp3_ob where ma_tb ='hcm_mvilb1';
select* from ttkdhcm_ktnv.ghtt_giao_688 where ma_Tb in ('bachtuyet_h4sp','nguyennhahong_2018');
select* from csS_hcm.phieutt_hd where ma_gd = 'HCM-TT/02803568';
select* from css_hcm.ct_phieutt where phieutt_id =8445097;
select* from tmp3_ob where lan = 3;
select* from v_Thongtinkm_all where ma_Tb = 'vanan_hoang';--thuebao_id in (11762034,11762066);
select * from qltn_hcm.ct_no;
select* from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_SUB_ONEB  where ma_gd = 'HCM-TT/02718218';
select* from ttkdhcm_ktnv.DS_CHUNGTU_NGANHANG_ONEB where chungtu_id = 66085;
with a as (Select a.PHIEU_ID, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ngay_cn, a.nguoidung_id, a.chungtu, a.nganhang_id, a.ngaynganhang, c.httt_id, c.hinhthuc, sum(b.tragoc) TRAGOC, sum(b.trathue)trathue
					  From Qltn_hcm.bangphieutra partition for(20240501) A, Qltn_hcm.ct_tra partition for(20240501) B, Qltn_Hcm.Hinhthuc_Tt C
					  WHERE a.PHIEU_ID=B.PHIEU_ID and a.httt_id=c.httt_id(+)
										and c.httt_id in (125, 192, 200, 16, 6, 116, 171, 167, 170, 4) and to_number(to_char(a.ngay_cn,'yyyymmdd')) = 20240608
--					    and a.ma_tt = 'HCM009935696'
					  group by a.PHIEU_ID, a.ma_tt, b.ma_tb, b.dichvuvt_id, a.ngay_cn, a.nguoidung_id, a.chungtu, a.nganhang_id, a.ngaynganhang, c.hinhthuc, c.httt_id
					 -- having sum(b.tragoc) >0
					)
select a.ma_tt, a.ma_tb, 1 c, lh.loaihinh_tb, db.ten_tb, db.diachi_tb, a.PHIEU_ID, 1 d
				, 1 e, a.ngay_cn, 1 f, 1 g, TRAGOC, TRATHUE
				, nv1.ma_nv manv_gach, dv1.ten_dvql tendv_gach
				, a.HINHTHUC, 1 s, nh.ten_nh, a.ngaynganhang, a.chungtu, ct.ma_ct, 1 vv--, httt_id
from a
				left join css_hcm.nganhang nh on a.nganhang_id=nh.nganhang_id
				left join admin_hcm.nguoidung c on (c.trangthai=1 and a.nguoidung_id=c.nguoidung_id)
				left join admin_hcm.nhanvien_onebss nv1 on (c.nhanvien_id=nv1.nhanvien_id)
				left join admin_hcm.donvi dv1 on (nv1.donvi_id=dv1.donvi_id)
				left join css_hcm.db_thuebao db on (a.ma_tb= db.ma_tb and a.dichvuvt_id=db.dichvuvt_id)
				left join css_hcm.loaihinh_tb lh on (db.loaitb_id=lh.loaitb_id)
				left join ttkdhcm_ktnv.ds_chungtu_nganhang_oneb ct on (a.chungtu=to_char(ct.chungtu_id));