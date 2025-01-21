with tttb as (
    select DISTINCT THUEBAO_ID, trangthai_Tb, A.TRANGTHAITB_ID
    from tinhcuoc_hcm.dbtb partition for (20240301) a
        left join css_hcm.trangthai_Tb b on a.trangthaitb_id = b.trangthaitb_id

)
SElect a.thang, A.THUEBAO_ID, a.rkm_id, a.ma_tt,a.MA_TB, a.TEN_DVVT, a.LOAIHINH_TB,  a.THANG_BD, a.THANG_KT, a.TON_CK, a.CUOC_TK, a.NGAY_KTDC, b.trangthaitb_id, B.trangthai_Tb 
from PB a
    left join TTTB b on a.THUEBAO_ID= b.THUEBAO_ID  
where a.thang = 202403;
select ma_Nv from admin_hcm.nhanvien_onebss where nhanvien_id = 451341;
select thungan_Tt_id from css_hcm.phieutt_hd where ma_gd in ('HCM-TT/02870744','HCM-TT/02923891','HCM-TT/02924118','HCM-TT/02920743','HCM-TT/02882424','HCM-TT/02874289',
'HCM-TT/02918460');
select* from ttkd_Bsc.ct_bsc_giahan_cntt where thang = 202407 and ma_Tb in ('hcm_ca_00102686');
select* from ttkdhcm_ktnv.ds_Chungtu_Nganhang_sub_oneb where ma_gd in ( '00913685',
'00913732',
'00913649',
'00913656',
'00913747',
'00913710',
'00912011',
'00913757',
'00913692',
'00913706');
select distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt,b.ma_gd,b.ma_Tb, b.tra_truoc
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.ct_bsc_chungtu b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 d
where a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=2  
and b.ma_gd=d.ma
and c.hoantat=1
and b.tra_truoc=1
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_1 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null 
and a.fcheck=0
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_gd=b.ma_gd) and b.thang = 202408 
union all
select distinct c.ma_ct,c.nd_ct,c.nhandien_thanhtoan ID600_nhandien,b.ma_tt,b.ma_gd,b.ma_Tb, b.tra_truoc
from ttkdhcm_ktnv.ds_chungtu_nganhang_xl_noidung_b1 a,nhuy.ct_bsc_chungtu b,ttkdhcm_ktnv.ds_chungtu_nganhang_oneb c,ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 d
where a.chungtu_id=c.chungtu_id
and c.ma_ct = b.ma_chungtu
and c.chungtu_id = d.chungtu_id
and a.loai_db_id=3  
and b.ma_tt=d.ma
and c.hoantat=1
and b.tra_truoc=0
and a.nhanvien_id is null 
and a.chungtu_id in (select chungtu_id from ttkdhcm_ktnv.ds_chungtu_nganhang_phieutt_hd_0 group by chungtu_id having count(*) =1)
and c.nhandien_thanhtoan is not null --1574 989
and a.fcheck=0
and EXISTS (select 1 from ttkdhcm_ktnv.ds_chungtu_nganhang_sub_oneb where ma_tt=b.ma_tt) and b.thang = 202408