select* from vietanhvh.khkt_bc_hoahong ;where nguon ='ghtt_nhuy';
insert into  vietanhvh.khkt_bc_hoahong

with ttin as (
    select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
    from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1  
)
select 
    202407 , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'B?ng r?ng c? ??nh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
    nv.ten_nv, a.sothang_dc, a.DTHU, null, null, 202407,null, nv.nhomld_id, 'hoahong', tien_thuyetphuc*heso_Chuky*heso_dichvu, tien_xuathd*heso_Chuky*heso_dichvu,
    'ghtt_nhuy',4,c.loaitb_id
from ttkd_Bsc.ct_dongia_tratruoc a
    left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
    left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
    left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
    left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1  
    left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
    left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
    join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = 202407 and nv.donvi = 'TTKD'
where A.thang = 202407 and loai_tinh = 'DONGIATRA_OB'
;
commit;
SELECT* FROM TTKD_bSC.CT_DONGIA_tRATRUOC WHERE THANG = 202407 AND loai_tinh = 'DONGIATRA_OB' AND MA_PB IS NOT NULL; AND MA_tB = 'htvc_0000009562';
select* from css_hcm.db_Thuebao_sub