select a.*
from tien_t8 a
    left join css_hcm.db_Thuebao b on a.thuebao_id = b.thuebao_id
where not exists (select 1 from hethong where b.ma_Tb =ma_Tb) and ton_Ck > 1000;
    and  not exists (select 1 from v_thongtinkm_all where b.ma_Tb =ma_Tb and rkm_id != a.rkm_id);
l?ch: 8,742,422,831;
select* from v_Thongtinkm_all where rkm_id =5328208;
select* from css_hcm.db_thuebao where thuebao_id = 8629634;
select sum(ton_Ck) from tien_t8 where to_number(to_char(ngay_cn,'yyyymmdd')) = 20230901 ;and ton_ck > 1000; and nvl(cuoc_tk,0) >0;
630,614,415;

insert into  ttkd_bct.khkt_bc_hoahong

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'B?ng r?ng c? ??nh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong',-- case when ipcc = 0 then 0 else
        tien_thuyetphuc*heso_Chuky*heso_dichvu  tien_Thuyetphuc,
        null,
--        tien_xuathd*heso_Chuky*heso_dichvu,
        'ghtt_nhuy',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang in ( 202409) and loai_tinh = 'DONGIATRA_OB' and
   ( nv.ma_vtcv  not in ('VNP-HNHCM_KDOL_15') 
   and A.MA_PB not IN 
   ('VNP0701100','VNP0701200','VNP0701300','VNP0701400','VNP0701500','VNP0701600',
            'VNP0701800','VNP0702100','VNP0702200')
   )
    ;
    commit;
    select* from ttkd_bct.khkt_bc_hoahong where thang_ptm = 202407 and  nguon ='ghtt_nhuy';
    select* from ttkd_Bsc.nhanvien where thang = 202408; and nguon ='ghtt_nhuy'