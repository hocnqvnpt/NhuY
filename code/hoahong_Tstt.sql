insert into  ttkd_bct.khkt_bc_hoahong
--select* from ttkd_Bsc.CT_BSC_tRASAU_TP_TRATRUOC a;
    with ttin as (
        select thang, a.thuebao_id,  a.ma_Gd,a.ngay_Tt , c.kieuld_id, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.CT_BSC_tRASAU_TP_TRATRUOC a 
            join css_hcm.hd_khachhang b on a.ma_gd = b.ma_gd
            join css_hcm.hd_Thuebao c on b.hdkh_id = c.hdkh_id and a.thuebao_id = c.thuebao_id--on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    where thang =202412
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'Bang roog c? dinh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong', DONGIA*TIEN_KHOP*heso_Chuky*heso_dichvu,NULL,
        'tstt',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
--        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on e.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang = 202412 and loai_tinh = 'DONGIA_TS_TP_TT'
    ;
    select* from   ttkd_bct.khkt_bc_hoahong;
    select a.* from  ttkd_Bsc.ct_dongia_tratruoc a
            join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD';
commit;
