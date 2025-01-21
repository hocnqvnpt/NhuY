insert into  ttkd_bct.khkt_bc_hoahong

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'Bang rong co dinh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong',null, tien_xuathd*heso_Chuky*heso_dichvu,
        'ghtt_nhuy_ctu',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.nhanvien_xuathd =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang IN ( 202412) and loai_tinh = 'DONGIATRA_OB' and nv.ma_pb != 'VNP0700600'
    ;
    COMMIT;
insert into  ttkd_bct.khkt_bc_hoahong

    with ttin as (
        select thang, thuebao_id, hdtb_id, ma_Gd,ngay_Tt, row_number() over (partition by a.thuebao_id, a.thang order by tien_thanhtoan desc ) rnk
        from ttkd_Bsc.nhuy_Ct_Bsc_ipcc_obghtt a --on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rownum = 1
    )
    select
        a.thang , e.ma_gd, b.ma_kh,a.thuebao_id, a.ma_tb, 'Bang rong co dinh', k.ten_kieuld, c.ten_tb, d.diachi_ld,e.ngay_tt, nv.ma_pb, nv.ten_pb, nv.ma_to, nv.ten_to, nv.ma_nv,
        nv.ten_nv, a.sothang_dc, a.DTHU, null, null, a.thang,null, nv.nhomld_id, 'hoahong', tien_thuyetphuc*heso_Chuky*heso_dichvu,null,
        'ghtt_nhuy',4,c.loaitb_id
    from ttkd_Bsc.ct_dongia_tratruoc a
        left join css_hcm.db_thuebao c on a.thuebao_id = C.thuebao_id
        left join css_hcm.db_khachhang b on c.khachhang_id =b.khachhang_Id
        left join css_hcm.db_Thuebao_sub d on a.thuebao_id =d.thuebao_id
        left join ttin e on a.thuebao_id = e.thuebao_id and A.THANG = E.THANG AND rnk = 1
        left join css_hcm.hd_thuebao hd on e.hdtb_id = hd.hdtb_id
        left join css_hcm.kieu_ld k on hd.kieuld_id = k.kieuld_id
        join ttkd_Bsc.nhanvien nv on a.ma_Nv =nv.ma_nv and nv.thang = a.thang and nv.donvi = 'TTKD'
    where A.thang IN (202412) and loai_tinh = 'DONGIATRA_OB' and nv.ma_pb != 'VNP0700600'
    ;
    select* from  ttkd_bct.khkt_bc_hoahong where thang_ptm = 202412  and nguon = 'ghtt_nhuy_ctu';
    rollback;
    commit;
    select* from ttkd_Bsc.ct_dongia_tratruoc a where A.thang IN (202412) and loai_tinh = 'DONGIATRA_OB'
        