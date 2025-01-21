select a.ma_tb, a.thuebao_id, b.so_dt sdt_kh, c.so_dt sdt_tt
from fiber_hh_t5 a
    left join css_hcm.db_khachhang b on a.khachhang_id =b.khachhang_id
    left join css_hcm.db_Thuebao d on a.thuebao_id = d.thuebao_id
    left join css_hcm.db_thanhtoan c on d.thanhtoan_id = c.thanhtoan_id
;

select* from ttkd_Bsc.ct_Dongia_Tratruoc a where thang >= 202404 and loai_tinh !='DONGIATRA30D' and ma_Tb in ('baolong1993',
'hcmbaolong1993',
'quyetthang032019',
'vtnh5696130',
'hcm_vtnh5696130',
'btrung0906',
'fibervnn-1331572',
'chhung021019',
'lethuan72',
'hai2023',
'ntlang_189q',
'qvu11072016',
'ngoctuantk84',
'vnguyen1990.hcm',
'hcmvnguyen-app',
'tranlinh1.ww',
'ngtnthi',
'hcmcaothien262tv',
'vobuicaothien262',
'mesh0127418',
'mesh0127419',
'kimhoa301222',
'hcm_kimhoa301222',
'ntbp925',
'mesh0191778',
'quyen15061993',
'hcmphduy2023',
'phduy2023',
'hcmdpttinh79',
'nhatro01-vvh') and exists (select thuebao_id from ttkd_Bsc.ct_Dongia_tratruoc  where thang >= 202404 and loai_tinh !='DONGIATRA30D' and ma_nv=  a.ma_nv and thuebao_id = a.thuebao_id);
select* from ttkd_Bsc.nhanvien_202404 where ma_Nv = 'VNP017254';
select* from ttkd_bsc.ct_Bsc_giahan_cntt where thang = 202405 and ma_Tb=  'hcm_ca_ivan_00018744';
select* from ttkdhcm_ktnv.ghtt_chotngay_271 where ma_tb = 'hcm_tmvn_00004554';