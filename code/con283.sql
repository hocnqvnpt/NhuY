select a.*, b.ten_pb, b.ten_Nv, b.ten_To,  
        case when c.thuebao_id is not null then 202406
        when d.thuebao_id is not null or a.thuebao_id in (4488700,8261113,2757049) then 202407
        when e.thuebao_id is not null or a.thuebao_id in (12034648,9076496,12059710,12268389) then 202408
        when f.thuebao_id is not null then 202409 
        when a.thuebao_id in (10748292,9427231,11804404,12163687,8111016,9427226,9427229) then 202410
        else null end thang_kt

from ttkd_Bsc.ct_Dongia_tratruoc A

    join ttkd_Bsc.nhanvien b on a.thang = b.thang and a.ma_Nv = b.ma_Nv
    left join ttkdhcm_ktnv.ghtt_giao_688 c on a.thuebao_id = c.thuebao_id and c.thang_kt = 202406
    left join ttkdhcm_ktnv.ghtt_giao_688 d on a.thuebao_id = d.thuebao_id and d.thang_kt = 202407
    left join ttkdhcm_ktnv.ghtt_giao_688 e on a.thuebao_id = e.thuebao_id and e.thang_kt = 202408
    left join ttkdhcm_ktnv.ghtt_giao_688 f on a.thuebao_id = f.thuebao_id and f.thang_kt = 202409
where a.thang =202408 and loai_Tinh = 'DONGIATRA_OB' and ten_to = 'T? Bán Hàng Online' and tien_khop > 0;
select distinct ten_To from ttkd_Bsc.nhanvien where thang = 202408 and donvi ='TTKD';
select* from  ttkdhcm_ktnv.ghtt_giao_688 g where g.thang_kt = 202410;
select* from ttkd_bsc.nhanvien where thang = 202408 and ten_to in ('T? Bán Hàng Online','T? Kinh Doanh Online');