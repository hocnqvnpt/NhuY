select tocdothuc from css_Hcm.tocdo_adsl
from aaarrrrghh a
alter table aaarrrrghh add (goicuoc_1 nvarchar2(202), tocdo_1 number, tien_1 number)
select* from homecb;
drop table ds
select* from hometv;
select GOI_TRATRUOC from
drop table kiennghimot;
create table kiennghiba as 
    select* from (
    with noi_ngoai as 
    (
        select distinct a.ma_Tb, a.noi_ngoai
        from noithanh_fiber_dadv a
    ),
    tocdogoi as (
        select c.tocdo_id, c.tocdothuc, a.goi_id
        from hometv a
        left join css_Hcm.goi_dadv_lhtb b on a.goi_id = b.goi_id
        left join css_hcm.tocdo_adsl c on b.tocdo_id = c.tocdo_id
        where c.loaitb_id in (58,59) and c.iptinh = 0
    )
    
    select a.THUEBAO_ID, a.MA_TB, a.CUOC_SAUKM, round(a.TIEN_TD/1.1), a.TOCDOTHUC, a.ACC_MYTV, a.DTHU_MYTV, a.THUEBAO_ID_MYTV, a.TEN_TB, 
    a.THANG_BDDC, a.THANG_KTDC, a.THANG_BD_MG, a.THANG_KT_MG, a.HUONG_DC, a.CUOC_SAUKM_MYTV, a.TEN_QUAN,b.noi_ngoai,
   round((a.tien_td/1.1)+ (nvl(dthu_mytv,0) + nvl(cuoc_saukm_mytv,0)) / 2) tien_thang, dmg.ma_goi, dmg.ten_goi, dmg.tien, c.tocdothuc tocdo_goi,
    row_number() over (partition by a.ma_tb order by dmg.tien desc ) rnk
    from ds a
        left join noi_ngoai b on a.ma_tb = b.ma_tb
        left join hometv dmg on b.noi_ngoai = dmg.noi_ngoai
        left join tocdogoi c on dmg.goi_id = c.goi_id
    where round((a.tien_td/1.1)+ (nvl(dthu_mytv,0) + nvl(cuoc_saukm_mytv,0)) / 2) >= round(dmg.tien) and a.tocdothuc <= c.tocdothuc 
    )  where rnk = 1;
commit;
create table kiennghi2 as 
    select* from (
    with fiber as 
    (
        select distinct a.ma_Tb, a.tocdothuc, a.tien_td
        from aaarrrrghh a
        where thang_kt_mg = 202403
    )
    
    select a.ma_Tb, a.tocdothuc,  round(a.tien_td/1.1) cu, b.noi_ngoai,dmg.tocdo_id, dmg.muccuoc, dmg.toc_do_thuc, round(dmg.goi_tratruoc) moi, row_number() over (partition by a.ma_tb order by dmg.goi_tratruoc   ) rnk
    from fiber a
        left join ghtt_t3_gg b on a.ma_tb = b.matb
        left join dmg on b.noi_ngoai = dmg.noi_ngoai
    where round(a.tien_td/1.1) <= round(dmg.goi_tratruoc) and a.tocdothuc <= dmg.toc_do_thuc  )  where rnk = 1;
    create table kinghi as
select b.thuebao_id, c.ma_tb, c.ten_tb, c.diachi_ld,c.tenphong,c.ma_Nv,c.cuoc_saukm, c.trangthaitb_id, c.ngay_sd, c.tratruoc_fiber, c.tien_td tien_vat , 
round(c.tien_td/1.1) tien_td_khong_vat, c.thang_bddc, c.thang_ktdc,c.thang_bD_mg, c.thang_kt_mg, c.huong_dc, c.tocdothuc, c.fiber,c.goi_tichhop, c.quan_id, 
c.acc_mytv, c.mytv, c.dthu_mytv,c.tratruoc_mytv,c.matb_mesh,c.sl_mesh, c.tong_dhu_mesh tong_dthu_mesh, c.ten_quan, c.khachhang_id, c.ma_toanha, c.ma_duan,
c.ttvt, c.iptinh,c.khdn,
a.noi_ngoai noi_ngoai_thanh, d.muccuoc goi_khuyennghi_1,d.tocdo_id tocdo_id_kiennghi_1, d.toc_do_thuc tocdo_khuyennghi_1, d.moi tien_khuyennghi_1, 
e.muccuoc goi_khuyennghi_2,e.tocdo_id tocdo_id_kiennghi_2, e.toc_do_thuc tocdo_khuyennghi_2,
e.moi tien_khuyennghi_2
from css_hcm.db_Thuebao b on a.maTb = b.ma_Tb and loaitb_id in (58,59)
    left join aaarrrrghh c on a.matb = c.ma_tb 
    left join kiennghi1 d on a.matb = d.ma_tb
    left join kiennghi2 e on a.matb = e.ma_tb
where  c.ma_Tb = 'minhphuc406'
;

select a.* , round(a.tien_td/1.1) tien_td_khong_vat,b.tien_thang, b.ma_goi ma_goi_kienghi_hometv, b.ten_goi ten_goi_kienghi_hometv, b.tien tien_kienghi_hometv , b.tocdo_goi tocdo_goi_kiennghi_hometv, 
c.ma_goi ma_goi_kienghi_homecb, c.ten_goi ten_goi_kienghi_homecb, c.tien tien_kienghi_homecb, c.tocdo_goi tocdo_goi_kiennghi_homecb
,d.ma_goi ma_goi_kienghi_3, d.ten_goi ten_goi_kienghi_3, d.tien tien_kienghi_3, d.tocdo_goi tocdo_goi_kiennghi_3

from ds a
    left join kiennghimot b on a.thuebao_id = b.thuebao_id
    left join kiennghihai c on a.thuebao_id = c.thuebao_id 
    left join kiennghiba d on a.thuebao_id = d.thuebao_id
select* from css_hcm.db_thuebao where ma_Tb = 'minhphuc406'
select * from  aaarrrrghh  where ma_Tb = 'minhphuc406'-- not in (select ma_tb from kinghi)