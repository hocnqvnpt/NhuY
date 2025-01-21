select* from css.v_db_datcoc where ma_tb=  'phuoclong090220';
drop table mytv_hh;
create table mytv_hh as
select 
    a.thuebao_id, a.ma_tb, a.ngay_sd, e.thuonghieu, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb) cuoc_tg
            , decode(skm.tyle_tb, null, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb), 0, nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb) - skm.cuoc_tb, (100 - skm.tyle_tb) * nvl2(goi1.goi_id, goi1.tien_goi, f.cuoc_tb)/100) cuoc_saukm
              , nvl2(km.thuebao_id,1,0) tratruoc
            , nvl(ten_goi.ma_goi, 0) goi_tichhop, a.thuebao_cha_id, d.madoicap, d.tocdo_id, e.tocdothuc, e.iptinh, e.soluong_ip, d.muccuoc_id, d.trangbi_id, a.mucuoctb_id, a.doituong_id, g.khu_id khuvuc_ld
            , goi.nhomtb_id, tbi.tentrangbi
from css.v_db_thuebao@dataguard a
                join css.v_db_adsl@dataguard d on a.thuebao_id = d.thuebao_id
                join css.trangbi@dataguard tbi on d.trangbi_id = tbi.trangbi_id
                join css.tocdo_adsl@dataguard e on d.tocdo_id = e.tocdo_id
                join css.v_muccuoc_tb@dataguard f on a.mucuoctb_id = f.mucuoctb_id
                left join css.v_diachi_tb@dataguard i on a.thuebao_id = i.thuebao_id
                left join css.v_diachi@dataguard g on i.diachild_id = g.diachi_id
                left join css.v_bd_goi_dadv@dataguard goi on a.thuebao_id = goi.thuebao_id and goi.trangthai = 1 and goi.goi_id not in (15414) and goi.goi_id < 100000
                left join css.v_goi_dadv@dataguard ten_goi on goi.goi_id = ten_goi.goi_id
                left join css.v_goi_dadv_lhtb@dataguard goi1 on goi.goi_id = goi1.goi_id and d.tocdo_id = goi1.tocdo_id and (goi1.GIAMCUOC_TB =100 or goi1.GIAMCUOC_SD = 100) and goi1.muccuoc_id = 1
                                                                                                   -- and goi1.goi_id not in (select goi_id from css.v_goi_dadv where goi_neo_id is not null)
                left join (select distinct km.thuebao_id, km.loaitb_id
                                        from v_thongtinkm_all_ol km
                                        where --Thay doi thang
                                                    202412 between thang_bddc and least(thang_ktdc, nvl(thang_kt_dc, 999999), nvl(thang_huy, 999999))
                                    ) km on a.thuebao_id = km.thuebao_id and km.loaitb_id = 61
               left join (select thuebao_id, loaitb_id, nvl(tyle_sd, tyle_tb) tyle_tb, nvl(cuoc_sd, cuoc_tb) cuoc_tb, congvan_id from v_thongtinkm_all_ol
                                                where ((tyle_sd <> 100 and tyle_sd + cuoc_sd > 0) or (tyle_tb <> 100 and tyle_tb + cuoc_tb > 0)) and thang_bddc >= 202002
                                                        and 202412 between thang_bd_mg and least(thang_kt_mg, nvl(thang_kt_dc, 99999999), nvl(thang_huy, 99999999))
                                                ) skm on a.thuebao_id = skm.thuebao_id and skm.loaitb_id = 61
               -- left join (select thuebao_id, count(thang_tru_cuoi) times from v_thongtinkm_all where thang_tru_cuoi >= thang_bddc and cuoc_dc > 0 group by thuebao_id) tt on a.thuebao_id = tt.thuebao_id 
where a.trangthaitb_id not in (7, 8, 9) and a.loaitb_id = 61 and chuquan_id in (266, 145, 264) and to_number(to_char(a.ngay_sd, 'yyyymm')) <= 202412
;;
select* from css.tocdo_adsl@dataguard;

select  from tinhcuoc_hcm.dbtb ;
select* from css.v_hd_thuebao@dataguard where kieuld_id ;
select* from css_Hcm.kieu_ld;
    select a.* ,  case when not exists (select 1 from css.v_db_Thuebao@dataguard where khachhang_id = b.khachhang_id and loaitb_id in (58,59)) 
        then 0 else 1 end co_Fiber
from MYTV_HH a
    left join css.v_db_thuebao@dataguard b on a.thuebao_id = b.thuebao_id