  
  
   select 202308 thang, a.thuebao_id ,a.ma_Tb,a.rkm_id--, b.ma_tt, a.ma_Tb,c.ten_dvvt, lh.loaihinh_Tb
        , a.thang_bd, a.thang_kt,a.ngay_cn,
        a.ton_ck,a.ton_dk,a.cuoc_Dc,a.cuoc_tk
     from qltn.v_db_datcoc@dataguard a
     where a.phanvung_id=28 and a.kyhoadon = 20230801 