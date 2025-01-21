update ds_giahan_tratruoc_4 a
                    set tratruoc = 0
-- select * from ds_giahan_tratruoc a
where not exists (select congvan_id from ds_giahan_tratruoc_4 where congvan_id not in (190, 343, 483, 491, 545, 8922) and a.thuebao_id = thuebao_id)
;

commit;
select * from ttkd_bsc.ct_Bsc_tratruoc_moi_30day where thang = 202401
select thang, COUNT( DISTINCT THUEBAO_ID) FROM ds_giahan_tratruoc_4  group by thang

;
select ma_pb, phong_giao , count(distinct case when to_number(to_char(ngay_tt ,'yyyymm')) = 202401 and tien_khop = 1 then thuebao_id else null end) sltb_TT, count(distinct thuebao_id) sltb_Giao
from ttkd_bsc.ct_Bsc_tratruoc_moi_30day where thang in (202401,202402)
group by ma_pb, phong_giao;
select ma_pb, phong_giao, count(distinct case when TT_Thang1 = 1 then thuebao_id else null end) tong_tc, count(distinct thuebao_id) tong_Giao from (
select a.*, a.thang thang_kt,  case when to_number(to_char(ngay_tt ,'yyyymm')) = 202401 and tien_khop = 1 then 1 else 0 end TT_Thang1
from ttkd_bsc.ct_Bsc_tratruoc_moi_30day a 
where thang =202402
union all 
select a.*, ( a.thang) thang_kt, case when to_number(to_char(ngay_tt ,'yyyymm')) = 202401 and tien_khop = 1 then 1 else 0 end TT_Thang1
from ttkd_bsc.ct_Bsc_tratruoc_moi_30day a
    where thang = 202401
  ) group by    ma_pb, phong_giao
select * from qltn_hcm.hinhthuc_Tt ;
with fiber as
(
    select a.thuebao_id, a.ma_tb, SUBSTR(b.so_dt,2,9) sdt_Tt,SUBSTR(c.so_dt,2,9) sdt_kh
    from css_hcm.db_Thuebao a
        left join css.v_db_thanhtoan@dataguard b on a.thanhtoan_id = b.thanhtoan_id
        left join css_hcm.db_khachhang c on a.khachhang_id = c.khachhang_id 
    where a.loaitb_id in (58,59) and a.trangthaitb_id not in (7,8,9)
)
select a.*,  b.ma_tb, b.sdt_Tt, b.sdt_kh, d.ma_to_hrm, d.tento, e.ma_pb, e.ten_pb, nv.ma_Nv, nv.ten_Nv
from wl a 
    left join fiber b on a.whitelist = nvl(b.sdt_tt, b.sdt_kh)
    left join  ttkd_bct.db_thuebao_ttkd c on b.thuebao_id = c.thuebao_id 
    left join (select ma_to_hrm, tento, pbh_id , tbh_id from ttkd_bct.tobanhang where hieuluc  = 1) d on d.tbh_id = c.tbh_ql_id and c.pbh_ql_id = d.pbh_id 
    left join (select ma_pb, ten_pb,pbh_id from ttkd_bsc.dm_phongban pb where  pb.active = 1) e on e.pbh_id = c.pbh_ql_id
    left join ttkd_Bsc.nhanvien_202402 nv on c.ma_nv =nv.ma_nv
    order by b.thuebao_id;
select* from whitelist