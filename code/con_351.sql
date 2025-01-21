select* from css_Hcm.kieu_ld where loaihd_id = 8; 
select* from css.v_hd_thuebao@dataguard where kieuld_id in (select kieuld_id from css_Hcm.kieu_ld where loaihd_id = 8);
select* from css_hcm.bd_thuebao where hdtb_id in (select hdtb_id from x_xgspon);
select a.*, c.muccuoc muccuoc_cu, cuoc_tg cuoc_Cu,  
from x_xgspon a 
    left join css_hcm.bd_Thuebao b on a.hdtb_id = b.hdtb_id
    left join css_hcm.muccuoc_Tb c on b.mucuoctb_id = c.mucuoctb_id
    left join css_hcm.db_Thuebao d on a.thuebao_id = d.thuebao_id
    left join css_Hcm.muc;
    select khachhang_id, loaitb_id from css_hcm.db_Thuebao where ma_tb in ('hcmbaotram2022','baotram2018');
    create index idx_mytv on MYTV_HH (thuebao_id);
    create table x_mytv as 
    with tb as (select thuebao_id, khachhang_id, loaitb_id, ma_tb from css.v_db_thuebao@dataguard)
      select a.* , c.loaitb_id, c.ma_Tb acc
from MYTV_HH a
    join tb b on a.thuebao_id = b.thuebao_id 
    LEFT JOIN tb C ON B.KHACHHANG_ID  = C.KHACHHANG_ID AND  c.loaitb_id in (57,58,59,11);
;
create index idx_tmp_mytv on X_MYTV(thuebao_id);
create index idx_tmp_mytv_2 on X_MYTV(thuebao_id);
with pbh as (
    select a.thuebao_id ,d.ten_kieuld, dv.ten_dv pb_ptm, nv.ten_nv
    from X_MYTV a
     join css_hcm.hd_Thuebao b on a.thuebao_id = b.thuebao_id and b.tthd_id =6 
         join css_hcm.hd_khachhang c on b.hdkh_id = c.hdkh_id and c.loaihd_id = 1
         join css_hcm.kieu_ld d on b.kieuld_id = d.kieuld_id
        left join admin_hcm.nhanvien nv on c.ctv_id = nv.nhanvien_id
        left join admin_hcm.donvi dv on nv.donvi_id = dv.donvi_id
    group by a.thuebao_id ,d.ten_kieuld, dv.ten_dv , nv.ten_nv
)
, pcs as (
    select a.thuebao_id , pb.ten_pb pb_ql
    from X_MYTV a
     left join ttkd_bct.db_Thuebao_ttkd cs on a.thuebao_id = cs.thuebao_id
        left join (select distinct ma_pb, ten_pb from ttkd_Bsc.nhanvien where thang = 202412) pb on cs.mapb_Ql = pb.ma_pb
    group by a.thuebao_id , pb.ten_pb 
)
SELECT a.*, pbh.ten_kieuld, pbh.pb_ptm, pbh.ten_nv,pcs.pb_Ql
FROM X_MYTV a
        left join pbh on a.thuebao_id = pbh.thuebao_id
        left join pcs on a.thuebao_id = pcs.thuebao_id
        
        
;
select* from X_MYTV where ma_Tb = '';
select * from css_Hcm.hd_Thuebao where hdtb_id = 17378313;
select * from css_Hcm.hd_khachhang where hdkh_id = 15886173