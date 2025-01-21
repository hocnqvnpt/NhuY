with tb as (
    select distinct thuebao_id, ma_Tb
    from tinhcuoc.v_dbtb@dataguard where ky_cuoc = 20230801 )
select a.THANG, a.THUEBAO_ID, RKM_ID, TRANGTHAI_TB, THANG_BD, THANG_KT, NGAY_CN, TON_CK, TON_DK, CUOC_DC, CUOC_TK ,tb.ma_Tb from tien_t8 a
    left join tb on a.thuebao_id = a.thuebao_id where rkm_id not in (select nvl(rkm_id,0) from thu_T8)
   ; 
   create table tmp_tttb as select distinct thuebao_id, trangthai_Tb from tttb;
   select a.*, b.trangthai_Tb from tien_pb_t8 a  left join tmp_tttb b on a.thuebao_id = b.thuebao_id
   where not exists (select * from thu_t8 where rkm_id = a.rkm_id) and nvl(ton_Ck ,0)!= 0;
create table tttb as select thuebao_id, trangthai_Tb from tinhcuoc.v_dbtb@dataguard a join css.trangthai_Tb@dataguard b on a.trangthaitb_id = b.trangthaitb_id
where ky_cuoc = 20230801;
select thuebao_id from tmp_tttb group by thuebao_id having count(trangthai_tb) >1