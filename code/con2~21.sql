select TEN_CHUDTU from css_hcm.DUAN where ma_duan = 'HM23006';
SELECT* FROM
select owner, table_name from all_tab_columns where column_name = 'TEN_CHUDTU' 
select * from css_hcm.chu_dtu;      

select* from css_hcm.khuvuc

kvttvt as
          select* from (select kv.KHUVUC_ID, kv.MA_KV, kv.TEN_KV khuvuc, il.ten_kv to_vt, ik.ten_kv ten_ttvt, kvpx.quan_id, kvpx.phuong_id--, ik.*
                                , row_number() over (partition by kvpx.quan_id, kvpx.phuong_id order by ik.khuvuc_id) rnk
                from css_hcm.khuvuc kv
                         left join css_hcm.KHUVUC_LKV kl on kl.loaikv_id = 4 and kv.khuvuc_id = kl.khuvuc_id
                         left join css_hcm.khuvuc il on kv.khuvuc_cha_id = il.khuvuc_id
                         left join css_hcm.khuvuc ik on il.khuvuc_cha_id = ik.khuvuc_id
                         left join css_hcm.khuvuc_px kvpx on kvpx.khuvuc_id = kv.khuvuc_id
                where ik.khuvuc_id is not null
                    ) where rnk = 1 --and quan_id = 	4004 and phuong_id = 54169;
                    
                    select* from css_hcm.khuvuc_dv