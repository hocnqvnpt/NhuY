create table hocnq_ttkd.dulieu_kn_ont as
              
                       select db.ma_tb, a.thuebao_id, a.SDVT_ID, a.THIETBI_ID, a.NGAY_SD, a.TRANGBI_ID, a.SERIAL
                                    , b.KHO_ID, b.VATTU_ID
                                    , c.TEN_VT, c.LOAITBI_ID, c.NHOMVT_ID, rank() over (partition by db.thuebao_id order by a.SDVT_ID desc) rnk
                       from qlvt.v_sudung_vt@dataguard a
                                        join css.v_db_thuebao@dataguard db on a.thuebao_id = db.thuebao_id 
                                        left join qlvt.v_thietbi@dataguard b on a.thietbi_id = b.thietbi_id
                                        left join qlvt.v_vattu@dataguard c on b.vattu_id = c.vattu_id
                       where c.loaitbi_id = 2 and c.nhomvt_id = 1 and db.loaitb_id in (58, 59) and db.trangthaitb_id not in (7,8,9)
               ;
            select * from  qlvt.vattu@dataguard_vttp;
            select * from hocnq_ttkd.dulieu_kn_ont where rnk = 1;
            create index hocnq_ttkd.idx_ont_tbid on hocnq_ttkd.dulieu_kn_ont(thuebao_id);