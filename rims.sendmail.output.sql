/*******************************************************************************************************************************************************
 �����:                     ���������� �.�.
 ����:                      ��������� ������� � Q9PB ��� ���������� �������� ������
 ��������:                  ����� �.�.
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������ ����������:         RIMS             
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������� ��� ��������:      --
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ������������� ����������:  ��������� MSB.System_Monitoring
--------------------------------------------------------------------------------------------------------------------------------------------------------
 ����������:                1. ���� ����������    - 02.05.2019
                            2. �������� ��������� - https://doc.pb.ua/#/doc=6900723&year=2018&folder=ANOTHER_UNDONE (����� 2.3)
*******************************************************************************************************************************************************/
---------------------------------------------------------------------------
-- ���� ������
---------------------------------------------------------------------------
-- ������ ���� ��������
select 'L2F'        as "LIM_TYPE", 
       count()      as "REP_CNT", 
       min(bq9_amt) as "MIN_LIM", 
       max(bq9_amt) as "MAX_LIM"
  into #min_limits
  from msb.tmpq9pb
 where bq9_amt > 0
   and bq9_amt < 10000
   and bqt_qt = 'L2F'
 union all
select 'L3F' as "LIM_TYPE", count(), min(bq9_amt), max(bq9_amt)
  from msb.tmpq9pb
 where bq9_amt > 0
   and bq9_amt < 10000
   and bqt_qt = 'L3F'
 union all
select 'GLP' as "LIM_TYPE", count(), min(bq9_amt), max(bq9_amt)
  from msb.tmpq9pb
 where bq9_amt > 0
   and bq9_amt < 10000
   and bqt_qt = 'GLP'
 union all
select 'LOF' as "LIM_TYPE", count(), min(bq9_amt), max(bq9_amt)
  from msb.tmpq9pb
 where bq9_amt > 0
   and bq9_amt < 50000
   and bqt_qt = 'LOF'
 union all
select 'GUL' as "LIM_TYPE", count(), min(bq9_amt), max(bq9_amt)
  from msb.tmpq9pb
 where bq9_amt > 0
   and bq9_amt < 50000
   and bqt_qt = 'GUL'
;
commit;
---------------------------------------------------------------------------
-- 2 �������� ������ (�������)
select 'GLP' as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15)))) as "QTY"
  into #temp_2_in_row
  from tmp.JOURNAL_GLP
 union all
select 'GUL' as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15))))
  from tmp.JOURNAL_GUL
 union all
select 'L2F' as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15))))
  from tmp.JOURNAL_L2F
 union all
select 'L3F' as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15))))
  from tmp.JOURNAL_L3F
 union all
select 'LOF' as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15))))
  from tmp.JOURNAL_LOF
;
commit;
---------------------------------------------------------------------------
-- ������ ��� ��������
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(cast(count() as int) as char(15))))                as "QTY", 
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15))))           as "SUM_LIM", 
       TMP.setMillionDelim(trim(cast(cast(round(avg(bq9_amt),-3) as int) as char(15)))) as "AVG_LIM", 
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt)  as int)  as char(15))))         as "MAX_LIM"
  into #temp_lim_score
  from tmp.LIMIT_WITHOUT_SCORE
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- ���������� �������� �� �����
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
                else null
	   end                                                                                            as "RATE_SUCCESS"
  into #temp_SUCCESS_RATE
  from tmp.GLP_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
             else null
	   end                                                                                            as "RATE_SUCCESS"
  from tmp.GUL_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
             else null
	   end                                                                                            as "RATE_SUCCESS"
  from tmp.L2F_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
             else null
	   end                                                                                            as "RATE_SUCCESS"
  from tmp.L3F_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
             else null
	   end                                                                                            as "RATE_SUCCESS"
  from tmp.LOF_SUCCESS_RATE
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- ������� � ������� � Q9PB, ������� ��� � �������

select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))      as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  into #temp_lim_no_calc
  from tmp.GLP_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))      as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.GUL_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))      as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))      as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L3F_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))      as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.LOF_NO_CALC
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- ����� ������� �� ��������� � ��������
select 'GLP'                                                                                                                              as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then bq9_amt - Lim_4_q9pb else 0 end) as int) as char(15)))) as "SUM_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb > bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_DOWN",
       TMP.setMillionDelim(trim(cast(sum(case when Lim_4_q9pb > bq9_amt then Lim_4_q9pb - bq9_amt else 0 end) as char(15))))              as "SUM_DOWN",
       TMP.setMillionDelim(trim(cast(cast(max(abs(Lim_4_q9pb - bq9_amt)) as int) as char(15))))                                           as "MAX_DELTA"
  into #temp_lim_journal
  from tmp.GLP_MONITORING
 union all
select 'GUL'                                                                                                                              as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15)))), 
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then bq9_amt - Lim_4_q9pb else 0 end) as int) as char(15)))) as "SUM_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb > bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_DOWN",
       TMP.setMillionDelim(trim(cast(sum(case when Lim_4_q9pb > bq9_amt then Lim_4_q9pb - bq9_amt else 0 end) as char(15))))              as "SUM_DOWN",
       TMP.setMillionDelim(trim(cast(cast(max(abs(Lim_4_q9pb - bq9_amt)) as int) as char(15))))                                           as "MAX_DELTA"
  from tmp.GUL_MONITORING
 union all
select 'L2F'                                                                                                                              as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then bq9_amt - Lim_4_q9pb else 0 end) as int) as char(15)))) as "SUM_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb > bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_DOWN",
       TMP.setMillionDelim(trim(cast(sum(case when Lim_4_q9pb > bq9_amt then Lim_4_q9pb - bq9_amt else 0 end) as char(15))))              as "SUM_DOWN",
       TMP.setMillionDelim(trim(cast(cast(max(abs(Lim_4_q9pb - bq9_amt)) as int) as char(15))))                                           as "MAX_DELTA"
  from tmp.L2F_MONITORING
 union all
select 'L3F'                                                                                                                              as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then bq9_amt - Lim_4_q9pb else 0 end) as int) as char(15)))) as "SUM_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb > bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_DOWN",
       TMP.setMillionDelim(trim(cast(sum(case when Lim_4_q9pb > bq9_amt then Lim_4_q9pb - bq9_amt else 0 end) as char(15))))              as "SUM_DOWN",
       TMP.setMillionDelim(trim(cast(cast(max(abs(Lim_4_q9pb - bq9_amt)) as int) as char(15))))                                           as "MAX_DELTA"
  from tmp.L3F_MONITORING
 union all
select 'LOF'                                                                                                                              as "LIM_TYPE",
       TMP.setMillionDelim(trim(cast(count() as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb < bq9_amt then bq9_amt - Lim_4_q9pb else 0 end) as int) as char(15)))) as "SUM_UP",
       TMP.setMillionDelim(trim(cast(cast(sum(case when Lim_4_q9pb > bq9_amt then 1 else 0 end) as int) as char(15))))                    as "QTY_DOWN",
       TMP.setMillionDelim(trim(cast(sum(case when Lim_4_q9pb > bq9_amt then Lim_4_q9pb - bq9_amt else 0 end) as char(15))))              as "SUM_DOWN",
       TMP.setMillionDelim(trim(cast(cast(max(abs(Lim_4_q9pb - bq9_amt)) as int) as char(15))))                                           as "MAX_DELTA"
  from tmp.LOF_MONITORING
;
commit;
---------------------------------------------------------------------------
-- ������ � �������� �����������
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_AMT",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_AMT"
  into #temp_red_light
  from tmp.L3F_TRAFFIC_LIGHT
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15))))
  from tmp.L2F_TRAFFIC_LIGHT
 group by bqt_qt
;
commit;

-- ������ � �������� ����������� (�����������)
select 'SVET_271' as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  into #temp_red_light_detail
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_271 = 'N'
 union all 
select 'SVET_272' as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))),
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15))))
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_272 = 'N'
 union all 
select 'SVET_327' as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_327 = 'N'
 union all 
select 'SVET_266' as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_266 = 'N'
;
commit;
---------------------------------------------------------------------------
-- ������ � ������� � Q9PB ��� �������� ������
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY", 
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM", 
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  into #temp_NO_DEAL
  from tmp.MONITORING_DEAL
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- ������ ������
---------------------------------------------------------------------------
-- ������� � ��������� ��� ��������
select trim(cast('konstantin.bondarenko.01@privatbank.ua, Vadim.Vjazov@privatbank.ua, Irina.Ivanova.10@privatbank.ua, Jana.Glushnjak@privatbank.ua' as char(1000))) as "Email"    -- 
  into #Addr
;
commit;

-- ��������� ������, ���� ������� ������� 1 (2 �������� ������)
select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || QTY || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA"
  into #REP_DATA
  from #temp_2_in_row
;
commit;

-- ��������� ������, ���� ������� ������� 2 (����� ��� ��������)
select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || AVG_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA2"
  into #REP_DATA2
  from #temp_lim_score
;
commit;

-- ��������� ������, ���� ������� ������� 3 (���������� �������� �� �����)
select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY_ALL || '</td>' || '<td>' || QTY_SUCCESS || '</td>' || '<td>' || RATE_SUCCESS || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA3"
  into #REP_DATA3
  from #temp_SUCCESS_RATE
;
commit;

-- ��������� ������, ���� ������� ������� 4 (������� � ������� � Q9PB, ������� ��� � �������)
select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA4"
  into #REP_DATA4
  from #temp_lim_no_calc
;
commit;

-- ��������� ������, ���� ������� ������� 5 (����� ������� �� ��������� � ��������)
select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || QTY_ALL || '</td>' || '<td>' || QTY_UP || '</td>' || '<td>' || SUM_UP || '</td>' || '<td>' || QTY_DOWN || '</td>' || '<td>' || SUM_DOWN || '</td>' || '<td>' || MAX_DELTA || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA5"
  into #REP_DATA5
  from #temp_lim_journal
;
commit;

-- ��������� ������, ���� ������� ������� 6 (������ � �������� �����������)
select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_AMT || '</td>' || '<td>' || MAX_AMT || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA6"
  into #REP_DATA6
  from #temp_red_light
;
commit;

-- ��������� ������, ���� ������� ������� 8 (������ � �������� ����������� (�����������))
select trim(cast (list('<tr><td>' || REP_SVET || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA8"
  into #REP_DATA8
  from #temp_red_light_detail
;
commit;

-- ��������� ������, ���� ������� ������� 7 (������ � ������� � Q9PB ��� �������� ������)
select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA7"
  into #REP_DATA7
  from #temp_NO_DEAL
;
commit;

-- ��������� ������, ���� ������� ������� 9 (������ ���� ��������)
select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || REP_CNT || '</td>' || '<td>' || MIN_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA9"
  into #REP_DATA9
  from #min_limits
;
commit;
---------------------------------------------------------------------------
-- ������������ ������
---------------------------------------------------------------------------
-- �������� ������ ��� ���������� ��������
select ' main(){String rglName = "Monitoring_letter"; String st = setStartExecutionTime(rglName);'                   as "REP_TXT",
       1                                                                                                             as "REP_ORDER"
  into #REP_RESULT
 union all
select ' sendMailGeneral("' || t1.email || '","������� ����������� �� ' || today() || '",' ||
       '"������ ����!<br><br> ' ||
       '������ � Q9PB ���� �������� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> ����� �-�� </th> <th> ����������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t10.REP_DATA9 ||
       '</table> <br><br>' || 
           '���������� �������� �� ����� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> ����� �-�� �������� </th> <th> �-�� �������� �������� </th> <th> ���������� ��������, % </th> </tr>' ||
       t2.REP_DATA3 ||
       '</table> <br><br>' || 
           '2 �������� ������ �� ��������� 7 ���� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> �-�� �������� </th> </tr>' ||
       t3.REP_DATA ||
       '</table> ������ � ������� tmp.JOURNAL_��������� <br><br>' ||
	   '������ � Q9PB � �������� ��� ��������, ��� 90+, � ��������� �������� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> �-�� �������� </th> <th> ��������� ����� </th> <th> ������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t4.REP_DATA2 ||
       '</table> ������ � ������� tmp.LIMIT_WITHOUT_SCORE <br><br>' ||
	   '������� � ������� � Q9PB, ������� ��� � ������� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> �-�� �������� </th> <th> ��������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t5.REP_DATA4 ||
       '</table> ������ � ������� tmp.���������_NO_CALC <br><br>' || 
	   '����� ������� �� ��������� � �������� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> ����� �-�� �������� </th> <th> �-�� (������) </th> <th> ������ (������) </th> <th> �-�� (������) </th> <th> ������ (������) </th> <th> ����. ������ </th> </tr>' ||
       t6.REP_DATA5 ||
       '</table> ������ � ������� tmp.���������_MONITORING <br><br>' ||  
	   '������ � �������� ����������� ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> �-�� �������� </th> <th> ��������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t7.REP_DATA6 ||
       '</table> ������ � ������� tmp.���������_TRAFFIC_LIGHT <br><br>' || 
	   '������ � �������� ����������� (�����������)' ||
       '<table border=1>' ||
       '<tr> <th> �������� </th> <th> �-�� �������� </th> <th> ��������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t9.REP_DATA8 ||
       '</table> ������ � ������� tmp.���������_TRAFFIC_LIGHT <br><br>' ||    
	   '������ � ������� � Q9PB ��� �������� ������ ' ||
       '<table border=1>' ||
       '<tr> <th> ��� ������ </th> <th> �-�� �������� </th> <th> ��������� ����� </th> <th> ������������ ����� </th> </tr>' ||
       t8.REP_DATA7 ||
       '</table> ������ � ������� tmp.MONITORING_DEAL <br><br>' ||       
       '<br><br>�� ��������� �� ��� ���������. ��������� ������� �������������.<br> Do not worry. Be happy :)");'    as "REP_TXT",        
       2                                                                                                             as "REP_ORDER"
  from #ADDR           as t1
 cross join #REP_DATA3 as t2
 cross join #REP_DATA  as t3 
 cross join #REP_DATA2 as t4
 cross join #REP_DATA4 as t5
 cross join #REP_DATA5 as t6
 cross join #REP_DATA6 as t7
 cross join #REP_DATA8 as t9
 cross join #REP_DATA7 as t8
 cross join #REP_DATA9 as t10
 union all
select ' setEndExecutionTime(rglName, st); return true; } return main();'                                            as "REP_TXT",
       3                                                                                                             as "REP_ORDER"
;
commit;

-- �������� ���� �� ���� ��� ����������� �������, ��� bsh
select REP_TXT
  from #REP_RESULT
 order by REP_ORDER
;
output to '/DATA/Gremlin/scripts/MSB/System_Monitoring/Monitoring_letter.out' format ascii
quote ''
;
commit;
------------------------------------------------------------------------------------------------------------------------------------------
-- ����� ������
