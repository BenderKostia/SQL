/* This script collects data for reports. The SQL data is then converted into html logic to generate reports in the form of tables in a letter in mail.
Then I export this data as a file and another program launch it to send mail.*/


/*******************************************************************************************************************************************************
 Author:                    #############
 Purpose:                   Zeroing limits in the absence of active trades
 Customer:                  #############
--------------------------------------------------------------------------------------------------------------------------------------------------------
 Server:                    ############# / DB type: Sybase IQ          
--------------------------------------------------------------------------------------------------------------------------------------------------------
 Export table:              --
--------------------------------------------------------------------------------------------------------------------------------------------------------
 Execution frequency:       every day
--------------------------------------------------------------------------------------------------------------------------------------------------------
 Notes     :                1. Day of release     - 02.05.2019
                            2. Document number    - #############
*******************************************************************************************************************************************************/
---------------------------------------------------------------------------
-- Data collection
---------------------------------------------------------------------------
-- Limits below the minimum
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
-- 2 limit changes in a row
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
-- Limits without scoring
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
-- Successful limit change for yesterday
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
            else null
	   end                                                                                                as "RATE_SUCCESS"
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
	   end                                                                                                as "RATE_SUCCESS"
  from tmp.GUL_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
            else null
	   end                                                                                                as "RATE_SUCCESS"
  from tmp.L2F_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
            else null
	   end                                                                                                as "RATE_SUCCESS"
  from tmp.L3F_SUCCESS_RATE
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                                               as "QTY_ALL",
       TMP.setMillionDelim(trim(cast(sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) as char(15)))) as "QTY_SUCCESS",
	   case when count() > 0
	        then cast(100.00 * sum(case when bq9_amt = Lim_4_q9pb then 1 else 0 end) / count() as numeric(15,2))
            else null
	   end                                                                                                as "RATE_SUCCESS"
  from tmp.LOF_SUCCESS_RATE
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- Clients with a limit who are not in the calculation
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  into #temp_lim_no_calc
  from tmp.GLP_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.GUL_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L3F_NO_CALC
 group by bqt_qt
 union all
select bqt_qt, 
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.LOF_NO_CALC
 group by bqt_qt
;
commit;
---------------------------------------------------------------------------
-- Client limit does not match log
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
-- Limits with red traffic lights
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

-- Limits with red traffic lights (detail)
select 'SVET_271'                                                             as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  into #temp_red_light_detail
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_271 = 'N'
 union all 
select 'SVET_272'                                                             as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_272 = 'N'
 union all 
select 'SVET_327'                                                             as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_327 = 'N'
 union all 
select 'SVET_266'                                                             as "REP_SVET",
       TMP.setMillionDelim(trim(cast(count() as char(15))))                   as "QTY",
       TMP.setMillionDelim(trim(cast(cast(sum(bq9_amt) as int) as char(15)))) as "SUM_LIM",
       TMP.setMillionDelim(trim(cast(cast(max(bq9_amt) as int) as char(15)))) as "MAX_LIM"
  from tmp.L2F_TRAFFIC_LIGHT
 where SVET_266 = 'N'
;
commit;
---------------------------------------------------------------------------
-- Limit client without active deal
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
-- Preliminary tables
---------------------------------------------------------------------------
-- Table with addresses for mailing
select trim(cast('################@mail.ua, ################@mail.ua, ################@mail.ua, ################@mail.ua' as char(1000))) as "Email"    
  into #Addr
;
commit;

-- I merge the row to make tables
select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || QTY || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA"
  into #REP_DATA
  from #temp_2_in_row
;
commit;

select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || AVG_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA2"
  into #REP_DATA2
  from #temp_lim_score
;
commit;

select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY_ALL || '</td>' || '<td>' || QTY_SUCCESS || '</td>' || '<td>' || RATE_SUCCESS || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA3"
  into #REP_DATA3
  from #temp_SUCCESS_RATE
;
commit;

select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA4"
  into #REP_DATA4
  from #temp_lim_no_calc
;
commit;

select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || QTY_ALL || '</td>' || '<td>' || QTY_UP || '</td>' || '<td>' || SUM_UP || '</td>' || '<td>' || QTY_DOWN || '</td>' || '<td>' || SUM_DOWN || '</td>' || '<td>' || MAX_DELTA || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA5"
  into #REP_DATA5
  from #temp_lim_journal
;
commit;

select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_AMT || '</td>' || '<td>' || MAX_AMT || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA6"
  into #REP_DATA6
  from #temp_red_light
;
commit;

select trim(cast (list('<tr><td>' || REP_SVET || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA8"
  into #REP_DATA8
  from #temp_red_light_detail
;
commit;

select trim(cast (list('<tr><td>' || bqt_qt || '</td>' || '<td>' || QTY || '</td>' || '<td>' || SUM_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA7"
  into #REP_DATA7
  from #temp_NO_DEAL
;
commit;

select trim(cast (list('<tr><td>' || LIM_TYPE || '</td>' || '<td>' || REP_CNT || '</td>' || '<td>' || MIN_LIM || '</td>' || '<td>' || MAX_LIM || '</td></tr>' ,'') as varchar(10000))) as "REP_DATA9"
  into #REP_DATA9
  from #min_limits
;
commit;
---------------------------------------------------------------------------
-- Forming a letter
---------------------------------------------------------------------------
-- I am forming a script to perform the mailing
select ' main(){String rglName = "Monitoring_letter"; String st = setStartExecutionTime(rglName);'                   as "REP_TXT",
       1                                                                                                             as "REP_ORDER"
  into #REP_RESULT
 union all
select ' sendMailGeneral("' || t1.email || '","Monitoring system for ' || today() || '",' ||
       '"Good afternoon!<br><br> ' ||
       'Limits below the minimum ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Total number </th> <th> Minimum limit </th> <th> Maximum limit </th> </tr>' ||
       t10.REP_DATA9 ||
       '</table> <br><br>' || 
       'Successful limit change for yesterday ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Total number of changes </th> <th> Number of successful changes </th> <th> Success of changes, % </th> </tr>' ||
       t2.REP_DATA3 ||
       '</table> <br><br>' || 
       '2 limit changes in a row for the last 7 days ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Total number </th> </tr>' ||
       t3.REP_DATA ||
       '</table> Details in the table tmp.JOURNAL_LimitType <br><br>' ||
	   'Limits without scoring ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Number of clients </th> <th> Total limit </th> <th> Average limit </th> <th> Maximum limit </th> </tr>' ||
       t4.REP_DATA2 ||
       '</table> Details in the table tmp.LIMIT_WITHOUT_SCORE <br><br>' ||
	   'Clients with a limit who are not in the calculation ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Number of clients </th> <th> Total limit </th> <th> Maximum limit </th> </tr>' ||
       t5.REP_DATA4 ||
       '</table> Details in the table tmp.LimitType_NO_CALC <br><br>' || 
	   'Client limit does not match log ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Total number of clients </th> <th> Qty (more) </th> <th> Delta (more) </th> <th> Qty (less) </th> <th> Delta (less) </th> <th> Maximum delta </th> </tr>' ||
       t6.REP_DATA5 ||
       '</table> Details in the table tmp.LimitType_MONITORING <br><br>' ||  
	   'Limits with red traffic lights ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Number of clients </th> <th> Total limit </th> <th> Maximum limit </th> </tr>' ||
       t7.REP_DATA6 ||
       '</table> Details in the table tmp.LimitType_TRAFFIC_LIGHT <br><br>' || 
	   'Limits with red traffic lights (detail)' ||
       '<table border=1>' ||
       '<tr> <th> Traffic light </th> <th> Number of clients </th> <th> Total limit </th> <th> Maximum limit </th> </tr>' ||
       t9.REP_DATA8 ||
       '</table> Details in the table tmp.LimitType_TRAFFIC_LIGHT <br><br>' ||    
	   'Limit client without active deal ' ||
       '<table border=1>' ||
       '<tr> <th> Limit type </th> <th> Number of clients </th> <th> Total limit </th> <th> Maximum limit </th> </tr>' ||
       t8.REP_DATA7 ||
       '</table> Details in the table tmp.MONITORING_DEAL <br><br>' ||       
       '<br><br>Do not reply to this message. Message created automatically.<br> Do not worry. Be happy :)");'       as "REP_TXT",        
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

-- I upload the file to sftp for further launch, like bsh
select REP_TXT
  from #REP_RESULT
 order by REP_ORDER
;
output to '/DATA/scripts/MSB/System_Monitoring/Monitoring_letter.out' format ascii
quote ''
;
commit;
------------------------------------------------------------------------------------------------------------------------------------------
-- End of the screen