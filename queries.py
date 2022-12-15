# Session level
query_conversionrate = """ 
SELECT  device_id, uniqExact(user_id) as nbrofusers 
, uniqExactIf(user_id, session_transactions_revenue_cents >0 ) as transaction_done,
       (transaction_done / nbrofusers) as cvr
FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
GROUP BY device_id
SETTINGS distributed_group_by_no_merge = 0;
  """

query_nbrofsessionspervisitor = """ 
SELECT device_id, nbrofsessions, uniqExact(user_id) as nbrofusers
    FROM
    (
SELECT  uniqExact(session_number) as sum_session, user_id ,
       if(sum_session >5, 5, sum_session) as nbrofsessions, device_id



FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})

GROUP BY user_id, device_id
    )
GROUP BY nbrofsessions, device_id
ORDER BY nbrofsessions asc
SETTINGS distributed_group_by_no_merge = 0;
  """

query_nbrofsessionsbeforeconverting = """ 
select  device_id, avg(first_transaction_session) as avg_sessions_before_first_transaction
    from (
                     select user_id, session_number
             from sessions
WHERE project_id = {project_id}
AND device_id in ({device_id})
AND session_date >= '2021-02-01'

AND session_number = 1

             ) as t1
         left join
         (  select project_id,
                    device_id,
                    user_id,
                    min(session_number) as first_transaction_session
             from sessions
WHERE project_id = {project_id}
AND device_id in ({device_id})
AND session_date >= '2021-02-01'

               and session_number_of_transactions > 0
             group by project_id, device_id, user_id


             ) as t2 on t1.user_id = t2.user_id
             where not project_id = 0 and not device_id = 0
             group by device_id
             order by  device_id
          settings distributed_group_by_no_merge = 0; """

query_nextsessionreturnrate = """ 
SELECT

    device_id, uniqExact(user_id_filter.session_id)          as exit_firstsession,
    uniqExact(next_session_info.session_id)       as return_secondsession,
    return_secondsession/ exit_firstsession as sessionsreturnrate
 FROM    (
         select distinct user_id,
                         session_id,
                         session_number, device_id

    FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})

         ) as user_id_filter

   LEFT JOIN

(select user_id,
             session_id,
             session_number


    FROM views
  WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})

         ) as next_session_info

     on next_session_info.user_id = user_id_filter.user_id and
     toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1
 GROUP BY device_id
    SETTINGS distributed_group_by_no_merge = 0;
  """


query_daydiffebetweentwosessions = """ 
SELECT device_id, uniqExact(users),
       if(nbrdaydifferencbetweentwosessions >10, +10,nbrdaydifferencbetweentwosessions ) as newnbrdaydifferencbetweentwosessions

FROM (SELECT user_id_filter.device_id as device_id,
user_id_filter.session_date                                                                firstdate,
             next_session_info.session_date                                                          as returndate,
             dateDiff('day', firstdate, returndate) as nbrdaydifferencbetweentwosessions,
             user_id_filter.user_id as users


      -- Put info about the first session
      FROM (
               select distinct user_id,
                               session_id,
                               session_number,
                               session_time,
                               session_date,
                               path, device_id

               FROM views
          WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})

               ) as user_id_filter

               INNER JOIN


           (select user_id,
                   session_id,
                   session_number,
                   session_date,
                   session_time,
                   path

            FROM views
       WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
               ) as next_session_info
           on next_session_info.user_id = user_id_filter.user_id and
              toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1

           --  WHERE dateDiff('minute', user_id_filter.session_time, next_session_info.session_time) <= 180000
      GROUP BY user_id_filter.session_date, next_session_info.session_date, users, next_session_info.session_time,device_id, user_id_filter.session_time )

GROUP BY newnbrdaydifferencbetweentwosessions, device_id
HAVING newnbrdaydifferencbetweentwosessions >= 0
ORDER BY newnbrdaydifferencbetweentwosessions asc
    SETTINGS distributed_group_by_no_merge = 0;
  """


query_hourdiffebetweentwosessions = """ 
SELECT device_id, uniqExact(users), hoursdiff

FROM (SELECT user_id_filter.session_date                                                                firstdate,
             next_session_info.session_date                                                          as returndate,
             dateDiff('hour', user_id_filter.session_time, next_session_info.session_time) as hoursdiff,
             user_id_filter.user_id as users, user_id_filter.device_id as device_id


      -- Put info about the first session
      FROM (
               select distinct user_id,
                               session_id,
                               session_number,
                               session_time,
                               session_date,
                               path, device_id

               
                      FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
         
          
               ) as user_id_filter

               INNER JOIN


           (select user_id,
                   session_id,
                   session_number,
                   session_date,
                   session_time,
                   path, device_id

            FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
         
               ) as next_session_info
           on next_session_info.user_id = user_id_filter.user_id and
              toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1

             WHERE dateDiff('minute', user_id_filter.session_time, next_session_info.session_time) <= 180000
           AND hoursdiff <= 24
      GROUP BY user_id_filter.session_date, next_session_info.session_date, users, next_session_info.session_time, user_id_filter.session_time,
          device_id     )

GROUP BY  hoursdiff, device_id
HAVING hoursdiff >= 0
ORDER BY hoursdiff asc
    SETTINGS distributed_group_by_no_merge = 0;
  """

query_boucereturners = """ 


  SELECT user_id_filter.device_id, uniqExact(user_id_filter.session_id)                                                    as firstsessioninfo,
                   uniqExactIf(user_id_filter.session_id, user_id_filter.is_last ) as bouncersfirstsession,
                   bouncersfirstsession/ firstsessioninfo as bounceratefirstsession,
                   uniqExactIf(next_session_info.session_id, user_id_filter.is_last) as bouncerreturners, --(number of sessions who have bounced during their first sessions and came back on the website n+1)
                   bouncerreturners /bouncersfirstsession as bouncersreturnersrate -- (among the sessions who have bounced, % who came back on the website),



      -- Put info about the first session
      FROM (

               select distinct user_id,
                               session_id,
                               session_number,
                               session_time,
                               session_date,
                               view_number, is_last, device_id, session_number_of_transactions



               FROM views
               WHERE  is_first
        AND  project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
      
               ) as user_id_filter

               LEFT JOIN


           (select user_id,
                   device_id,
                   session_id,
                   session_number,
                   session_date,
                   session_time,
                   path,
                   session_number_of_transactions



            FROM views
        WHERE  project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
               ) as next_session_info
           on next_session_info.user_id = user_id_filter.user_id and
              toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1
              GROUP BY user_id_filter.device_id
             SETTINGS distributed_group_by_no_merge = 0;
  """


# Page level

query_nbrofsessionspervisitorpagelevel = """ 
SELECT device_id, nbrofsessions, uniqExact(user_id) as nbrofusers
    FROM
    (
   WITH ({page_condition}) as Page
SELECT  uniqExact(session_number) as sum_session, user_id ,
       if(sum_session >5, 5, sum_session) as nbrofsessions, device_id



FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
AND Page
GROUP BY user_id, device_id
    )
GROUP BY nbrofsessions, device_id
ORDER BY nbrofsessions asc
SETTINGS distributed_group_by_no_merge = 0;
  """

query_returonthesiteafterexitingonapage = """ 
SELECT

    device_id, uniqExact(user_id_filter.session_id)          as exit_firstsession,
    uniqExact(next_session_info.session_id)       as return_secondsession,
    return_secondsession/ exit_firstsession as sessionsreturnrate
 FROM    (  WITH (({page_condition})) as Page
         select distinct user_id,
                         session_id,
                         session_number, device_id

    FROM views
WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})
AND Page
AND is_last = 1

         ) as user_id_filter

   LEFT JOIN

(select user_id,
             session_id,
             session_number


    FROM views
  WHERE project_id = {project_id}
AND session_date >= '{start_date}'
AND session_date <= '{end_date}'
AND device_id in ({device_id})

         ) as next_session_info

     on next_session_info.user_id = user_id_filter.user_id and
     toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1
 GROUP BY device_id
    SETTINGS distributed_group_by_no_merge = 0;
  """

query_boucerspagelevelcomingbackonthesite = """   SELECT user_id_filter.device_id, uniqExact(user_id_filter.session_id)                                                    as firstsessioninfo,
                   uniqExactIf(user_id_filter.session_id, user_id_filter.is_last ) as bouncersfirstsession,
                   bouncersfirstsession/ firstsessioninfo as bounceratefirstsession,
                   uniqExactIf(next_session_info.session_id, user_id_filter.is_last) as bouncerreturners, --(number of sessions who have bounced during their first sessions and came back on the website n+1)
                   bouncerreturners /bouncersfirstsession as bouncersreturnersrate -- (among the sessions who have bounced, % who came back on the website),



      -- Put info about the first session
      FROM (

               select distinct user_id,
                               session_id,
                               session_number,
                               session_time,
                               session_date,
                               view_number, is_last, device_id, session_number_of_transactions



               FROM views
               WHERE  is_first
        AND  project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
      
               ) as user_id_filter

               LEFT JOIN


           (select user_id,
                   device_id,
                   session_id,
                   session_number,
                   session_date,
                   session_time,
                   path,
                   session_number_of_transactions



            FROM views
        WHERE  project_id = {project_id}
        AND session_date >= '{start_date}'
        AND session_date <= '{end_date}'
        AND device_id in ({device_id})
               ) as next_session_info
           on next_session_info.user_id = user_id_filter.user_id and
              toUInt64(next_session_info.session_number) = toUInt64(user_id_filter.session_number) + 1
              GROUP BY user_id_filter.device_id
             SETTINGS distributed_group_by_no_merge = 0;
  """


