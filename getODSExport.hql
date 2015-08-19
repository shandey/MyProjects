set mapred.job.queue.name = etl;
add jar ${hiveconf:BIGGIE_HOME}/app/lib/udfs/hive/moveHiveUDFs.jar;
create temporary function bytes as 'com.move.hive.udf.Bytes';

FROM (SELECT * FROM ${hiveconf:database}.${hiveconf:tableName}  WHERE source_file IN (${hiveconf:SRC_FILE})
AND (${hiveconf:PARTITION_FILTER})) instrumentation

INSERT OVERWRITE TABLE ${hiveconf:database}.${hiveconf:eventtableName}  PARTITION(batch_id="${hiveconf:BATCH_ID}")
SELECT 
 event odseventkey
,concat(concat(logdate_gmt,' '),time_gmt) eventdatetime
,time_taken timetaken
,substr(client_ip,1,16) clientip
,uri_query['tm'][0] clientdatetime
,uri_query['tz'][0] clienttimezone
,uri_query['ptnid'][0] eventpatternid
,uri_query['v'][0] version
,uri_query['visitor'][0] visitorid
,uri_query['session'][0] sessionid
,uri_query['accid'][0] accountid
,uri_query['starttime'][0] starttime
,uri_query['actg'][0] actiongoal
,uri_query['actt'][0] actiontype
,uri_query['src'][0] sourceapplication
,uri_query['sver'][0] sourceversion
,uri_query['dmn'][0] domain
,uri_query['env'][0] sourceenvironment
,uri_query['adid'][0] adid
,uri_query['adtsid'][0] advertiserid
,uri_query['adsrc'][0] adsource
,uri_query['adtyp'][0] adtype
,uri_query['err'][0] actionerror
,uri_query['chnl'][0] channel
,uri_query['flw'][0] flow
,uri_query['xcmpid'][0] externalcampaignid
,uri_query['xcmptyp'][0] externalcampaigntype
,uri_query['xscr'][0] externalsourceid
,uri_query['xstyp'][0] externalsourcetype
,uri_query['dstp'][0] destinationpage
,uri_query['dsturl'][0] destinationurl
,uri_query['dstwdgt'][0] destinationwidget
,uri_query['dstsct'][0] destinationsection
,uri_query['ref'][0] referrer
,uri_query['refvrt'][0] referringvertical
,split(uri_query['gate'][0],'#')[0] sourcegate
,uri_query['page'][0] sourcepage
,uri_query['wigt'][0] sourcewidget
,uri_query['sctn'][0] sourcesection
,uri_query['ssctn'][0] sourcesubsection
,uri_query['penv'][0] parentenvironment
,uri_query['ppage'][0] parentpage
,uri_query['psrc'][0] parentsource
,uri_query['pwigt'][0] parentwidget
,case when cast(bytes(uri_query['lnkel'][0]) as INT) > 16 then null else uri_query['lnkel'][0] end linkelement
,uri_query['cntry'][0] criteriacountry
,uri_query['gxt'][0] criteriageoextent
,uri_query['pxm'][0] criteriaproximity
,case when length(uri_query['state'][0]) > 2 then NULL else uri_query['state'][0] end criteriastate
,case when cast(bytes(uri_query['st'][0]) as INT) > 64 then null else uri_query['st'][0] end criteriastreet
,uri_query['age'][0] criteriaage
,uri_query['bath'][0] criteriabath
,uri_query['bed'][0] criteriabeds
,uri_query['pmax'][0] criteriapricemax
,uri_query['pmin'][0] criteriapricemin
,uri_query['sqft'][0] criteriasqft
,uri_query['schkw'][0] criteriakeyword
,uri_query['schls'][0] criterialotsize
,uri_query['rdes'][0] criteriarealtordesignation
,uri_query['rmn'][0] criteriarealtorname
,uri_query['tspn'][0] criteriatimespan
,uri_query['rescnt'][0] resultcount
,uri_query['othr'][0] resultother
,uri_query['resst'][0] resultset
,uri_query['ressrc'][0] resultsource
,uri_query['respp'][0] resultperpage
,uri_query['resby'][0] resultssortby
,uri_query['schid'][0] searchid
,uri_query['scht'][0] searchtype
,uri_query['cntt'][0] contacttime
,uri_query['esbs'][0] estimatedbuyselltime
,uri_query['fname'][0] firstname
,uri_query['lname'][0] lastname
,uri_query['infot'][0] informationtype
,uri_query['phone'][0] phone
,uri_query['qcom'][0] questioncomments
,uri_query['rest'][0] responsetime
,uri_query['email'][0] senderemail
,uri_query['cncat'][0] contentcategory
,uri_query['cnsrc'][0] contentsource
,uri_query['cntyp'][0] contenttype
,uri_query['fldcb'][0] coborrower
,uri_query['ldid'][0] leadid
,uri_query['lnat'][0] leadnature
,uri_query['x'][0] screenx
,uri_query['y'][0] screeny
,uri_query['ndtyp'][0] nodetype
,uri_query['ndclr'][0] nodecolor
,uri_query['ndsz'][0] nodesize
,uri_query['ndlbl'][0] nodelabel
,uri_query['ndtag'][0] nodealternatetag
,uri_query['ndval'][0] nodevalue
,uri_query['dn'][0] xhtmldomnode
,sitename sitename
,method methodname
,status serverstatus
,CONCAT('<UA><OS>',case when trim(os['family']) = 'Other' then 'UNKNOWN' else upper(trim(os['family'])) end,'</OS><BR>',case when trim(user_agent['family']) = 'Other' then 'UNKNOWN' else upper(trim(user_agent['family'])) end,'</BR><BRV>',case when trim(user_agent['major']) = 'None' then 'UNKNOWN' else upper(trim(user_agent['major'])) end,'</BRV><DEVICE>',case when trim(device['family']) = 'Other' then 'UNKNOWN' else upper(trim(device['family'])) end,'</DEVICE></UA>') useragent
,uri_stem uristem
,host host
,null errorseverity 

INSERT OVERWRITE TABLE ${hiveconf:database}.${hiveconf:stattableName}  PARTITION(batch_id="${hiveconf:BATCH_ID}")
SELECT
  concat('MinimumEventDateInLoad = ',min(concat(concat(logdate_gmt,' '),time_gmt))) Mineventdatetime
, concat('MaximumEventDateInLoad = ',max(concat(concat(logdate_gmt,' '),time_gmt))) Maxeventdatetime
, concat('EventRowsPerBatch = ',count(1)) numofrecord
;
