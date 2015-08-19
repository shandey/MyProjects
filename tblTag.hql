set mapred.job.queue.name = etl;

INSERT OVERWRITE TABLE ${hiveconf:database}.${hiveconf:tagtableName} PARTITION(batch_id="${hiveconf:BATCH_ID}", Tag) 
SELECT 
odseventkey,
value,
tag
FROM	
(SELECT
 event odseventkey
,tagvalue
,tag
FROM
   ${hiveconf:database}.${hiveconf:tableName}  LATERAL VIEW explode(uri_query) tt as tag, tagvalue
WHERE source_file IN (${hiveconf:SRC_FILE})
AND (${hiveconf:PARTITION_FILTER})
AND tag not in (${hiveconf:TAG})
) a LATERAL VIEW explode(tagvalue) t as value
;
