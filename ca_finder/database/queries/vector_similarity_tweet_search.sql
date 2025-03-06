WITH queryid AS (SELECT '1691226340253372417'::BIGINT AS tweetid)
SELECT content
FROM tweets
WHERE tweetid != (SELECT tweetid FROM queryid) -- Exclude the reference tweet
AND  (createdat::timestamp - (select createdat::timestamp from tweets where tweetid = (select tweetid from queryid )))
         < INTERVAL '3 day' -- time window filter
ORDER BY content_embedding <=> (SELECT content_embedding FROM tweets WHERE tweets.tweetid = (SELECT tweetid FROM queryid)) -- order by vector similarity
LIMIT 100;