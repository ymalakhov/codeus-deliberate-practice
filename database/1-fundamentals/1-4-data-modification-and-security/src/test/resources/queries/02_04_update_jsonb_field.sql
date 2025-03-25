-- check the schema for challenges table in 'resources/schema.sql'
-- update the salary for days amount to 10 for 'algo-challenge'
UPDATE challenges
SET challenge_task = jsonb_set(challenge_task, '{days}', '10')
WHERE challenge_name = 'algo-challenge';