SET 'auto.offset.reset' = 'earliest';

CREATE
SOURCE CONNECTOR mysql_source_connector
WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'connection.url' = 'jdbc:mysql://mysql:3306/football',
  'connection.user' = 'root',
  'connection.password' = 'root',
  'table.whitelist' = 'players',
  'mode' = 'incrementing',
  'incrementing.column.name' = 'id',
  'topic.prefix' = '',
  'key'='id',
  'key.converter'='org.apache.kafka.connect.storage.StringConverter',
  'value.converter'='org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable' = false
);

CREATE TABLE players
(
    ID          VARCHAR PRIMARY KEY,
    name        VARCHAR(50),
    team        VARCHAR(50),
    nationality VARCHAR(50)
)
    WITH (
        KAFKA_TOPIC = 'players',
        VALUE_FORMAT = 'JSON',
        PARTITIONS = 1
        );

CREATE
STREAM match_event (
    id VARCHAR KEY,
    event_type VARCHAR,
    player_id VARCHAR,
    home boolean
) WITH (
    KAFKA_TOPIC='match_event',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);

CREATE
STREAM match_event_player WITH (
    KAFKA_TOPIC='match_event_player',
    VALUE_FORMAT='JSON'
) AS
SELECT id, event_type, player_id
FROM match_event PARTITION BY player_id;

INSERT INTO match_event
VALUES ('1', 'GOAL', '1', true);

SELECT *
FROM match_event_player mep
         LEFT JOIN players p ON p.id = mep.player_id emit changes;

SELECT p.id, p.name, p.nationality, count(mep.id) AS goals
FROM match_event_player mep
         LEFT JOIN players p ON p.id = mep.player_id
GROUP BY p.id, p.name, p.nationality emit changes;

INSERT INTO match_event
VALUES ('1', 'GOAL', '2', false);

-- -----------------

INSERT INTO match_event
VALUES ('1', 'ASSIST', '1', true);


INSERT INTO match_event
VALUES ('2', 'ASSIST', '1', true);

SELECT id,
       sum(
               CASE
                   WHEN home AND event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           ) home_goals,
       sum(
               CASE
                   WHEN NOT home AND event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           ) away_goals
FROM match_event
WHERE home IS NOT NULL
GROUP BY id emit changes;

SELECT p.id, p.name, p.nationality, count(mep.id) AS goals
FROM match_event_player mep
         JOIN players p ON p.id = mep.player_id
WHERE mep.event_type = 'GOAL'
GROUP BY p.id, p.name, p.nationality emit changes;

SELECT p.id AS player_id,
       p.name AS player_name,
       p.nationality AS nationality,
       sum(
               CASE
                   WHEN mep.event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           )                                                     goals,
       CAST(sum(
               CASE
                   WHEN mep.event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           )
           as double) / cast(COUNT_DISTINCT((mep.id)) as double) avg_goals,
       sum(
               CASE
                   WHEN mep.event_type = 'ASSIST' THEN 1
                   ELSE 0
                   END
           ) assists
FROM match_event_player mep
         JOIN players p
              ON p.id = mep.player_id
GROUP BY p.id emit changes;



CREATE TABLE player_stats
WITH (
    KAFKA_TOPIC = 'player_stats',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
    ) AS
SELECT p.id AS player_id,
       LATEST_BY_OFFSET(p.name) AS player_name,
       LATEST_BY_OFFSET(p.nationality) AS nationality,
       sum(
               CASE
                   WHEN mep.event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           )                                                     goals,
       CAST(sum(
               CASE
                   WHEN mep.event_type = 'GOAL' THEN 1
                   ELSE 0
                   END
           )
           AS DOUBLE) / cast(COUNT_DISTINCT((mep.id)) AS DOUBLE) avg_goals,
       sum(
               CASE
                   WHEN mep.event_type = 'ASSIST' THEN 1
                   ELSE 0
                   END
           ) assists
FROM match_event_player mep
         JOIN players p
              ON p.id = mep.player_id
GROUP BY p.id emit changes;
