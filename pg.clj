(ns klog.pg
  (:require [klog.core :as klog]
            [clj-pg.honey :as hpg]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :as json]
            [pg.core :as pg]))

(defn pg-appender [db]
  (klog/add-appender
   :pg
   (fn [l]
     (jdbc/insert! db "logs" {:ev (name (:ev l))
                              :ts (:ts l)
                              :resource (json/generate-string (dissoc l :ev :ts))}))))

(comment

  (def db-spec (pg.core/db-spec-from-env))

  (def root-db (pg.core/connection (assoc db-spec :user "root" :port 5333)))

  (pg.core/create-database root-db {:database "_logs" :port 5333 :user "root"})

  (def log-db (pg.core/connection (assoc db-spec :database "_logs" :port 5333 :user "root")))

  (hpg/exec! log-db
"
create extension pipelinedb;

CREATE FOREIGN TABLE IF NOT EXISTS logs (
  id serial,
  ts timestamptz,
  ev text,
  resource jsonb
) SERVER pipelinedb;

")

  (jdbc/execute! log-db ["
drop view events_stat;
CREATE VIEW events_stat WITH (action=materialize)
AS
SELECT ev, count(*),
sql as sql,
sum(d) as sum,
avg(d) as avg,
min(d) as min,
max(d) as max,
max(ts) as last
FROM (
  SELECT ev,
  resource->>'sql' as sql,
  coalesce((resource->>'d'), '0')::numeric as d,
  ts
  from logs
) _
group by ev, sql
; "])


  (hpg/query log-db "select 1")

  (spit "/tmp/moni.yaml"
        (clj-yaml.core/generate-string 
         (jdbc/query log-db ["select * from events_stat order by avg desc"])
         ))

  (pg-appender log-db)

  (klog/rm-appender :stdout)

  (klog/rm-appender :pg)
  @klog/appenders
  (klog/compile-appenders)
  


  (klog/log ::test {})

  )

