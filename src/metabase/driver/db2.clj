(ns metabase.driver.db2
	"Driver for DB2 databases. Uses the official IBM DB2 JDBC driver(use LIMIT OFFSET support DB2 v9.7 https://www.ibm.com/developerworks/community/blogs/SQLTips4DB2LUW/entry/limit_offset?lang=en)"
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as hsql]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.driver.generic-sql :as sql]
            [metabase.util
             [honeysql-extensions :as hx]
             [ssh :as ssh]]))

(defn- column->base-type
  "Mappings for DB2 types to Metabase types.
   See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmdb2/ibmdb2_data_types.htm"
  [column-type]
  ({:bigint           :type/BigInteger
    :blob           	:type/*
    :char             :type/Text
    :date             :type/Date
    :decimal          :type/Decimal
    :double           :type/Float
    :integer          :type/Integer
    :smallint         :type/Integer
    :clob             :type/Text
    :dbclob           :type/*
    :decfloat					:type/*
    :time             :type/Time
    :timestamp        :type/DateTime
    :varchar          :type/Text
    :xml              :type/*
    :graphic          :type/*
    :vargraphic       :type/*
    :boolean          :type/Boolean
    :real             :type/*
    (keyword "long varchar")                	:type/*
    (keyword "long vargraphic")               :type/*
    (keyword "char () for bit data")          :type/*
		(keyword "long varchar for bit data")     :type/*
		(keyword "varchar () for bit data")       :type/*} column-type))

(defn- connection-details->spec
  [{:keys [host port db]
    :or   {host "localhost", port 50000, db ""}
    :as   details}]
  (merge {:classname "com.ibm.db2.jcc.DB2Driver" ; must be in classpath
          :subprotocol "db2"
          :subname (str "//" host ":" port "/" db)}
         (dissoc details :host :port :db)))

(defn- can-connect? [details]
  (let [connection (connection-details->spec (ssh/include-ssh-tunnel details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT 1 FROM SYSIBM.SYSDUMMY1"])))))))

(defn- date-format [format-str expr] (hsql/call :varchar_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defn- date
  "Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
  See [this page](https://www.ibm.com/developerworks/data/library/techarticle/0211yip/0211yip3.html) for details on the functions we're using."
  [unit expr]
  (case unit
    :default         expr
    :minute          (trunc-with-format "YYYY-MM-DD HH24:MI" expr)
    :minute-of-hour  (hsql/call :minute expr)
    :hour            (trunc-with-format "YYYY-MM-DD HH24" expr)
    :hour-of-day     (hsql/call :hour expr)
    :day             (hsql/call :date expr)
    :day-of-week     (hsql/call :dayofweek expr)
    :day-of-month    (hsql/call :day expr)
    :day-of-year     (hsql/call :dayofyear expr)
    :week            (hx/- expr (hsql/raw (format "%d days" (int (hx/- (hsql/call :dayofweek expr) 1)))))
    :week-of-year    (hsql/call :week expr)
    :month           (str-to-date "YYYY-MM-DD" (hx/concat (date-format "YYYY-MM" expr) (hx/literal "-01")))
    :month-of-year   (hsql/call :month expr)
    :quarter         (str-to-date "YYYY-MM-DD" (hsql/raw (format "%d-%d-01" (int (hx/year expr)) (int ((hx/- (hx/* (hx/quarter expr) 3) 2))))))
    :quarter-of-year (hsql/call :quarter expr)
    :year            (hsql/call :year expr)))

(defn- date-interval
  [unit amount]
	(case unit
		:second  (hsql/raw (format "current timestamp + %d seconds" (int amount)))
    :minute  (hsql/raw (format "current timestamp + %d minutes" (int amount)))
    :hour    (hsql/raw (format "current timestamp + %d hours" (int amount)))
    :day     (hsql/raw (format "current timestamp + %d days" (int amount)))
    :week    (hsql/raw (format "current timestamp + %d days" (int (hx/* amount (hsql/raw 7)))))
    :month   (hsql/raw (format "current timestamp + %d months" (int amount)))
    :quarter (hsql/raw (format "current timestamp + %d months" (int (hx/* amount (hsql/raw 3)))))
    :year    (hsql/raw (format "current timestamp + %d years" (int amount)))))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
  (case seconds-or-milliseconds
    :seconds      (hx/+ (hsql/raw "timestamp('1970-01-01-00.00.00')") (hsql/raw (format "%d seconds" (int expr))))
    :milliseconds (hx/+ (hsql/raw "timestamp('1970-01-01-00.00.00')") (hsql/raw (format "%d seconds" (int (hx// expr 1000)))))))


(defn- string-length-fn [field-key]
  (hsql/call :length field-key))

;;(def ^:private db2-date-formatter (driver/create-db-time-formatter "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSZ"))
(def ^:private db2-db-time-query "SELECT current timestamp FROM sysibm.sysdummy1")
(def ^:private ^:const now             (hsql/raw "current timestamp"))

(defrecord DB2Driver []
  clojure.lang.Named
  (getName [_] "DB2"))

(u/strict-extend DB2Driver
  driver/IDriver
  (merge (sql/IDriverSQLDefaultsMixin)
         {:can-connect?                      (u/drop-first-arg can-connect?)
          :date-interval                     (u/drop-first-arg date-interval)
          :details-fields                    (constantly (ssh/with-tunnel-config
                                                           [{:name         "host"
                                                             :display-name "Host"
                                                             :default      "localhost"}
                                                            {:name         "port"
                                                             :display-name "Port"
                                                             :type         :integer
                                                             :default      50000}
                                                            {:name         "db"
                                                             :display-name "Database name"
									                                         	 :placeholder  "BirdsOfTheWorld"
									                                           :required     true}
                                                            {:name         "user"
                                                             :display-name "Database username"
                                                             :placeholder  "What username do you use to login to the database?"
                                                             :required     true}
                                                            {:name         "password"
                                                             :display-name "Database password"
                                                             :type         :password
                                                             :placeholder  "*******"}]))
        ;;  :current-db-time                   (driver/make-current-db-time-fn db2-date-formatter db2-db-time-query)
					})

  sql/ISQLDriver
  (merge (sql/ISQLDriverDefaultsMixin)
         {:column->base-type         (u/drop-first-arg column->base-type)
          :connection-details->spec  (u/drop-first-arg connection-details->spec)
          :current-datetime-fn       (constantly now)
          :date                      (u/drop-first-arg date)
          :excluded-schemas          (constantly #{"SQLJ" "SYSCAT" "SYSFUN" "SYSIBMADM" "SYSIBMINTERNAL" "SYSIBMTS" "SYSPROC" "SYSPUBLIC" "SYSSTAT" "SYSTOOLS"})
          :set-timezone-sql          (constantly "SET SESSION TIME ZONE = '%s'")
          :string-length-fn          (u/drop-first-arg string-length-fn)
          :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defn -init-driver
  "Register the DB2 driver when the JAR is found on the classpath"
  []
  ;; only register the DB2 driver if the JDBC driver is available
  (when (u/ignore-exceptions
         (Class/forName "com.ibm.db2.jcc.DB2Driver"))
    (driver/register-driver! :db2 (DB2Driver.))))
