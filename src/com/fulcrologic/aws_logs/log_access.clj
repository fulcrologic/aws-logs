(ns com.fulcrologic.aws-logs.log-access
  (:require
    [cheshire.core :refer [parse-string]]
    [clojure.string :as str])
  (:import
    (com.joestelmach.natty Parser DateGroup)
    (com.amazonaws.services.logs AWSLogsClientBuilder AWSLogsClient)
    (com.amazonaws.services.logs.model DescribeLogStreamsRequest LogStream GetLogEventsRequest OutputLogEvent)
    (java.util Date)
    (java.text SimpleDateFormat)))

(defonce ^AWSLogsClient client (AWSLogsClientBuilder/defaultClient))

(defn- describe-log-streams
  "Get the names of the log streams that are in a given log group

  group - The name of the log group
  prefix - A prefix on the stream name to limit the stream results
  token - A result continuation token

  See `recent-streams` for the more usable version of this function.
  "
  ([group] (describe-log-streams group nil nil))
  ([group prefix] (describe-log-streams group prefix nil))
  ([^String group ^String prefix ^String token]
   (let [req (cond-> (DescribeLogStreamsRequest. group)
               (seq prefix) (.withLogStreamNamePrefix prefix)
               (not (seq prefix)) (.withOrderBy "LastEventTime")
               (not (seq prefix)) (.withDescending true)
               (seq token) (.withNextToken token))]
     (.describeLogStreams client req))))

(defn recent-streams
  "Returns a list of the streams that have recently received log messages (past hour) as
  a sequence of maps:

  ```
  [{:group log-group
    :stream stream-name}
   ...]
  ```
  "
  [log-group]
  (let [hour   (- (System/currentTimeMillis) (* 3600 1000))
        result (atom [])]
    (loop [descr (describe-log-streams log-group)]
      (let [stream-list (.getLogStreams descr)
            items       (keep
                          (fn [^LogStream s]
                            (let [last-update (.getLastEventTimestamp s)]
                              (when (and last-update (> last-update hour))
                                {:group  log-group
                                 :stream (.getLogStreamName s)}))) stream-list)]
        (if-let [token (and (seq items) (.getNextToken descr))]
          (do
            (swap! result into items)
            (recur (describe-log-streams log-group nil token)))
          @result)))))

(defn get-log-events
  "Given a {:group grp :stream stream-name}, and a start and end time (in ms, use `inst-ms`),
   returns the log events between those two timestamps. The raw log JSON will be decoded into
   EDN maps (lower-case keys), and the message timestamps will be converted to `inst?`.

   You probably want `get-logs` or `show-logs`.
   "
  [{:keys [group stream]} start-time end-time]
  (let [req     (fn [token]
                  (-> (GetLogEventsRequest.)
                    (cond->
                      end-time (.withEndTime end-time)
                      token (.withNextToken token))
                    (.withStartTime start-time)
                    (.withStartFromHead true)
                    (.withLogGroupName group)
                    (.withLogStreamName stream)))
        results (atom [])]
    (loop [token  nil
           result (.getLogEvents client (req nil))]
      (let [next-token (.getNextForwardToken result)]
        (if (= next-token token)
          @results
          (do
            (swap! results into
              (map (fn [^OutputLogEvent evt]
                     (let [raw-message (.getMessage evt)]
                       (try
                         (update (parse-string raw-message (comp keyword str/lower-case)) :timestamp #(Date. (long %)))
                         (catch Exception e
                           (println "Cannot format message: " (.getMessage e))
                           raw-message)))))
              (.getEvents result))
            (recur next-token (.getLogEvents client (req token)))))))))

(let [fmt (SimpleDateFormat. "HH:mm:ss")]
  (defn format-log-message
    "Format a run-of-the-mill AWS log event into a format that resembles a normal *NIX log message."
    [host {:keys [msg level timestamp ns line]}]
    (if (or ns line)
      (format "%s %s %6s %s:%d %s"
        host
        (if (instance? Date timestamp) (.format fmt timestamp) timestamp)
        (str/upper-case (or level "STDOUT"))
        ns
        line
        msg)
      (format "%s %s %6s %s"
        host
        (if (instance? Date timestamp) (.format fmt timestamp) timestamp)
        (str/upper-case (or level "STDOUT"))
        msg))))

(defn watch-log-events
  "Watch for log events on the given group-stream, and call `action` for each event seen.

  Never* returns.  *if you run this in a thread and leverage the `running-atom?`, then you can start/stop
  a background watch. See the source of `watch` in this ns.

  `running-atom?` - an atom you create that holds `true`. If you set that atom to false (alt thread) at any point then this function
                    will return.
  `group-stream` - One of the items returned from `recent-streams`
  `options` - A map containing:

  ** action - A `(fn [stream msg-map] ...)` that is called for each message. Assumed to side-effect. Defaults to printing the message.
  ** include-stdout? - Boolean. Default false. Include messages that do not have a level (are likely raw stdout)?
  "
  [running-atom?
   {:keys [group stream]}
   {:keys [action
           include-stdout?]
    :or   {include-stdout? false
           action          (fn [stream msg]
                             (println (format-log-message stream msg)))}}]
  (let [req (fn [token]
              (-> (GetLogEventsRequest.)
                (cond->
                  token (.withNextToken token))
                (.withStartTime (System/currentTimeMillis))
                (.withStartFromHead true)
                (.withLogGroupName group)
                (.withLogStreamName stream)))]
    (try
      (loop [token  nil
             result (.getLogEvents client (req nil))]
        (let [next-token (.getNextForwardToken result)]
          (cond
            (not @running-atom?) nil
            (= next-token token) (do
                                   (Thread/sleep 3000)
                                   (recur next-token (.getLogEvents client (req next-token))))
            :else (let [events (.getEvents result)]
                    (doseq [^OutputLogEvent evt events]
                      (try
                        (let [raw-message (parse-string (.getMessage evt) (comp keyword str/lower-case))
                              {:keys [level] :as msg} (try
                                                        (update raw-message :timestamp #(Date. (long %)))
                                                        (catch Exception _ raw-message))]
                          (when (or include-stdout? level)
                            (action stream msg)))
                        (catch Exception _ (println "log event not understood" (.getMessage evt)))))
                    (Thread/sleep 2000)
                    (recur next-token (.getLogEvents client (req token)))))))
      (catch Exception e
        (println "WATCHER DIED" (.getMessage e))))))

(defn watch
  "Start a thread for every stream on the given `log-group` that has seen activity in the last hour. This function
   is compatible with Clojure deps -X."
  [{:keys [log-group
           include-stdout?
           strip-prefix]}]
  (when-not (string? log-group)
    (println ":log-group required")
    (System/exit 1))
  (let [running? (atom true)
        streams  (recent-streams log-group)]
    (println "Found the following streams:\n" streams)
    (let [futures (mapv (fn [strm]
                          (future
                            (watch-log-events running? strm
                              {:include-stdout? (boolean include-stdout?)
                               :action          (fn [stream-name msg]
                                                  (let [nm (cond-> (str/replace stream-name #"-i-(.......).*$" "-$1")
                                                             strip-prefix (str/replace strip-prefix ""))]
                                                    (println (format-log-message nm msg))))})
                            true))
                    streams)]
      (doseq [f futures]
        (deref f)))))

(defn parse-date [s]
  (first
    (mapcat
      (fn [^DateGroup grp]
        (.getDates grp))
      (.parse (Parser.) s))))

(defn get-logs
  "Get the events from the given log-group over the specified time range, and call `action` `(fn [stream message-map])` on each
   (which is assumed to side-effect).

   log-group - The name of the log group to pull from
   start - The start time. Supports natural language (e.g. \"1 hour ago\"). See http://natty.joestelmach.com/try.jsp. Defaults to \"5 minutes ago\"
   end - The end time. Supports natural language. See http://natty.joestelmach.com/try.jsp. Default to \"now\".
   include-stdout? - Include messages that have no log level.

   If you use something like `1 pm` then the JVM will use your locale to properly offset the time. Use `1pm UTC` if you really
   mean UTC.

   This function must sort the resulting log messages in RAM, so do not grab too many log messages at once!
   "
  [{:keys [log-group
           start
           end
           include-stdout?]
    :or   {start "5 minutes ago"
           end   "now"}}]
  (let [start-inst         (parse-date start)
        end-inst           (parse-date end)
        streams            (recent-streams log-group)
        beginning-of-time  (Date. 0)
        compare-timestamps (fnil compare beginning-of-time beginning-of-time)]
    (when-not (and (inst? start-inst) (inst? end-inst))
      (throw (ex-info "Invalid start/end time" {})))
    (when-not (seq streams)
      (println "No streams found."))
    (sort-by :timestamp compare-timestamps
      (into []
        (comp
          (mapcat
            (fn [stream-group]
              (map
                (fn [m] (assoc m :stream (:stream stream-group)))
                (get-log-events stream-group (inst-ms start-inst) (inst-ms end-inst)))))
          (filter (fn [{:keys [level timestamp]}] (and timestamp (or level include-stdout?)))))
        streams))))


(defn show-logs
  "Like watch, but over a fixed time range. Default side-effect is to print to stdout.

   log-group - The name of the log group to pull from. Pulls from all streams that have recent (less than 1 hour ago) events.
   start - The start time. Supports natural language (e.g. \"1 hour ago\"). See http://natty.joestelmach.com/try.jsp. Defaults to \"5 minutes ago\"
   end - The end time. Supports natural language. See http://natty.joestelmach.com/try.jsp. Default to \"now\".
   include-stdout? - Include message that do not have a log level (appear to be from stdout)
   strip-prefix - When showing the log message, strip the given prefix from the stream name
   action - An (optional) `(fn [{:keys [stream msg type timestamp ...]}])`. Assumed to side-effect. Defaults to emitting
     the events to *out*.

  Side-effects on `action` for each event.
  "
  [{:keys [log-group
           start
           end
           include-stdout?
           strip-prefix
           action]
    :or   {start           "5 minutes ago"
           end             "now"
           include-stdout? false}
    :as   options}]
  (let [options (update options :include-stdout? boolean)
        action  (or action
                  (fn [{:keys [stream] :as msg}]
                    (let [nm (cond-> (str/replace stream #"-i-(.......).*$" "-$1")
                               strip-prefix (str/replace strip-prefix ""))]
                      (println (format-log-message nm msg)))))
        events  (get-logs options)]
    (doseq [evt events]
      (action evt))))

(comment
  (show-logs {:log-group    "datomic-dataico" :start "1pm" :end "1:01pm"
              :strip-prefix "dataico-dataico"})
  (parse-date "1pm UTC")
  )