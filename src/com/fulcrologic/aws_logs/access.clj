(ns com.fulcrologic.aws-logs.access
  (:require
    [cheshire.core :refer [parse-string]]
    [taoensso.timbre :as log]
    [clojure.string :as str])
  (:import (com.amazonaws.services.logs AWSLogsClientBuilder AWSLogsClient)
           (com.amazonaws.services.logs.model DescribeLogGroupsResult LogGroup DescribeLogStreamsRequest LogStream DescribeLogStreamsResult GetLogEventsRequest GetLogEventsResult OutputLogEvent)
           (java.util Date)
           (java.text SimpleDateFormat)))

(log/set-level! :info)

(def ^AWSLogsClient client (AWSLogsClientBuilder/defaultClient))

(defn describe-log-streams
  ([group] (describe-log-streams group nil nil))
  ([group prefix] (describe-log-streams group prefix nil))
  ([^String group ^String prefix ^String token]
   (let [req (cond-> (DescribeLogStreamsRequest. group)
               (seq prefix) (.withLogStreamNamePrefix prefix)
               (not (seq prefix)) (.withOrderBy "LastEventTime")
               (not (seq prefix)) (.withDescending true)
               (seq token) (.withNextToken token))]
     (.describeLogStreams client req))))

(defn recent-streams [log-group]
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

(defn get-log-events [{:keys [group stream]} start-time end-time]
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
                     (let [raw-message (parse-string (.getMessage evt) (comp keyword str/lower-case))]
                       (try
                         (update raw-message :timestamp #(Date. (long %)))
                         (catch Exception e
                           (log/error e)
                           raw-message)))))
              (.getEvents result))
            (recur next-token (.getLogEvents client (req token)))))))))

(let [fmt (SimpleDateFormat. "HH:mm:ss")]
  (defn format-log-message [host {:keys [msg level timestamp ns line]}]
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

(defn watch-log-events [running-atom? {:keys [group stream]} {:keys [action
                                                                     include-stdout?]
                                                              :or   {include-stdout? false
                                                                     action          (fn [msg]
                                                                                       (println (format-log-message "" msg)))}}]
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
                      (let [raw-message (parse-string (.getMessage evt) (comp keyword str/lower-case))
                            {:keys [level] :as msg} (try
                                                      (update raw-message :timestamp #(Date. (long %)))
                                                      (catch Exception _ raw-message))]
                        (when (or include-stdout? level)
                          (action msg))))
                    (Thread/sleep 2000)
                    (recur next-token (.getLogEvents client (req token)))))))
      (catch Exception e
        (log/error e "WATCHER DIED")))))


(comment
  (recent-streams "datomic-dataico")

  (def running-atom? (atom true))

  (doseq [{:keys [group stream] :as strm} (recent-streams "datomic-dataico")
          :let [nm (-> stream
                     (str/replace #"-i-(.......).*$" "-$1")
                     (str/replace #"^dataico-dataico-" ""))]]
    (future
      (watch-log-events running-atom? strm {:action (fn [msg]
                                                      (println (format-log-message nm msg)))})))

  (reset! running-atom? false)

  (let [start  (- (System/currentTimeMillis) 10000)
        end    (- (System/currentTimeMillis) 1000)
        events (get-log-events
                 {:group  "datomic-dataico"
                  :stream "dataico-dataico-main-query-group-i-0ec48f1ab10a8d14a-2021-03-18-15-00-07-"}
                 start
                 end)]
    (doseq [evt events]
      (println (format-log-message evt))
      )
    ))