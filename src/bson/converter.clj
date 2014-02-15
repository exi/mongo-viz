(ns bson.converter
  (:import [org.bson BasicBSONObject BSONEncoder BSONDecoder]))

(defonce *encoder* (BSONEncoder.))

(defonce *decoder* (BSONDecoder.))

;; XXX Does not accept keyword arguments. Convert clojure.lang.Keyword in map to java.lang.String first.
(defn map->bson [m]
  (->> m (BasicBSONObject.) (.encode *encoder*)))

(defn bson->map [^BasicBSONObject b]
  (->> (.readObject *decoder* b) (.toMap) (into {})))
