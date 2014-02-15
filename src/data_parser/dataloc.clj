(ns data-parser.dataloc
  (:require [data-parser.reader :as reader])
  (:use data-parser.readerProto))

(defrecord DataLoc [file-number offset])

(defn from-reader [r]
  (let [file-number (read-int r)
        offset (read-int r)]
    (map->DataLoc {:file-number file-number
                   :offset offset})))

(defn create
  ([filename] (create filename 0))
  ([filename offset]
   (let [r (reader/open filename)]
     (seek r offset)
     (from-reader r))))
