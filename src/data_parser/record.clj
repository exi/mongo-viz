(ns data-parser.record
  (:require [data-parser.reader :as reader]
            [data-parser.recordProto :as recp]
            [data-parser.readerProto :as rep]))

(def demo-record-header {:length-with-headers 1000
                         :extent-offset 123
                         :next-offset 1234
                         :prev-offset 1234
                         :data-offset 2344})

(defrecord RecordHeader
  [length-with-headers
   length
   extent-offset
   next-offset
   prev-offset
   data-offset])

(declare create)

(defrecord Record
  [filename
   header]
  recp/RecordProtocoll
  (next [this] (create filename (:next-offset header)))
  (prev [this] (create filename (:prev-offset header)))
  (data [this] (let [r (reader/open filename)
                     _ (rep/seek r  (:data-offset header))
                     data (rep/read-bson r (:length header))]
                 (rep/close r)
                 data)))

(defn create-header
  [filename offset]
  (let [r (reader/open filename)
        _ (rep/seek r offset)
        length-with-headers (rep/read-int r)
        extent-offset (rep/read-int r)
        next-offset (rep/read-int r)
        prev-offset (rep/read-int r)
        data-offset (rep/offset r)]
    (rep/close r)
    (map->RecordHeader {:length-with-headers length-with-headers
                        :length (- length-with-headers (- data-offset offset))
                        :extent-offset extent-offset
                        :next-offset next-offset
                        :prev-offset prev-offset
                        :data-offset data-offset})))

(defn create [filename offset]
  (let [header (create-header filename offset)]
    (map->Record {:filename filename
                  :header header})))
