(ns data-parser.extent
  (:require [data-parser.dataloc :as dataloc]
            [data-parser.reader :as reader]
            [data-parser.record :as record]
            [data-parser.extentProto :as extp]
            [data-parser.readerProto :as rep]
            [data-parser.recordProto :as recp]))

;what i want:
(def demo-extent-header {:magic 5
                         :my-loc {:file-number 3
                                  :offset 2048}
                         :x-next {:file-number 3
                                  :offset 4048}
                         :x-prev {:file-number 3
                                  :offset 1048}
                         :namespace "blafoo\0"
                         :length 123123
                         :first-record {:file-number 3
                                        :offset 1048}
                         :last-record {:file-number 3
                                       :offset 1048}
                         :data-offset 123123})

(defrecord ExtentHeader
  [magic
   my-loc
   x-next
   x-prev
   namespace
   length
   first-record
   last-record
   data-offset])

(declare create)

(defn records-list [rec]
  (cons rec
        (if (neg? (get-in rec [:header :next-offset]))
          '()
          (lazy-seq (records-list (recp/next rec))))))

(defrecord Extent
  [filename header]
  extp/ExtentProtocol
  (next [this] (create filename (get-in header [:x-next :offset])))
  (prev [this] (create filename (get-in header [:x-prev :offset])))
  (first-record [this] (let [rec (:first-record header)
                             file (:file-number rec)
                             offs (:offset rec)]
                         (when-not (neg? file)
                           (record/create filename offs))))
  (records [this] (when-let [fr (extp/first-record this)]
                    (records-list fr))))

(defn create-header
  [filename offset]
  (let [r (reader/open filename)
        _ (rep/seek r offset)
        magic (rep/read-uint r)
        my-loc (dataloc/from-reader r)
        x-next (dataloc/from-reader r)
        x-prev (dataloc/from-reader r)
        namespace (rep/read-string r 128)
        length (rep/read-int r)
        first-record (dataloc/from-reader r)
        last-record (dataloc/from-reader r)
        data-offset (rep/offset r)]
    (rep/close r)
    (map->ExtentHeader {:magic magic
                        :my-loc my-loc
                        :x-next x-next
                        :x-prev x-prev
                        :namespace namespace
                        :length length
                        :first-record first-record
                        :last-record last-record
                        :data-offset data-offset})))

(defn create [filename offset]
  (let [header (create-header filename offset)]
    (map->Extent {:filename filename
                  :header header})))
