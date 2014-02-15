(ns data-parser.datafile
  (:require [data-parser.reader :as reader]
            [data-parser.dataloc :as dataloc]
            [data-parser.extent :as extent]
            [data-parser.readerProto :as rep]
            [data-parser.datafileProto :as dfp]
            [data-parser.extentProto :as extp]))


;what i want from datafile-header:
(def demo-datafile-header {:version 1
                           :versionMinor 20
                           :filelength 100000
                           :unused {:file-number 1
                                    :offset 1000}
                           :unusedLength 1000000
                           :free-list-start {:file-number 3
                                             :offset 100}
                           :free-list-end {:file-number 3
                                           :offset 500}
                           :reserved [:bitmap-here]
                           :first-extent 12314
                           })
;what i want from datafile:
(def demo-datafile {:header demo-datafile-header
                    :extents [:seq-of-extents]})

(defrecord DataFileHeader
  [version
   version-minor
   file-length
   unused
   unused-length
   free-list-start
   free-list-end
   reserved
   data-offset])

(defn ok? [df]
  (> (get-in df [:header :version]) 0))

(defn extents-list [ext]
  (cons ext
        (if (neg? (get-in ext [:header :x-next :file-number]))
          '()
          (lazy-seq (extents-list (extp/next ext))))))

(defrecord DataFile [filename header]
  dfp/DataFileProtocol
  (first-extent [this]
                (when (ok? this)
                  (extent/create filename (:data-offset header))))
  (extents [this]
           (when (ok? this)
             (extents-list (dfp/first-extent this)))))

(defn create-header
  ([filename]
   (let [r (reader/open filename)
         _ (rep/seek r 0)
         version (rep/read-int r)
         version-minor (rep/read-int r)
         file-length (rep/read-int r)
         unused (dataloc/from-reader  r)
         unused-length (rep/read-int r)
         free-list-start (dataloc/from-reader r)
         free-list-end (dataloc/from-reader r)
         reserved (rep/read-bytes r (- 8192 (* 4 4) (* 8 3)))
         data-offset (rep/offset r)]
     (rep/close r)
     (map->DataFileHeader {:version version
                           :version-minor version-minor
                           :file-length file-length
                           :unused unused
                           :unused-length unused-length
                           :free-list-start free-list-start
                           :free-list-end free-list-end
                           :reserved reserved
                           :data-offset data-offset}))))

(defn create [filename]
  (let [header (create-header filename)]
    (map->DataFile {:filename filename
                    :header header})))
