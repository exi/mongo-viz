(ns data-parser.reader
  (:import (java.io.RandomAccessFile))
  (:require [data-parser.readerProto :as rep]
            [bson.converter :as bson]))

(defn two-complement->int [inbytes]
  (apply bit-or
         (map
          (fn [b idx]
            (bit-shift-left
             (if (not= (dec (count inbytes)) idx) (bit-and b 16rff) b)
             (* idx 8)))
          inbytes
          (range (count inbytes)))))

(defn two-complement->uint [inbytes]
  (apply bit-or
         (map
          (fn [b idx]
            (bit-shift-left
             (bit-and b 16rff)
             (* idx 8)))
          inbytes
          (range (count inbytes)))))

(defn do-read-int [file] (two-complement->int (rep/read-bytes file 4)))
(defn do-read-uint [file] (two-complement->uint (rep/read-bytes file 4)))

(defrecord FileReader [file]
  rep/FileReaderProtocol
  (seek [this offset] (.seek file offset))
  (read-byte [this] (.readByte file))
  (read-bytes [this length]
              (let [buffer (byte-array (int length))]
                (.readFully file buffer)
                buffer))
  (read-int [this] (do-read-int this))
  (read-uint [this] (do-read-uint this))
  (read-string [this length] (apply str (map char (rep/read-bytes this length))))
  (read-bson [this length] (bson.converter/bson->map (rep/read-bytes this length)))
  (offset [this] (.getFilePointer file))
  (close [this] (.close file)))

(defn open [filename]
  (FileReader. (new java.io.RandomAccessFile filename "r")))
