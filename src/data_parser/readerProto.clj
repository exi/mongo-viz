(ns data-parser.readerProto
  (:refer-clojure :exclude (read-string)))

(defprotocol FileReaderProtocol
  (seek [this offset])
  (read-byte [this])
  (read-bytes [this length])
  (read-int [this])
  (read-uint [this])
  (read-string [this length])
  (read-bson [this length])
  (offset [this])
  (close [this]))
