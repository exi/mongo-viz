(ns data-parser.recordProto
  (:refer-clojure :exclude (next)))

(defprotocol RecordProtocoll
  (next [this])
  (prev [this])
  (extent [this])
  (data [this]))
