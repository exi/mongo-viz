(ns data-parser.extentProto
  (:refer-clojure :exclude (next)))

(defprotocol ExtentProtocol
  (next [this])
  (prev [this])
  (first-record [this])
  (records [this]))
