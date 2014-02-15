(ns data-parser.datafileProto)

(defprotocol DataFileProtocol
  (first-extent [this])
  (extents [this]))
