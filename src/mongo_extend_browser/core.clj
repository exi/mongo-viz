(ns mongo-extend-browser.core
  (:gen-class)
  (:require [bson.converter :as bson]))

(defn -main []
  (let [m {"foo" "bar"}]
    (bson/map->bson m)))
