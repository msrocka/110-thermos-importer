(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]))

(defn node
  "Node the buildings & roads, emitting a geojson output file"
  [buildings roads output]

  (let [buildings (geoio/load buildings)
        paths (geoio/load roads)
        connections (spatial/add-connections buildings paths)
        noding (spatial/node-paths (concat paths connections))
        ]

    (geoio/save (concat buildings noding) output)))

(defn main- [command & args]
  (case command
    "node" (apply node args)
    (println "Subcommands: node buildings roads outputfile")))
