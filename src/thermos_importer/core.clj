(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            ))

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


(def roads-path "/home/hinton/p/110-thermos/thermos-data/mapzen-imposm-extracts/alba-iulia/ex_YDUybxrn56x9Efj6qD4G5m1gP8CmW_osm_roads.shp")

(def buildings-path "/home/hinton/p/110-thermos/thermos-data/mapzen-imposm-extracts/alba-iulia/ex_YDUybxrn56x9Efj6qD4G5m1gP8CmW_osm_buildings.shp")

(def roads-data (geoio/load roads-path))
(def buildings-data (geoio/load buildings-path))

(def noded-roads (spatial/node-paths roads-data))

(def connected-data (spatial/add-connections buildings-data noded-roads))

(let [[buildings _] connected-data]

  (doseq [b buildings]
    (println (::geoio/id b) (::spatial/connects-to-node b))
    )
  )

(def road-index (spatial/features->index noded-roads))

(def index-first (spatial/features->index [(first noded-roads)]))
