(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [clojure.string :as string]
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


(def roads-path "/home/hinton/p/110-thermos/thermos-data/mapzen-imposm-extracts/jelgava_latvia.imposm-shapefiles/ex_FjNeineFVwDNEswTyLN1KbXVA5Jt7_osm_roads.shp")

(def buildings-path "/home/hinton/p/110-thermos/thermos-data/mapzen-imposm-extracts/jelgava_latvia.imposm-shapefiles/ex_FjNeineFVwDNEswTyLN1KbXVA5Jt7_osm_buildings.shp")

(let [{roads-data ::geoio/features
       roads-crs ::geoio/crs}
      (geoio/load roads-path)

      {buildings-data ::geoio/features
       buildings-crs ::geoio/crs}
      (geoio/load buildings-path)

      _ (when (not= roads-crs buildings-crs)
          (println "Warning: roads and buildings CRS differ"
                   roads-crs buildings-crs))

      noded-roads (spatial/node-paths roads-data)

      [buildings paths]
      (spatial/add-connections buildings-data noded-roads)

      crs->srid
      (fn [crs]
        (if (and crs (.startsWith crs "EPSG:"))
          (string/replace crs "EPSG:" "srid=")
          (do (println "Unknown authority" crs) "srid=4326")))
      ]

  (geoio/save buildings "/home/hinton/temp/buildings.geojson"
              {"id"
               {:value ::geoio/id :type "String"}
               "geometry"
               {:value ::geoio/geometry :type (format "Polygon:%s" (crs->srid buildings-crs))}
               "connector"
               {:value #(string/join "," (::spatial/connects-to-node %)) :type "String"}
               })

  (geoio/save paths "/home/hinton/temp/paths.geojson"
              {"id"
               {:value ::geoio/id :type "String"}
               "geometry"
               {:value ::geoio/geometry :type (format "LineString:%s" (crs->srid roads-crs))}
               "start"
               {:value (comp ::geoio/id ::spatial/start-node) :type "String"}
               "end"
               {:value (comp ::geoio/id ::spatial/end-node) :type "String"}
               "type"
               {:value :type :type "String"}
               })

  (println "Finished!")
  )

;; other stuff we need
;; - add surface area from lidar
;; - add height from lidar
;; - add computed footprint
;;   - add fake surface area / height where no lidar
;; - add path lengths
;;   - add path / service intersections?
;; - add addresses (/ construct address table with related IDs)
