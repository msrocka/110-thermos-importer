(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            ))

(defn- connect [roads-path buildings-path roads-out buildings-out]
  (let [{roads-data ::geoio/features roads-crs ::geoio/crs}
        (geoio/load roads-path)

        {buildings-data ::geoio/features buildings-crs ::geoio/crs}
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

    (let [dodgy-paths (filter #(> (count (second %)) 1)
                              (group-by ::geoio/id paths))]
      (when (not-empty dodgy-paths)
        (println (count dodgy-paths) "paths are duplicated:")
        (doseq [[id paths] dodgy-paths]
          (println id)
          (doseq [p paths]
            (pprint p))
          )))

    (geoio/save buildings buildings-out
                {"id"
                 {:value ::geoio/id :type "String"}
                 "geometry"
                 {:value ::geoio/geometry :type (format "Polygon:%s" (crs->srid buildings-crs))}
                 "connector"
                 {:value #(string/join "," (::spatial/connects-to-node %)) :type "String"}
                 })

    (geoio/save paths roads-out
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

    (println "Finished!")))

(defn- dimension
  "Given shapes in file-in, write them out to file-out but with length and area properties in m2 and m"
  [file-in file-out]
  (println "not impl")
  )

(defn- lidar
  "Given shapes in SHAPES, and LIDAR data in VRT, stick height, surface and volume properties on shapes into SHAPES-OUT"
  [shapes vrt shapes-out]
  (println "not impl")
  )

(defn main- [command & args]
  (case command
    "connect" (apply connect args)
    "dimension" (apply dimension args)
    "lidar" (apply lidar args)

    (println
"Usage:: <command> connect roads buildings roads-out buildings-out
                   dimension file file-out
                   lidar shapes vrt out")))

;; other stuff we need
;; - add surface area from lidar
;; - add height from lidar
;; - add computed footprint
;;   - add fake surface area / height where no lidar
;; - add path lengths
;;   - add path / service intersections?
;; - add addresses (/ construct address table with related IDs)
