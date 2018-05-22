(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [data.csv :as csv]
            ))

(defn- add-road-costs [roads-data road-costs]
  (keep
   (fn [{type :type
         class :class
         :as road}]
     (when-let [stuff (road-costs [type class])]
       (let [[new-class road-cost] stuff]
         (assoc road
                :unit-cost road-cost
                :subtype new-class))))
   roads-data))

(defn- parse-number
  "Reads a number from a string. Returns nil if not a number."
  [s]
  (if (re-find #"^-?\d+\.?\d*([Ee]\+\d+|[Ee]-\d+|[Ee]\d+)?$" (.trim s))
    (read-string s)))

(defn- connect [road-costs roads-path buildings-path roads-out buildings-out]
  (let [{roads-data ::geoio/features roads-crs ::geoio/crs}
        (geoio/load roads-path)

        ;; we need to preprocess the cost features onto the roads,
        ;; and delete roads which have infini-cost
        road-costs (with-open [reader (io/reader road-costs)]
                     (let [rows (csv/read-csv reader)
                           header (repeat (map keyword (first rows)))
                           data (rest rows)]
                       (doall (map zipmap header data))))

        ;; now we need to transform road-costs to be something we can
        ;; join to
        (->> road-costs
             (keep (fn [{type :osm.type
                         class :osm.class
                         subtype :classification
                         unit-cost :cse.cost.per.metre}]
                     (when-let [unit-cost (parse-number unit-cost)]
                       (when (and type class subtype)
                         [[type class] [subtype unit-cost]]))))
             (into {}))
        
        roads-data (add-road-costs roads-data road-costs)
        
        {buildings-data ::geoio/features buildings-crs ::geoio/crs}
        (geoio/load buildings-path)

        _ (when (not= roads-crs buildings-crs)
            (println "Warning: roads and buildings CRS differ"
                     roads-crs buildings-crs))

        noded-roads (spatial/node-paths roads-data)

        [buildings paths]
        (spatial/add-connections buildings-crs buildings-data noded-roads)

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
            (pprint (dissoc p ::geoio/geometry)))
          )))

    (geoio/save buildings buildings-out
                {"id"   {:value ::geoio/id :type "String"}
                 "orig_id" {:value :osm_id :type "String"}
                 
                 "name" {:value :name :type "String"}
                 "type" {:value (constantly "demand") :type "String"}
                 "subtype" {:value :type :type "String"}
                 
                 "area" {:value :area :type "Double"}
                 "demand" {:value :kwh_annual :type "Double"}

                 "geometry"
                 {:value ::geoio/geometry :type (format "Polygon:%s" (crs->srid buildings-crs))}
                 "connection_id"
                 {:value #(string/join "," (::spatial/connects-to-node %)) :type "String"}

                 })

    (geoio/save paths roads-out
                {"id" {:value ::geoio/id :type "String"}
                 "orig_id" {:value :osm_id :type "String"}
                 
                 "name" {:value :name :type "String"}
                 "type" {:value (constantly "path") :type "String"}
                 "subtype" {:value :subtype :type "String"}
                 
                 "length" {:value ::spatial/length :type "Double"}
                 "cost" {:value #(* (::spatial/length %) (:unit-cost %)) :type "Double"}

                 "geometry"
                 {:value ::geoio/geometry :type (format "LineString:%s" (crs->srid roads-crs))}
                 "start_id"
                 {:value (comp ::geoio/id ::spatial/start-node) :type "String"}
                 "end_id"
                 {:value (comp ::geoio/id ::spatial/end-node) :type "String"}

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

(defn -main [command & args]
  (case command
    "connect" (apply connect args)
    "dimension" (apply dimension args)
    "lidar" (apply lidar args)

    (println
"Usage:: <command> connect costs roads buildings roads-out buildings-out
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

;; (when false
;;   (defn gen [name] (connect (format "../good-bits/%s-roads.shp" name) (format  "../good-bits/%s-buildings.shp" name) (format  "../gen/%s-ways.geojson" name) (format  "../gen/%s-buildings.geojson" name)))

;;   (for [fi ["alba-iulia_romania"
;;             "granollers_spain"
;;             "jelgava_latvia"
;;             "lisbon_portugal"
;;             "warsaw_poland"
;;             "berlin_germany"
;;             "london_england"]]
;;     (do (println fi)
;;         (gen fi)))
;;   )

