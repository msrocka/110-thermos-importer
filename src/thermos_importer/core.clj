(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [thermos-importer.overpass :as overpass]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.data.csv :as csv]
            ))

(defn crs->srid [crs]
  (if (and crs (.startsWith crs "EPSG:"))
    (string/replace crs "EPSG:" "srid=")
    (do (println "Unknown authority" crs) "srid=4326")))

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

(defn buildings-fields [crs]
  {"id" {:value ::geoio/id :type "String"}
   "orig_id" {:value :osm_id :type "String"}
   "name" {:value :name :type "String"}
   "type" {:value (constantly "demand") :type "String"}
   "subtype" {:value
              #(let [x (:subtype %)]
                 (if (or (nil? x) (= "" x)) nil x))
              :type "String"}
   "area" {:value :area :type "Double"}
   "demand" {:value :kwh_annual :type "Double"}
   "connection_id"
   {:value #(string/join "," (::spatial/connects-to-node %))
    :type "String"}
   
   "geometry" {:value ::geoio/geometry
               :type (format "Polygon:%s" (crs->srid crs))}
   })

(defn ways-fields [crs]
  (let [length*cost
        (fn [{l ::spatial/length c :unit-cost}]
          (* l c ))
        ]
    (merge
     (select-keys (buildings-fields crs) ["id" "orig_id" "name" "subtype"])
     {"length" {:value ::spatial/length :type "Double"}
      "cost" {:value length*cost :type "Double"}
      "geometry" {:value ::geoio/geometry
                  :type (format "LineString:%s" (crs->srid crs))}
      "start_id" {:value (comp ::geoio/id ::spatial/start-node)
                  :type "String"}

      "end_id" {:value (comp ::geoio/id ::spatial/end-node)
                :type "String"}
      })))

(defn- csv-file->map [path]
  (with-open [reader (io/reader path)]
    (let [rows (csv/read-csv reader)
          header (repeat (map keyword (first rows)))
          data (rest rows)]
      (doall (map zipmap header data)))))

(defn- numberize [k0 m]
  ;; there must be a better way to do this operation
  (into {}
        (for [[k v] m]
          [k (update v k0
                     #(try (Double/parseDouble %)
                           (catch NumberFormatException e)))])))

(defn- connect-overpass [& {:keys [area-name
                                   path-costs benchmarks

                                   default-path-cost
                                   default-benchmark
                                   
                                   buildings-out ways-out]
                            :or {default-path-cost 1000
                                 default-benchmark 1000}
                            }]

  (let [path-costs (->> path-costs
                        (csv-file->map)
                        (group-by :highway)
                        (map #(update % 1 first))
                        (into {})
                        (numberize :unit-cost))
        
        benchmarks (->> benchmarks
                        (csv-file->map)
                        (group-by :building)
                        (map #(update % 1 first))
                        (into {})
                        (numberize :benchmark))
        
        add-path-cost
        (fn [{st :subtype :as path}]
          (let [{unit-cost :unit-cost subtype :subtype} (path-costs st)]
            (assoc path
                   :unit-cost (or unit-cost default-path-cost)
                   :subtype   (or subtype st))))

        add-benchmark
        (fn [{st :subtype :as building}]
          (let [{benchmark :benchmark subtype :subtype} (benchmarks st)]
            (assoc building
                   :benchmark (or benchmark default-benchmark)
                   :subtype   (or subtype st))))

        crs "EPSG:4326"

        _ (println "Querying overpass")
        
        query-results (overpass/get-geometry area-name)

        ;; split into buildings and ways
        {ways :line-string buildings :polygon}
        (group-by ::geoio/type query-results)

        buildings (filter :building buildings)

        _ (println (count ways) "ways" (count buildings) "buildings")

        ways
        (spatial/node-paths ways)
        
        _ (println (count ways) "noded ways")
        
        [buildings ways]
        (spatial/add-connections crs buildings ways)

        ways
        (map add-path-cost ways)

        buildings
        (map add-benchmark buildings)
        
        _ (println (count ways) "connected ways")
        ]


    (println "building subtypes" (frequencies (map :subtype buildings)))

    (geoio/save buildings buildings-out
                (assoc-in (buildings-fields crs)
                          ["demand" :value]
                          (fn [{b :benchmark a ::spatial/area :as x}]
                            (* b a))))
    (geoio/save ways ways-out (ways-fields crs))))

(defn- lidar
  [shapes-file lidar-files shapes-out]
  ;; first of all we want to build an index for the bounding regions
  ;; of the lidar files, in an appropriate CRS
  
  
  (println "not impl"))

