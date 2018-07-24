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

        _ (println (count ways) "connected ways")
        
        ways
        (map add-path-cost ways)

        buildings
        (map add-benchmark buildings)
        
        buildings
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm_id
               name :name
               subtype :subtype
               area ::spatial/area
               benchmark :benchmark
               connections ::spatial/connects-to-node}
              buildings]
          {:id id
           :orig_id osm-id
           :name name
           :type "demand"
           :subtype (if (or (nil? subtype) (= "" subtype)) nil subtype)
           :area area
           :benchmark benchmark
           :demand (* area benchmark)
           :connection_id (string/join "," connections)
           :geometry geometry})

        ways
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm_id
               name :name
               subtype :subtype
               length ::spatial/length
               unit-cost :unit-cost
               {start-id ::geoio/id} ::spatial/start-node
               {end-id ::geoio/id} ::spatial/end-node}
              
              ways]
          {:id id
           :orig_id osm-id
           :type "path"
           :subtype (if (or (nil? subtype) (= "" subtype)) nil subtype)
           :length length
           :unit-cost unit-cost
           :cost (* length unit-cost)
           :start-id start-id
           :end-id end-id
           :geometry geometry}
          )
        ]

    (def last-buildings buildings)
    (def last-ways ways)
    (println "building subtypes" (frequencies (map :subtype buildings)))

    (geoio/write-to buildings buildings-out)
    (geoio/write-to ways ways-out)))

(defn- lidar
  [shapes-file lidar-files shapes-out]
  )


(defn- just-node
  [ways-file buildings-file
   ways-out buildings-out
   ]
  (let [{ways ::geoio/features ways-crs ::geoio/crs}           (geoio/read-from ways-file)
        {buildings ::geoio/features buildings-crs ::geoio/crs} (geoio/read-from buildings-file)

        orig-way-fields (set (mapcat keys ways))
        orig-building-fields (set (mapcat keys buildings))
        
        ways (spatial/node-paths ways)

        [buildings ways] (spatial/add-connections ways-crs buildings ways)

        buildings
        (for [b buildings]
          (merge (select-keys b orig-building-fields)
                 {:connection_id (string/join ","
                                              (::spatial/connects-to-node b))}))

        ways
        (for [w ways]
          (merge (select-keys w (orig-way-fields))
                 {:start-id (::geoio/id (::spatial/start-node w))
                  :end-id (::geoio/id (::spatial/end-node w))}))
        ]

    (geoio/write-to ways ways-out)
    (geoio/write-to buildings buildings-out)))
