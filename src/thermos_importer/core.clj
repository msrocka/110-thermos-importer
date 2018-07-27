(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.lidar :as lidar]
            [thermos-importer.spatial :as spatial]
            [thermos-importer.overpass :as overpass]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.data.csv :as csv]

            [clojure.set :as set]))

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

(defn- connect-overpass [area-name buildings-out ways-out
                         & {:keys [path-costs
                                   benchmarks

                                   default-path-cost
                                   default-benchmark]
                            
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

    ;; TODO make CRS requirement for geoio, don't infer from SRID on geom
    ;; as that is not reliable.
    
    (geoio/write-to {::geoio/features buildings
                     ::geoio/crs "EPSG:4326"}
                    buildings-out)
    (geoio/write-to {::geoio/features ways
                     ::geoio/crs "EPSG:4326"}
                    ways-out)))

(defn- add-lidar
  [shapes-file lidar-directory shapes-out]

  (let [shapes (geoio/read-from shapes-file)
        
        lidar-files (->> (file-seq (io/file lidar-directory))
                         (filter #(and (.isFile %)
                                       (or (.endsWith (.getName %) ".tif")
                                           (.endsWith (.getName %) ".tiff")))))
        
        shapes (lidar/add-lidar-to-shapes lidar-files shapes)]

    (geoio/write-to shapes shapes-out)))

(defn- just-node
  [ways-file buildings-file
   ways-out buildings-out
   & {:keys [chunk-size omit-fields]
      :or {chunk-size nil}}
   ]
  (let [{ways ::geoio/features ways-crs ::geoio/crs}           (geoio/read-from ways-file)
        {buildings ::geoio/features buildings-crs ::geoio/crs} (geoio/read-from buildings-file)

        orig-way-fields (set/difference (set (mapcat keys ways))
                                        (set omit-fields))
        
        orig-building-fields (set/difference (set (mapcat keys buildings))
                                             (set omit-fields))

        _ (printf "%d buildings [%s], %d ways [%s]\n"
                  (count buildings) buildings-crs
                  (count ways) ways-crs)
        
        ways (spatial/node-paths ways)

        _ (printf "%d noded ways\n" (count ways))

        [buildings ways] (spatial/add-connections ways-crs buildings ways)

        _ (printf "%d connected ways\n" (count ways))
        
        buildings
        (for [b buildings]
          (merge (select-keys b orig-building-fields)
                 {:connection_id (string/join "," (::spatial/connects-to-node b))}))

        ways
        (for [w ways]
          (merge (select-keys w orig-way-fields)
                 {:length (::spatial/length w)
                  :start-id (::geoio/id (::spatial/start-node w))
                  :end-id (::geoio/id (::spatial/end-node w))}))
        ]
    (geoio/write-to {::geoio/features ways ::geoio/crs ways-crs} ways-out
                    :chunk-size chunk-size)
    
    (geoio/write-to {::geoio/features buildings ::geoio/crs buildings-crs} buildings-out
                    :chunk-size chunk-size)))

(defn run-with-arguments [command options check-positional arguments]
  (let [{:keys [options arguments errors summary]} (parse-opts arguments options)
        errors (seq (filter identity (concat errors (check-positional arguments))))
        ]
    (cond (:help options)
          (do (println summary))
          
          errors
          (do (doseq [e errors]
                (println "Error:" e))
              (println)
              (println summary))

          :default
          (apply command (concat arguments (apply concat options))))))

(defn file-exists [file]
  (when-not (.exists (io/as-file file))
    (format "file %s does not exist" file)))

(defn file-not-exists [file]
  (when (.exists (io/as-file file))
    (format "file %s already exists" file)))

(defn -main [& args]
  (let [[com & args] args]
    (case com
      "overpass"
      (run-with-arguments
       connect-overpass
       [[nil "--path-costs PATH-COSTS" "Road costs file"
         :missing "A road costs file is required"
         :validate [#(.exists (io/as-file %))
                    "The path costs file must exist"]
         ]
        [nil "--benchmarks BENCHMARKS" "Benchmarks file"
         :missing "A benchmarks file is required"
         :validate [#(.exists (io/as-file %))
                    "The benchmarks file must exist"]]
        [nil "--default-path-cost DEFAULT-PATH-COST" "Cost of unknown road type"
         :parse-fn #(Double/parseDouble %)
         :default 1000]
        [nil "--default-benchmark DEFAULT-BENCHMARK" "Unit demand of unknown building type"
         :default 1000
         :parse-fn #(Double/parseDouble %)]]
       #(if (= 3 (count %))
          [(file-exists (nth % 1))
           (file-not-exists (nth % 2))]
          ["Required arguments: <area name> <buildings output> <ways output>"])
       args)

      "lidar"
      (run-with-arguments
       add-lidar
       []
       #(if (= 3 (count %))
          [(file-exists (nth % 0))
           (file-exists (nth % 1))
           (file-not-exists (nth % 2))]
          ["Required arguments: <shapefile in> <lidar directory> <shapefile out>"])
       args)

      "node"
      (run-with-arguments
       just-node
       [[nil "--omit-field FIELD" "Leave out the given field"
         :id :omit-fields
         :assoc-fn
         (fn [m k v]
           (update m k conj v))]
        [nil "--chunk-size CHUNK-SIZE" "Make geojson into chunks of this size"
         :parse-fn #(Integer/parseInt %)]]
       #(if (= 4 (count %))
          [(file-exists (nth % 0))
           (file-exists (nth % 1))
           (file-not-exists (nth % 2))
           (file-not-exists (nth % 3))
           ]
          ["Required arguments: <ways input> <buildings input> <ways output> <buildings output>"])
       args)
      
      
      (do (when com (println "Unknown command" com))
          (println "Usage: <this command> overpass | lidar | node")))))


