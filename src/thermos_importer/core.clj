(ns thermos-importer.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.lidar :as lidar]
            [thermos-importer.spatial :as spatial]
            [thermos-importer.overpass :as overpass]
            [thermos-importer.svm-predict :as svm]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [clojure.data.csv :as csv]

            [clojure.set :as set]
            [clojure.data.json :as json]))

(defn crs->srid [crs]
  (if (and crs (.startsWith crs "EPSG:"))
    (string/replace crs "EPSG:" "srid=")
    (do (println "Unknown authority" crs) "srid=4326")))

(defn- try-parse-double [s]
  (try (Double/parseDouble s)
       (catch NumberFormatException e s)))

(defn- csv-file->map [path]
  (with-open [reader (io/reader path)]
    (let [rows (csv/read-csv reader)
          header (repeat (map keyword (first rows)))
          data (rest rows)
          data (map data (partial map try-parse-double))
          ]
      (doall (map zipmap header data)))))

(defn- assoc-by [f s]
  (reduce #(assoc %1 (f %2) %2)) {} s)

(defn- connect-overpass [area-name buildings-out ways-out
                         & {:keys [path-subtypes
                                   building-subtypes]}
                         ]
  (let [path-subtypes
        (if path-subtypes
          (-> path-subtypes
              csv-file->map
              assoc-by :highway)
          {})

        building-subtypes
        (if building-subtypes
          (-> path-subtypes
              csv-file->map
              assoc-by :building)
          {})

        update-subtype
        (fn [m {st :subtype :as thing}]
          (assoc thing
                 :subtype (or (get m st) st)))

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
        (map (partial update-subtype path-subtypes) ways)

        buildings
        (map (partial update-subtype building-subtypes) buildings)
        
        buildings
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm_id
               name :name
               subtype :subtype
               area ::spatial/area
               connections ::spatial/connects-to-node}
              buildings]
          {:id id
           :orig_id osm-id
           :name name
           :type "building"
           :subtype (if (or (nil? subtype) (= "" subtype)) nil subtype)
           :area area
           :connection_id (string/join "," connections)
           :geometry geometry})

        ways
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm_id
               name :name
               subtype :subtype
               length ::spatial/length
               {start-id ::geoio/id} ::spatial/start-node
               {end-id ::geoio/id} ::spatial/end-node}
              ways]
          {:id id
           :orig_id osm-id
           :type "path"
           :subtype (if (or (nil? subtype) (= "" subtype)) nil subtype)
           :length length
           :start-id start-id
           :end-id end-id
           :geometry geometry}
          )
        ]

    (println "building subtypes" (frequencies (map :subtype buildings)))

    ;; TODO make CRS requirement for geoio, don't infer from SRID on geom
    ;; as that is not reliable.
    
    (geoio/write-to {::geoio/features buildings
                     ::geoio/crs "EPSG:4326"}
                    buildings-out)
    (geoio/write-to {::geoio/features ways
                     ::geoio/crs "EPSG:4326"}
                    ways-out)))

(def default-path-cost {:cost-per-m 500 :cost-per-kwm 20})

(defn- path-cost-fn
  "Construct a path cost model from a string.

  If it names a file, assumed to be a CSV file with a subtype column
  and cost-per-m and cost-per-kwm columns. Joins on these.
  "
  [path-costs]
  (cond
    (and path-costs (.exists (io/file path-costs)))
    (let [path-costs (->> path-costs csv-file->map (assoc-by :subtype))]
      (fn [x]
        (select-keys (get path-costs (:subtype x)
                          default-path-cost)
                     [:cost-per-m :cost-per-kwm])))
    :default (constantly default-path-cost)))

(defn- demand-fn
  "Construct a demand model from a string.
  If the string names a json file, assumed to be a support-vector regression
  "
  [demands]
  (cond
    (and demands (.exists (io/file demands))
         (.endsWith demands ".json"))
    (let [model
          (with-open [r (io/file demands)]
            (svm/predictor (json/read r :key-fn keyword)))]
      (fn [x]
        {:demand-kwh-per-year (model x)})))
  
  :default (constantly {:demand 1000}))

(defn- add-estimates [shapes-file output-file
                      & {:keys [path-costs demands]}]
  (let [path-cost (path-cost-fn path-costs)
        demand    (demand-fn demands)

        add-estimate
        (fn [thing]
          (case (:type thing)
            "path" (merge thing (path-cost thing))
            "building" (merge thing (demand thing))
            thing))
        ]
    (-> (geoio/read-from shapes-file)
        (update ::geoio/features #(map add-estimate %))
        (geoio/write-to output-file))))

(defn- explode-multipolygons [shapes]
  (let [explode-geometry
        (fn [thing]
          (case (::geoio/type thing)
            :multi-polygon
            (let [geom (::geoio/geometry thing)]
              (for [n (range (.getNumGeometries geom))]
                (assoc thing ::geoio/geometry (.getGeometryN thing n))))
            
            [thing]))]
    (update shapes
            ::geoio/features
            #(mapcat explode-geometry %))))

(defn- add-lidar
  [shapes lidar-directory shapes-out &
   {:keys [buffer-size ground-level-threshold]
    :or {buffer-size 1.5 ground-level-threshold -5}}]

  (let [shape-files (->> (file-seq (io/file shapes))
                         (filter #(and (.isFile %)
                                       (.endsWith (.getName %) ".shp"))))
        
        lidar-files (->> (file-seq (io/file lidar-directory))
                         (filter #(and (.isFile %)
                                       (or (.endsWith (.getName %) ".tif")
                                           (.endsWith (.getName %) ".tiff")))))

        lidar-index (lidar/rasters->index lidar-files)

        output-location (io/file shapes-out)

        get-output-path
        (fn [input-file]
          (io/file output-location (.replaceAll (.getName input-file)
                                                "\\.shp$"
                                                ".geojson")))
        ]
    
    (when (not (.exists output-location))
      (.mkdirs output-location)
      (printf "creating output directory %s\n" output-location))
    
    (doseq [shape-file shape-files]
      (let [output-path (get-output-path shape-file)
            ]
        (printf "%s -> %s\n" shape-file output-path)
        (-> shape-file
            (geoio/read-from)
            (explode-multipolygons)
            (lidar/add-lidar-to-shapes
             lidar-index
             :buffer-size buffer-size
             :ground-level-threshold ground-level-threshold)
            (geoio/write-to output-path)))))
  )

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

(defn directory-or-missing [file]
  (when (and (.exists (io/as-file file))
             (.isFile (io/as-file file)))
    (format "file %s already exists and is not a directory" file)))

(defn -main [& args]
  
  (binding [*out* *err*]
    (let [[com & args] args]
      (case com
        "overpass"
        (run-with-arguments
         connect-overpass
         [[nil "--path-subtypes PATH-SUBTYPES" "A table mapping overpass highway to subtype"
           :missing "A road subtype file is required"
           :validate [#(.exists (io/as-file %))
                      "The path subtype file must exist"]]
          
          [nil "--building-subtypes BUILDING-SUBTYPES" "Building subtypes mapping file"
           :missing "A building subtypes file is required"
           :validate [#(.exists (io/as-file %))
                      "The building subtypes file must exist"]]]
         
         #(if (= 3 (count %))
            [(file-exists (nth % 1))
             (file-not-exists (nth % 2))]
            ["Required arguments: <area name> <buildings output> <ways output>"])
         args)

        "estimate"
        (run-with-arguments
         add-estimates
         [[nil "--path-costs PATH-COSTS-TABLE"]
          [nil "--demands DEMAND-MODEL"]]
         #(if (not= 2 (count %))
            ["Required arguments: <input file> <output file>"]))
        
        "lidar"
        (run-with-arguments
         add-lidar
         [[nil "--buffer-size BUFFER-SIZE"
           :parse-fn #(Double/parseDouble %)
           :default 1.5]
          [nil "--ground-level-threshold THRESHOLD"
           :parse-fn #(Double/parseDouble %)
           :default -5.0
           ]]
         
         #(if (= 3 (count %))
            [(file-exists (nth % 0))
             (file-exists (nth % 1))
             (directory-or-missing (nth % 2))
             ]
            ["Required arguments: <shapefile|directory in> <lidar directory> <shapefile|directory out>"])
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
            (println "Usage: <this command> overpass | lidar | node | estimate"))))))


