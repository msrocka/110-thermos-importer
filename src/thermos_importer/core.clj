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
            [clojure.data.json :as json]
            [thermos-importer.util :as util]
            [thermos-importer.util :refer [has-extension]])
  (:import [org.locationtech.jts.geom Geometry]))

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
          ]
      (doall (map zipmap header data)))))

(defn- assoc-by [s f]
  (reduce #(assoc %1 (f %2) %2)  {} s))

(defn- connect-overpass [area-name buildings-out ways-out
                         & {:keys [path-subtypes
                                   building-subtypes
                                   resi-subtypes
                                   overpass-api
                                   ]}
                         ]
  (let [path-subtypes
        (if path-subtypes
          (-> path-subtypes
              csv-file->map
              (assoc-by :highway))
          {})

        building-subtypes
        (if building-subtypes
          (-> building-subtypes
              csv-file->map
              (assoc-by :building))
          {})

        resi-subtypes
        (set
         (when resi-subtypes
           (with-open [r (io/reader resi-subtypes)]
             (doall (line-seq r)))))

        is-resi-subtype
        (fn [x] (if (or (string/blank? x)
                        (resi-subtypes x))
                  true
                  false))
                
        add-residential-field
        (fn [input]
          (assoc input :residential
                 (is-resi-subtype (:subtype input))))
                
        update-subtype
        (fn [m {st :subtype :as thing}]
          (assoc thing
                 :subtype (or (:subtype (get m st)) st)))

        crs "EPSG:4326"

        _ (println "Querying overpass")
        
        query-results (overpass/get-geometry area-name
                                             :overpass-api overpass-api)

        _ (println "Processing results")
        
        ;; split into buildings and ways
        {ways :line-string buildings :polygon}
        (group-by ::geoio/type query-results)

        buildings (filter :building buildings)

        raw-building-types (frequencies (map :subtype buildings))
        raw-way-types (frequencies (map :subtype ways))
        
        ways
        (spatial/node-paths ways)
        
        [buildings ways]
        (spatial/add-connections crs buildings ways)

        ;; recode road types
        ways
        (map (partial update-subtype path-subtypes) ways)

        ;; determine resi status for buildings
        buildings
        (map add-residential-field buildings)
        
        ;; recode building types
        buildings
        (map (partial update-subtype building-subtypes) buildings)
        
        buildings
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm-id
               name :name
               subtype :subtype
               area ::spatial/area
               resi :residential
               connections ::spatial/connects-to-node}
              buildings]
          {:id id
           :orig_id osm-id
           :name name
           :type "building"
           :residential resi
           :subtype (if (or (nil? subtype) (= "" subtype)) nil subtype)
           :area area
           :connection_id (string/join "," connections)
           :geometry geometry})

        ways
        (for [{id ::geoio/id
               geometry ::geoio/geometry
               osm-id :osm-id
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
    ;; TODO make CRS requirement for geoio, don't infer from SRID on geom
    ;; as that is not reliable.

    (println "Building types:")
    (doseq [[k v] raw-building-types]
      (println k v))
    (println)
    
    (println "Way types:")
    (doseq [[k v] raw-way-types]
      (println k v))

    (println "Resi count:")
    (println (frequencies (map :residential buildings)))
    (println)
    
    (geoio/write-to {::geoio/features buildings
                     ::geoio/crs "EPSG:4326"}
                    buildings-out)
    (geoio/write-to {::geoio/features ways
                     ::geoio/crs "EPSG:4326"}
                    ways-out)))

(defn- run-demand-model [input-file output-file
                         & {:keys [demand-model peak-model]
                            :or {peak-model [0.0004963 21.84]}
                            }]
  (let [peak-m (first peak-model)
        peak-c (second peak-model)

        demand-models
        (doall (for [filename demand-model]
                 (with-open [r (io/reader filename)]
                   (svm/predictor (json/read r :key-fn keyword)))))

        add-demand-estimate
        (fn [feature]
          ;; find first demand model which will work, or log an error
          (let [estimates (map #(% feature) demand-models)
                estimate (first (filter identity estimates))
                peak-estimate (when estimate (+ peak-c (* peak-m estimate)))]
            (when-not estimate
              (println "No estimator worked for" (dissoc feature ::geoio/geometry)))
            
            (assoc feature
                   :demand-kwh-per-year estimate
                   :demand-kwp peak-estimate)))]
    
    (-> (geoio/read-from (io/file input-file))
        (geoio/update-features :add-demand-estimate add-demand-estimate)
        (geoio/write-to (io/file output-file)))))

(defn- explode-multi-geometries [shapes]
  (let [explode-geometry
        (fn [thing]
          (case (::geoio/type thing)
            (:multi-polygon :multi-line-string :multi-point)
            (let [geom ^Geometry (::geoio/geometry thing)]
              (for [n (range (.getNumGeometries geom))
                    :let [^Geometry sub-geom (.getGeometryN geom n)]]
                (geoio/update-geometry thing sub-geom)))
            
            [thing]))]
    (update shapes
            ::geoio/features
            #(mapcat explode-geometry %))))

(defn- add-lidar
  [shapes lidar-directory shapes-out &
   {:keys [buffer-size ground-level-threshold storey-height]
    :or {buffer-size 1.5 ground-level-threshold -5}}]

  (let [shape-files (->> (file-seq (io/file shapes))
                         (filter #(and (.isFile %)
                                       (or (has-extension % "shp")
                                           (has-extension % "json")
                                           ))))
        
        lidar-files (->> (file-seq (io/file lidar-directory))
                         (filter #(and (.isFile %)
                                       (or (has-extension % "tif")
                                           (has-extension % "tiff")))))

        lidar-index (lidar/rasters->index lidar-files)
        
        output-location (io/file shapes-out)

        get-output-path
        (fn [input-file]
          (let [old-name (.getName input-file)
                new-name (if (.endsWith old-name ".json")
                           (.replaceAll old-name "\\.json$" "-lidar.json")
                           (.replaceAll old-name "\\.shp$" ".json"))
                
                ]
            (io/file output-location new-name))
          )
        ]
    
    (when (not (.exists output-location))
      (.mkdirs output-location)
      (printf "creating output directory %s\n" output-location))
    
    (doseq [shape-file shape-files]
      (let [output-path (.getPath (get-output-path shape-file))]
        (printf "%s -> %s\n" shape-file output-path)
        (-> shape-file
            (geoio/read-from)
            (explode-multi-geometries)
            (lidar/add-lidar-to-shapes
             lidar-index
             :buffer-size buffer-size
             :storey-height storey-height
             :ground-level-threshold ground-level-threshold)
            (geoio/write-to output-path)))))
  )

(defn- just-node
  [ways-file buildings-file
   ways-out buildings-out
   & {:keys [chunk-size omit-fields canonize-ids connect-to-connectors]
      :or {chunk-size nil}}
   ]
  (let [_ (println "Reading ways...")
        {ways ::geoio/features ways-crs ::geoio/crs}           (explode-multi-geometries
                                                                (geoio/read-from ways-file))

        _ (println "Reading buildings...")
        {buildings ::geoio/features buildings-crs ::geoio/crs} (explode-multi-geometries
                                                                (geoio/read-from buildings-file))

        orig-way-fields (set/difference (set (mapcat keys ways))
                                        (set omit-fields))
        
        orig-building-fields (set/difference (set (mapcat keys buildings))
                                             (set omit-fields))

        _ (do (printf "%d buildings [%s], %d ways [%s]\n"
                   (count buildings) buildings-crs
                   (count ways) ways-crs)
              (flush))
        
        ways (spatial/node-paths ways)

        _ (do (printf "%d noded ways\n" (count ways))
              (flush))

        [buildings ways] (spatial/add-connections ways-crs buildings ways
                                                  :connect-to-connectors connect-to-connectors)

        _ (do (printf "%d connected ways\n" (count ways))
              (flush))

        idmap (util/canonizer)

        buildings (if canonize-ids
                    (for [b buildings]
                      (util/replace-ids b [::geoio/id ::spatial/connects-to-node] idmap))
                    buildings)
        
        buildings
        (for [b buildings]
          (merge (select-keys b orig-building-fields)
                 {:connection_id (string/join "," (::spatial/connects-to-node b))}))

        ways
        (for [w ways]
          (merge (select-keys w orig-way-fields)
                 {:length (::spatial/length w)
                  :start-id (::geoio/id (::spatial/start-node w))
                  :end-id   (::geoio/id (::spatial/end-node w))}))

        ways (if canonize-ids
               (for [w ways]
                 (util/replace-ids w [::geoio/id :start-id :end-id] idmap))
               ways)
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
  (let [[com & args] args]
    (case com
      "overpass"
      (run-with-arguments
       connect-overpass
       [[nil "--overpass-api URL" "The overpass API URL (see https://wiki.openstreetmap.org/wiki/Overpass_API)"]
        [nil "--resi-subtypes FILE"
         "A file listing resi subtypes, one per line. Places with no value are resi."
         :validate [#(.exists (io/as-file %))
                    "The list of resi subtypes must exist"]]
        
        [nil "--path-subtypes FILE" "A table mapping overpass highway to subtype"
         :validate [#(.exists (io/as-file %))
                    "The path subtype file must exist"]]
        
        [nil "--building-subtypes FILE" "Building subtypes mapping file"
         :validate [#(.exists (io/as-file %))
                    "The building subtypes file must exist"]]]
       
       #(if (= 3 (count %))
          [(file-not-exists (nth % 1))
           (file-not-exists (nth % 2))]
          ["Required arguments: <area name> <buildings output> <ways output>"])
       args)

      "model-demand"
      (run-with-arguments
       run-demand-model
       [[nil "--demand-model FILE"
         :default []
         :assoc-fn (fn [m k v] (update m k conj v))
         :validate [#(.exists (io/as-file %)) "Demand model files must exist"]]
        [nil "--peak-model M,C"
         :default [0.0004963 21.84]
         :parse-fn (fn [i] (map #(Double/parseDouble %) (.split i "")))
         ]]
       #(if (= 2 (count %))
          [(file-exists (first %))
           (file-not-exists (second %))]
          ["Required arguments: <input file> <output file>"])
       args)
      
      "lidar"
      (run-with-arguments
       add-lidar
       [[nil "--storey-height NUMBER"
         :parse-fn #(Double/parseDouble %)
         :default 4.1
         ]
        [nil "--buffer-size BUFFER-SIZE"
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
        [nil "--canonize-ids" "Convert IDs to integers"]
        [nil "--connect-to-connectors" "Allow connectors to connect to other connectors (if false, prefer non-connectors)"]
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
          (println "Usage: <this command> overpass | lidar | node | estimate")))))


