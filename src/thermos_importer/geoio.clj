;; This file is part of THERMOS, copyright © Centre for Sustainable Energy, 2017-2021
;; Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

(ns thermos-importer.geoio
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set :refer [map-invert]]
            [thermos-importer.util :as util]
            [thermos-importer.util :refer [has-extension file-extension]]
            [clojure.tools.logging :as log]
            [cljts.core :as jts]
            [clojure.data.json :as json])
  (:import  [java.security MessageDigest]
            [java.util Base64 Base64$Encoder]
            [org.locationtech.jts.geom Geometry Coordinate PrecisionModel]
            [org.locationtech.jts.precision GeometryPrecisionReducer]
            [org.geotools.geometry.jts Geometries JTS]
            [org.geotools.geojson.feature FeatureJSON]
            [org.geotools.geojson.geom GeometryJSON]
            [org.geotools.geopkg GeoPackage FeatureEntry]
            [org.geotools.data FileDataStoreFinder DataUtilities DataStoreFinder]
            [org.geotools.data.simple SimpleFeatureCollection]
            [org.geotools.data.collection ListFeatureCollection]
            [org.geotools.feature.simple SimpleFeatureBuilder]
            [org.geotools.feature FeatureIterator]
            [org.opengis.feature Feature Property]
            [org.opengis.feature.simple SimpleFeature]
            [org.geotools.referencing CRS]
            [org.geotools.data.shapefile ShapefileDataStore]
            [java.nio.charset StandardCharsets]))

(defn update-features [m tag f & args]
  (update m ::features
          #(let [start (System/currentTimeMillis)
                 total (count %)]
             (util/seq-counter
              (for [feature %] (apply f feature args))
              (int (/ total 20))
              (fn [n]
                (let [now (System/currentTimeMillis)
                      delta (- now start)]
                  (when (> delta 5000)
                    (log/info (format "\n%s [%s%%, %.1fm]"
                                      tag
                                      (int (/ (* 100 n) total))
                                      (float (/ (* (- total n)
                                                   (/ delta n))
                                                60000))))
                    (when (.isInterrupted (Thread/currentThread))
                      (throw (InterruptedException.)))
                    (flush))))))))

(defn- feature-iterator-seq
  "Make a feature iterator into a lazy sequence.
  Note that if you do not exhaust the sequence the iterator will not be closed.
  This is not intended for use in any other context."
  [^FeatureIterator feature-iterator]
  (lazy-seq
   (if (.hasNext feature-iterator)
     (cons (.next feature-iterator)
           (feature-iterator-seq feature-iterator))
     (do (.close feature-iterator)
         nil))))

(defn- feature-geometry [^SimpleFeature feature]
  (if-let [^Geometry geometry (.getDefaultGeometry feature)]
    (jts/make-singular geometry)
    (do (log/warn "Feature has missing geometry" (::id feature))
        nil)))

(defn- feature-attributes [^Feature feature key-transform]
  (let [geometry-property (.getDefaultGeometryProperty feature)]
    (into {}
          (for [^Property p (.getProperties feature)
                :when (not (= (.getName p)
                              (.getName geometry-property)))]
            [(key-transform (.getLocalPart (.getName p)))
             (.getValue p)]))))

(defn geometry->id [geometry]
  (jts/ghash geometry))

(defn update-geometry [feature ^Geometry geom]
  (if geom
    (assoc feature
         ::geometry geom
         ::type (jts/geometry-type geom)
         ::id (jts/ghash geom))
    (dissoc feature ::geometry ::type ::id)))

(defn decode-crs [crs]
  (cond
    (instance? org.opengis.referencing.crs.CoordinateReferenceSystem crs) crs
    (string? crs) (CRS/decode crs true)
    (number? crs) (CRS/decode (str "EPSG:" crs) true)
    :else (throw (IllegalArgumentException. (format "Not a CRS: %s" crs)))))

(defn reprojector [from-crs to-crs]
  (if (= from-crs to-crs) identity
      (let [from-crs (decode-crs from-crs)
            to-crs   (decode-crs to-crs)
            transform (CRS/findMathTransform from-crs to-crs true)]
        (fn [f]
          (update-geometry f (JTS/transform (::geometry f) transform))))))

(defn reproject [features to-crs]
  (let [transform (reprojector (::crs features) to-crs)]
    {::features (map transform (::features features))
     ::crs to-crs}))

(defn set-precision [features precision]
  (if precision
    (let [precision-model
          (cond
            (= precision :float) (PrecisionModel.)
            (= precision :single-float) (PrecisionModel. PrecisionModel/FLOATING_SINGLE)
            (number? precision) (PrecisionModel. (double precision))
            :else (throw (ex-info "Invalid precision" {:precision precision})))
          change-precision
          (fn [f]
            (let [f (update-geometry f (GeometryPrecisionReducer/reduce
                                        (::geometry f) precision-model))]
              (when-not (.isEmpty (::geometry f)) f)))]
      (let [features'
            (update features ::features
                    #(keep change-precision %))
            old-count (count (::features features))
            new-count (count (::features features'))
            ]
        (when (< new-count old-count)
          (log/warn (- old-count new-count) "features removed by precision reduction"))
        features'))
    features))

(defn- feature->map [^Feature feature key-transform]
  (update-geometry
   (feature-attributes feature key-transform)
   (feature-geometry feature)))

(defn geom->map [geom]
  (update-geometry {} geom))

(defn- read-from-store [store & {:keys [force-crs key-transform table-name]
                                 :or {table-name :all}
                                 :as opts}]
  (try (.setCharset store (StandardCharsets/UTF_8))
       (catch Exception e))
  (loop [type-names (if (= :all table-name)
                      (into [] (.getTypeNames store))
                      [table-name])
         features   []
         crs        force-crs]
    (if-not (empty? type-names)
      (let [[tn & type-names] type-names
            source (.getFeatureSource store tn)
            source-crs (-> source .getInfo .getCRS)

            source-features
            (for [feature (doall (->> source
                                      (.getFeatures)
                                      (.features)
                                      (feature-iterator-seq)))
                  :when feature
                  :let [m (feature->map feature key-transform)]
                  :when (::geometry m)]
              (assoc m ::table tn))

            target-crs (or crs source-crs)

            source-features
            (map (reprojector source-crs target-crs) source-features)
            ]
        (recur type-names
               (into features source-features)
               target-crs))

      ;; output
      {::features features ::crs (-> crs
                                     (decode-crs)
                                     (CRS/lookupIdentifier true))})))

(defn read-from-geojson-2 [reader & {:keys [force-crs key-transform homogenise-keys]
                                     :or {key-transform keyword}}]
  (let [do-homogenise-keys
        (fn [features]
          (let [null-properties (into {} (for [k (set (mapcat keys features))] [k nil]))]
            (for [f features]
              (merge null-properties f))))
        
        obj (json/read reader)
        fix-keys (fn [p]
                   (into {} (for [[k v] p] [(key-transform k) v])))
        crs (get-in obj ["crs" "properties" "name"] "EPSG:4326")
        ]
    {::crs (or force-crs crs)
     ::features
     (cond->
         (case (get obj "type")
           "FeatureCollection"
           
           (vec
            (for [feature (get obj "features")]
              (update-geometry
               (fix-keys (get feature "properties"))
               (jts/map->geom (get feature "geometry")))))
           
           "Feature"
           [(update-geometry
             (fix-keys (get obj "properties"))
             (jts/map->geom (get obj "geometry")))]

           [(update-geometry
             {}
             (jts/map->geom obj))])
       
       homogenise-keys
       (do-homogenise-keys)

       force-crs
       (->> (map (reprojector crs force-crs)))
       )}))

(defn read-from-geojson [filename & {:keys [force-crs key-transform]
                                     :or {key-transform keyword}}]
  (let [io (FeatureJSON.)
        crs (or (try (.readCRS io filename)
                     (catch Exception e))
                (decode-crs "EPSG:4326"))
        
        feature-collection (.readFeatureCollection io filename)

        features  (doall (->> feature-collection
                              .features
                              feature-iterator-seq))
        
        features (for [feature features
                       :when feature
                       :let [m (feature->map feature key-transform)]
                       :when m]
                   m)

        features (if force-crs
                   (map (reprojector crs force-crs) features)
                   features)

        crs-id (or force-crs
                   (try (CRS/lookupIdentifier crs true)
                        (catch Exception e
                          (log/error e "Error reading CRS from" filename)
                          "EPSG:4326")))
        ]
    {::features features ::crs crs-id}))

(defn read-from
  "Load some geospatial data into a format we like.
  The format is a map containing
  ::geometry
  ::type [:line :polygon :point ...]
  ::id - wkt-md5

  plus: keywordized fields from the feature
  "
  [filename & {:keys [force-crs key-transform force-precision
                      remove-junk]
               :or {key-transform keyword remove-junk true}}]
  {:pre [(.exists (io/as-file filename))]}
  (let [filename (io/as-file filename)
        store (FileDataStoreFinder/getDataStore filename)]
    (cond->
        (cond
          store
          (try
            (read-from-store store :force-crs force-crs :key-transform key-transform)
            (finally (.dispose store)))

          (or (has-extension filename "gpkg")
              (has-extension filename "geopackage"))
          (let [store (DataStoreFinder/getDataStore {"dbtype" "geopkg"
                                                     "database" (.getCanonicalPath filename)})]
            (try
              (read-from-store store :force-crs force-crs :key-transform key-transform)
              (finally (.dispose store))))
          
          
          (or (has-extension filename "json")
              (has-extension filename "geojson"))
          (with-open [r (io/reader filename)]
            (read-from-geojson-2 r
                                 :force-crs force-crs
                                 :key-transform key-transform
                                 :homogenise-keys true))

          :otherwise
          (throw (Exception. (str "Unable to read features from " filename))))
      
      force-precision
      (set-precision force-precision)
      
      remove-junk
      (update ::features
              (fn [features]
                (let [features' (filter (comp jts/useful? ::geometry) features)
                      old-count (count features)
                      new-count (count features')
                      ]
                  (when (< new-count old-count)
                    (log/warn (- old-count new-count) "features removed due to invalid geometry or emptiness"))
                  features'))))))

(def can-read?
  (comp
   #{"shp" "json" "geojson" "gpkg" "geopackage"}
   file-extension))

(defn read-from-multiple [filenames &
                          {:keys [force-crs key-transform]
                           :or {key-transform keyword}}]
  (let [filenames (filter can-read? filenames)
        data (map #(read-from % :force-crs force-crs :key-transform key-transform)
                  filenames)
        crss (map ::crs data)]
    ;; TODO assuming they have a compatible CRS is a bad plan
    {::crs (first (filter identity crss))
     ::features (mapcat ::features data)}))

(defn geometry-field-type [srid values]
  (let [srid (or srid (.getSRID (first values)))
        classes (set (map #(.getClass %) values))]
    (format "%s:srid=%d"
            (or (and (= (count classes) 1)
                     (cond
                       (classes org.locationtech.jts.geom.Polygon)
                       "Polygon"
        
                       (classes org.locationtech.jts.geom.Point)
                       "Point"

                       (classes org.locationtech.jts.geom.MultiPolygon)
                       "MultiPolygon"
                       
                       (classes org.locationtech.jts.geom.MultiPoint)
                       "MultiPoint"
                       
                       (classes org.locationtech.jts.geom.MultiLineString)
                       "MultiLineString"
                       
                       (classes org.locationtech.jts.geom.GeometryCollection)
                       "GeometryCollection"

                       (classes org.locationtech.jts.geom.LineString)
                       "LineString"))
              "Geometry")
            srid)))

(defn infer-field-type [srid get-value values]
  ;; it would be better to reduce over values and find the most general
  ;; type mapping here.
  (let [value (first values)]
    (cond
      (instance? Geometry value)
      ;; this needs special thought as we need most general type of geometry
      {:type (geometry-field-type srid values) :value get-value}

      ;; TODO this will fail for Longs
      (int? value)
      {:type "Integer"  :value get-value}
      
      (double? value)
      {:type "Double"  :value get-value}

      (float? value)
      {:type "Float"  :value get-value}
      
      (keyword? value)
      {:type "String" :value #(when-let [x (get-value %)] (str x))}

      (string? value)
      {:type "String" :value get-value}

      (boolean? value)
      {:type "Boolean" :value get-value}

      (number? value)
      {:type "Double"  :value #(when-let [v (get-value %)]
                                 (double v))}
      
      :otherwise
      (do (log/warn "no inferred field type for value of type" (type value))
          nil))))

(defn clean-string-for-output [^String s]
  (.. s
      (toLowerCase)
      (replaceAll "^:" "")
      (replaceAll "[^0-9a-z]+" "_")))

(defn clean-keys-for-output [keys]
  (map-invert
   (reduce
    (fn [out key]
      (let [k-name (clean-string-for-output (name key))
            k-full-name (clean-string-for-output (str key))]
        (assoc out
               (cond (= key ::geometry) "geometry"
                     (contains? out k-name) k-full-name
                     :default k-name)
               key)))
    {} keys)))

(defn clean-key-for-output [key]
  (if (= key ::geometry)
    "geometry"
    (let [s (str key)]
      (.. s
          (toLowerCase)
          (replaceAll "^:" "")
          (replaceAll "[^0-9a-z]+" "_")))))

(defn infer-fields [srid data]
  (let [all-keys (set (mapcat keys data))
        clean-keys (clean-keys-for-output all-keys)]
    (into {}
          (for [key all-keys
                :let [values (->> data
                                  (map #(get % key))
                                  (filter identity))
                      get-value (if (keyword? key) key #(get % key))
                      field-type (infer-field-type srid get-value values)]
                :when field-type]
            [(get clean-keys key) field-type]))))

(defn write-to
  "Store some geospatial data into a form that we like."
  [data filename & {:keys [fields chunk-size table-name geometry-column]
                    :or   {table-name      "features"
                           geometry-column "geometry"}}]
  (let [filename (.getPath (io/file filename))
        
        crs (::crs data)
        epsg (CRS/lookupEpsgCode (CRS/decode crs true) true)
        
        data (::features data)

        fields (or fields (infer-fields epsg data))
        ;; we need a type descriptor for geotools to be happy:
        type
        (DataUtilities/createType
         table-name
         ;; this does something; it is a descriptor for the column types, like:
         ;; col1:string,col2:double,col3:LineString:srid=...
         (string/join
          ","
          (for [[name {type :type}] fields]
            (str name ":" type))))

        ;; now we can build features using the type
        feature-builder
        (SimpleFeatureBuilder. type)

        make-feature-collection
        (fn ^SimpleFeatureCollection [data]
          (ListFeatureCollection.
           type
           (for [datum data]
             ;; TODO add details to builder here
             ;; it seems that on buildfeature it resets
             (do (doseq [[field-name {value-function :value}] fields]
                   (.add feature-builder (value-function datum)))
                 (.buildFeature feature-builder
                                (when-let [id (or (::id datum) (:id datum))]
                                  (str id)))
                 ))))
        
        write-chunk
        (cond
          (has-extension filename "gpkg")
          (fn [filename data]
            (let [feature-entry (FeatureEntry.)
                  out (GeoPackage. (io/file filename))]
              (try
                (.setSrid feature-entry epsg)
                (.setTableName feature-entry table-name)
                (.setGeometryColumn feature-entry geometry-column)
                (.setGeometryType feature-entry Geometries/GEOMETRY)
                (.add out feature-entry (make-feature-collection data))
                (finally (.close out)))))
          
          
          :default ;; geojson
          (fn [filename data]
            (with-open [writer (io/writer filename)]
              (.writeFeatureCollection
               (let [x (FeatureJSON. (GeometryJSON. 8))]
                     (.setEncodeNullValues x true)
                     (.setEncodeFeatureCollectionCRS x true)
                     x)
               (make-feature-collection data)
               writer))))
        ]
    (if chunk-size
      (doseq [[i data]
              (map-indexed vector (partition chunk-size data))]
        (write-chunk (str filename "." i) data))
      (write-chunk filename data))))

(defn bounding-box ^org.locationtech.jts.geom.Envelope.
  ([features]
   (bounding-box features (org.locationtech.jts.geom.Envelope.)))

  ([features box]
   (doseq [{g ::geometry} (::features features)]
     (.expandToInclude box (.getEnvelopeInternal g)))
   box))

(defn explode-multi
  "Given a feature map, if it is a multi-part geometry produce a list of single-part geometries for each part.
  If it's a single part geometry, don't explode it."
  [feature]
  (case (::type feature)
    (:multi-polygon :multi-line-string :multi-point)
    (let [geom ^Geometry (::geometry feature)]
      (for [n (range (.getNumGeometries geom))
            :let [^Geometry sub-geom (.getGeometryN geom n)]]
        (update-geometry feature sub-geom)))
    [feature]))

(defn explode-multis [shapes] (mapcat explode-multi shapes))
