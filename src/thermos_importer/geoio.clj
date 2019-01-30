(ns thermos-importer.geoio
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set :refer [map-invert]]
            [thermos-importer.util :as util]
            [thermos-importer.util :refer [has-extension]])
  (:import  [java.security MessageDigest]
            [java.util Base64 Base64$Encoder]
            [org.locationtech.jts.geom Geometry Coordinate]
            [org.geotools.geometry.jts Geometries JTS]
            [org.geotools.geojson.feature FeatureJSON]
            [org.geotools.geojson.geom GeometryJSON]
            [org.geotools.geopkg GeoPackage FeatureEntry]
            [org.geotools.data FileDataStoreFinder DataUtilities]
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
                    (printf "\r%s [%s%%, %.1fm]"
                            tag
                            (int (/ (* 100 n) total))
                            (float (/ (* (- total n)
                                         (/ delta n))
                                      60000)))
                    (flush))))))))

(defn- kebab-case [^String class-name]
  (.toLowerCase
   (.replaceAll class-name "(.)([A-Z])" "$1-$2")))

(defn geometry-type [^Geometry geometry]
  (let [type (.getGeometryType geometry)]
    (case type
      "Polygon" :polygon
      "MultiPolygon" :multi-polygon
      "MultiPoint" :multi-point
      "LineString" :line-string
      "MultiLineString" :multi-line-string
      (keyword (kebab-case type)))))

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
    (let [n (.getNumGeometries geometry)]
      (if (and (= n 1)
               (#{:multi-point :multi-line-string :multi-polygon}
                (geometry-type geometry)))
        (.getGeometryN geometry 0)
        geometry))
    (do (println "Feature has missing geometry" feature)
        (flush)
        nil)))

(defn- feature-attributes [^Feature feature]
  (let [geometry-property (.getDefaultGeometryProperty feature)]
    (into {}
          (for [^Property p (.getProperties feature)
                :when (not (= (.getName p)
                              (.getName geometry-property)))]
            (do
              [(keyword (.getLocalPart (.getName p)))
               (.getValue p)])))))

(let [^MessageDigest md5 (MessageDigest/getInstance "MD5")
      ^Base64$Encoder base64 (.withoutPadding (Base64/getEncoder))
      ]
  (defn geometry->id [^Geometry geometry]
    (.reset md5)
    (.update md5 (.getBytes (.getGeometryType geometry)))
    (doseq [^Coordinate c (.getCoordinates geometry)]
      (let [x (Double/doubleToLongBits (.-x c))
            y (Double/doubleToLongBits (.-y c))]
        (.update md5 (unchecked-byte (bit-and 0xFF x)))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right x 8))))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right x 16))))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right x 24))))
        (.update md5 (unchecked-byte (bit-and 0xFF y)))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right y 8))))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right y 16))))
        (.update md5 (unchecked-byte (bit-and 0xFF (bit-shift-right y 24))))))
    ;; Throw away some bytes. This should give us 16 * 6 bits = 96
    ;; collision probability for 60 million objects of around
    ;; 2e-14 which is probably good enough.
    (.substring (.encodeToString base64 (.digest md5)) 0 16)))

(defn update-geometry [feature ^Geometry geom]
  (if geom
    (assoc feature
         ::geometry geom
         ::type (geometry-type geom)
         ::id (geometry->id geom))
    (dissoc feature ::geometry ::type ::id)))

(defn- feature->map [^Feature feature]
  (update-geometry
   (feature-attributes feature)
   (feature-geometry feature)))

(defn geom->map [geom]
  (update-geometry {} geom))

(defn- read-from-store [store]
  (.setCharset store (StandardCharsets/UTF_8))
  (let [feature-source (->> store .getTypeNames first (.getFeatureSource store))
        crs (-> feature-source .getInfo .getCRS)
        crs-id (CRS/lookupIdentifier crs true)

        features (for [feature (doall (->> feature-source
                                           .getFeatures
                                           .features
                                           feature-iterator-seq))
                       :when feature
                       :let [m (feature->map feature)]
                       :when (::geometry m)]
                   m)]
    {::features features ::crs crs-id}))

(defn- read-from-geojson [filename]
  (let [io (FeatureJSON.)
        crs-id (try (CRS/lookupIdentifier (.readCRS io filename) true)
                 (catch Exception e
                   (println "Error reading CRS from" filename)
                   "EPSG:4326"))

        feature-collection (.readFeatureCollection io filename)

        features  (doall (->> feature-collection
                              .features
                              feature-iterator-seq))
        
        features (for [feature features
                       :when feature
                       :let [m (feature->map feature)]
                       :when m]
                   m)]
    {::features features ::crs crs-id}))

(defn read-from
  "Load some geospatial data into a format we like.
  The format is a map containing
  ::geometry
  ::type [:line :polygon :point ...]
  ::id - wkt-md5

  plus: keywordized fields from the feature
  "
  [filename]

  (let [filename (io/as-file filename)
        store (FileDataStoreFinder/getDataStore filename)]
    (cond
      store
      (try
        (read-from-store store)
        (finally (.dispose store)))

      (has-extension filename "json")
      (read-from-geojson filename)

      :otherwise
      (throw (Exception. (str "Unable to read features from " filename)))
      )))

(defn read-from-multiple [filenames]
  (let [data (map read-from filenames)
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
  (let [value (first values)]
    (cond
      (instance? Geometry value)
      ;; this needs special thought as we need most general type of geometry
      {:type (geometry-field-type srid values) :value get-value}

      (int? value)
      {:type "Integer"  :value get-value}
      
      (double? value)
      {:type "Double"  :value get-value}

      (keyword? value)
      {:type "String" :value #(let [x (get-value %)] (and x (str x)))}

      (string? value)
      {:type "String" :value get-value}

      (boolean? value)
      {:type "Boolean" :value get-value}
      
      :otherwise
      nil)))

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
  [data filename & {:keys [fields chunk-size]}]

  (let [filename (.getPath (io/file filename))

        crs (::crs data)
        epsg (CRS/lookupEpsgCode (CRS/decode crs true) true)
        
        data (::features data)

        fields (or fields (infer-fields epsg data))

        geo-writer (let [x (FeatureJSON. (GeometryJSON. 8))]
                     (.setEncodeNullValues x true)
                     (.setEncodeFeatureCollectionCRS x true)
                     x)

        ;; we need a type descriptor for geotools to be happy:
        type
        (DataUtilities/createType
         "Data" ;; I think this does nothing
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
                (.setTableName feature-entry "features")
                (.setGeometryColumn feature-entry "geometry")
                (.setGeometryType feature-entry Geometries/GEOMETRY)
                (.add out feature-entry (make-feature-collection data))
                (finally (.close out)))))
          
          
          :default ;; geojson
          (fn [filename data]
            (println "writing" filename "...")
            (with-open [writer (io/writer filename)]
              (.writeFeatureCollection
               geo-writer
               (make-feature-collection data)
               writer))))
        
        ]
    
    (if chunk-size
      (doseq [[i data]
              (map-indexed vector (partition chunk-size data))]
        (write-chunk (str filename "." i) data))
      (write-chunk filename data)
      )
    ))


(defn reproject [features to-crs]
  (let [input-crs (CRS/decode (::crs features))
        output-crs (CRS/decode to-crs)]
    (if (= input-crs output-crs)
      features
      (let [transform (CRS/findMathTransform input-crs output-crs true)]
        {::features
         (for [f features]
           (update-geometry f (JTS/transform (::geometry f) transform)))
         ::crs to-crs
         }))))
