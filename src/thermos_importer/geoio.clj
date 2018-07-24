(ns thermos-importer.geoio
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [digest])
  (:import [com.vividsolutions.jts.geom Geometry]
           [org.geotools.geojson.feature FeatureJSON]
           [org.geotools.geojson.geom GeometryJSON]
           [org.geotools.data FileDataStoreFinder DataUtilities]
           [org.geotools.data.collection ListFeatureCollection]
           [org.geotools.feature.simple SimpleFeatureBuilder]
           [org.geotools.referencing CRS]
           [org.geotools.data.shapefile ShapefileDataStore]
           [java.nio.charset StandardCharsets]))

(defn- kebab-case [class-name]
  (.toLowerCase
   (.replaceAll class-name "(.)([A-Z])" "$1-$2")))

(defn geometry-type [geometry]
  (keyword (kebab-case (.getGeometryType geometry))))

(defn- feature-iterator-seq
  "Make a feature iterator into a lazy sequence.
  Note that if you do not exhaust the sequence the iterator will not be closed.
  This is not intended for use in any other context."
  [feature-iterator]
  (lazy-seq
   (if (.hasNext feature-iterator)
     (cons (.next feature-iterator)
           (feature-iterator-seq feature-iterator))
     (do (.close feature-iterator)
         nil))))

(defn- feature-geometry [feature]
  (let [geometry (.getDefaultGeometry feature)
        n (.getNumGeometries geometry)]
    (if (and (= n 1)
             (#{:multi-point :multi-line-string :multi-polygon}
              (geometry-type geometry)))
      (.getGeometryN geometry 0)
      geometry)))

(defn- feature-attributes [feature]
  (into {}
        (for [p (.getProperties feature)]
          [(keyword (.getLocalPart (.getName p)))
           (.getValue p)])))

(defn geometry->id [geometry]
  (digest/md5 (.toText geometry)))

(defn- feature->map [feature]
  (let [geometry (feature-geometry feature)
        identity (geometry->id geometry)
        type (geometry-type geometry)
        other-fields (feature-attributes feature)]
    (merge other-fields {::geometry geometry ::type type ::id identity})))

(defn geom->map [geom]
  {::geometry geom
   ::type (geometry-type geom)
   ::id (geometry->id geom)})

(defn read-from
  "Load some geospatial data into a format we like.
  The format is a map containing
  ::geometry
  ::type [:line :polygon :point ...]
  ::id - wkt-md5

  plus: keywordized fields from the feature
  "
  [filename]

  (let [store (FileDataStoreFinder/getDataStore (io/as-file filename))

        _ (.setCharset store (StandardCharsets/UTF_8))

        feature-source (->> store .getTypeNames first (.getFeatureSource store))
        crs (-> feature-source .getInfo .getCRS)
        crs-id (CRS/lookupIdentifier crs true)

        features (try
                   (doall
                    (for [feature (->> feature-source
                                       .getFeatures
                                       .features
                                       feature-iterator-seq)]
                      (feature->map feature)))
                   (finally (.dispose store)))
        ]
    {::features features ::crs crs-id}
    ))

(defn geometry-field-type [values]
  (let [srid (.getSRID (first values))
        classes (set (map #(.getClass %) values))]
    (format "%s:srid=%d"
            (or (and (= (count classes) 1)
                     (cond
                       (classes com.vividsolutions.jts.geom.Polygon)
                       "Polygon"
        
                       (classes com.vividsolutions.jts.geom.Point)
                       "Point"

                       (classes com.vividsolutions.jts.geom.MultiPolygon)
                       "MultiPolygon"
                       
                       (classes com.vividsolutions.jts.geom.MultiPoint)
                       "MultiPoint"
                       
                       (classes com.vividsolutions.jts.geom.MultiLineString)
                       "MultiLineString"
                       
                       (classes com.vividsolutions.jts.geom.GeometryCollection)
                       "GeometryCollection"

                       (classes com.vividsolutions.jts.geom.LineString)
                       "LineString"))
              "Geometry")
            srid)))

(defn infer-field-type [get-value values]
  (let [value (first values)]
    (cond
      (instance? Geometry value)
      ;; this needs special thought as we need most general type of geometry
      {:type (geometry-field-type values) :value get-value}

      (int? value)
      {:type "Integer"  :value get-value}
      
      (double? value)
      {:type "Double"  :value get-value}

      (keyword? value)
      {:type "String" :value #(let [x (get-value %)] (and x (str x)))}

      (string? value)
      {:type "String" :value get-value}

      :otherwise
      nil)))

(defn clean-key-for-output [key]
  (let [s (str key)]
    (.. s
        (toLowerCase)
        (replaceAll "^:" "")
        (replaceAll "[^0-9a-z]+" "_"))))

(defn infer-fields [data]
  (let [all-keys (set (mapcat keys data))]
    (into {}
          (for [key all-keys
                :let [values (->> data
                                  (map #(get % key))
                                  (filter identity))
                      get-value (if (keyword? key) key #(get % key))
                      field-type (infer-field-type get-value values)]
                :when field-type]
            [(clean-key-for-output key) field-type]))))

(defn write-to
  "Store some geospatial data into a form that we like."
  [data filename & {:keys [fields]}]

  (let [fields (or fields (infer-fields data))

        geo-writer (FeatureJSON. (GeometryJSON. 8))

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

        feature-collection
        (ListFeatureCollection.
         type
         (for [datum data]
           ;; TODO add details to builder here
           ;; it seems that on buildfeature it resets
           (do (doseq [[field-name {value-function :value}] fields]
                 (.add feature-builder (value-function datum)))
               (.buildFeature feature-builder nil))))
        ]
    (with-open [writer (io/writer filename)]
      (.writeFeatureCollection
       geo-writer
       feature-collection
       writer))))
