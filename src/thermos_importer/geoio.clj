(ns thermos-importer.geoio
  (:require [clojure.java.io :as io]
            [digest])
  (:import [org.geotools.data FileDataStoreFinder DataUtilities]))

(defn- kebab-case [class-name]
  (.toLowerCase
   (.replaceAll class-name "(.)([A-Z])" "$1-$2")))

(defn- geometry-type [geometry]
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

(defn load
  "Load some geospatial data into a format we like.
  The format is a map containing
  ::geometry
  ::type [:line :polygon :point ...]
  ::id - wkt-md5

  plus: keywordized fields from the feature
  "
  [filename]

  (let [store (FileDataStoreFinder/getDataStore (io/as-file filename))]
    (try
      (doall
       (for [feature (->> store
                          .getTypeNames
                          first
                          (.getFeatureSource store)
                          .getFeatures
                          .features
                          feature-iterator-seq)]
         (feature->map feature)))
      (finally (.dispose store)))))

(defn save
  "Store some geospatial data into a form that we like."
  [filename]
  )
