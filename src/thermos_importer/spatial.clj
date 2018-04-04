(ns thermos-importer.spatial
  (:require [thermos-importer.geoio :as geoio])
  (:import [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries]))

(defn- feature->rect [feature]
  (let [bbox (.getEnvelopeInternal (::geoio/geometry feature))]
    (Geometries/rectangle
     (.getMinX bbox) (.getMinY bbox)
     (.getMaxX bbox) (.getMaxY bbox))))

(defn- index-features [features]
  (let [index (.create (RTree/star))]
    (loop [[feature & features] features]
      (.add index feature (feature->rect feature))
      (recur features))
    index))

(defn add-connections
  [buildings paths]

  (let [paths (atom paths) ;; we want to be able to update these
        building-index (index-features buildings)
        ;; now we want to find all the buildings

        ]

    )
  )

(defn node-paths [paths]
  paths)
