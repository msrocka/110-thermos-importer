(ns thermos-importer.spatial
  (:refer-clojure :exclude [cond])
  (:require [thermos-importer.geoio :as geoio]
            [better-cond.core :refer [cond]])
  (:import [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries]

           [com.vividsolutions.jts.geom GeometryFactory Coordinate]
           [com.vividsolutions.jts.operation.distance DistanceOp]
           [com.vividsolutions.jts.noding
            MCIndexNoder NodedSegmentString IntersectionAdder]
           [com.vividsolutions.jts.algorithm RobustLineIntersector]
           ))

(def SMALL_DISTANCE 0.000001)
(def NEARNESS 0.01) ;; degrees latlon
(def NEIGHBOURS 6) ;; number of neighbours to consider
;; TODO we may later want to find all properly intersecting lines

(defn feature->rect [feature]
  (let [bbox (.getEnvelopeInternal (::geoio/geometry feature))]
    (Geometries/rectangle
     (.getMinX bbox) (.getMinY bbox)
     (.getMaxX bbox) (.getMaxY bbox))))


(defn index-remove! [index feature]
  (let [size-before (.size @index)]
    (swap! index #(.delete % feature (feature->rect feature) true))
    (when (= size-before (.size @index))
      (println "Unable to remove feature from index?")))
  )

(defn index-insert! [index feature]
  (swap! index #(.add % feature (feature->rect feature))))

(defn features->index [features]
  (let [index (atom (.create (RTree/star)))]
    (doseq [feature features]
      (index-insert! index feature))

    index))

(defn index->features [index]
  (map #(.value %) (-> @index .entries .toBlocking .toIterable)))

(defn- feature-neighbours
  "Given an INDEX and a FEATURE, find the values in the index which are
  near to the feature's bounding-box."
  [index feature]
  (for [entry (-> (.nearest @index (feature->rect feature) (double NEARNESS) (int NEIGHBOURS))
                  .toBlocking .toIterable)]
    (.value entry)))

(defn feature-overlaps
  "Given an INDEX and a FEATURE, find the values in the index whose
  bounding boxes overlap the feature's bounding box"
  [index feature]
  (let [bbox (feature->rect feature)]
    (for [entry (-> (.search @index (feature->rect feature))
                    .toBlocking .toIterable)]
      (.value entry))))

(defn- features-intersect? [{a ::geoio/geometry} {b ::geoio/geometry}]
  (.intersects a b))

(defn- feature-intersections [index feature]
  (->> feature ;; take the feature
       (feature-overlaps index) ;; find things which could intersect
       (filter (partial features-intersect? feature)))) ;; restrict to things which do

(defn add-connections
  [buildings noded-paths]
  (println "Connect" (count buildings) "with" (count noded-paths))
  (let [building-nodes (atom {}) ;; maps building IDs to node IDs
                                 ;; where they connect

        _ (println "Creating path index")
        path-index (features->index noded-paths)

        endpoints (fn [f] [(::start-node f) (::end-node f)])

        ;; these are all the unique vertices the paths touch
        nodes (set (mapcat endpoints noded-paths))

        _ (println "Creating node index")
        ;; an index of all the vertices that exist
        node-index (features->index nodes)

        ;; TODO factor this repeated code :
        factory (GeometryFactory.)
        make-node
        #(let [p (.createPoint factory %)]
           {::geoio/geometry p
            ::geoio/id (geoio/geometry->id p)})

        make-path
        (fn [meta coords]
          (let [geom (.createLineString factory
                                        (into-array Coordinate coords))]
            (assoc meta
                   ::geoio/geometry geom
                   ::geoio/id (geoio/geometry->id geom))))

        split-connect-path!
        (fn [p b & [op]]
          (let [op (or op (DistanceOp. (::geoio/geometry p)
                                       (::geoio/geometry b)))
                [on-p on-b] (.nearestLocations op)
                distance (.distance op)

                path-coordinates (.getCoordinates (::geoio/geometry p))
                split-position (.getSegmentIndex on-p)
                split-point (.getCoordinate on-p)

                connect-to-node
                (let [
                      [p-start p-end] (split-at (inc split-position) path-coordinates)

                      ;; convert coordinate chains to features:
                      p-start (make-path p (concat p-start [split-point]))
                      p-end (make-path p (concat [split-point] p-end))

                      ;; update start and end vertices of the two new paths
                      new-node (make-node split-point)
                      p-start (assoc p-start ::end-node new-node :type "start-half")
                      p-end (assoc p-end ::start-node new-node :type "end-half")
                      ]
                  ;; we need to delete p from the path-index
                  (index-remove! path-index p)

                  ;; we need to add p-start and p-end to the path index
                  (index-insert! path-index p-start)
                  (index-insert! path-index p-end)

                  ;; we need to add new-node to the node index
                  (index-insert! node-index new-node)

                  new-node)
                ]

            (if (> distance SMALL_DISTANCE)
              ;; we need a connecting line!
              (let [new-end-node (make-node (.getCoordinate on-b))
                    connector (make-path {::start-node connect-to-node ::end-node new-end-node
                                          :type "Connector"}
                                         [split-point (.getCoordinate on-b)])]
                ;; we need to add the connector to the path index
                (index-insert! path-index connector)
                ;; we need to add the connector's end node to the node index
                (index-insert! node-index new-end-node)
                ;; we need to write down the building connection
                (swap! building-nodes update (::geoio/id b) conj (::geoio/id new-end-node)))

              ;; otherwise we just need to connect the building:
              (swap! building-nodes update (::geoio/id b) conj (::geoio/id connect-to-node))
              )))
        ]
    (doseq [building buildings]
      (cond
        ;; step 1: find any intersecting nodes and connect to those
        :let [intersecting-nodes (feature-intersections node-index building)]

        (not-empty intersecting-nodes)
        ;; connect the building directly to a node
        (swap! building-nodes
               assoc (::geoio/id building)
               (map ::geoio/id intersecting-nodes))

        ;; step 2: there were no intersecting nodes, what about paths?
        :let [intersecting-paths (feature-intersections path-index building)]

        (not-empty intersecting-paths)
        (doseq [path intersecting-paths] (split-connect-path! path building))

        ;; step 3: there were no intersecting paths, try a nearby path?
        :when-let [nearby-paths (feature-neighbours path-index building)]
        :let [distance-ops (map #(vector
                                  %
                                  (DistanceOp. (::geoio/geometry %)
                                               (::geoio/geometry building)))
                                nearby-paths)
              [nearest-path op] (first (sort-by #(.distance (second %)) distance-ops))
              ]

        nearest-path
        ;; can reuse the distanceop here to save a tiny bit of time
        (split-connect-path! nearest-path building ;; op
                             )
        ))

    ;; at this point the node and path indices contain the network
    ;; structure and the building-nodes map contains the connection
    ;; from each building to a path. We want to output some new
    ;; information which is the revised set of paths and buildings.
    (let [paths (index->features path-index)
          building-nodes @building-nodes
          buildings (map
                     #(assoc % ::connects-to-node (building-nodes (::geoio/id %)))
                     buildings)]
      [buildings paths])))

(defn feature->segment-string
  "This makes a nodedsegmentstring which refers back to the feature
  which it came from, so we can preserve the feature metadata (IDs etc)."
  [feature]
  (NodedSegmentString.
   (.getCoordinates (::geoio/geometry feature))
   feature))

(defn node-paths
  "Takes paths, nodes it, and returns the noded paths. Original metadata
  are transferred onto the new paths.
  The noded paths may reasonably form a multigraph, which is interesting.
  "
  [paths]
  (let [noder (MCIndexNoder.)
        intersector (IntersectionAdder. (RobustLineIntersector.))
        factory (GeometryFactory.)
        make-point #(let [p (.createPoint factory %)]
                      {::geoio/geometry p
                       ::geoio/id (geoio/geometry->id p)})
        make-linestring #(.createLineString factory %)
        point-id #(geoio/geometry->id (make-point %))
        ]

    (.setSegmentIntersector noder intersector)
    (let [segments (map feature->segment-string paths)]
      (println "Computing nodes...")
      (.computeNodes noder segments)

      (let [noded-segments (.getNodedSubstrings noder)]
        (println "Noding completed" (count paths) "before noding"
                 (count noded-segments) "after noding"
                 )
        (for [seg noded-segments
              :let [feature (.getData seg)
                    coords (.getCoordinates seg)
                    new-geom (make-linestring coords)
                    ]]
          (assoc feature
                 ::geoio/geometry new-geom
                 ::geoio/id (geoio/geometry->id new-geom)
                 ;; this topology construction depends entirely on the
                 ;; noder producing identical points at the touching
                 ;; parts of segments
                 ::start-node (make-point (first coords))
                 ::end-node (make-point (last coords))
                 ))))))
