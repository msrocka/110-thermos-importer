(ns thermos-importer.spatial
  (:refer-clojure :exclude [cond])
  (:require [thermos-importer.geoio :as geoio]
            [better-cond.core :refer [cond]]
            [clojure.tools.logging :as log])
  (:import [com.github.davidmoten.rtree RTree Entry]
           [com.github.davidmoten.rtree.geometry Geometries Rectangle]

           [org.locationtech.jts.geom Geometry Envelope PrecisionModel GeometryFactory Coordinate]
           [org.locationtech.jts.operation.distance DistanceOp GeometryLocation]
           [org.locationtech.jts.noding
            SegmentString MCIndexNoder NodedSegmentString IntersectionAdder
            SegmentStringDissolver]
           [org.locationtech.jts.algorithm RobustLineIntersector]

           [org.geotools.geometry.jts JTS]
           [org.geotools.referencing CRS]
           [org.opengis.referencing.operation MathTransform]
           
           [org.geotools.geometry Envelope2D]
           ))

(def SMALL_DISTANCE 0.1)
(def NEARNESS 500) ;; metres, since we go into our equal-area projection
(def NEIGHBOURS 6) ;; number of neighbours to consider
;;(def coordinates (Class/forName "[Lorg.locationtech.jts.geom.Coordinate;"))

(defn feature->rect ^Rectangle [feature]
  (let [^Geometry geometry (::geoio/geometry feature)
        ^Envelope bbox (.getEnvelopeInternal geometry)]
    (Geometries/rectangle
     (.getMinX bbox) (.getMinY bbox)
     (.getMaxX bbox) (.getMaxY bbox))))

(let [delete
      (fn [^RTree index feature]
        (.delete index feature (feature->rect feature) true))]
  (defn index-remove! [index feature]
    (swap! index delete feature)))

(let [insert
      (fn [^RTree index feature]
        (.add index feature (feature->rect feature)))]
  (defn index-insert! [index feature]
    (swap! index insert feature)))

(defn features->index [features]
  (let [index (atom (.create (RTree/star)))]
    (doseq [feature features]
      (index-insert! index feature))

    index))

(defn index->features [index]
  (map (fn [^Entry e] (.value e))
       (let [^RTree index @index]
         (-> index .entries .toBlocking .toIterable))))

(defn- feature-neighbours
  "Given an INDEX and a FEATURE, find the values in the index which are
  near to the feature's bounding-box."
  [index feature]
  
  (let [^RTree index @index]
    (for [^Entry entry
          (-> (.nearest index (feature->rect feature) (double NEARNESS) (int NEIGHBOURS))
              .toBlocking .toIterable)]
     (.value entry))))

(defn feature-overlaps
  "Given an INDEX and a FEATURE, find the values in the index whose
  bounding boxes overlap the feature's bounding box"
  [index feature]
  (let [bbox (feature->rect feature)
        ^RTree index @index]
    (for [^Entry entry (-> (.search index (feature->rect feature))
                    .toBlocking .toIterable)]
      (.value entry))))

(defn- features-intersect? [{^Geometry a ::geoio/geometry}
                            {^Geometry b ::geoio/geometry}]
  (try (.intersects a b)
       (catch Exception e
         (log/error e "Intersecting features produced an exception, possibly due to invalid geometry")
         false)))

(defn- feature-intersections [index feature]
  (->> feature ;; take the feature
       (feature-overlaps index) ;; find things which could intersect
       (filter (partial features-intersect? feature)))) ;; restrict to things which do

(defn sensible-projection
  "Create a projection of a certain type for the given features in their input CRS"
  [type input-crs features]
  (let [input-crs (CRS/decode input-crs true)
        ^Envelope2D bounding-box
        (reduce (fn ^Envelope2D [^Envelope2D box feat]
                  (.include box (JTS/getEnvelope2D
                                 (.getEnvelopeInternal ^Geometry (::geoio/geometry feat))
                                 input-crs))
                  box)
                (Envelope2D.)
                features)

        wkt
        (case type
          :lambert-conformal-conic
          (let [parallel-1 (.getMinimum bounding-box 1)
                parallel-2 (.getMaximum bounding-box 1)
                latitude-origin (.getMedian bounding-box 1)
                longitude-origin (.getMedian bounding-box 0)]
            (format "PROJCS[\"Lambert_Conformal_Conic\",
    GEOGCS[\"GCS_European_1950\",
        DATUM[\"European_Datum_1950\",
            SPHEROID[\"International_1924\",6378388,297]],
        PRIMEM[\"Greenwich\",0],
        UNIT[\"Degree\",0.017453292519943295]],
    PROJECTION[\"Albers_Conic_Equal_area\"],
    PARAMETER[\"False_Easting\",0],
    PARAMETER[\"False_Northing\",0],
    PARAMETER[\"longitude_of_center\",%f],
    PARAMETER[\"Standard_Parallel_1\",%f],
    PARAMETER[\"Standard_Parallel_2\",%f],
    PARAMETER[\"latitude_of_center\",%f],
    UNIT[\"Meter\",1]]"
                    longitude-origin
                    parallel-1
                    parallel-2
                    latitude-origin
                    ))
          :azimuthal-equidistant
          
          (let [latitude-origin (.getMedian bounding-box 1)
                longitude-origin (.getMedian bounding-box 0)]
            (format "PROJCS[\"unnamed\",
    GEOGCS[\"WGS 84\",
        DATUM[\"unknown\",
            SPHEROID[\"WGS84\",6378137,298.257223563]],
        PRIMEM[\"Greenwich\",0],
        UNIT[\"degree\",0.0174532925199433]],
    PROJECTION[\"Azimuthal_Equidistant\"],
    PARAMETER[\"latitude_of_center\",%f],
    PARAMETER[\"longitude_of_center\",%f],
    PARAMETER[\"false_easting\",0],
    PARAMETER[\"false_northing\",0],
    UNIT[\"Meter\", 1.0]]"
                    latitude-origin
                    longitude-origin
                    ))
          
          (throw (ex-info "Unknown type of projection"
                          {:type type})))]
    (CRS/findMathTransform input-crs (CRS/parseWKT wkt) true)))

;; it seems like this does reproject into metres, but it doesn't make
;; the connectors look perpendicular?
(defn reproject [features ^MathTransform transform]
  (map
   (fn [feature]
     (let [^Geometry g (::geoio/geometry feature)
           g2 (JTS/transform g transform)
           id2 (geoio/geometry->id g2)]
       (assoc feature
              ::geoio/id id2
              ::geoio/geometry g2)))
   features))

(defn add-connections
  [crs buildings noded-paths]
  (println "Connect" (count buildings) "with" (count noded-paths))
  (let [transform (sensible-projection :azimuthal-equidistant crs buildings)
        buildings (reproject buildings transform)
        noded-paths (reproject noded-paths transform)

        building-nodes (atom {}) ;; maps building IDs to node IDs
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
        factory (GeometryFactory. (PrecisionModel.)
                                  (CRS/lookupEpsgCode
                                   (CRS/decode crs true)
                                   true))
        make-node
        (fn [^Coordinate n]
          (let [p (.createPoint factory n)]
            (geoio/update-geometry {} p)))

        make-path
        (fn [meta coords]
          (let [^"[Lorg.locationtech.jts.geom.Coordinate;"
                coords (into-array Coordinate coords)
                geom (.createLineString factory coords)]
            (geoio/update-geometry meta geom)))

        split-connect-path!
        (fn [p b & [^DistanceOp op]]
          (let [^DistanceOp
                op (or op (DistanceOp. ^Geometry (::geoio/geometry p)
                                       ^Geometry (::geoio/geometry b)))
                [^GeometryLocation on-p
                 ^GeometryLocation on-b] (.nearestLocations op)
                distance (.distance op)

                path-coordinates (.getCoordinates ^Geometry (::geoio/geometry p))
                split-position (.getSegmentIndex on-p)
                split-point (.getCoordinate on-p)

                connect-to-node
                (cond
                  (and (zero? split-position)
                       (= split-point (first path-coordinates)))
                  (::start-node p)

                  (and (= split-position (- (count path-coordinates) 2))
                       (= split-point (last path-coordinates)))
                  (::end-node p)

                  :otherwise
                  (let [
                        [p-start p-end] (split-at (inc split-position) path-coordinates)

                        ;; convert coordinate chains to features:
                        p-start (make-path p (concat p-start [split-point]))
                        p-end (make-path p (concat [split-point] p-end))

                        ;; update start and end vertices of the two new paths
                        new-node (make-node split-point)
                        p-start (assoc p-start ::end-node new-node ::split-type "start-half")
                        p-end (assoc p-end ::start-node new-node ::split-type "end-half")
                        ]
                    ;; we need to delete p from the path-index
                    (index-remove! path-index p)

                    ;; we need to add p-start and p-end to the path index
                    (index-insert! path-index p-start)
                    (index-insert! path-index p-end)

                    ;; we need to add new-node to the node index
                    (index-insert! node-index new-node)

                    new-node)
                  )
                ]

            (if (> distance SMALL_DISTANCE)
              ;; we need a connecting line!
              (let [new-end-node (make-node (.getCoordinate on-b))
                    connector (make-path {::start-node connect-to-node
                                          ::end-node new-end-node
                                          :subtype "Connector"}
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

        building-count
        (count buildings)

        buildings-done
        (atom 0)]
    
    (println "Processing buildings")
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
              [nearest-path op] (first (sort-by #(.distance ^DistanceOp (second %)) distance-ops))
              ]

        nearest-path
        ;; can reuse the distanceop here to save a tiny bit of time
        (split-connect-path! nearest-path building op))

      (swap! buildings-done inc)
      (when (zero? (mod @buildings-done 1000))
        (println @buildings-done "/" building-count)))

    ;; at this point the node and path indices contain the network
    ;; structure and the building-nodes map contains the connection
    ;; from each building to a path. We want to output some new
    ;; information which is the revised set of paths and buildings. we
    ;; also stick on the length and area while we're here as we're in
    ;; the right coordinate system
    (let [paths (index->features path-index)

          add-length #(assoc % ::length (.getLength ^Geometry (::geoio/geometry %)))
          
          paths (map add-length paths)
          
          building-nodes @building-nodes
          buildings (map
                     #(assoc % ::connects-to-node (building-nodes (::geoio/id %)))
                     buildings)

          add-area #(assoc % ::area (.getArea ^Geometry (::geoio/geometry %)))
          buildings (map add-area buildings)

          ;; finally put it back into our input CRS
          inverse-transform (.inverse transform)
          buildings (reproject buildings inverse-transform)
          paths (reproject paths inverse-transform)
          ]
      [buildings paths])))

(defn feature->segment-string
  "This makes a nodedsegmentstring which refers back to the feature
  which it came from, so we can preserve the feature metadata (IDs etc)."
  [feature]
  (NodedSegmentString.
   ^"[Lorg.locationtech.jts.geom.Coordinate;"
   (.getCoordinates ^Geometry (::geoio/geometry feature))
   feature))

(let [factory (GeometryFactory.)]
  (defn- make-point [^Coordinate x]
    (let [p (.createPoint factory x)]
      (geoio/update-geometry {} p)))
  
  (defn- make-linestring ^Geometry [^"[Lorg.locationtech.jts.geom.Coordinate;" coords]
    (.createLineString factory coords)))

(defn- concat-linestrings [^Geometry a ^Geometry b]
  (let [coords-a (.getCoordinates a)
        coords-b (.getCoordinates b)
        a0 (first coords-a) an (last coords-a)
        b0 (first coords-b) bn (last coords-b)
        ]
    (cond
      (= a0 b0)
      (make-linestring (into-array Coordinate (concat (reverse coords-b) coords-a)))
      
      (= a0 bn)
      (make-linestring (into-array Coordinate (concat coords-b coords-a)))
      
      (= an bn)
      (make-linestring (into-array Coordinate (concat coords-a (reverse coords-b))))

      (= an b0)
      (make-linestring (into-array Coordinate (concat coords-a coords-b)))

      :otherwise (throw (IllegalArgumentException. (format "No touching endpoints: %s %s %s %s"
                                                           a0 an b0 bn))))))


(defn- add-endpoints [feature]
  (let [coords (.getCoordinates ^Geometry (::geoio/geometry feature))]
    (assoc feature
           ::start-node (make-point (first coords))
           ::end-node (make-point (last coords)))))

(defn- assoc-by [f s]
  (reduce #(assoc %1 (f %2) %2)  {} s))

(defn- collapse-paths
  "Takes paths which have been noded and so contain start and end IDs,
  and collapses them together at junctions of degree 2 where this will
  not affect the properties of the resulting paths."
  [paths]
  (println "Remove redundant vertices from" (count paths) "paths")
  (let [safe-inc #(inc (or % 0))

        paths-by-id (assoc-by ::geoio/id paths)
        
        paths-by-vertex
        (reduce (fn [a p]
                  (-> a
                      (update (::geoio/id (::start-node p))
                              conj p)
                      (update (::geoio/id (::end-node p))
                              conj p)))
                {} paths)

        collapsible-vertices
        (->> paths-by-vertex
             (filter 
              (fn [[v p]]
                (and (= 2 (count p))
                     (= (dissoc (first p)
                                ::geoio/id ::geoio/geometry ::start-node ::end-node)
                        (dissoc (second p)
                                ::geoio/id ::geoio/geometry ::start-node ::end-node)))))
             (into {}))

        _ (println (count collapsible-vertices) "redundant vertices to remove")
        
        find-new-name
        #(loop [new-names %1 id %2]
           (if (contains? new-names id)
             (recur new-names (get new-names id))
             id))

        paths-by-id
        (loop [paths-by-id paths-by-id
               new-names {}
               collapsible-vertices (vals collapsible-vertices)]
          (if (seq collapsible-vertices)
            (let [[a b] (first collapsible-vertices)
                  ;; we might have collapsed the input paths other ends
                  ;; already so we need to look them up in the new-names
                  ;; map
                  
                  a (paths-by-id (find-new-name new-names (::geoio/id a)))
                  b (paths-by-id (find-new-name new-names (::geoio/id b)))

                  new-path (-> a
                               (geoio/update-geometry
                                (concat-linestrings
                                 (::geoio/geometry a)
                                 (::geoio/geometry b)))
                               (add-endpoints))

                  _ (when (= (::geoio/id (::start-node new-path))
                           (::geoio/id (::end-node new-path))
                           )
                      (println "Endpoints of new path collide")
                      (println a)
                      (println b)
                      (println new-path)
                      )
                  
                  new-id (::geoio/id new-path)

                  ;; record this renaming
                  new-names (assoc new-names
                                   (::geoio/id a) new-id
                                   (::geoio/id b) new-id)

                  ;; add new path and delete old ones
                  paths-by-id (-> paths-by-id
                                  (assoc new-id new-path)
                                  (dissoc (::geoio/id a)
                                          (::geoio/id b)))
                  ]
              (recur paths-by-id new-names (rest collapsible-vertices)))
            paths-by-id))
        
        paths (vals paths-by-id)
        ]
    paths
    ))

(defn node-paths
  "Takes paths, nodes it, and returns the noded paths. Original metadata
  are transferred onto the new paths.
  The noded paths may reasonably form a multigraph, which is interesting.
  "
  [paths]
  (let [noder (MCIndexNoder.)
        intersector (IntersectionAdder. (RobustLineIntersector.))
        ]

    (.setSegmentIntersector noder intersector)
    (let [segments (map feature->segment-string paths)]
      (println "Computing nodes...")
      (.computeNodes noder segments)

      (let [noded-segments (.getNodedSubstrings noder)
            dissolver (SegmentStringDissolver.)
            _ (.dissolve dissolver noded-segments)
            noded-segments (.getDissolved dissolver)
            ]
        (println "Noding completed" (count paths) "before noding"
                 (count noded-segments) "after noding")

        (collapse-paths
         (for [^SegmentString seg noded-segments
               :let [feature (.getData seg)
                     coords (.getCoordinates seg)
                     new-geom (make-linestring coords)
                     new-id (geoio/geometry->id new-geom)]]
           (-> feature
               (geoio/update-geometry new-geom)
               (add-endpoints))))))))

