;; This file is part of THERMOS, copyright © Centre for Sustainable Energy, 2017-2021
;; Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

(ns thermos-importer.spatial
  (:refer-clojure :exclude [cond])
  (:require [thermos-importer.geoio :as geoio]
            [better-cond.core :refer [cond]]
            [clojure.tools.logging :as log]
            [cljts.core :as jts]
            [clojure.set :as set])
  (:import [com.github.davidmoten.rtree RTree Entry]
           [com.github.davidmoten.rtree.geometry Geometries Rectangle]

           [org.locationtech.jts.geom Geometry Point Envelope PrecisionModel GeometryFactory Coordinate LineString]
           [org.locationtech.jts.operation.distance DistanceOp GeometryLocation]
           [org.locationtech.jts.noding
            SegmentString MCIndexNoder NodedSegmentString IntersectionAdder
            SegmentStringDissolver]
           [org.locationtech.jts.algorithm RobustLineIntersector]
           [org.geotools.geometry.jts JTS]
           [org.geotools.referencing CRS]
           [org.opengis.referencing.operation MathTransform]
           [org.geotools.referencing.operation.transform IdentityTransform]
           [org.geotools.geometry Envelope2D]
           ))



(def SMALL_DISTANCE 0.1)
(def NEARNESS 500) ;; metres, since we go into our equal-area projection
(def NEIGHBOURS 6) ;; number of neighbours to consider
;;(def coordinates (Class/forName "[Lorg.locationtech.jts.geom.Coordinate;"))


(let [factory (GeometryFactory.)]
  (defn- make-point [^Coordinate x]
    (let [p (.createPoint factory x)]
      (geoio/update-geometry {} p)))
  
  (defn- make-linestring ^Geometry [^"[Lorg.locationtech.jts.geom.Coordinate;" coords]
    (.createLineString factory coords))

  (defn- make-multipoint ^Geometry [coordinates]
    (let [points (->> coordinates
                      (filter identity)
                      (map #(.createPoint factory ^Coordinate %))
                      (into-array Point))]
      (.createMultiPoint factory ^"[Lorg.locationtech.jts.geom.Point;" points))))

(defn feature->rect ^Rectangle [feature]
  (let [^Geometry geometry (::geoio/geometry feature)
        ^Envelope bbox (.getEnvelopeInternal geometry)]
    (try
      (Geometries/rectangle
       (.getMinX bbox) (.getMinY bbox)
       (.getMaxX bbox) (.getMaxY bbox))
      (catch Exception ex
        (throw (ex-info "Feature had invalid bounds" {:feature feature :bbox bbox}))))))

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

(defn index->features
  "Given an index, return a seq of the features in it (not in a geoio map, just features)"
  [index]
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

(defn point-neighbours
  "Given `index` and a `point`, find values in the index which are near the point"
  [index point & {:keys [distance limit]
                  :or {distance NEARNESS limit NEIGHBOURS}}]
  (let [point 
        (cond
          (instance? com.github.davidmoten.rtree.geometry.Point point)
          point

          (vector? point)
          (Geometries/point ^double (first point) ^double (second point))

          (instance? Point point)
          (let [coord (.getCoordinate ^Point point)]
            (Geometries/point (.getX coord) (.getY coord)))

          (instance? Coordinate point)
          (Geometries/point (.getX ^Coordinate point) (.getY ^Coordinate point))
          
          :else (throw (ex-info "Unable to convert to a point" {:point point})))

        ^RTree index @index]
    (for [^Entry entry
          (-> (.nearest index
                        ^com.github.davidmoten.rtree.geometry.Point point
                        (double distance) (int limit))
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

(defn utm-zone-epsg-code [lon lat]
  (let [utm-band (+ 1 (mod (int (Math/floor (/ (+ lon 180.0) 6.0))) 60))]
    (if (>= lat 0)
      (format "326%02d" utm-band)
      (format "327%02d" utm-band))))


(defn extent 
  "Returns a bounding box that encloses a set of geometries"
  ^Envelope2D [features crs]
  (let [crs (geoio/decode-crs crs)]
    (reduce (fn ^Envelope2D [^Envelope2D box feat]
              (.include box (JTS/getEnvelope2D
                             (.getEnvelopeInternal ^Geometry (::geoio/geometry feat))
                             crs))
              box)
            (Envelope2D.)
            features)))

(defn centroid
  "Returns the centroid of the bounding box that encloses a set of geometries"
  [features crs]
  (let [crs (geoio/decode-crs crs)
        ^Envelope2D bounding-box (extent features crs)]
    
        {:lat (.getMedian bounding-box 1)
         :lng (.getMedian bounding-box 0)}))

(defn is-in-metres? [^org.opengis.referencing.crs.CoordinateReferenceSystem crs]
  (and crs
       (let [crs (.getCoordinateSystem crs)
             naxes (.getDimension crs)
             units (for [i (range naxes)] (.getUnit (.getAxis crs i)))]
         (every? #(= tec.uom.se.unit.Units/METRE %) units))))

(defn sensible-projection
  "Create a projection of a certain type for the given features in their input CRS"
  ^MathTransform [type input-crs features]
  
  (let [input-crs (CRS/decode input-crs true)]
    (if (is-in-metres? input-crs)
      (-> input-crs
          (.getCoordinateSystem)
          (.getDimension)
          (IdentityTransform/create))
      
      (let [^Envelope2D bounding-box (extent features input-crs)

            lat-centre (.getMedian bounding-box 1)
            lon-centre (.getMedian bounding-box 0)

            from-wkt (fn [wkt] (CRS/findMathTransform input-crs (CRS/parseWKT wkt) true))
            from-epsg (fn [epsg]
                        (CRS/findMathTransform input-crs
                                               (CRS/decode (str "EPSG:" epsg) true)
                                               true))
            ]

        (case type
          :lambert-conformal-conic
          (let [parallel-1 (.getMinimum bounding-box 1)
                parallel-2 (.getMaximum bounding-box 1)]
            
            (from-wkt
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
                     lon-centre
                     parallel-1
                     parallel-2
                     lat-centre
                     )))
          :azimuthal-equidistant
          
          (from-wkt
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
                   lat-centre
                   lon-centre))

          ;; :oblique-mercator
          ;; (let [latitude-origin (.getMedian bounding-box 1)
          ;;         longitude-origin (.getMedian bounding-box 0)]
          ;;     (format
          ;;      "PROJCS[\"OBLIQUE MERCATOR\",
          ;; GEOGCS[\"WGS 84\", 
          ;;        DATUM[\"WGS_1984\", SPHEROID[\"WGS 84\",6378137,298.257223563]],
          ;;        PRIMEM[\"Greenwich\",0],
          ;;        UNIT[\"degree\",0.01745329251994328]],
          ;; PROJECTION[\"Hotine_Oblique_Mercator\"],
          ;; PARAMETER[\"latitude_of_center\", %f],
          ;; PARAMETER[\"longitude_of_center\", %f],
          ;; PARAMETER[\"azimuth\",89.999999],
          ;; PARAMETER[\"rectified_grid_angle\",89.999999],
          ;; PARAMETER[\"scale_factor\",1],
          ;; PARAMETER[\"false_easting\",0],
          ;; PARAMETER[\"false_northing\",0],
          ;; UNIT[\"Meter\",1]]"
          
          ;;             latitude-origin
          ;;             longitude-origin
          ;;             ))


          :utm-zone
          (from-epsg (utm-zone-epsg-code lon-centre lat-centre))
          
          (throw (ex-info "Unknown type of projection"
                          {:type type})))
        ))))

(defn reproject-1 [feature ^MathTransform transform]
  (let [^Geometry g (::geoio/geometry feature)
        g2 (JTS/transform g transform)
        id2 (geoio/geometry->id g2)]
    (assoc feature
           ::geoio/id id2
           ::geoio/geometry g2)))

(defn reproject [features ^MathTransform transform]
  (map
   #(reproject-1 % transform)
   features))

(defn add-connections
  "Draw connectors from paths to buildings.

  `buildings` and `noded-paths` are both seqs of geoio feature maps.

  Returns a tuple of updated buildings and paths, so that:
  - every building has also got `::connects-to-node` on it
  - paths contains new connectors + subdivided paths so that buildings are connected to paths

  If `source-field` and `target-field` are provided in `:copy-field`, then
  buildings will also take on the value of `source-field` in the input path they got connected to.

  If `connect-to-connectors` is true, connectors may be drawn to other connectors.

  `shortest-face-length` affects where connectors to go - ideally to the midpoint of a face at least this long.
  Otherwise they go to wherever (a corner or something)
  "
  [crs buildings noded-paths & {[source-field target-field] :copy-field
                                :keys [connect-to-connectors shortest-face-length]
                                :or {connect-to-connectors false
                                     shortest-face-length 3}}]
  
  (log/info "Connect" (count buildings) "with" (count noded-paths))
  (let [reproject-endpoints (fn [paths transform]
                              (for [path paths]
                                (-> path
                                    (update ::start-node reproject-1 transform)
                                    (update ::end-node reproject-1 transform))))

        transform (sensible-projection :utm-zone crs buildings)
        buildings (reproject buildings transform)
        
        noded-paths (-> noded-paths
                        (reproject transform)
                        ;; we have to reproject the endpoints also, to
                        ;; make IDs consistent. TODO there is a risk
                        ;; here that this will break the noding?
                        (reproject-endpoints transform))

        building-nodes (atom {}) ;; maps building IDs to nodes where they connect

        path-index (features->index noded-paths)

        endpoints (fn [f] [(::start-node f) (::end-node f)])

        ;; these are all the unique vertices the paths touch
        nodes (set (mapcat endpoints noded-paths))

        ;; an index of all the vertices that exist
        node-index (features->index nodes)

        ;; this atom below maps from node ID to source-field value of
        ;; input path, for use when :copy-field is supplied. it is
        ;; kept up-to-date by operations below that introduce new
        ;; paths, and used at the end to assoc target-field onto
        ;; buildings.
        node->source-field
        (atom
         (when (and target-field source-field)
           (reduce
            (fn [out path]
              (let [source-field (get path source-field)]
                (assoc out
                       (::geoio/id (::start-node path)) source-field
                       (::geoio/id (::end-node path)) source-field)))
            {} noded-paths)))

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

                    ;; copy the source-field from the path into the node lookup.
                    (when source-field
                      (swap! node->source-field assoc
                             (::geoio/id new-node)
                             (get p source-field)))
                    
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
                    connector (make-path (cond->
                                             {::start-node connect-to-node
                                              ::end-node new-end-node
                                              :connector true
                                              :subtype "Connector"
                                              }
                                           source-field (assoc source-field (get p source-field)))
                                         [split-point (.getCoordinate on-b)])]
                ;; we need to add the connector to the path index
                (index-insert! path-index connector)
                ;; we need to add the connector's end node to the node index
                (index-insert! node-index new-end-node)
                ;; we need to record the new-end-node as having relevant source field
                (when source-field
                  (swap! node->source-field assoc (::geoio/id new-end-node) (get p source-field)))
                ;; we need to write down the building connection
                (swap! building-nodes update (::geoio/id b) conj new-end-node))

              ;; otherwise we just need to connect the building:
              (swap! building-nodes update (::geoio/id b) conj connect-to-node)
              )))

        building-count
        (count buildings)

        buildings-done
        (atom 0)

        connect-freely
        (fn [building path]
          (let [op (DistanceOp. (::geoio/geometry path)
                                (::geoio/geometry building))]
            {:path path
             :op op
             :distance (.distance op)
             :is-to-connector (:connector path)}))

        connect-faces
        (fn [building path]
          (let [boundary ^Geometry (.getBoundary ^Geometry (::geoio/geometry building))
                boundary-coords (.getCoordinates boundary)
                connection-points (make-multipoint
                                   (for [[^Coordinate a ^Coordinate b] (partition 2 1 boundary-coords)
                                         :let [distance (.distance a b)]
                                         :when (> distance shortest-face-length)]
                                     (Coordinate.
                                      (/ (+ (.getX a)
                                            (.getX b)) 2.0)
                                      (/ (+ (.getY a)
                                            (.getY b)) 2.0))))]
            (when-not (.isEmpty connection-points)
              (let [solution (connect-freely {::geoio/geometry connection-points} path)]
                (update solution :distance - 5.0)))))
        ]
    
    (doseq [building buildings]
      (cond
        ;; step 1: find any intersecting nodes and connect to those
        :let [intersecting-nodes (feature-intersections node-index building)]

        (not-empty intersecting-nodes)
        ;; connect the building directly to a node
        (swap! building-nodes
               assoc (::geoio/id building)
               intersecting-nodes)

        ;; step 2: there were no intersecting nodes, what about paths?
        :let [intersecting-paths (feature-intersections path-index building)]

        (not-empty intersecting-paths)
        (doseq [path intersecting-paths] (split-connect-path! path building))

        ;; step 3: there were no intersecting paths, try a nearby path?

        ;; In this condition we are going to be dropping a line, so we
        ;; may also want to try connecting to midpoints of building
        ;; faces instead of corners.

        :when-let [nearby-paths (feature-neighbours path-index building)]
        :let [connections
              (filter identity
                      (concat
                       (map (partial connect-freely building) nearby-paths)
                       (map (partial connect-faces building) nearby-paths)))
              
              sort-rule (if connect-to-connectors
                          :distance
                          (juxt :is-to-connector :distance))
              
              {nearest-path :path op :op}
              (first (sort-by sort-rule connections))
              ]

        nearest-path
        ;; can reuse the distanceop here to save a tiny bit of time
        (split-connect-path! nearest-path building op))

      (swap! buildings-done inc))

    ;; at this point the node and path indices contain the network
    ;; structure and the building-nodes map contains the connection
    ;; from each building to a path. We want to output some new
    ;; information which is the revised set of paths and buildings. we
    ;; also stick on the length and area while we're here as we're in
    ;; the right coordinate system

    (let [inverse-transform (.inverse transform)

          paths (index->features path-index)

          add-length #(assoc % ::length (.getLength ^Geometry (::geoio/geometry %)))
          
          paths (map add-length paths)

          building-nodes @building-nodes
          
          ;; relate buildings to their connecting nodes
          ;; we reproject the nodes back out to make their IDs good first.
          buildings (for [building buildings]
                      (cond->
                          (assoc building ::connects-to-node
                                 (for [node (building-nodes (::geoio/id building))]
                                   (::geoio/id (reproject-1 node inverse-transform))))

                        ;; if we have a target field, copy the first
                        ;; source-field from a connecting node
                        (and source-field target-field)
                        (assoc target-field
                               (->> (building-nodes (::geoio/id building))
                                    (map ::geoio/id)
                                    (keep @node->source-field)
                                    (first)))))

          ;; swap to the output coordinate system for start/end nodes
          add-area #(assoc % ::area (.getArea ^Geometry (::geoio/geometry %)))
          buildings (map add-area buildings)

          ;; finally put it back into our input CRS
          buildings (reproject buildings inverse-transform)

          ;; to get the paths out we need to invert their endpoints as well.
          ;; this is a bit wasteful as we have inverted a lot of them already
          ;; when we did buildings above, but hey.
          paths (-> paths
                    (reproject inverse-transform)
                    (reproject-endpoints inverse-transform))
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

(defn- concat-linestrings [^Geometry a ^Geometry b]
  (let [coords-a (.getCoordinates a)
        coords-b (.getCoordinates b)
        ^Coordinate a0 (first coords-a) ^Coordinate an (last coords-a)
        ^Coordinate b0 (first coords-b) ^Coordinate bn (last coords-b)
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

      ;;; nearly there
      (< (Math/abs (.distance a0 b0)) 0.00001)
      (make-linestring (into-array Coordinate (concat (reverse coords-b) coords-a)))
      
      (< (Math/abs (.distance a0 bn)) 0.00001)
      (make-linestring (into-array Coordinate (concat coords-b coords-a)))
      
      (< (Math/abs (.distance an bn)) 0.00001)
      (make-linestring (into-array Coordinate (concat coords-a (reverse coords-b))))

      (< (Math/abs (.distance an b0)) 0.00001)
      (make-linestring (into-array Coordinate (concat coords-a coords-b)))
      
      :otherwise
      (throw
       (ex-info "Concatenating linestrings which do not touch"
                {:a a :b b}))
      )))


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
                      (log/warn "Endpoints of new path collide"
                                {:a a :b b :new-path new-path}))
                  
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

(defn- snap-to [path position path-index distance]
  (let [^LineString geometry (::geoio/geometry path)
        index (case position
                   :first 0
                   :last  (dec (.getNumPoints geometry))
                   (int position))
        coord (.getCoordinateN geometry index)

        [neighbour ^Coordinate snap-point distance]
        (loop [ns (remove #{path}
                          (point-neighbours path-index coord
                                            :distance distance :limit 100))
               neighbour nil
               d Double/MAX_VALUE
               c nil]
          (if (seq ns)
            (let [[n & ns] ns
                  geom (::geoio/geometry n)
                  [d' c'] (jts/find-closest-coord geom coord)
                  d' (double d')]
              (cond
                (zero? d') nil

                (and (<= d' distance) ;; it's close and we already overlap, skip
                     (jts/intersects? geometry geom))
                nil
                
                (and (<= d' d)
                     (<= d' distance))
                
                (recur ns n d' c')
                
                :else (recur ns neighbour d c)))
            [neighbour c d]))
        ]
    (if snap-point
      (do
        (index-remove! path-index path) ;; icky side-effects
        (let [old-coords (.getCoordinates geometry)
              coords (make-array Coordinate (inc (.getNumPoints geometry)))
              dx (- (.getX snap-point) (.getX coord))
              dy (- (.getY snap-point) (.getY coord))

              neigbhour-coordinates (.getCoordinates (::geoio/geometry neighbour))
              
              snap-point
              (if (.contains (java.util.Arrays/asList neigbhour-coordinates) snap-point)
                snap-point ;; if the point is definitely within the geometry, just keep it

                ;; otherwise we shift it a little to make sure it crosses over
                ;; to fix issue with numerical instability
                (Coordinate. (+ (* 1.001 dx) (.getX coord))
                             (+ (* 1.001 dy) (.getY coord))))
              
              index (if (= position :last) (inc index) index)
              ]
          ;; copy the new coordinate
          (aset coords index snap-point)
          ;; copy the old coordinates
          (dotimes [i (alength old-coords)]
            (aset coords
                  (+ i (if (>= i index) 1 0))
                  (aget old-coords i)))

          (let [new-path (-> (geoio/update-geometry
                              path (make-linestring coords))
                             (update ::snap conj [index distance]))]
            (index-insert! path-index new-path) ;; urgh
            new-path)))

      path)))

(defn connect-nearly-connected-paths
  "Function for filling in tiny holes in a network.
  
  - `paths` is a sequence of geometry maps that are all polylines.
  - `distance` is how many CRS units we may extend any path

  Return a modified version of `paths` in which any path whose
  endpoint was near but not touching another path is moved onto that
  second path.
  "
  [paths & {:keys [distance] :or {distance SMALL_DISTANCE}}]

  (let [path-index (features->index paths)]
    ;; this is a bit grotty, as snap-to-path is impure
    ;; and mutates path-index.
    (for [path paths]
      (-> path
          (snap-to 0      path-index distance)
          (snap-to :last  path-index distance)))))

(defn node-paths
  "Takes paths, nodes it, and returns the noded paths. Original metadata
  are transferred onto the new paths.
  The noded paths may reasonably form a multigraph, which is interesting.

  If `snap-tolerance` is a number, try snapping points at this distance.
  If you give `snap-tolerance` in metres and paths are in lon/lat, supply a CRS also

  The paths will be reprojected into a sensible CRS, where the noding will happen.
  "
  [paths & {:keys [snap-tolerance crs]}]
  
  (let [[project unproject-1]
        (if crs
          (let [transform   (sensible-projection :utm-zone crs paths)
                transform'  (.inverse transform)]
            [#(reproject % transform) #(reproject-1 % transform')])
          [identity identity])

        paths (project paths)
        
        paths (cond-> paths
                (number? snap-tolerance)
                (connect-nearly-connected-paths :distance snap-tolerance))
        
        noder (MCIndexNoder.)
        intersector (IntersectionAdder. (RobustLineIntersector.))
        ]

    (.setSegmentIntersector noder intersector)
    (let [segments (map feature->segment-string paths)]
      (log/info "Computing nodes...")
      (.computeNodes noder segments)

      (let [noded-segments (.getNodedSubstrings noder)
            dissolver (SegmentStringDissolver.)
            _ (.dissolve dissolver noded-segments)
            noded-segments (.getDissolved dissolver)
            ]
        (log/info "Noding completed" (count paths) "before noding"
                 (count noded-segments) "after noding")

        (collapse-paths
         (for [^SegmentString seg noded-segments
               :let [feature (.getData seg)
                     coords (.getCoordinates seg)
                     new-geom (make-linestring coords)
                     new-id (geoio/geometry->id new-geom)]]
           (-> feature
               (geoio/update-geometry new-geom)
               (unproject-1) ;; hmm
               (add-endpoints))))))))

(defn- conjs [s x] (conj (or s #{}) x))

(defn trim-dangling-paths
  "Given some `paths` which have been noded and connected to `buildings`,
  trim any dangling paths"
  [paths buildings]

  ;; a path is a dangling path if it has a non-building vertex with degree 1

  (let [by-vertex
        (reduce
         (fn [by-vertex path]
           (let [path-id  (::geoio/id path)
                 start-id (::geoio/id (::start-node path))
                 end-id   (::geoio/id (::end-node path))]
             (-> by-vertex
                 (update start-id conjs path-id)
                 (update end-id   conjs path-id))))
         
         {} paths)

        degree
        (reduce-kv
         (fn [degree vtx paths]
           (assoc degree vtx (count paths)))
         {} by-vertex)

        degree ;; count up buildings as well (so a vertex at a
               ;; building has degree at least 3); this is a hack
               ;; which means when we remove vertices below that are
               ;; to do with paths, we never get a building down to 1
        (reduce
         (fn [degree building]
           (merge-with + degree (zipmap (::connects-to-node building) (repeat 2))))
         degree buildings)

        paths-by-id (->> (for [p paths] [(::geoio/id p) p]) (into {}))
        ]
    ;; so now we loop until we have pruned everything prune-able
    (loop [degree degree
           paths-by-id paths-by-id
           by-vertex by-vertex
           n 0
           ]

      ;; find all the degree-1 vertices; we can delete their paths
      (let [d1-vertices (for [[vertex degree] degree :when (== 1 degree)] vertex)]
        (if (and (seq d1-vertices) (< n 10))
          (let [[degree paths-by-id by-vertex]
                ;; loop over these vertices and delete them, updating degree & paths-by-id
                (loop [d1-vertices d1-vertices
                       degree degree
                       paths-by-id paths-by-id
                       by-vertex by-vertex]
                  (if (seq d1-vertices)
                    (let [[vertex & d1-vertices] d1-vertices ;; get a vertex

                          ;; by definition, by-vertex vertex has degree 1
                          ;; unless we already reduced its degree to zero elsewhere.
                          path-id (first (get by-vertex vertex))
                          ]
                      
                      (if path-id
                        (let [path     (get paths-by-id path-id)
                              start-id (::geoio/id (::start-node path))
                              end-id   (::geoio/id (::end-node path))

                              ;; reduce degree of the path's nodes
                              degree (-> degree
                                         (update start-id dec)
                                         (update end-id dec))

                              ;; delete the path
                              paths-by-id (dissoc paths-by-id path-id)

                              ;; remove the path from by-vertex so that
                              ;; the above destructuring [path] works next
                              ;; time round.
                              by-vertex (-> by-vertex
                                            (update start-id disj path-id)
                                            (update end-id   disj path-id))
                              ]
                          (recur d1-vertices degree paths-by-id by-vertex))
                        ;; skip it
                        (recur d1-vertices degree paths-by-id by-vertex)))
                    [degree paths-by-id by-vertex]))]
            
            ;; retry the outer loop
            (recur degree paths-by-id by-vertex (inc n)))

          ;; no more degree-1 vertices:
          (vals paths-by-id))))))

