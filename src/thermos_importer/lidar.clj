;; This file is part of THERMOS, copyright © Centre for Sustainable Energy, 2017-2021
;; Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

(ns thermos-importer.lidar
  (:require [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [thermos-importer.util :as util]
            [clojure.tools.logging :as log])
  (:import com.github.davidmoten.rtree.geometry.Geometries
           com.github.davidmoten.rtree.RTree
           [org.locationtech.jts.geom Coordinate GeometryFactory Polygon]
           [org.locationtech.jts.geom.util LineStringExtracter]
           org.geotools.coverage.grid.GridCoverage2D
           org.geotools.coverage.grid.io.GridFormatFinder
           org.geotools.factory.Hints
           org.geotools.gce.geotiff.GeoTiffFormat
           org.geotools.geometry.DirectPosition2D
           org.geotools.geometry.jts.ReferencedEnvelope
           org.geotools.geometry.jts.JTS
           org.geotools.referencing.CRS))

(def ^:dynamic *storey-height* 3.0)

(defn load-raster* [raster]
  (let [format (GridFormatFinder/findFormat raster)
        hints (when (instance? GeoTiffFormat format)
                (Hints. Hints/FORCE_LONGITUDE_FIRST_AXIS_ORDER true))
        reader (.getReader format raster hints)]
    (.read reader nil)))


(let [load-it (util/soft-memoize load-raster*)]
  (defn ^GridCoverage2D load-raster [raster]
    (load-it raster)))

; we memoize these fully,
; since the outputs are small
; and this gets called a lot.
(def raster-facts
  (memoize
   (fn [raster]
     (log/info "Load summary information for" raster)
     (let [raster-data (load-raster raster)

           envelope (.getEnvelope2D raster-data)
           bounds  (Geometries/rectangle
                    (.getMinimum envelope 0) (.getMinimum envelope 1)
                    (.getMaximum envelope 0) (.getMaximum envelope 1))

           crs (CRS/lookupIdentifier (.getCoordinateReferenceSystem2D raster-data) true)]
           ;; the thing we return is just the filename & summary. if
           ;; we really need the raster's data later we will load it
           ;; again and hopefully hit a cache in the process.
       {:raster raster :bounds  bounds :crs crs}))))

(defn rasters->index
    "Make an index which says which of these rasters (filenames) is where.
  The index is a map from EPSG code to an Rtree of rasters that have that EPSG.
  "
    [rasters]
    (log/info "Indexing rasters...")
    
    (let [properties                      ; first lookup the properties for each raster
          (map raster-facts rasters)
          
          by-crs                          ; bin them by CRS
          (group-by :crs properties)

          indices                         ; for each CRS, stuff them into an Rtree
          (for [[crs rasters] by-crs]
            [crs
             (reduce
              (fn [index {raster :raster bounds :bounds}]
                (.add index raster bounds))
              (RTree/create) rasters)])
          ]
      (into {} indices)))

(defn- find-rasters
  "Locate all the rasters that overlap the bounds of shape."
  [tree rect]
  (util/search-rtree tree rect))

(defn- sample-coords
  "Sample coordinates within shape from raster.
  Presumes coords are in the raster's CRS."
  [raster coords]
  (let [raster ^GridCoverage2D (load-raster raster)
        no-data (set (.getNoDataValues (.getSampleDimension raster 0)))]

    (filter
     identity
     (for [[x y] coords]
       (let [position ^DirectPosition2D (DirectPosition2D. x y)
             result (try
                      (.evaluate raster position)
                      (catch org.opengis.coverage.PointOutsideCoverageException e
                        nil))
             z (and result (aget result 0))]
         (when (and z (not (no-data z)))
           [x y z]))))))

(defn- grid-over
  "Make a seq of coordinates covering the SHAPE with a buffer of 1m

  TODO: make faster; delaunay triangulation + area weighted choice of
  triangle + uniform random point inside triangle

  http://mathworld.wolfram.com/TrianglePointPicking.html
  "
  [shape buffer-size]
  (let [shape (.buffer shape buffer-size)
        
        envelope (.getEnvelopeInternal shape)
        x-min (.getMinX envelope)
        x-max (.getMaxX envelope)
        y-min (.getMinY envelope)
        y-max (.getMaxY envelope)


        extent (max (Math/abs (- x-max x-min)) (Math/abs (- y-max y-min)))
        
        grid-step (max 1.0 (Math/round (/ extent 50.0)))
        ]

    (for [x (range x-min x-max grid-step)
          y (range y-min y-max grid-step)
          :when (.covers shape
                         (.createPoint
                          (.getFactory shape)
                          (Coordinate. x y)))]
      [x y])))

(defn envelope-covers-tree [raster-crs raster-tree
                            shapes-crs ^org.locationtech.jts.geom.Envelope shapes-envelope]

  (let [raster-mbr (.mbr raster-tree)]
    (when (.isPresent raster-mbr)
      (let [raster-mbr (.get raster-mbr)
            raster-crs (CRS/decode raster-crs true)
            transform (CRS/findMathTransform shapes-crs raster-crs)
            shapes-envelope (JTS/transform shapes-envelope transform)
            shapes-envelope (util/envelope->rect shapes-envelope)]
        (.intersects raster-mbr shapes-envelope)))))

(defn count-corners [geom]
  (case (.getGeometryType geom)
    ("Polygon" "MultiPolygon")
    (let [geom (org.locationtech.jts.simplify.TopologyPreservingSimplifier/simplify geom 7.5)]
      (dec (.getNumPoints geom)))
    0))

(defn estimate-height [tree shape
                       buffer-size ground-level-threshold]
  (let [rect    (util/geom->rect shape)
        rasters (find-rasters tree rect)
        grid    (grid-over shape buffer-size)
        coords  (mapcat #(sample-coords % grid) rasters)]
    (if (empty? coords)
      {::num-samples 0}
      (let [heights (map last coords)
            heights (filter #(> % ground-level-threshold) heights)
            
            ground (if (seq heights)
                     (apply min heights)
                     0)
            
            heights (map #(- % ground) heights)

            heights (filter #(> % 0.5) heights)
            mean-height (if (empty? heights)
                          0
                          (/ (apply + heights) (count heights)))
            ]
        {::num-samples (count coords)
         ::height mean-height
         ::ground-height ground}))))

(defn add-height-from-lidar
  "Update shapes to add ::height to any building which intersects the LIDAR"
  [shapes index & {:keys [buffer-size ground-level-threshold]
                   :or {buffer-size 1.5 ground-level-threshold -5}}]

  (let [shapes-crs (::geoio/crs shapes)
        decoded-shapes-crs (CRS/decode shapes-crs true)
        bounds (geoio/bounding-box shapes)
        index  (filter (fn [[raster-crs raster-tree]]
                         (envelope-covers-tree
                          raster-crs raster-tree decoded-shapes-crs bounds))
                       index)]
    (if (empty? index)
      shapes
      
      (reduce
       (fn [shapes [raster-crs raster-tree]]
         (let [T (CRS/findMathTransform
                  decoded-shapes-crs
                  (CRS/decode raster-crs true))]
           (geoio/update-features
            shapes :intersect-with-lidar
            (fn [feature]
              (cond-> feature
                (not= :polygon (::geoio/type feature))
                (merge {::num-samples 0})
                
                (= :polygon (::geoio/type feature))
                (merge
                 (estimate-height
                  raster-tree
                  (JTS/transform
                   (::geoio/geometry feature) T)
                  buffer-size
                  ground-level-threshold)))
              ))))
       
       shapes index))))

(defmacro d0 [x y] `(let [x# ~x](if (zero? x#) 0 (/ x# ~y))))

(defn add-other-attributes
  [shapes & {:keys [reproject-with] :or {reproject-with identity}}]
  (let [reprojected-geoms (into {} (for [s (::geoio/features shapes)]
                                     [(::geoio/id s) (reproject-with (::geoio/geometry s))]))
        reprojected-geoms (fn [s] (get reprojected-geoms (::geoio/id s)))
        feature-index     (util/index-features (::geoio/features shapes) reprojected-geoms)]
    (geoio/update-features
     shapes :add-other-attributes
     (fn [feature]
       (let [geometry  (reprojected-geoms feature)
             perimeter (.getLength geometry)
             footprint (.getArea geometry)

             ;; work out party wall area
             party-perimeter-proportion
             (try
               (let [rect       (util/geom->rect   geometry)
                     neighbours (util/search-rtree feature-index rect)
                     boundary   (.getBoundary geometry)]
                 (if-let [inter-bounds
                          (seq
                           (for [n neighbours :when (not= n feature)]
                             (let [n-boundary (.getBoundary (reprojected-geoms n))]
                               (LineStringExtracter/getGeometry
                                (.intersection boundary n-boundary)))))]
                   
                   (let [party-bounds    (reduce (fn u [a b] (.union a b)) inter-bounds)
                         party-perimeter (.getLength party-bounds)]
                     (d0 party-perimeter perimeter))
                   0))
               (catch Exception e (log/warn e "Error computing party walls")
                      0))

             [height height-source]
             (cond
               (:height feature)
               [(:height feature) :given]

               ;; detect out-of-date LiDAR height:
               ;; * no fallback height given, and LiDAR height is <= 1.1m;
               ;; * fallback height is over 1m different from LiDAR height
               ;;   and LiDAR height is <= 1.1m
               (let [lidar-height (::height feature)
                     fallback-height (:fallback-height feature)]
                 (and lidar-height
                      (> lidar-height 0)
                      (cond
                        (nil? fallback-height)
                        (> lidar-height 1.1)

                        (some? fallback-height)
                        (or (> lidar-height 1.1)
                            (<= (Math/abs (- fallback-height lidar-height)) 1)))))
               [(::height feature) :lidar]

               (and (:fallback-height feature) (> (:fallback-height feature) 0))
               [(:fallback-height feature) :fallback]

               (:storeys feature)
               [(* *storey-height* (:storeys feature)) :storeys]

               :else
               [*storey-height* :default])

             _ (assert (number? height)
                       (format "height %s (%s) should be a number"
                               height height-source))
             
             storeys
             (max 1
                  (or (:storeys feature)
                      (and height (int (Math/floor (/ height *storey-height*))))
                      1))

             floor-area (or (:floor-area feature)
                            (* footprint storeys))

             wall-area             (* perimeter height)
             party-wall-area       (* party-perimeter-proportion wall-area)
             external-wall-area    (- wall-area party-wall-area)
             external-surface-area (+ external-wall-area footprint footprint)
             total-surface-area    (+ wall-area footprint footprint)
             volume                (* footprint height)
             
             ext-surface-proportion (d0 external-surface-area total-surface-area)
             ext-surface-per-volume (d0 external-surface-area volume)
             
             tot-surface-per-volume (d0 total-surface-area volume)
             ]
         (assoc feature
                ::perimeter-per-footprint (d0 perimeter footprint)

                ::footprint footprint
                ::perimeter perimeter
                ::corners   (count-corners geometry)

                ::shared-perimeter party-perimeter-proportion
                ::shared-perimeter-m (* party-perimeter-proportion perimeter)

                ::height  height
                ::height-source height-source
                ::storeys storeys
                
                ::floor-area floor-area
                ::wall-area wall-area
                ::party-wall-area party-wall-area
                ::external-wall-area external-wall-area
                ::total-surface-area total-surface-area
                ::volume volume
                ::ext-surface-proportion ext-surface-proportion
                ::ext-surface-per-volume ext-surface-per-volume
                ::tot-surface-per-volume tot-surface-per-volume
                ))))))


(defn add-lidar-to-shapes
  [shapes index &
   {:keys [buffer-size ground-level-threshold]
    :or {buffer-size 1.5
         ground-level-threshold -5}}]

  (let [shapes (add-height-from-lidar shapes index
                                      :buffer-size buffer-size
                                      :ground-level-threshold ground-level-threshold)
        sensible-transform (spatial/sensible-projection
                            :utm-zone (::geoio/crs shapes) (::geoio/features shapes))
        ]
    (add-other-attributes
     shapes
     :reproject-with #(JTS/transform % sensible-transform))))

;; TODO this used to filter out zero-area polygons
;; not sure why or if it still should
