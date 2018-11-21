(ns thermos-importer.lidar
  (:require [thermos-importer.geoio :as geoio]
            [thermos-importer.spatial :as spatial]
            [thermos-importer.util :as util]
            [thermos-importer.lidar :as lidar])
  (:import com.github.davidmoten.rtree.geometry.Geometries
           com.github.davidmoten.rtree.RTree
           [org.locationtech.jts.geom Coordinate GeometryFactory Polygon]
           [org.locationtech.jts.geom.util LineStringExtracter]
           org.geotools.coverage.grid.io.GridFormatFinder
           org.geotools.factory.Hints
           org.geotools.gce.geotiff.GeoTiffFormat
           org.geotools.geometry.DirectPosition2D
           org.geotools.geometry.jts.JTS
           org.geotools.referencing.CRS))

(def load-raster
  (util/soft-memoize
   (fn [raster]
     ;; do the load here
     (let [format (GridFormatFinder/findFormat raster)
           hints (when (instance? GeoTiffFormat format)
                   (Hints. Hints/FORCE_LONGITUDE_FIRST_AXIS_ORDER true))
           reader (.getReader format raster hints)
           ]
       (.read reader nil)))))

(defn get-raster-bounds [raster]
  (let [raster (load-raster raster)
        geom (.getEnvelope2D raster)]
    ;; get the bounds out and put them in a rectangle
    (Geometries/rectangle
     (.getMinimum geom 0) (.getMinimum geom 1)
     (.getMaximum geom 0) (.getMaximum geom 1))))

(defn get-raster-crs [raster]
  (let [raster (load-raster raster)
        crs (.getCoordinateReferenceSystem2D raster)]
    (CRS/lookupIdentifier crs true)))

(defn rasters->index
  "Make an index which says which of these rasters (filenames) is where.
  The index is a map from EPSG code to an Rtree of rasters that have that EPSG.
  "
  [rasters]
  (println "Indexing rasters...")
  
  (let [properties                      ; first lookup the properties for each raster
        (for [raster rasters]
          (let [crs (get-raster-crs raster)]
            (printf "-> %s [%s]\r" (.getName raster) crs)
            (flush)
            {:raster raster
             :bounds (get-raster-bounds raster)
             :crs crs}))

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
    (println)
    (into {} indices)))

(defn- find-rasters
  "Locate all the rasters that overlap the bounds of shape."
  [tree rect]
  (util/search-rtree tree rect))

(defn- sample-coords
  "Sample coordinates within shape from raster.
  Presumes coords are in the raster's CRS."
  [raster coords]
  (let [raster (load-raster raster)
        no-data (set (.getNoDataValues (.getSampleDimension raster 0)))]
    
    (filter
     identity
     (for [[x y] coords]
       (let [z (try (first
                     (.evaluate raster
                                (DirectPosition2D. x y)
                                nil))
                    (catch org.opengis.coverage.PointOutsideCoverageException e
                      nil)
                    )]
         (when (and z (not (no-data z)))
           [x y z]))))))

(defn- summarise
  "Approximately summarise the building from this set of x/y/z values."
  [shape coords ground-level-threshold]
  (if (not (empty? coords))
    (let [perimeter (.getLength shape)

          heights (map last coords)
          heights (filter #(> % ground-level-threshold) heights)
          
          ground (if (seq heights)
                   (apply min heights)
                   0)
          
          heights (map #(- % ground) heights)

          heights (filter #(> % 0.5) heights)
          mean-height (if (empty? heights)
                        0
                        (/ (apply + heights) (count heights)))

          footprint (.getArea shape)
          ]
      (when (> mean-height 1000)
        (let [heights (map last coords)]
          (println mean-height)
          (println (apply min heights) (apply max heights))))
      
      {::perimeter perimeter
       ::footprint footprint
       ::ground-height ground
       ::height mean-height
       ::num-samples (count heights)
       })
    {::num-samples 0
     ::footprint (.getArea shape)
     ::perimeter (.getLength shape)}))

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
        ]
    (for [x (range x-min x-max 1.0)
          y (range y-min y-max 1.0)
          :when (.covers shape
                         (.createPoint
                          (.getFactory shape)
                          (Coordinate. x y)))]
      [x y])))

(defn shape->dimensions
  "Given an rtree for a set of rasters, and a shape which is a JTS geometry.

  Presumes the shape has been projected into the CRS for all the rasters in the rtree.

  ::surface-area
  ::volume
  ::floor-area"
  [tree shape buffer-size ground-level-threshold]

  (let [rect    (util/geom->rect shape)
        rasters (find-rasters tree rect)
        grid    (grid-over shape buffer-size)
        coords  (mapcat #(sample-coords % grid) rasters)]
    (summarise shape coords ground-level-threshold)))

(defn estimate-party-walls [feature index]
  (try
    (let [geom (::geoio/geometry feature)
          rect (util/geom->rect geom)
          neighbours (util/search-rtree index rect)
          perimeter (.getLength geom)

          boundary (.getBoundary geom)

          inter-bounds (for [n neighbours :when (not= n feature)]
                         (let [n-boundary (.getBoundary (::geoio/geometry n))]
                           (LineStringExtracter/getGeometry
                            (.intersection boundary n-boundary))))
          
          ]
      (if (seq inter-bounds)
        (let [party-bounds (reduce
                            (fn [a b] (.union a b))
                            inter-bounds)
              party-perimeter (.getLength party-bounds)

              party-perimeter-proportion (/ party-perimeter perimeter)]
          (assoc feature ::shared-perimeter party-perimeter-proportion))
        
        (assoc feature ::shared-perimeter 0)))
    (catch Exception e
      (printf
       "Error computing party walls for %s: %s\n"
       (dissoc feature ::geoio/geometry)
       (.getMessage e))
      feature
      )))

(defn- derive-2d-fields [feature]
  (let [{shared-perimeter ::shared-perimeter
         perimeter ::perimeter
         footprint ::footprint} feature

        perimeter (or perimeter 0)
        footprint (or footprint 0.1)
        shared-perimeter (or shared-perimeter 0)

        ;; in meteres
        shared-perimeter-m (* shared-perimeter perimeter)

        perimeter-per-footprint (/ perimeter footprint)
        ]
    (assoc feature
           ::shared-perimeter-m shared-perimeter-m
           ::perimeter-per-footprint perimeter-per-footprint)))

(defn- get-ntile
  "This is just yet another interpolating function.
  VALUES is the x-axis and TILES is the y-axis.
  Given a VALUE, it will find the x-axis values either side and
  use them to interpolate into the tiles.

  Inputs below the bottom or above the top of VALUES are clamped
  into the range.
  "
  [^double value ^doubles values ^doubles tiles]
  (let [pos (java.util.Arrays/binarySearch values value)]
    (if (< pos 0)
      ;; - (insertion point + 1)
      (let [pos (- pos)] ;; insertion point + 1
        (cond (>= pos (count tiles))
              (last tiles)
              (= 1 pos)
              (first tiles)
              :otherwise
              (let [val0 (aget values (dec pos))
                    val1 (aget values pos)
                    til0 (aget tiles (dec pos))
                    til1 (aget tiles pos)]
                (+ til0
                   (* (- til1 til0)
                      (/ (- value val0)
                         (- val1 val0)))))))
      (aget tiles pos))))


(defn- derive-3d-fields [feature ^double storey-height volumes tiles]
  (let [{shared-perimeter ::shared-perimeter
         perimeter ::perimeter
         height ::height
         footprint ::footprint} feature

        wall-area (* perimeter height)

        party-wall-area (* shared-perimeter wall-area)
        external-wall-area (- wall-area party-wall-area)
        external-surface-area (+ external-wall-area (* 2 footprint))
        total-surface-area (+ wall-area (* 2 footprint))

        number-of-floors (int (Math/round (/ height storey-height)))
        total-floor-area (* footprint number-of-floors)

        ;; in meteres
        volume (* footprint height)

        ext-surface-proportion (/ external-surface-area total-surface-area)
        ext-surface-per-volume (/ external-surface-area volume)
        ext-surface-per-floor-area (if (> total-floor-area 0)
                                     (/ external-surface-area total-floor-area)
                                     0)
        tot-surface-per-volume (/ total-surface-area volume)

        volume-ntile (get-ntile volume volumes tiles)
        ]
    (assoc feature
           ::wall-area wall-area
           ::party-wall-area party-wall-area
           ::external-wall-area external-wall-area
           ::external-surface-area external-surface-area
           ::total-surface-area total-surface-area
           ::number-of-floors number-of-floors
           ::total-floor-area total-floor-area
           ::volume volume
           ::ext-surface-proportion ext-surface-proportion
           ::ext-surface-per-volume ext-surface-per-volume
           ::ext-surface-per-floor-area ext-surface-per-floor-area
           ::tot-surface-per-volume tot-surface-per-volume
           ::volume-ntile volume-ntile)))

(defn- derive-more-fields [feature ^double storey-height volumes tiles]
  (cond-> feature
    (::shared-perimeter feature)
    (derive-2d-fields)

    (and (::shared-perimeter feature)
         (pos? (::num-samples feature)))
    (derive-3d-fields storey-height volumes tiles)))

(defn add-lidar-to-shapes
  "Given a raster index from `rasters->index` and a `shapes`, which is a
  geoio feature thingy i.e. a map with ::geoio/crs
  and ::geoio/features in it

  return an updated `shapes`, in which the features have
  got ::lidar/surface-area etc. from shape->dimensions."
  [shapes index & {:keys [buffer-size ground-level-threshold
                          storey-height
                          volume-tiles
                          ]
                   :or {buffer-size 1.5 ground-level-threshold -5
                        storey-height 4.1 volume-tiles [[0 0]]}
                   }]

  (println (count (::geoio/features shapes)) "shapes to lidarize")
  
  (let [tile-values (double-array (map first volume-tiles))
        tile-tiles  (double-array (map second volume-tiles))

        shapes-crs (::geoio/crs shapes)
        feature-index (util/index-features (::geoio/features shapes))
        lcc-transform (spatial/create-lcc shapes-crs (::geoio/geatures shapes))

        add-footprint-and-perimeter (fn [feature]
                                      (let [shape (::geoio/geometry feature)
                                            shape (JTS/transform shape lcc-transform)]
                                        (merge feature
                                               {::num-samples 0
                                                ::footprint (.getArea shape)
                                                ::perimeter (.getLength shape)})))
        ]

    (as-> shapes shapes
      (geoio/update-features shapes :estimate-party-walls estimate-party-walls feature-index)
      (geoio/update-features shapes :footprint-and-perimeter add-footprint-and-perimeter)

      (reduce
       (fn [shapes [raster-crs raster-tree]]
         (let [transform (CRS/findMathTransform
                          (CRS/decode shapes-crs true)
                          (CRS/decode raster-crs))]
           (geoio/update-features shapes :intersect-with-lidar
                                  (fn [feature]
                                    (merge feature
                                            (try
                                              (shape->dimensions
                                               raster-tree
                                               (JTS/transform
                                                (::geoio/geometry feature)
                                                transform)
                                               
                                               buffer-size
                                               ground-level-threshold)
                                              (catch Exception e
                                                (printf
                                                 "Error adding lidar data to %s: %s\n"
                                                 (dissoc feature ::geoio/geometry)
                                                 (.getMessage e))
                                                {})))))))
       shapes index)
      (geoio/update-features shapes :derive-fields derive-more-fields storey-height tile-values tile-tiles))))

