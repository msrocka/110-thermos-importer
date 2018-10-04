(ns thermos-importer.lidar
  (:require [thermos-importer.geoio :as geoio]
            [thermos-importer.util :as util])
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
          (do
            (println "  *" raster)
            {:raster raster
             :bounds (get-raster-bounds raster)
             :crs (get-raster-crs raster)}))

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
  (let [raster (load-raster raster)]
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
         (when z [x y z]))))))

(defn- summarise
  "Approximately summarise the building from this set of x/y/z values."
  [shape coords ground-level-threshold]
  (when-not (empty? coords)
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
       })))

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

  (let [rect (util/geom->rect shape)
        rasters (find-rasters tree rect)
        grid    (grid-over shape buffer-size)
        coords  (mapcat #(sample-coords % grid) rasters)]
    (summarise shape coords ground-level-threshold)))

(defn estimate-party-walls [features]
  (println "Estimating party walls...")
  (let [index (util/index-features features)]
    (for [feature features]
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
            feature))
        (catch Exception e
          (printf
           "Error computing party walls for %s: %s\n"
           (dissoc feature ::geoio/geometry)
           (.getMessage e))
          feature
          )))))

(defn add-lidar-to-shapes
  "Given a raster index from `rasters->index` and a `shapes`, which is a
  geoio feature thingy i.e. a map with ::geoio/crs
  and ::geoio/features in it

  return an updated `shapes`, in which the features have
  got ::lidar/surface-area etc. from shape->dimensions."
  [shapes index {:keys [buffer-size ground-level-threshold]
                 :or {buffer-size 1.5 ground-level-threshold -5}
                 }]
  (let [shapes-crs (::geoio/crs shapes)
        shapes (update shapes ::geoio/features estimate-party-walls)]

    (reduce
     (fn [shapes [raster-crs raster-tree]]
       (printf "Raster CRS: %s, geometry CRS: %s"
               raster-crs
               shapes-crs)
       
       (let [transform (CRS/findMathTransform
                        (CRS/decode shapes-crs true)
                        (CRS/decode raster-crs))
             total (count (::geoio/features shapes))
             start (atom (System/currentTimeMillis))
             ]
         
         (update shapes ::geoio/features
                 #(doall
                   (util/seq-counter
                    (for [feature %]
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
                                  "Error in adding lidar data to %s: %s\n"
                                  (dissoc feature ::geoio/geometry)
                                  (.getMessage e))
                                 {}))))
                    
                    500
                    (fn [n] (let [now (System/currentTimeMillis)
                                  ds (/ (- now @start) 1000.0)
                                  ps (/ 500 ds)
                                  rem (- total n)
                                  rte (/ rem ps)
                                  ]
                              (reset! start now)
                              (println n "/" total ds "seconds")
                              (println ps "/sec" (/ rte 60) "mins remain")
                              ))

                    )))))
     shapes index)))
