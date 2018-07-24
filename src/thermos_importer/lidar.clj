(ns thermos-importer.lidar
  (:require [thermos-importer.geoio :as geoio]
            [thermos-importer.util :as util])

  (:import  [com.github.davidmoten.rtree RTree]
            [com.github.davidmoten.rtree.geometry Geometries]

            [com.vividsolutions.jts.geom GeometryFactory Coordinate Polygon]
            
            [org.geotools.coverage.grid.io GridFormatFinder]
            [org.geotools.gce.geotiff GeoTiffFormat]
            [org.geotools.factory Hints]
            [org.geotools.geometry DirectPosition2D]

            [org.geotools.geometry.jts JTS]
            [org.geotools.referencing CRS]))

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
          {:raster raster
           :bounds (get-raster-bounds raster)
           :crs (get-raster-crs raster)})

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
                    (catch Exception e nil))]
         (when z [x y z]))))))

(defn- summarise
  "Approximately summarise the building from this set of x/y/z values."
  [shape coords]
  (when-not (empty? coords)
    (let [perimeter (.getLength shape)

          heights (map last coords)
          heights (filter #(> % -5) heights)
          
          ground (apply min heights)
          heights (map #(- % ground) heights)

          heights (filter #(> % 0.5) heights)
          mean-height (if (empty? heights)
                        0
                        (/ (apply + heights) (count heights)))

          footprint (.getArea shape)
          surface-area (+ (* perimeter mean-height) footprint)
          volume (* footprint mean-height)
          ]
      (when (> mean-height 1000)
        (let [heights (map last coords)]
          (println mean-height)
          (println (apply min heights) (apply max heights))))
      
      {::surface-area surface-area
       ::volume volume
       ::height mean-height
       })))

(defn- grid-over
  "Make a seq of coordinates covering the SHAPE with a buffer of 1m"
  [shape]
  (let [shape (.buffer shape 1.5)
        
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
  [tree shape]

  (let [rect (util/geom->rect shape)
        rasters (find-rasters tree rect)
        grid    (grid-over shape)
        coords  (mapcat #(sample-coords % grid) rasters)]
    (summarise shape coords)))

(defn add-lidar-to-shapes
  "Given a list of `rasters` and a `shapes`, which is a geoio feature thingy
  i.e. a map with ::geoio/crs and ::geoio/features in it

  return an updated `shapes`, in which the features have
  got ::lidar/surface-area etc. from shape->dimensions."
  [rasters shapes]
  (let [index (rasters->index rasters)
        shapes-crs (::geoio/crs shapes)]
    (reduce
     (fn [shapes [raster-crs raster-tree]]
       (let [transform (CRS/findMathTransform
                        (CRS/decode shapes-crs true)
                        (CRS/decode raster-crs))]
         
         (printf "Raster CRS: %s, geometry CRS: %s, %d shapes to process"
                 raster-crs
                 shapes-crs
                 (count (::geoio/features shapes)))

         (println)
         
         (update shapes ::geoio/features
                 #(doall (for [feature %]
                           (merge feature
                                  (shape->dimensions
                                   raster-tree
                                   (JTS/transform
                                    (::geoio/geometry feature)
                                    transform))))))))
     shapes index)))
