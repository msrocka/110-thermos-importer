(ns thermos-importer.lidar
  (:require [thermos-importer.geoio :as geoio])
  (:import [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries]

           [com.vividsolutions.jts.geom GeometryFactory Coordinate Polygon]
           
           [org.geotools.coverage.grid.io GridFormatFinder]
           [org.geotools.gce.geotiff GeoTiffFormat]
           [org.geotools.factory Hints]
           [org.geotools.geometry DirectPosition2D]

           [org.geotools.geometry.jts JTS]
           [org.geotools.referencing CRS]
           ))

(defn- mutable-memoize
  [f #^java.util.Map map]
  (fn [& args]
    (if-let [e (find map args)]
      (val e)
      (let [ret (apply f args)]
        (.put map args ret)
        ret))))

(defn soft-memoize
  [f]
  (let [m (java.util.concurrent.ConcurrentHashMap.)
        rq (java.lang.ref.ReferenceQueue.)
        memoized (mutable-memoize
                  #(java.lang.ref.SoftReference. (apply f %&) rq)
                  m)]
    (fn clear-fn [& args]
                                        ; clearCache conveniently exists to poll the queue and scrub a CHM
                                        ; used in Clojure's Keyword and DynamicClassLoader, so it's not going anywhere
      (clojure.lang.Util/clearCache rq m)
      (let [^java.lang.ref.SoftReference ref (apply memoized args)
            val (.get ref)]
        (if (.isEnqueued ref)
                                        ; reference enqueued since our clearCache call above, retry
          (apply clear-fn args)
          val)))))

(def load-raster
  (soft-memoize
   (fn [raster]
     ;; do the load here
     (let [format (GridFormatFinder/findFormat raster)
           hints (when (instance? GeoTiffFormat format)
                   (Hints. Hints/FORCE_LONGITUDE_FIRST_AXIS_ORDER true))
           reader (.getReader format raster hints)
           ]
       (.read reader nil)))))

(defn get-raster-bounds [raster]
  ;; needed in the form of an rtree geometry object
  
  ;; (Geometries/rectangle
  ;;  a b c d)
  (let [raster (load-raster raster)
        geom (.getEnvelope2D raster)]
    ;; get the bounds out and put them in a rectangle
    (Geometries/rectangle
     (.getMinimum geom 0) (.getMinimum geom 1)
     (.getMaximum geom 0) (.getMaximum geom 1))))

(defn- add-raster-to-index [index raster]
  (println "  Indexing" raster)
  (let [raster-bounds (get-raster-bounds raster)]
    (.add index raster raster-bounds)))

(defn rasters->index
  "Make an index which says which of these rasters (filenames) is where."
  [rasters]
  (println "Indexing rasters...")
  {::index
   (reduce add-raster-to-index (RTree/create) rasters)})

(defn- find-rasters
  "Locate all the rasters that overlap the bounds of shape."
  [index rect]
  (let [rasters (.search (::index index) rect)]
    (for [entry (-> rasters .toBlocking .toIterable)]
      (.value entry))))

(defn- sample-coords
  "Sample coordinates within shape from raster"
  [raster coords]
  (let [raster (load-raster raster)
        ]
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
  "Given the output from rasters->index, and a shape which is a JTS geometry

  ::surface-area
  ::volume
  ::floor-area
  
  Presumes shape CRS compatible with rasters' CRSs

  It will make sense to iterate over the shapes in a good order
  that relates to the rasters we are thinking about."
  [index shape]

  ;; let's do the rubbish thing first, namely evaluating a bunch of
  ;; points inside all the matching rasters for the shape.
  
  (let [bbox (.getEnvelopeInternal shape)
        rect (Geometries/rectangle
              (.getMinX bbox) (.getMinY bbox)
              (.getMaxX bbox) (.getMaxY bbox))
        
        rasters (find-rasters index rect)
        grid    (grid-over shape)
        coords  (mapcat #(sample-coords % grid) rasters)]
    
    (summarise shape coords)))

(defn add-lidar-to-shapes [rasters shapes]
  ;; shapes are some geoio things (maps with stuff)
  ;; rasters is a list of filenames for things
  
  (let [index (rasters->index rasters)]
    (for [shape shapes]
      (merge shape
             (shape->dimensions index (::geoio/geometry shape))))))

(defn reproject-shapes [shapes from-crs to-crs]
  (let [matrix (CRS/findMathTransform
                (CRS/decode from-crs true)
                (CRS/decode to-crs))]

    (for [shape shapes]
      (JTS/transform shape matrix))))


;; TODO
;; load shapefile, index lidars, get CRS, reproject, run loop, done.
