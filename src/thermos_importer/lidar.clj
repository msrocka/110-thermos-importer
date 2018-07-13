(ns thermos-importer.lidar
  (:import [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries])
  )

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
     raster
     )
   )
  )

(defn get-raster-bounds [raster]
  ;; needed in the form of an rtree geometry object
  
  ;; (Geometries/rectangle
  ;;  a b c d)
  (let [raster (load-raster raster)]
    ;; get the bounds out and put them in a rectangle
    )
  )

(defn- add-raster-to-index [index raster]
  (println "  Indexing" raster)
  (let [raster-bounds (get-raster-bounds raster)]
    (.add index raster raster-bounds)))

(defn rasters->index
  "Make an index which says which of these rasters (filenames) is where."
  [rasters]
  (println "Indexing rasters...")
  {::index
   (reduce add-raster-to-index (RTree/star) rasters)
   ;; it might be useful to have a weak map of rasters
   ;; so we don't reload them
   })

(defn- find-rasters
  "Locate all the rasters that overlap the bounds of shape."
  [index shape]
  (let [bbox (.getEnvelopeInternal shape)
        rasters (.search (::index index)
                         (Geometries/rectangle
                          (.getMinX bbox) (.getMinY bbox)
                          (.getMaxX bbox) (.getMaxY bbox)))
        ]
    rasters))



(defn- sample-coords
  "Sample coordinates within shape from raster"
  [index raster shape]
  (let [raster (load-raster index raster)]

    )
  )

(defn- triangulate
  "Construct a triangulation of a list of coordinates."
  [coords]
  
  )

(defn- summarise
  "Approximately summarise the building from this generated surface."
  [surf]
  
  )

(defn shape->dimensions
  "Given the output from rasters->index, and a shape which is a JTS geometry

  ::surface-area
  ::volume
  ::floor-area

  Presumes shape CRS compatible with rasters' CRS."
  [index shape]

  ;; let's do the rubbish thing first, namely evaluating a bunch of
  ;; points inside all the matching rasters for the shape.
  
  (let [rasters (find-rasters index shape)
        coords (mapcat #(sample-coords index % shape) rasters)
        geom (triangulate coords)]
    (summarise geom)))

