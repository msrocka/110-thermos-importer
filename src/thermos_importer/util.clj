(ns thermos-importer.util
  (:import
   [com.github.davidmoten.rtree RTree]
   [com.github.davidmoten.rtree.geometry Geometries]))

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

(defn geom->rect [geom]
  (let [bbox (.getEnvelopeInternal geom)]
    (try (Geometries/rectangle
          (.getMinX bbox) (.getMinY bbox)
          (.getMaxX bbox) (.getMaxY bbox))
         (catch IllegalArgumentException e
           (println "Invalid bounding box"
                    (.getMinX bbox) (.getMinY bbox)
                    (.getMaxX bbox) (.getMaxY bbox)
                    geom)
           (throw e)))))

(defn search-rtree [tree rect]
  (for [entry (.. tree
                  (search rect)
                  (toBlocking)
                  (toIterable))]
    (.value entry)))
