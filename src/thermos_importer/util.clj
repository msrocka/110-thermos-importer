(ns thermos-importer.util
  (:require [thermos-importer.geoio :as geoio])
  (:import com.github.davidmoten.rtree.geometry.Geometries
           com.github.davidmoten.rtree.RTree))

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

(declare index-features search-rtree geom->rect)

(defn index-features [features]
  (reduce
   (fn [tree feature]
     (let [box (geom->rect (::geoio/geometry feature))]
       (.add tree feature box)))
   
   (RTree/create)
   features))

(defn search-rtree [tree rect]
  (for [entry (.. tree
                  (search rect)
                  (toBlocking)
                  (toIterable))]
    (.value entry)))

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

(defn seq-counter 
  "calls callback after every n'th entry in sequence is evaluated. 
  Optionally takes another callback to call once the seq is fully evaluated."
  ([sequence n callback]
     (map #(do (if (= (rem %1 n) 0) (callback %1)) %2) (iterate inc 1) sequence))
  ([sequence n callback finished-callback]
     (drop-last (lazy-cat (seq-counter sequence n callback) 
                  (lazy-seq (cons (finished-callback) ())))))) 
