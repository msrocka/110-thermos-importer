(ns thermos-importer.util
  (:import [com.github.davidmoten.rtree.geometry Geometries Rectangle]
           [org.locationtech.jts.geom Geometry]
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
   (fn [^RTree tree feature]
     (let [box (geom->rect (:thermos-importer.geoio/geometry feature))]
       (.add tree feature box)))
   
   (RTree/create)
   features))

(defn search-rtree [^RTree tree ^Rectangle rect]
  (for [entry (.. tree
                  (search rect)
                  (toBlocking)
                  (toIterable))]
    (.value entry)))

(defn geom->rect ^Rectangle [^Geometry geom]
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
   (map #(do (if (= (rem %1 (max 1 n)) 0) (callback %1)) %2) (iterate inc 1) sequence))
  ([sequence n callback finished-callback]
     (drop-last (lazy-cat (seq-counter sequence n callback) 
                  (lazy-seq (cons (finished-callback) ())))))) 

(defn canonizer []
  (let [state (atom {:next 0 :mapping {}})]
    (fn self [v]
      (cond
        (nil? v)
        nil

        (and (not (string? v)) (seqable? v))
        (map self (seq v))

        :otherwise
        (get-in (swap! state (fn [{n :next m :mapping :as st}]
                               (if (contains? m v)
                                 st
                                 {:next (inc n)
                                  :mapping (assoc m v n)})))
                [:mapping v])))))

(defn replace-ids [m fields f]
  (reduce
   (fn [m field] (update m field f))
   m fields))

(defprotocol HasExtension
  (has-extension [s x]))

(extend-type String
  HasExtension
  (has-extension [s x]
    (and s (.endsWith s (str "." x)))))

(extend-type java.io.File
  HasExtension
  (has-extension [f x]
    (and f (has-extension (.getName f) x))))

(extend-type java.nio.file.Path
  HasExtension
  (has-extension [p x]
    (and p (has-extension (.toString (.getFileName p)) x))))
