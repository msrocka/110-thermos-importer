(ns cljts.core
  "A wrapper for jts in JVM or jtsjs in javascript"
  #?(:clj (:import
           [org.locationtech.jts.geom
            GeometryFactory PrecisionModel
            Coordinate
            Geometry Polygon Point LineString MultiLineString
            LinearRing
            MultiPoint MultiPolygon]
           [org.locationtech.jts.operation.distance DistanceOp]
           [java.security MessageDigest]
           [java.util Base64 Base64$Encoder]))
  #?(:cljs (:require [cljsjs.jsts :as jsts]
                     [goog.crypt.Md5 :as Md5]
                     [goog.crypt.base64 :as base64])))


#?(:cljs
   (do (def Geometry jsts/geom.Geometry)
       (def Coordinate jsts/geom.Coordinate)
       (def Point jsts/geom.Point)
       (def Polygon jsts/geom.Polygon)
       (def LineString jsts/geom.LineString)
       (def MultiPolygon jsts/geom.MultiPolygon)
       (def MultiLineString jsts/geom.MultiLineString)
       (def LinearRing jsts/geom.LinearRing)))

(def ^:dynamic *geometry-factory*
  #?(:clj
     (GeometryFactory. (PrecisionModel.) 4326)

     :cljs
     (jsts/geom.GeometryFactory.)))

(def ^:dynamic *geojson-reader*
  #?(:cljs (jsts/io.GeoJSONReader. *geometry-factory*)))

(def ^:dynamic *geojson-writer*
  #?(:cljs (jsts/io.GeoJSONWriter. *geometry-factory*)))

(defn geom->json [geom]
  (and geom
       #?(:cljs (->> geom
                     (.write *geojson-writer*)
                     (js/JSON.stringify)))))

(defn json->geom [json]
  (and json (.read *geojson-reader* json)))

(defn- kebab-case [^String class-name]
  (.toLowerCase
   (.replaceAll class-name "(.)([A-Z])" "$1-$2")))

(defn geometry-type [^Geometry geom]
  (let [type (.getGeometryType geom)]
    (case type
      "Polygon" :polygon
      "MultiPolygon" :multi-polygon
      "MultiPoint" :multi-point
      "LineString" :line-string
      "MultiLineString" :multi-line-string
      (keyword (kebab-case type)))))

(defn create-coordinate [^double x ^double y]
  (Coordinate. x y))

(defn create-point
  ([p]
   (cond (instance? Point p)
         p
         (instance? Coordinate p)
         (.createPoint *geometry-factory* p)
         (vector? p)
         (create-point (first p) (second p))))
  
  ([^double x ^double y]
   (create-point (create-coordinate x y))))

(defn coordinate-seq [coordinate-likes]
  (seq coordinate-likes)) ;; not sure

(defn- clockwise?
  "True iff the given seq of coordinates are in clockwise order"
  [coordinates]
  (pos?
   (reduce
    +
    (map
     (fn [^Coordinate a ^Coordinate b]
       (let [xa (.getX a) xb (.getX b)
             ya (.getY a) yb (.getY b)]
         (* (- xb xa) (+ yb ya))))
     coordinates (rest coordinates)))))

(defn- make-clockwise [coordinates]
  (if (clockwise? coordinates)
    coordinates (reverse coordinates)))

(defn- make-anticlockwise [coordinates]
  (if (clockwise? coordinates)
    (reverse coordinates) coordinates))

(defn create-linear-ring [coordinates]
  (.createLinearRing
   *geometry-factory*
   (into-array Coordinate coordinates)))

(defn create-polygon
  [outer inners]
  
  (let [outer (coordinate-seq outer)
        inner (map coordinate-seq inners)
        outer (make-anticlockwise outer)
        inner (map make-clockwise inner)]
    (.createPolygon
     *geometry-factory*
     (create-linear-ring outer)
     (into-array
      LinearRing
      (map create-linear-ring inner)))))

(defn create-multipolygon
  [polygons]

  (.createMultiPolygon
   *geometry-factory*
   (into-array Polygon polygons)))

(defn create-line-string
  [coordinates]
  (.createLineString
   *geometry-factory*
   (into-array Coordinate coordinates)))

(defn create-multi-line-string
  [line-strings]
  (.createMultiLineString
   *geometry-factory*
   (into-array LineString line-strings)))

(defn intersects? [^Geometry a ^Geometry b]
  (and a b (.intersects a b)))

(defn valid? [^Geometry a]
  (and a (.isValid a)))

(defn number-of-parts [geom]
  (.getNumGeometries geom))

(defn nth-part [geom n]
  (.getGeometryN geom n))

(defn is-multi? [geom]
  (#{:multi-point :multi-line-string :multi-polygon} (geometry-type geom)))

(defn parts [geometry]
  (if (is-multi? geometry)
    (for [i (range (number-of-parts geometry))]
      (nth-part geometry i))
    (list geometry)))

(defn make-singular
  "If the given geom is a multi-geometry, but it has one element,
  get that element out."
  [geom]
  (if (and (is-multi? geom)
           (= (number-of-parts geom) 1))
    (nth-part geom 0)
    geom))

(defn coordinates [geom]
  (.getCoordinates geom))

#?(:clj
   (let [^MessageDigest md5 (MessageDigest/getInstance "MD5")
         ^Base64$Encoder base64 (.withoutPadding (Base64/getEncoder))]
     (defn ghash [^Geometry geometry]
       (.reset md5)
       (.update md5 (.getBytes (.toLowerCase (.getGeometryType geometry))))
       (doseq [^Coordinate c (.getCoordinates geometry)]
         (let [x (Double/doubleToLongBits (.-x c))
               y (Double/doubleToLongBits (.-y c))]
           (dotimes [i 8]
             (->> (bit-shift-right x (* i 8))
                  (bit-and 0xFF)
                  (unchecked-byte)
                  (.update md5)))
           (dotimes [i 8]
             (->> (bit-shift-right y (* i 8))
                  (bit-and 0xFF)
                  (unchecked-byte)
                  (.update md5)))))
       ;; Throw away some bytes. This should give us 16 * 6 bits = 96
       ;; collision probability for 60 million objects of around
       ;; 2e-14 which is probably good enough.
       (.substring (.encodeToString base64 (.digest md5)) 0 16)))

   ;; clojurescript equivalent; this may depend on byte order in an
   ;; unsavoury way
   :cljs
   (let [double-to-long-bits
         (fn [d]
           (let [buffer (js/ArrayBuffer. 8)
                 num    (js/Float64Array. buffer)]
             (aset num 0 d)
             (js/Array.from (js/Int8Array. buffer))))

         md5 (goog.crypt.Md5.)]
     (defn ghash [geometry]
       (.reset md5)
       (.update md5 (.toLowerCase (.getGeometryType geometry)))

       (doseq [c (.getCoordinates geometry)]
         (let [x (double-to-long-bits (.-x c))
               y (double-to-long-bits (.-y c))]
           (.update md5 x)
           (.update md5 y)))
       (.substring (base64/encodeByteArray (.digest md5)) 0 16))))


(comment
  (ghash (create-point 0 0))
  ;; => "LKnKAOio3Dx9y6qq"
  ;; this matches in the browser, hallelujah
  (ghash (create-point 3 10))
  ;; => "M5xdgRY/fEIiqoTM"
  ;; same, we are winners
  )


(defn cut-line-string
  "Given a linestring geometry and a point geometry,
  return one or two linestrings which are cut as close as possible to
  the point.

  A single linestring is returned if the point is closest to one of
  the ends of the input.

  In all cases the result is a seq of line strings."
  [line-string point]

  (let [point (create-point point) ;; make sure it's a point
        
        op (#?(:cljs
               jsts/operation.distance.DistanceOp.
               :clj
               DistanceOp.)
            point
            line-string)

        split-location
        (aget (.nearestLocations op) 1)

        split-index
        (.getSegmentIndex split-location)

        split-coordinate
        (.getCoordinate split-location)

        line-coordinates
        (.getCoordinates line-string)

        ;; we can cut the line into two in this place
        [line-head line-tail]
        (split-at (inc split-index) line-coordinates)

        line-head (concat line-head [split-coordinate])
        line-tail (concat [split-coordinate] line-tail)
        ]

    (println line-head "-" split-coordinate "-" line-tail)
    
    ;; now we want to check if either line-head or line-tail is 2 of same point
    ;; in which case we yield just the other

    (cond
      (and (= 2 (count line-head))
           (= (first line-head) (last line-head)))
      [line-string]
      
      (and (= 2 (count line-tail))
           (= (first line-tail) (last line-tail)))
      [line-string]
      
      :else
      [(create-line-string line-head) (create-line-string line-tail)])))

(let [d2r (/ Math/PI 180.0)]
  (defn- to-radians [^double deg]
    (* deg d2r)))

(let [a 6378137.0                       ; These are the WGS84 parameters
      b 6356752.314245
      f (/ 1.0 298.257223563)]
  
  (defn geodesic-distance [p1 p2]
    (let [φ1 (to-radians (.-x p1))
          φ2 (to-radians (.-x p2))
          λ1 (to-radians (.-y p1))
          λ2 (to-radians (.-y p2))

          L (- λ2 λ1)

          tanU1 (* (- 1 f) (Math/tan φ1))
          cosU1 (/ 1 (Math/sqrt (+ 1 (* tanU1 tanU1))))
          sinU1 (* tanU1 cosU1)

          tanU2 (* (- 1 f) (Math/tan φ2))
          cosU2 (/ 1 (Math/sqrt (+ 1 (* tanU2 tanU2))))
          sinU2 (* tanU2 cosU2)

          antimeridian (> (Math/abs L) Math/PI)]

      (loop [λ L
             i 0]
        (let [sinλ (Math/sin λ);
              cosλ (Math/cos λ);
              sinSqσ (+ (* cosU2 sinλ cosU2 sinλ)
                        (* (- (* cosU1 sinU2)
                              (* sinU1 cosU2 cosλ))
                           (- (* cosU1 sinU2)
                              (* sinU1 cosU2 cosλ))))]
          (if (< (Math/abs sinSqσ)
                 #?(:cljs js/Number.epsilon
                    :clj Double/MIN_VALUE))
            ;; terminate
            0
            ;; else

            (let [sinσ (Math/sqrt sinSqσ)
                  cosσ (+ (* sinU1 sinU2) (* cosU1 cosU2 cosλ))
                  σ (Math/atan2 sinσ cosσ)
                  sinα  (/ (* cosU1 cosU2 sinλ)
                           sinσ);
                  cosSqα (- 1 (* sinα sinα))
                  cos2σM (if (zero? cosSqα) 0
                             (- cosσ (/ (* 2 sinU1 sinU2) cosSqα)))
                  C (* (/ f 16)
                       cosSqα
                       (+ 4 (* f (- 4 (* 3 cosSqα)))))
                  λ1 λ
                  λ (+ L (* (- 1 C)
                            f sinα
                            (+ σ
                               (* C sinσ
                                  (+ cos2σM
                                     (* C cosσ
                                        (dec (* 2 cos2σM cos2σM))))))))]

              (if (or (> i 1000)
                      (< (Math/abs (- λ1 λ))
                         #?(:cljs js/Number.epsilon
                            :clj Double/MIN_VALUE)))
                (let [uSq (/ (* cosSqα (- (* a a) (* b b )))
                             (* b b))
                      A (+ 1 (* (/ uSq 16384.0)
                                (+ 4096 (* uSq (+ -768 (* uSq (- 320 (* 175 uSq))))))))
                      
                      
                      B (* (/ uSq 1024) (+ 256 (* uSq (+ -128 (* uSq (- 74 (* 47 uSq)))))))
                      Δσ (* B sinσ
                            (+ cos2σM (* (/ B 4)
                                         (- (* cosσ (+ -1 (* 2 cos2σM cos2σM)))
                                            (* (/ B 6)
                                               cos2σM
                                               (+ -3 (* 4 sinσ sinσ))
                                               (+ -3 (* 4 cos2σM cos2σM)))))))

                      s (* b A (- σ Δσ))]
                  s)
                (recur λ (inc i))))))))))

(defn geodesic-length [line-string]
  (reduce +
          (map geodesic-distance
               (coordinates line-string)
               (rest (coordinates line-string)))))

  
