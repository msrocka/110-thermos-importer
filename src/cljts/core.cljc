(ns cljts.core
  "A wrapper for jts in JVM or jtsjs in javascript"
  #?(:clj (:import
           [org.locationtech.jts.geom
            GeometryFactory PrecisionModel
            Coordinate
            Geometry Polygon Point LineString MultiLineString
            LinearRing GeometryCollection
            MultiPoint MultiPolygon]
           [org.locationtech.jts.operation.distance DistanceOp GeometryLocation]
           [org.locationtech.jts.algorithm ConvexHull]
           [java.security MessageDigest]
           [java.util Base64 Base64$Encoder]
           [org.geotools.geojson.geom GeometryJSON]))
  #?(:clj (:require [clojure.java.io :as io]))
  #?(:cljs (:require [cljsjs.jsts :as jsts]
                     [goog.crypt.Md5 :as Md5]
                     [goog.crypt.base64 :as base64])))

#?(:cljs
   (do (def Geometry jsts/geom.Geometry)
       (def GeometryCollection jsts/geom.GeometryCollection)
       (def Coordinate jsts/geom.Coordinate)
       (def Point jsts/geom.Point)
       (def Polygon jsts/geom.Polygon)
       (def LineString jsts/geom.LineString)
       (def MultiPolygon jsts/geom.MultiPolygon)
       (def MultiLineString jsts/geom.MultiLineString)
       (def LinearRing jsts/geom.LinearRing)
       (def ConvexHull jsts/algorithm.ConvexHull)
       ))

(def ^:dynamic *geometry-factory*
  #?(:clj
     (GeometryFactory. (PrecisionModel.) 4326)
     :cljs
     (jsts/geom.GeometryFactory.)))

(def ^:dynamic *geojson-reader*
  #?(:cljs (jsts/io.GeoJSONReader. *geometry-factory*)))

(def ^:dynamic *geojson-writer*
  #?(:cljs (jsts/io.GeoJSONWriter. *geometry-factory*)
     :clj  (GeometryJSON.
            (.getMaximumSignificantDigits
             (.getPrecisionModel *geometry-factory*)))))

(defn- coord->vec [^Coordinate c]
  [(.getX c) (.getY c)])

(defn- linestring->vec [^LineString ls]
  (vec (map coord->vec (.getCoordinates ls))))

(declare create-point create-line-string create-polygon
         create-multi-line-string create-multi-point create-multi-polygon
         create-geometry-collection
         coordinate-seq)

(defn map->geom
  "Convert a geojson geometry map into a jts / jsts geometry directly"
  [m]

  (let [coordinates (or (:coordinates m)
                        (get m "coordinates"))
        type (or (:type m)
                 (get m "type"))
        ]
    (case type
      "Point"
      (create-point coordinates)
      "LineString"
      (create-line-string coordinates)
      "Polygon"
      (create-polygon (first coordinates)
                      (rest coordinates))
      "MultiPoint"
      (create-multi-point
       (for [cs coordinates]
         (create-point cs)))
      
      "MultiLineString"
      (create-multi-line-string
       (for [cs coordinates]
         (create-line-string cs)))
      
      "MultiPolygon"
      (create-multi-polygon
       (for [cs coordinates]
         (create-polygon (first cs) (rest cs))))
      
      "GeometryCollection"
      (create-geometry-collection
       (for [g (or (:geometries m) (get m "geometries"))]
         (map->geom g)))

      (throw (ex-info "I'm confused" {:geometry-type type}))
      )))

(defn geom->map
  "Convert a jts / jsts geometry into a clojure map that looks like the
  geometry part of a geojson."
  [^Geometry geom]

  (let [type-string (.getGeometryType geom)]
    (assoc
     {:type type-string}

     (if (= "GeometryCollection" type-string)
         :geometries :coordinates)
     
     (case (.getGeometryType geom)
       "Point"
       (-> geom (.getCoordinate) (coord->vec))

       "LineString"
       (linestring->vec geom)
       
       "Polygon"
       (vec (concat [(linestring->vec (.getExteriorRing geom))]
                    (for [i (range (.getNumInteriorRing geom))]
                      (linestring->vec (.getInteriorRingN geom i)))))
       
       ("MultiPoint"
        "MultiLineString"
        "MultiPolygon"
        "GeometryCollection")
       (vec (for [i (range (.getNumGeometries geom))]
              (geom->map (.getGeometryN geom i)))))
     )))

(defn geom->json [geom]
  (and geom
       #?(:cljs (->> geom
                     (.write *geojson-writer*)
                     (js/JSON.stringify))
          :clj  (let [sw (java.io.StringWriter.)]
                  (.write *geojson-writer* geom sw)
                  (.toString sw))
          )))

(defn json->geom [json]
  (and json
       #?(:cljs
          (.read *geojson-reader* json)
          :clj
          (.read *geojson-writer*
                 (java.io.StringReader. json)
                 ))))


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

(defn create-geometry-collection [gs]
  (.createGeometryCollection
   *geometry-factory*
   (into-array Geometry gs)))

(defn as-coordinate [x]
  (cond (instance? Coordinate x) x
        (vector? x) (create-coordinate (first x) (second x))))

(defn coordinate-seq [coordinate-likes]
  (map as-coordinate coordinate-likes))

(defn- clockwise?
  "True iff the given seq of coordinates are in clockwise order"
  [coordinates]
  (pos?
   (reduce
    +
    (map
     (fn [^Coordinate a ^Coordinate b]
       (let [xa (.-x a) xb (.-x b)
             ya (.-y a) yb (.-y b)]
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
  ([outer] (create-polygon outer nil))
  ([outer inners]
   
   (let [outer (coordinate-seq outer)
         inner (map coordinate-seq inners)
         outer (make-anticlockwise outer)
         inner (map make-clockwise inner)]
     (.createPolygon
      *geometry-factory*
      (create-linear-ring outer)
      (when-not (empty? outer)
        (into-array
         LinearRing
         (map create-linear-ring inner)))))))

(defn create-multi-polygon
  [polygons]

  (.createMultiPolygon
   *geometry-factory*
   (into-array Polygon polygons)))

(defn create-line-string
  [coordinates]
  (.createLineString
   *geometry-factory*
   (into-array Coordinate (coordinate-seq coordinates))))

(defn create-multi-line-string
  [line-strings]
  (.createMultiLineString
   *geometry-factory*
   (into-array LineString line-strings)))

(defn create-multi-point
  [points]
  (.createMultiPoint
   *geometry-factory*
   (into-array Point points)))

(defn create-geometry-collection
  [geoms]
  (.createGeometryCollection
   *geometry-factory*
   (into-array Geometry geoms)))

(defn intersects? [^Geometry a ^Geometry b]
  (and a b (.intersects a b)))

(defn valid? [^Geometry a]
  (and a (.isValid a)))

(defn number-of-parts [^Geometry geom]
  (.getNumGeometries geom))

(defn nth-part [^Geometry geom n]
  (.getGeometryN geom n))

(defn is-multi? [geom]
  (#{:multi-point :multi-line-string :multi-polygon} (geometry-type geom)))

(defn is-empty? [^Geometry geom] (.isEmpty geom))

(defn useful? [^Geometry geom]
  (and (valid? geom) (not (is-empty? geom))))

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

(defn coordinates [^Geometry geom] (.getCoordinates geom))

#?(:clj
   (let [^MessageDigest md5 (MessageDigest/getInstance "MD5")
         ^Base64$Encoder base64 (.withoutPadding (Base64/getEncoder))
         df (java.text.DecimalFormat. "0.00000000")
         to-fixed #(.format df %)
         ]
     (defn ghash [^Geometry geometry]
       (locking md5
         (.reset md5)
         (.update md5 (.getBytes (.toLowerCase (.getGeometryType geometry))
                                 java.nio.charset.StandardCharsets/UTF_8))

         (doseq [^Coordinate c (.getCoordinates geometry)]
           (.update md5 (.getBytes (to-fixed (.-x c)) java.nio.charset.StandardCharsets/UTF_8))
           (.update md5 (.getBytes (to-fixed (.-y c)) java.nio.charset.StandardCharsets/UTF_8))
           )
         ;; Throw away some bytes. This should give us 16 * 6 bits = 96
         ;; collision probability for 60 million objects of around
         ;; 2e-14 which is probably good enough.
         (.substring (.encodeToString base64 (.digest md5)) 0 16))))

   :cljs
   (let [md5 (goog.crypt.Md5.)]
     (defn ghash [geometry]
       (.reset md5)
       (.update md5 (.toLowerCase (.getGeometryType geometry)))

       (doseq [c (.getCoordinates geometry)]
         (.update md5 (.toFixed (.-x c) 8))
         (.update md5 (.toFixed (.-y c) 8)))
       (.substring (base64/encodeByteArray (.digest md5)) 0 16))))

(defn find-closest-coord
  "Given geometry `geom` and a point, return a tuple [distance, coord on geom]
  which minimises distance."
  [geom point]

  (let [point (create-point point) ;; make sure it's a point
        
        op (#?(:cljs
               jsts/operation.distance.DistanceOp.
               :clj
               DistanceOp.)
            geom
            point)
        ]

    [(.distance op) (aget (.nearestPoints op) 0)]))

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
    (* deg d2r))
  (defn to-degrees [^double rad]
    (/ rad d2r)))


(let [a 6378137.0                       ; These are the WGS84 parameters
      b 6356752.314245
      f (/ 1.0 298.257223563)

      lonlat (fn [point]
                (cond
                      (instance? Point point)
                      (let [c (.getCoordinate point)]
                        [(.-x c) (.-y c)])

                      (instance? Coordinate point)
                      [(.-x point) (.-y point)]

                      (vector? point)
                      point))

      lonlat-rads (fn [point] (let [[a b] (lonlat point)]
                                [(to-radians a) (to-radians b)]))
      ]
  
  (defn geodesic-distance [p1 p2]
    ;; x is lon, y is lat
    (let [[λ1 φ1] (lonlat-rads p1)
          [λ2 φ2] (lonlat-rads p2)]
      (if (and (= λ1 λ2) (= φ1 φ2)) 0.0
          (let [L (- λ2 λ1)

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
                      (recur λ (inc i)))))))))))

  (defn geodesic-translation [point distance bearing]
    ;; x is lon y is lat
    (let [[λ1 φ1] (lonlat-rads point)
          α1 (to-radians bearing)
          s  distance

          sinα1 (Math/sin α1)
          cosα1 (Math/cos α1)
          tanU1 (* (- 1 f) (Math/tan φ1))
          cosU1 (/ 1 (Math/sqrt (+ 1 (* tanU1 tanU1))))
          sinU1 (* tanU1 cosU1)
          σ1    (Math/atan2 tanU1 cosα1)
          sinα  (* cosU1 sinα1)

          cosSqα (- 1 (* sinα sinα))
          uSq    (* cosSqα (/ (- (* a a) (* b b))
                              (* b b)))

          A      (+ 1 (* (/ uSq 16384)
                         (+ 4096 (* uSq (+ -768 (* uSq (- 320 (* 175 uSq))))))))
          B      (* (/ uSq 1024)
                    (+ 256 (* uSq (+ -128 (* uSq (- 74 (* 47 uSq)))))))
          ]
      (loop [σ (/ s (* b A))
             i 0]
        (let [cos2σₘ (Math/cos (+ σ (* 2 σ1)))
              sinσ   (Math/sin σ)
              cosσ   (Math/cos σ)
              Δσ     (* B sinσ
                        (+ cos2σₘ
                           (* (/ B 4)
                              (-
                               (* cosσ (+ -1 (* 2 cos2σₘ cos2σₘ)))
                               (* (/ B 6)
                                  cos2σₘ
                                  (+ -3 (* 4 sinσ sinσ))
                                  (+ -3 (* 4 cos2σₘ cos2σₘ)))))))
              σ1     (+ Δσ (/ s (* b A)))
              error  (Math/abs (- σ1 σ))
              ]
          (if (and (> error 1e-12)
                   (< i 100))
            (recur σ1 (inc i))

            (let [x  (- (* sinU1 sinσ)
                        (* cosU1 cosσ cosα1))

                  φ2 (Math/atan2
                      (+ (* sinU1 cosσ)
                         (* cosU1 sinσ cosα1))
                      (* (- 1 f)
                         (Math/sqrt (+ (* sinα sinα)
                                       (* x x)))))

                  λ  (Math/atan2
                      (* sinσ sinα1)
                      (- (* cosU1 cosσ)
                         (* sinU1 sinσ cosα1)))
                  
                  C  (* (/ f 16)
                        cosSqα
                        (+ 4 (* f (- 4 (* 3 cosSqα)))))

                  L  (- λ
                        (* (- 1 C)
                           f sinα
                           (+ σ
                              (* C sinσ
                                 (+ cos2σₘ (* C cosσ
                                              (+ -1 (* 2 cos2σₘ cos2σₘ))))))))

                  λ2 (+ λ1 L)
                  α2 (Math/atan2 sinα (- x))]
              (create-point (to-degrees λ2) (to-degrees φ2)))))))))

(def nan? #(#?(:clj Double/isNaN :cljs js/isNaN) %))

(defn geodesic-length [line-string]
  (reduce +
          (map geodesic-distance
               (coordinates line-string)
               (rest (coordinates line-string)))))

(defn distance-between
  "Given two geometries, find how far apart they are at their closest points
  If :geodesic true, then assume WGS84 lat/lon coordinates and give result in meters."
  [g1 g2 & {:keys [geodesic]}]
  (let [op (#?(:cljs
               jsts/operation.distance.DistanceOp.
               :clj
               DistanceOp.)
            g1 g2)]
    (if geodesic
      (let [[c1 c2] (.nearestPoints op)]
        (geodesic-distance c1 c2))
      
      (.distance op))))

(defn centroid [^Geometry g] (.getCentroid g))

(defn convex-hull ^Geometry [gs]
  (cond
    (instance? Geometry gs)
    (.getConvexHull (ConvexHull. gs))
        
    (and (seq gs) (every? #(instance? Geometry %) gs))
    (convex-hull (create-geometry-collection gs))

    :else (throw (IllegalArgumentException.
                  "convex-hull accepts a geometry or seq of geometries."))))
