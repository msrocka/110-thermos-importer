;; This file is part of THERMOS, copyright © Centre for Sustainable Energy, 2017-2021
;; Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

(ns thermos-importer.overpass
  (:require [clojure.xml :as xml]
            [clj-http.client :as http]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.util :as util]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.test :as test]
            [cljts.core :as jts])
  (:import [java.io ByteArrayInputStream]
           [java.net URLEncoder]

           [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries]
           
           [org.geotools.geometry.jts JTSFactoryFinder]
           
           [org.locationtech.jts.geom GeometryFactory Coordinate Polygon]))

(def default-overpass-api "http://overpass-api.de/api/interpreter")
  ;;"http://overpass-api.de/api/interpreter")

(defn query-name [area-name & {:keys [include-buildings include-highways]
                               :or {include-buildings true include-highways true}}]
  {:pre [(or (string? area-name)
             (and (vector? area-name)
                  (= 4 (count area-name))
                  (every? number? area-name)))]}
  (let [[area-query area-filter]
        (cond
          (vector? area-name)
          ["" (string/join "," area-name)]
          (.startsWith area-name "relation:")
          [(format "(rel(%s); map_to_area;) -> .a;" (.substring area-name 9))
           "area.a"]
          
          (.startsWith area-name "way:")
          [(format "(way(%s); map_to_area;) -> .a;" (.substring area-name 4))
           "area.a"]
          :default
          [(format "(area[name=%s];)->.a;" (pr-str area-name))
           "area.a"])
        ]
    (str area-query
         "("
         (if include-highways
              (str "way[highway] (" area-filter ");")
              "")
         (if include-buildings
              (str "way[landuse] ("area-filter");"
                   "way[building] ("area-filter");"
                   "rel[building] ("area-filter");")
              "")
         "); out geom meta;")))

(def geometry-factory
  (GeometryFactory. (org.locationtech.jts.geom.PrecisionModel.) 4326))

(defn- node->coordinate
  "Convert an OSM xml nd entity to a JTS coordinate object"
  [{{lat :lat lon :lon} :attrs}]
  (Coordinate. (Double/parseDouble lon) (Double/parseDouble lat)))

(defn- way->coordinates
  "Convert an OSM xml way entity to a seq of JTS coordinates"
  [{content :content}]
  (->> content
       (filter #(= :nd (:tag %)))
       (map node->coordinate)))

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

(defn- coordinates->polygon
  "Given a list of rings, create a polygon

  The first ring is the outer ring, and any remainders are inner
  rings. This function will normalize the rings to follow the geojson
  winding rules, so the outer ring will be made to go anticlockwise
  and any inner rings to go clockwise."
  [[exterior & interior]]
  (let [exterior (make-anticlockwise exterior)
        interior (map make-clockwise interior)]
    
    (.createPolygon
     geometry-factory
     (.createLinearRing geometry-factory
                        (into-array Coordinate exterior))
     (when-not (empty? interior)
       (into-array (map #(.createLinearRing
                          geometry-factory
                          (into-array Coordinate %)) interior))))))

(defn- closed? [coordinates] (= (first coordinates) (last coordinates)))

(defn- shares-endpoint? [a b]
  (or (= (first a) (first b))
      (= (first a) (last b))
      (= (last a) (first b))
      (= (last a) (last b))))

(defn- join-at-endpoint
  {:test #(do (test/is (= (join-at-endpoint [1 2 3] [3 4 5])
                         [1 2 3 4 5]))
              (test/is (= (join-at-endpoint [1 2 3] [5 4 3])
                         [1 2 3 4 5]))
              (test/is (= (join-at-endpoint [3 2 1] [5 4 3])
                         [5 4 3 2 1]))
              (test/is (= (join-at-endpoint [3 2 1] [3 4 5])
                         [5 4 3 2 1])))}
  [a b]
  (cond (= (last a) (first b))
        (into [] (concat a (rest b)))

        (= (last b) (first a))
        (into [] (concat b (rest a)))

        (= (first a) (first b))
        (into [] (concat (reverse b) (rest a)))

        (= (last a) (last b))
        (into [] (concat a (rest (reverse b))))))

(defn- close-rings
  "Given a set of rings which might be split up, try to close them."
  {:test #(let [rings [[1 2 3] [3 4 5] [1 5]
                       [7 8 9] [9 10 7]]
                closed (close-rings rings)]
            (test/is (= 2 (count closed))))}
  [rings]

  (let [{closed true open false} (group-by closed? rings)]
    (concat
     closed

     (:closed
      (reduce
       (fn [{closed :closed partial :partial} o]
         ;; find a ring in partial that touches o
         (if-let [adjacent (some #(and (shares-endpoint? % o) %) partial)]
           (let [joined (join-at-endpoint adjacent o)]
             (if (closed? joined)
               {:closed (conj closed joined)
                :partial (disj partial adjacent)}
               {:closed closed
                :partial (conj (disj partial adjacent) joined)}))
           {:closed closed
            :partial (conj partial o)}))
       {:closed #{} :partial #{}}
       open)))))

(defmulti osm->geom :tag)

(defmethod osm->geom :default [_] nil)

(defmethod osm->geom :relation [rel]
  ;; Relations contain a bunch of members, with associated roles.
  (let [{inner-rings "inner" outer-rings "outer"}
        (->> (:content rel)
             (filter #(= (:tag % :member))) ;; just the members
             (group-by (comp :role :attrs)))

        is-type-way? (comp #{"way"} :type :attrs)
        
        inner-rings (->> inner-rings
                         (filter is-type-way?)
                         (map way->coordinates)
                         (close-rings))
        
        outer-rings (->> outer-rings
                         (filter is-type-way?)
                         (map way->coordinates)
                         (close-rings))
        
        any-open-rings? (or (some (comp not closed?) inner-rings)
                            (some (comp not closed?) outer-rings))

        nouter (count outer-rings)
        ninner (count inner-rings)
        
        one-outer-ring? (= 1 nouter)
        no-inner-rings? (zero? ninner)
        some-outer-rings? (> nouter 0)
        ]
    (cond
      any-open-rings?
      (log/warn "WARNING: Open rings in" rel) ;; returns nil, as failure case

      ;; A normal polygon, with maybe an inner ring
      one-outer-ring?
      (coordinates->polygon (concat outer-rings inner-rings))

      ;; A multipolygon, without any inner rings
      (and no-inner-rings? some-outer-rings?)
      (->> outer-rings
           (map vector)
           (map coordinates->polygon)
           (into-array Polygon)
           (.createMultiPolygon geometry-factory))

      :otherwise
      ;; assign the inner rings to whichever outer rings cover them
      (let [outer-polygons (for [o outer-rings]
                             [o (coordinates->polygon [o])])

            inner-rings (group-by
                         (fn [i]
                           (let [p (coordinates->polygon [i])]
                             (some (fn [[o p2]]
                                     (and (.covers p2 p) o)) outer-polygons)))
                         inner-rings)]
        ;; make a multipolygon
        (->> (for [[outer inners] inner-rings]
               (coordinates->polygon (concat [outer] inners)))
             (into-array Polygon)
             (.createMultiPolygon geometry-factory))))))

(defmethod osm->geom :way [way]
  (let [nodes (way->coordinates way)]
    ;; TODO I am not sure whether a closed way is always a polygon
    (when-not (empty? nodes)
      (if (closed? nodes)
        (.createPolygon geometry-factory
                        (.createLinearRing geometry-factory (into-array Coordinate nodes))
                        nil)
        (.createLineString geometry-factory (into-array Coordinate nodes))))))

(defmethod osm->geom :nd [node]
  (.createPoint geometry-factory (node->coordinate node)))

(defn osm-tags->map
  "Take the OSM tags from inside a query result, and puts them into a map."
  [osm]
  (let [tags (filter #(= :tag (:tag %)) (:content osm))
        tags  (into {} (for [{{k :k v :v} :attrs} tags] [(keyword k) v]))
        osm-id (get-in osm [:attrs :id])]
    (assoc tags :osm-id osm-id)))

(def ^:dynamic *overpass-retries* 4)
(def ^:dynamic *overpass-retry-delay* 6000)

(defn- sleep-and-retry [exception n]
  (let [status (:status (ex-data exception) 0)]
    (when (and (< n *overpass-retries*)
               (<= 500 status 599))
      (let [delay (* n n *overpass-retry-delay*)]
        (log/warnf "Overpass query produced status %s on attempt %d - retry in %dms"
                   status n delay)
        (Thread/sleep delay))
      true)))

(defn query-overpass [query & {:keys [overpass-api retry?]
                               :or {retry? sleep-and-retry}}]
  (let [query-body (str "data=" (URLEncoder/encode query "UTF-8"))
        result
        (loop [n 1]
          (let [result
                (try
                  (http/post
                   (or overpass-api default-overpass-api)
                   {:as :stream :body query-body})
                  (catch Exception e
                    (if (sleep-and-retry e n)
                      ::retry
                      (let [ed (ex-data e)]
                        (throw (ex-info (format "Error querying openstreetmap: %s %s"
                                                (:status ed)
                                                (:reason-phrase ed))
                                        (assoc ed :overpass-query query) e))))))]
            (if (= result ::retry)
              (recur (inc n))
              result)))
        ]
    (-> result
        (:body)
        (xml/parse)
        (:content))))

(defn get-geometry [area-name & {:keys [overpass-api
                                        include-buildings
                                        include-highways]
                                 :or {include-buildings true
                                      include-highways true
                                      overpass-api default-overpass-api}}]
  (let [oxml (query-overpass (query-name area-name
                                         :include-buildings include-buildings
                                         :include-highways include-highways)
                             :overpass-api overpass-api)

        objects
        (keep
         #(let [ts (osm-tags->map %)
                g (try (osm->geom %)
                       (catch Exception e
                         (log/warn e "Invalid geometry for OSM feature")
                         nil))]
            (when (jts/valid? g)
              (merge (geoio/geom->map g) ts)))
         oxml)

        {land-uses false candidates true}
        (group-by (comp nil? :landuse) objects)

        _ (log/info (count candidates) "candidates"
                    (count land-uses) "land uses")

        landuse-rtree
        (reduce
         (fn [tree land-use]
           (let [geom (::geoio/geometry land-use)
                 rect (util/geom->rect geom)]
             (.add tree land-use rect)))
         (RTree/create)
         land-uses)
        
        find-landuse
        (fn [{geom ::geoio/geometry :as a}]
          (let [rect (try (util/geom->rect geom)
                          (catch Exception e
                            (throw (ex-info "Error looking up land-use for OSM object"
                                            {:entity a}
                                            e))))
                
                possibles (util/search-rtree landuse-rtree rect)]
            (->> possibles
                 (filter #(.intersects geom (::geoio/geometry %)))
                 (first)
                 (:landuse))))

        ;; remove any invalid geometries
        candidates
        (filter #(and (.isValid (::geoio/geometry %))
                      (not (.isEmpty (::geoio/geometry %))))
                candidates)
        ]
    (for [c candidates]
      (let [landuse (find-landuse c)]
        (assoc c :landuse landuse)))))


