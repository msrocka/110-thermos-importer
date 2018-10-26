(ns thermos-importer.overpass
  (:require [clojure.xml :as xml]
            [clj-http.client :as http]
            [thermos-importer.geoio :as geoio]
            [thermos-importer.util :as util]
            [clojure.string :as string]
            
            )
  
  (:import [java.io ByteArrayInputStream]
           [java.net URLEncoder]

           [com.github.davidmoten.rtree RTree]
           [com.github.davidmoten.rtree.geometry Geometries]
           
           [org.geotools.geometry.jts JTSFactoryFinder]
           
           [org.locationtech.jts.geom GeometryFactory Coordinate Polygon]))

(def default-overpass-api "http://overpass-api.de/api/interpreter")
  ;;"http://overpass-api.de/api/interpreter")

(defn query-name [area-name]
  (format "(area[name=%s];)->.a;
  (
  way[landuse] (area.a);
  way[highway] (area.a);
  way[building] (area.a);
  rel[building] (area.a);
  );
  out geom meta;" (pr-str area-name)))

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

(defn- coordinates->polygon [[exterior & interior]]
  (.createPolygon geometry-factory
                  (.createLinearRing geometry-factory (into-array Coordinate exterior))
                  (when-not (empty? interior)
                    (into-array (map #(into-array Coordinate %) interior)))))

(defn- closed? [coordinates] (= (first coordinates) (last coordinates)))

(defmulti osm->geom :tag)

(defmethod osm->geom :default [_] nil)

(defmethod osm->geom :relation [rel]
  ;; Relations contain a bunch of members, with associated roles.
  (let [{inner-rings "inner" outer-rings "outer"}
        (->> (:content rel)
             (filter #(= (:tag % :member))) ;; just the members
             (group-by (comp :role :attrs)))

        is-type-way? #(= "way" (:type %))
        
        inner-rings (->> inner-rings
                         (filter is-type-way?)
                         (map way->coordinates))
        
        outer-rings (->> outer-rings
                         (filter is-type-way?)
                         (map way->coordinates))

        ;; In-principle, the inner / outer rings can be split up into
        ;; several parts. This is something to deal with later, but we
        ;; can detect it by noting whether any of the rings are
        ;; unclosed

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
      (println "WARNING: Open rings in" rel) ;; returns nil, as failure case

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

      ;; worse-case is a multipolygon with inner rings. For this, we
      ;; need to associate the inners with the outers, which is
      ;; annoying geometry.
      :otherwise
      (println "WARNING: Difficult multipolygons in" rel))))

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
    (assoc tags :osm_id osm-id)))

(def highway-subtype :highway)

(defn- building-subtype [find-landuse
                         {b :building a :amenity :as feature}]
  (or
   (when-not (= b "yes") b)
   a
   (when-let [l (find-landuse feature)]
     (str l " land"))))

(defn- add-subtype [find-landuse feature]
  (let [highway (:highway feature)
        building (:building feature)

        subtype
        (cond
          highway
          (highway-subtype feature)
          
          building
          (building-subtype find-landuse feature))
        ]
    (assoc feature :subtype subtype)))

(defn query-overpass [query & {:keys [overpass-api]}]
  (let [query-body (str "data=" (URLEncoder/encode query "UTF-8"))
        result (http/post (or overpass-api default-overpass-api)
                          {:as :stream :body query-body})]
    (println "Status" (:status result))
    (-> result
        (:body)
        (xml/parse)
        (:content))))

(defn get-geometry [area-name & {:keys [overpass-api]}]
  (let [oxml (query-overpass (query-name area-name) :overpass-api overpass-api)

        objects
        (keep
         #(let [ts (osm-tags->map %)
                g (osm->geom %)]
            (when g
              (merge (geoio/geom->map g) ts)))
         oxml)

        {land-uses false candidates true}
        (group-by (comp nil? :landuse) objects)

        _ (println (count candidates) "candidates"
                   (count land-uses) "land uses")

        landuse-rtree
        (reduce
         (fn [tree land-use]
           (let [geom (::geoio/geometry land-use)
                 rect (util/geom->rect geom)]
             (.add tree land-use rect)))
         (RTree/create)
         land-uses)
        
        _ (println "Made land-use index")
        
        find-landuse
        (fn [{geom ::geoio/geometry :as a}]
          (let [rect (util/geom->rect geom)
                possibles (util/search-rtree landuse-rtree rect)]
            (->> possibles
                 (filter #(.intersects geom (::geoio/geometry %)))
                 (first)
                 (:landuse))))
        ]
    (map (partial add-subtype find-landuse)
         candidates)))


