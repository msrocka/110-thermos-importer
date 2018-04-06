(def project 'thermos-importer)
(def version "0.1.0-SNAPSHOT")

(def geo-repos
  [["jts" {:url "https://repo.locationtech.org/content/groups/releases"}]
   ["jts-snapshots" {:url "https://repo.locationtech.org/content/repositories/jts-snapshots"}]
   ["geotools" {:url "http://download.osgeo.org/webdav/geotools"}]])

(set-env! :resource-paths #{"resources" "src"}
          :source-paths   #{"test"}
          :repositories #(concat % geo-repos)
          :dependencies   '[[org.clojure/clojure "1.9.0"]
                            [org.clojure/tools.cli "0.3.5"]

                            [digest "1.4.6"]
                            [org.geotools/gt-data "18.2"]
                            [org.geotools/gt-shapefile "18.2"]
                            [org.geotools/gt-geojson "18.2"]
                            [org.geotools/gt-referencing "18.2"] ; Coordinate transformations
                            [org.geotools/gt-epsg-hsql "18.2"] ; Coordinate system definitions
                            [com.github.davidmoten/rtree "0.8.0.4"]

                            [better-cond "1.0.1"]

                            [adzerk/boot-test "RELEASE" :scope "test"]])

(task-options!
 aot {:namespace   #{'thermos-importer.core}}
 pom {:project     project
      :version     version
      :description "FIXME: write description"
      :url         "http://example/FIXME"
      :scm         {:url "https://github.com/yourname/thermos-importer"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}}
 jar {:main        'thermos-importer.core
      :file        (str "thermos-importer-" version "-standalone.jar")})

(deftask build
  "Build the project locally as a JAR."
  [d dir PATH #{str} "the set of directories to write to (target)."]
  (let [dir (if (seq dir) dir #{"target"})]
    (comp (aot) (pom) (uber) (jar) (target :dir dir))))

(deftask run "Run the jar"
  []
  (require '[thermos-importer.core :as app])
  (apply (resolve 'app/-main) *args*)
  )

;; (deftask import
;;   "Import base spatial data into a geojson file"
;;   [r roads VAL str "An OSM extract of roads (shapefile)"
;;    b buildings VAL str "An OSM extract of buildings (shapefile)"
;;    a addresses VAL str "An address dataset (columnar, with x,y)"
;;    l lidar VAL str "A LIDAR dataset"
;;    b bounding-box VAL str "A shapefile containing named regions"
;;    ]
;;   (require '[thermos-importer.core :as app])
;;   ((resolve 'app/run)

;;    ))


(require '[adzerk.boot-test :refer [test]])
