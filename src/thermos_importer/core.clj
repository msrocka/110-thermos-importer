(ns thermos-importer.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [thermos-importer.util :refer [has-extension]]
            [thermos-importer.lidar :as lidar]
            [thermos-importer.geoio :as geoio]
            [clojure.string :as s])
  (:gen-class))

(def cli-options
  [["-l" "--lidar DIR" "LIDAR directory / file"
    :assoc-fn (fn [m k v] (update m k conj v))
    :default []]
   [nil "--id FIELD" "ID field in input" :default "id"]
   ["-o" "--output FILE" "File to write output table to" :default "-"]
   [nil "--preferred-height HEIGHT" "Height field you like more than LIDAR"]
   [nil "--fallback-height HEIGHT" "Height field you like less than LIDAR"]
   [nil "--storey-height HEIGHT" "How high is a storey"
    :default 3.0 :parse-fn #(Double/parseDouble %)]
   ["-h" "--help" "me Obi-Wan Kenobi. You're my only hope."]])

(defn- output [thing]
  (if (= thing "-")
    (proxy [java.io.FilterWriter] [*out*]
      (close []
        (proxy-super flush)))
    (io/writer (io/file thing))))

(declare make-predictors)
(def predictors [::lidar/footprint
                 ::lidar/perimeter
                 ::lidar/shared-perimeter
                 ::lidar/shared-perimeter-m
                 ::lidar/perimeter-per-footprint
                 ::lidar/floor-area
                 ::lidar/corners
                 ::lidar/storeys
                 
                 ::lidar/height
                 ::lidar/wall-area
                 ::lidar/party-wall-area
                 ::lidar/external-wall-area
                 ::lidar/external-surface-area
                 ::lidar/total-surface-area
                 ::lidar/volume
                 
                 
                 ::lidar/ext-surface-proportion
                 ::lidar/ext-surface-per-volume
                 ::lidar/tot-surface-per-volume])

(defn print-header []
  (print "id")
  (doseq [p predictors]
    (print ",")
    (print (name p)))
  (println))

(defn print-predictors [id f]
  (print id)
  (doseq [p predictors]
    (print "," )
    (print (get f p "")))
  (println))

(defn -main [& args]
  ;; given a shapefile(s) and a lidar directory, output a table of stuff
  (let [{{out-file :output help :help lidar :lidar id-col :id
          preferred-height :preferred-height
          fallback-height  :fallback-height
          storey-height    :storey-height} :options
         args :arguments
         errs :errors
         sum :summary} (parse-opts args cli-options)]
    (if (or (seq errs) help)
      (do
        (println "Usage: java -jar dm-predictor.jar [-l] [-o] [--id] [-h] [--storey-height] [--fallback-height] [--preferred-height] <shape> <shape> ...")
        (println sum))
      
      (let [lidar (lidar/rasters->index
                   (filter
                    (fn [f] (and (.isFile f)
                                 (or (has-extension f "tif")
                                     (has-extension f "tiff"))))
                    (mapcat (comp file-seq io/file) lidar)))]
        (with-open [out (output out-file)]
          (binding [*out* out
                    lidar/*storey-height* storey-height]
            (print-header)
            (doseq [shape-file args]
              (make-predictors shape-file id-col preferred-height fallback-height lidar))))))))

(defn make-predictors
  "Load the shapes in shape-file, and iterate through printing all the predictors"
  [shape-file id-col preferred-height fallback-height lidar]

  (let [shapes (-> (geoio/read-from shape-file :key-transform identity)
                   (lidar/add-lidar-to-shapes lidar)
                   (geoio/update-features :fix-heights
                                          (fn [f]
                                            (assoc f
                                                   ::lidar/height
                                                   (or (and preferred-height
                                                            (get preferred-height f))
                                                       (::lidar/height f)
                                                       (and fallback-height
                                                            (get fallback-height f))))))
                   (geoio/update-features :update-predictors lidar/derive-more-fields))]

    (doseq [f (::geoio/features shapes)]
      (let [id (get f id-col)]
        (print-predictors id f)))))



