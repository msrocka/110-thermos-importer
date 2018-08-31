(ns thermos-importer.svm-predict
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn- transpose-map-of-lists [x]
  (map (fn [m] (zipmap (keys x) m)) (apply map vector (vals x))))

(defn- rbf
  "Create a radial basis kernel with parameter SIGMA."
  ([sigma]
   (fn [^doubles x ^doubles y]
     (let [dim (count x)
           dist (loop [i 0
                       t 0]
                  (if (< i dim)
                    (recur (inc i)
                           (let [xi (aget x i)
                                 yi (aget y i)
                                 d (- xi yi)
                                 d2 (* d d)]
                             (- t d2)))
                    t))]
       (Math/exp (* sigma dist))))))

(defn- kernel-fn [json]
  (-> json :kpar :sigma rbf))

(defn- input-scaler [json]
  (let [offsets (-> json :scaling :x.scale :scaled:center)
        factors (-> json :scaling :x.scale :scaled:scale)]
    #(reduce-kv
      (fn [m k v]
        (let [offset (get offsets k 0)
              factor (get factors k 1)
              result (/ (- v offset) factor)
              ]
          
          (assoc m k result)))
      {}
      %)))

(defn- output-scaler [json]
  (let [^double offset (-> json :scaling :y.scale :scaled:center)
        ^double factor (-> json :scaling :y.scale :scaled:scale)]
    (fn [^double x]
      (+ offset (* factor x)))))

(defn predictor
  "Create a predictor function from some json from R's libsvm wrpaper"
  [json]

  (let [key-order (sort (keys (:svs json)))
        svs (transpose-map-of-lists (:svs json))
        map->sv #(double-array (for [k key-order] (get % k 0)))
        
        svs (into-array (map map->sv svs))
        
        alpha (double-array (:alpha json))

        scale-inputs (input-scaler json)
        scale-output (output-scaler json)

        kernel (kernel-fn json)
        
        offset (:offset json)
        ]

    (fn [val]
      (let [val (scale-inputs val)
            val (map->sv val)
            result (reduce + (map (fn [^double a sv]
                                    (* a (kernel sv val))) alpha svs))
            result (- result offset)
            ]
        (scale-output result)))))

;; (def some-json (json/read-str (slurp "/home/hinton/temp/svm.json") :key-fn keyword))
;; (def p (predictor some-json))

;; (defn csv-data->maps [csv-data]
;;   (map zipmap
;;        (->> (first csv-data) ;; First row is the header
;;             (map keyword) ;; Drop if you want string keys instead
;;             repeat)
;;        (rest csv-data)))
;; (def test-data
;;   (with-open [reader (io/reader "/home/hinton/temp/svm.csv")]
;;     (doall (csv-data->maps (csv/read-csv reader)))))


;; (def delta
;;   (for [t test-data]
;;    {:mine (p (into {}
;;                    (for [k (keys (:svs some-json))]
;;                      [k (Double/parseDouble (t k))])))
    
;;     :theirs (Double/parseDouble (:predicted t))}
;;    ))

;; (def simple {:svs {:x [-1.4863011 -1.1560120 -0.8257228 -0.4954337 0.4954337 0.8257228 1.1560120 1.4863011 ]}
;;              :scaling
;;              {:x.scale {:scaled:center {:x 5.5}
;;                         :scaled:scale {:x 3.02765}
;;                         }
;;               :y.scale {:scaled:center 15.5 :scaled:scale 3.02765}}
;;              :offset -7.394432e-10
;;              :alpha [-1.0000000 -0.2062952 -0.4106167 -0.1191324  0.1191324  0.4106167  0.2062952 1.0000000]
;;              :kpar {:sigma 4.81023102310231}
;;              })

;; (def p (predictor
;;         simple
;;         ))

