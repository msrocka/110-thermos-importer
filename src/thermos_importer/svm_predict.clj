(ns thermos-importer.svm-predict
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]))

(defn- transpose-map-of-lists [x]
  (map (fn [m] (zipmap (keys x) m)) (apply map vector (vals x))))

(defn- rbf
  "Create a radial basis kernel with parameter SIGMA."
  ([sigma]
   (fn [^doubles x ^doubles y]
     (let [dim (alength x)
           dist (loop [i 0 t (double 0)]
                  (if (< i dim)
                    (recur (unchecked-inc-int i)
                           (let [xi (aget x i)
                                 yi (aget y i)
                                 d (- xi yi)
                                 d2 (* d d)]
                             (- t d2)))
                    t))]
       (Math/exp (* sigma dist))))))

(defn- kernel-fn [json]
  (-> json :kpar :sigma rbf))

(defn- input-scaler [key-order json]
  (let [offsets (-> json :scaling :x.scale :scaled:center)
        factors (-> json :scaling :x.scale :scaled:scale)
        offsets (into-array (for [k key-order]
                              (get offsets k 0)))
        factors (into-array (for [k key-order]
                              (get factors k 1)))]
    (fn [sv]
      (dotimes [i (alength sv)]
        (aset sv i
              (/ (- (aget sv i)
                    (aget offsets i))
                 (aget factors i))))
      sv)))

(defn- output-scaler [json]
  (let [^double offset (-> json :scaling :y.scale :scaled:center)
        ^double factor (-> json :scaling :y.scale :scaled:scale)]
    (fn [^double x]
      (+ offset (* factor x)))))

(defn predictor
  "Create a predictor function from some json from R's libsvm wrpaper
  The predictor function returns two values in a double array.
  The first is the value of the SVM output.
  The second is the value of the scaled input dimension having largest absolute value.
  If this value is very large, then the input is unusual on some dimension or other.
  "
  [json]

  (let [key-order (sort (keys (:svs json)))
        svs (transpose-map-of-lists (:svs json))
        map->sv #(double-array (for [k key-order]
                                 (let [v (get % k 0)]
                                   (if (boolean? v)
                                     (if v 1 0)
                                     v))))
        
        svs ^"[[D" (into-array (map map->sv svs))
        
        alpha (double-array (:alpha json))

        sv-count (alength svs)

        scale-inputs (input-scaler key-order json)
        scale-output (output-scaler json)

        kernel (kernel-fn json)
        
        offset (double (:offset json))

        has-required-keys (fn [x] (every? (comp not nil? x) key-order))]
    (with-meta
      (fn [input]
        #dbg (when (has-required-keys input)
          (let [input (map->sv input)
                input (scale-inputs input)

                result
                (loop [idx 0 ret 0.0]
                  (if (< idx sv-count)
                    (recur (unchecked-inc-int idx)
                           (+ ret
                              (* (aget alpha idx)
                                 (kernel (aget svs idx) input))))
                    ret))

                result (- result offset)
                result (scale-output result)
                out (make-array Double/TYPE 2)
                ]
            (aset out 0 result)
            (aset out 1 (areduce input i absmax 0.0 (Math/max
                                                     absmax
                                                     (Math/abs (aget input i)))))
            out)))
      {:predictors key-order})))

(defn load-predictor [file]
  (with-open [r (io/reader file)]
    (predictor (json/read r :key-fn keyword))))


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

