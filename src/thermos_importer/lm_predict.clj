;; This file is part of THERMOS, copyright © Centre for Sustainable Energy, 2017-2021
;; Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.

(ns thermos-importer.lm-predict
  "Functions to run linear model predictors"
  (:require [clojure.test :as test]))

(defn predictor
  "Create a predictor function from some json from josh's email

  These look like

  { :intercept constant
    [:predictor mul]* }

  Therefore we cannot have a predictor called intercept."
  {:test #(let [a (predictor {:intercept 100.0})
                b (predictor {:x 1})
                c (predictor {:x 3 :y -9})
                d (predictor {:x 1 :y -1 :intercept 1000})]
            (test/is (= (a {}) 100.0))
            (test/is (= (b {:x 1}) 1.0))
            (test/is (= (b {:x 2}) 2.0))
            (test/is (= (c {:x 1 :y 0}) 3.0))
            (test/is (= (c {:x 1 :y 1}) -6.0))
            (test/is (= (d {:x -1 :y 1}) 998.0)))}

  [json]

  (let [constant ^double (double (:intercept json 0.0))
        json (dissoc json :intercept)
        key-order (into-array (sort (keys json)))
        muls (double-array (map (comp double json) key-order))
        len (alength muls)
        has-required-keys (fn [x] (every? (comp not nil? x) key-order))]
    (with-meta
      (fn [val]
        (when (has-required-keys val)
          (areduce
           key-order ix acc constant
           (+ acc (* (aget muls ix)
                     (let [x (get val (aget key-order ix))]
                       (cond
                         (number? x)
                         (double x)

                         x 1.0
                         true 0.0))))
           )))
      {:predictors key-order})))




