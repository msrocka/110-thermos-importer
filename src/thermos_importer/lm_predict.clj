(ns thermos-importer.lm-predict
  "Functions to run linear model predictors")

(defn predictor
  "Create a predictor function from some json from josh's email

  These look like

  { :intercept constant
    [:predictor mul]* }

  Therefore we cannot have a predictor called intercept."
  [json]

  (let [constant ^double (or (double (:intercept json)) 0.0)
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
           (+ constant (* (aget muls ix)
                          (get val (aget key-order ix)))))))
      {:predictors key-order})))




