(ns thermos-importer.spatial-test
  (:require [thermos-importer.spatial :as spatial]
            [clojure.test :as t]
            [thermos-importer.geoio :as geoio]
            [clojure.java.io :as io]
            [clojure.test :as test]))

(t/deftest test-node-paths
  (let [cross (geoio/read-from (io/resource "thermos_importer/cross-paths.json"))
        noded (spatial/node-paths (::geoio/features cross))
        ]
    ;; cross is two lines which cross, having property "label" A and B.
    ;; this should produce four lines, two A and two B, which all share a common vertex

    (t/is (= 4 (count noded)))
    (t/is (= 2 (count (filter (comp #{"A"} :label) noded))))
    (t/is (= 2 (count (filter (comp #{"B"} :label) noded))))

    
    (t/is (->> noded ;; 5 distinct vertices
               (mapcat (juxt ::spatial/start-node ::spatial/end-node))
               (map ::geoio/id)
               (frequencies)
               (vals)
               (frequencies)
               ;; i.e. 4 vertices occur once, 1 occurs 4 times
               (= {4 1, 1 4})))
    ))

(t/deftest test-snap-paths
  (let [snap (geoio/read-from (io/resource "thermos_importer/snap-paths.json"))
        noded (spatial/node-paths (::geoio/features snap)
                                  :snap-tolerance 5
                                  :crs (::geoio/crs snap))
        noded-no-snap (spatial/node-paths (::geoio/features snap)
                                          :snap-tolerance 0
                                          :crs (::geoio/crs snap))
        ]
    ;; the input contains four paths:
    ;; A, a vertical line to the west, which crosses B and snaps to nothing
    ;; B, a horizontal line
    ;; C, a vertical line partway east which should be left on its own
    ;; D, a vertical line further east which should get snapped to join B

    ;; so the output should have
    ;; A north and A south
    ;; B west, B middle and B east
    ;; C on its own
    ;; D south and maybe a tiny D north because of snapping.

    ;; connections should be A north & south to B west & middle
    ;; B middle & east to D south & perhaps north

    (t/is 
     (->> noded
          (map :label)
          (frequencies)
          (= {"B" 3 "A" 2 "C" 1 "D" 2})
          ))

    (t/is
     (->> noded-no-snap
          (map :label)
          (frequencies)
          (= {"B" 2 "A" 2 "C" 1 "D" 1})))
    ))


(t/deftest test-connect-buildings
  (let [input (geoio/read-from (io/resource "thermos_importer/connect-buildings.json"))

        {:keys [line-string polygon]}
        (group-by ::geoio/type (::geoio/features input))
        
        noded-lines (spatial/node-paths line-string)

        connected-by-label (spatial/add-connections
                            (::geoio/crs input)
                            polygon
                            noded-lines
                            :copy-field [:label :group])

        [buildings paths] connected-by-label
        ]

    ;; everyone got connected to the road
    (test/is (= #{"road"}
                (set (map :group buildings))))
    
    (test/is (= (count paths)
                (+ 8 ;; connectors
                   2 ;; vertical split
                   10 ;; horizontal bits
                   )))))

(t/deftest test-trim-paths
  (let [input (geoio/read-from (io/resource "thermos_importer/connect-buildings.json"))

        {:keys [line-string polygon]}
        (group-by ::geoio/type (::geoio/features input))
        
        noded-lines (spatial/node-paths line-string)

        connected-by-label (spatial/add-connections
                            (::geoio/crs input)
                            polygon
                            noded-lines
                            :copy-field [:label :group])

        [buildings paths] connected-by-label

        trimmed (spatial/trim-dangling-paths paths buildings)
        ]
    (test/is (= (count trimmed)
                (+ 8 ;; connectors
                   8 ;; chunks of path

                   ;; this is 8 not 7 because we still have a bogus
                   ;; node in the middle of the topology now. It could
                   ;; get simplified out at some point.
                   )))))
