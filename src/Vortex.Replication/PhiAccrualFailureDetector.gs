// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Replication

import System
import System.Collections.Generic

// Implementation of the Hayashibara et al. Phi Accrual Failure Detector algorithm.
// Instead of binary up/down states, it outputs a continuous suspicion metric (Phi).
class PhiAccrualFailureDetector {
    var MinStdDeviationMs float64 = 100.0
    var MaxSampleSize int32 = 1000
    var Intervals List[float64] = List[float64]()
    var LastHeartbeatTimestampMs int64 = -1
    var SyncLock Object = Object()

    // Records the arrival of a node heartbeat.
    func Heartbeat(timestampMs int64) {
        lock SyncLock {
            if LastHeartbeatTimestampMs > 0 {
                var delta float64 = float64(timestampMs - LastHeartbeatTimestampMs)
                if delta > 0.0 {
                    Intervals.Add(delta)
                    if Intervals.Count > MaxSampleSize {
                        Intervals.RemoveAt(0)
                    }
                }
            }
            LastHeartbeatTimestampMs = timestampMs
        }
    }

    // Calculates the current Phi suspicion metric for the monitored node.
    func Phi(currentTimestampMs int64) float64 {
        lock SyncLock {
            if LastHeartbeatTimestampMs <= 0 || Intervals.Count < 2 {
                return 0.0
            }

            var elapsed float64 = float64(currentTimestampMs - LastHeartbeatTimestampMs)
            if elapsed <= 0.0 {
                return 0.0
            }

            // Calculate mean of historical intervals
            var sum float64 = 0.0
            for i in 0 ... Intervals.Count {
                sum = sum + Intervals[i]
            }
            var mean float64 = sum / float64(Intervals.Count)

            // Calculate variance and standard deviation
            var varianceSum float64 = 0.0
            for i in 0 ... Intervals.Count {
                var diff float64 = Intervals[i] - mean
                varianceSum = varianceSum + (diff * diff)
            }
            var variance float64 = varianceSum / float64(Intervals.Count)
            var stdDev float64 = Math.Sqrt(variance)
            if stdDev < MinStdDeviationMs {
                stdDev = MinStdDeviationMs
            }

            // Normal distribution cumulative probability P(X >= elapsed)
            var y float64 = (elapsed - mean) / stdDev
            var e float64 = Math.Exp(-y * (1.5976 + 0.070566 * y * y))
            var pLater float64 = 0.0
            if elapsed > mean {
                pLater = e / (1.0 + e)
            } else {
                pLater = 1.0 - (1.0 / (1.0 + e))
            }

            if pLater <= 0.0000000001 {
                return 16.0
            }

            return -Math.Log10(pLater)
        }
    }

    // Determines whether the node is considered alive based on suspicion threshold.
    func IsAvailable(currentTimestampMs int64, threshold float64) bool {
        return Phi(currentTimestampMs) < threshold
    }
}
