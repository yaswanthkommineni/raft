package raft;
import {
	"math/rand/v2"
	"time"
}

func RandomDuration(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.IntN(int(max - min) + 1));
}