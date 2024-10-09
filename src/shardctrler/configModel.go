package shardctrler

import (
	"container/heap"
	"log"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// --------------------------------------------------------------------- //

const magicNullGid = 0

type GroupShards struct {
	gid    int
	shards []int
}

// MinHeap implementation
type GroupShardsMinHeap []GroupShards

func (h GroupShardsMinHeap) Len() int { return len(h) }

// func (h GroupShardsMinHeap) Less(i, j int) bool { return len(h[i].shards) < len(h[j].shards) }
func (h GroupShardsMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h GroupShardsMinHeap) Less(i, j int) bool {
	if len(h[i].shards) == len(h[j].shards) {
		return h[i].gid < h[j].gid
	}
	return len(h[i].shards) < len(h[j].shards)
}
func (h *GroupShardsMinHeap) Push(x interface{}) {
	*h = append(*h, x.(GroupShards))
}

func (h *GroupShardsMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MaxHeap implementation
type GroupShardsMaxHeap []GroupShards

func (h GroupShardsMaxHeap) Len() int { return len(h) }

// func (h GroupShardsMaxHeap) Less(i, j int) bool { return len(h[i].shards) > len(h[j].shards) }
func (h GroupShardsMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h GroupShardsMaxHeap) Less(i, j int) bool {

	if len(h[i].shards) == len(h[j].shards) {
		return h[i].gid < h[j].gid
	}
	return len(h[i].shards) > len(h[j].shards)
}

func (h *GroupShardsMaxHeap) Push(x interface{}) {
	*h = append(*h, x.(GroupShards))
}

func (h *GroupShardsMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// DualHeap structure
type DualHeap struct {
	minHeap         *GroupShardsMinHeap
	maxHeap         *GroupShardsMaxHeap
	gidToMinHeapIdx map[int]int // Map of gid to minHeap index
	gidToMaxHeapIdx map[int]int
}

func NewDualHeap() *DualHeap {
	minH := &GroupShardsMinHeap{}
	maxH := &GroupShardsMaxHeap{}
	heap.Init(minH)
	heap.Init(maxH)
	return &DualHeap{
		minHeap: minH,
		maxHeap: maxH,
	}
}

func (dh *DualHeap) Add(g GroupShards) {
	heap.Push(dh.minHeap, g)
	heap.Push(dh.maxHeap, g)
}
func (dh *DualHeap) updateHeapIndices() {
	dh.gidToMinHeapIdx = make(map[int]int)
	dh.gidToMaxHeapIdx = make(map[int]int)

	for idx, groupShards := range *dh.minHeap {
		dh.gidToMinHeapIdx[groupShards.gid] = idx
	}

	for idx, groupShards := range *dh.maxHeap {
		dh.gidToMaxHeapIdx[groupShards.gid] = idx
	}
}

func (dh *DualHeap) UpdateMaxToMin(maxGid int, minGid int) {
	// Ensure the heap indices maps are up-to-date
	dh.updateHeapIndices()

	// Logic for handling the special case when maxGid is 0
	// if maxGid == 0 {
	// 	log.Print("srcGid==0")
	// 	if idx, exists := dh.gidToMaxHeapIdx[0]; exists {
	// 		srcElement := (*dh.maxHeap)[idx]
	// 		if len(srcElement.shards) > 0 {
	// 			// Move the first shard from max to min
	// 			tgtShard := srcElement.shards[0]
	// 			(*dh.maxHeap)[idx].shards = srcElement.shards[1:]
	// 			heap.Fix(dh.maxHeap, idx)
	//
	// 			// Update the corresponding element in the minHeap
	// 			if minIdx, minExists := dh.gidToMinHeapIdx[srcElement.gid]; minExists {
	// 				(*dh.minHeap)[minIdx].shards = srcElement.shards[1:]
	// 				heap.Fix(dh.minHeap, minIdx)
	// 			}
	//
	// 			dh.updateHeapIndices()
	// 			log.Printf("After src: MaxHeap: %v\nMinHeap: %v",
	// 				dh.maxHeap, dh.minHeap)
	// 			if minIdx, exists := dh.gidToMinHeapIdx[0]; exists {
	// 				tmpElement0 := heap.Remove(dh.minHeap, minIdx)
	// 				(*dh.minHeap)[0].shards = append((*dh.minHeap)[0].shards, tgtShard)
	// 				minElement := (*dh.minHeap)[0]
	// 				log.Printf("MinElement; %v", minElement)
	// 				minElementGid := minElement.gid
	// 				heap.Push(dh.minHeap, tmpElement0)
	// 				log.Printf("MinElementGid: %v", minElementGid)
	// 				maxIdx, _ := dh.gidToMaxHeapIdx[minElementGid]
	// 				(*dh.maxHeap)[maxIdx].shards = minElement.shards
	// 				heap.Fix(dh.maxHeap, maxIdx)
	// 				log.Printf("After dst: MaxHeap: %v\nMinHeap: %v",
	// 					dh.maxHeap, dh.minHeap)
	// 				// }
	// 			}
	// 		}
	// 	}
	// } else {
	// log.Print("srcGid != 0")
	// Handle the general case
	log.Printf("MaxGid: %v, MinGid: %v", maxGid, minGid)
	if maxIdx, exists := dh.gidToMaxHeapIdx[maxGid]; exists {
		maxElement := (*dh.maxHeap)[maxIdx]
		log.Printf("MaxElement: %v", maxElement)
		if len(maxElement.shards) > 0 {
			tgtShard := maxElement.shards[0]
			(*dh.maxHeap)[maxIdx].shards = maxElement.shards[1:]

			log.Printf("maxHeap: %v\nminHeap: %v", dh.maxHeap, dh.minHeap)
			heap.Fix(dh.maxHeap, maxIdx)

			dh.updateHeapIndices()
			// Update the corresponding element in the minHeap
			if minIdx, minExists := dh.gidToMinHeapIdx[maxGid]; minExists {
				(*dh.minHeap)[minIdx].shards = maxElement.shards[1:]
				log.Printf("maxHeap: %v\nminHeap: %v", dh.maxHeap, dh.minHeap)
				heap.Fix(dh.minHeap, minIdx)
			}
			dh.updateHeapIndices()
			// log.Printf("After src: MaxHeap: %v\nMinHeap: %v",
			// 	dh.maxHeap, dh.minHeap)
			//TODO: remove 0 before get min
			idx0, _ := dh.gidToMinHeapIdx[minGid]
			// tmpElement0 := heap.Remove(dh.minHeap, idx0)
			minElement := (*dh.minHeap)[idx0]
			log.Printf("MinElement: %v", minElement)
			(*dh.minHeap)[idx0].shards = append((*dh.minHeap)[idx0].shards, tgtShard)
			shardsToUpdate := append(minElement.shards, tgtShard)
			log.Printf("maxHeap: %v\nminHeap: %v", dh.maxHeap, dh.minHeap)
			// heap.Push(dh.minHeap, tmpElement0)
			// Update the corresponding element in the maxHeap
			if maxIdx, maxExists := dh.gidToMaxHeapIdx[minGid]; maxExists {

				(*dh.maxHeap)[maxIdx].shards = shardsToUpdate
				heap.Fix(dh.maxHeap, maxIdx)
				log.Printf("After dst: MaxHeap: %v\nMinHeap: %v",
					dh.maxHeap, dh.minHeap)
			}
		}
	}
}

// Update the indices after modifications
// dh.updateHeapIndices()
// }

func (dh *DualHeap) Update_MaxToMin(maxGid int, minGid int) {
	// check for gid0
	var tgtShard int
	if maxGid == 0 {
		for i, srcElement := range *dh.maxHeap {
			if srcElement.gid == 0 && len(srcElement.shards) > 0 {
				destGid := -1
				(*dh.maxHeap)[i].shards = srcElement.shards[1:]
				for j, element := range *dh.minHeap {
					if element.gid == 0 {
						heap.Remove(dh.minHeap, j)
						(*dh.minHeap)[0].shards = append((*dh.minHeap)[0].shards, srcElement.shards[0])
						destGid = (*dh.minHeap)[0].gid
						// log.Printf("DestGid: %v", destGid)
						element.shards = element.shards[1:]
						dh.minHeap.Push(element)
						break
					}
				}
				for j, element := range *dh.maxHeap {
					if element.gid == destGid {
						(*dh.maxHeap)[j].shards = append((*dh.maxHeap)[j].shards, srcElement.shards[0])
						break
					}
				}
				break

			}
		}
	} else {
		maxGid = (*dh.maxHeap)[0].gid
		tgtShard = (*dh.maxHeap)[0].shards[0]
		for i, minElement := range *dh.minHeap {
			if minElement.gid == maxGid {
				(*dh.minHeap)[i].shards = minElement.shards[1:]
				(*dh.maxHeap)[0].shards = minElement.shards[1:]
				break
			}
		}

		minGid := (*dh.minHeap)[0].gid
		(*dh.minHeap)[0].shards = append((*dh.minHeap)[0].shards, tgtShard)

		for i, maxElement := range *dh.maxHeap {
			if maxElement.gid == minGid {
				(*dh.maxHeap)[i].shards = append((*dh.maxHeap)[i].shards, tgtShard)
				break
			}
		}
	}
	heap.Init(dh.maxHeap)
	heap.Init(dh.minHeap)
}

func (dh *DualHeap) GetMax() GroupShards {
	dh.updateHeapIndices()
	idx0, _ := dh.gidToMaxHeapIdx[0]
	element := (*dh.maxHeap)[idx0]
	if element.gid == 0 && len(element.shards) > 0 {
		return element
	}
	rtn := heap.Pop(dh.maxHeap)
	heap.Push(dh.maxHeap, rtn)
	// return (*dh.maxHeap)[0]
	return rtn.(GroupShards)
}

func (dh *DualHeap) GetMin() GroupShards {

	tmpElement0 := heap.Pop(dh.minHeap).(GroupShards)
	if tmpElement0.gid == 0 && len(tmpElement0.shards) != 0 {
		rtn := heap.Pop(dh.minHeap).(GroupShards)
		heap.Push(dh.minHeap, rtn)
		return rtn
	}
	heap.Push(dh.minHeap, tmpElement0)
	return tmpElement0
	// element0 := (*dh.minHeap)[0]
	// if element0.gid == 0 && len(element0.shards) == 0 {
	// 	element := heap.Pop(dh.minHeap).(GroupShards)
	// 	rtn := (*dh.minHeap)[0]
	// 	heap.Push(dh.minHeap, element)
	// 	return rtn
	// }
	// return (*dh.minHeap)[0]
}

type ConfigModel struct {
	configs []Config // indexed by config num
	me      int      // for debug
}

func NewConfigModel(me int) *ConfigModel {
	cfg := ConfigModel{make([]Config, 1), me}
	cfg.configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for i := range cfg.configs[0].Shards {
		cfg.configs[0].Shards[i] = magicNullGid
	}
	return &cfg
}

func (cm *ConfigModel) getGroup2Shards(config *Config) map[int][]int {
	group2shard := map[int][]int{}
	for gid, _ := range config.Groups {
		group2shard[gid] = []int{}
	}
	group2shard[magicNullGid] = []int{}

	for shard, gid := range config.Shards {
		group2shard[gid] = append(group2shard[gid], shard)
	}
	return group2shard
}

func (cm *ConfigModel) reBalance(config *Config) {
	// special judge
	if len(config.Groups) == 0 { // if none group, init shards
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// Initialize DualHeap
	dualHeap := NewDualHeap()
	group2shard := cm.getGroup2Shards(config)
	for gid, shards := range group2shard {
		dualHeap.Add(GroupShards{gid: gid, shards: shards})
	}

	for {
		maxGroup := dualHeap.GetMax()
		minGroup := dualHeap.GetMin()

		// log.Printf("CM%v: maxGr: %v, minGr: %v\nmaxHeap: %v\nminHeap: %v",
		// cm.me, maxGroup, minGroup, dualHeap.maxHeap, dualHeap.minHeap)
		if maxGroup.gid != magicNullGid && len(maxGroup.shards)-len(minGroup.shards) <= 1 {
			break
		}

		// Update the heaps
		dualHeap.UpdateMaxToMin(maxGroup.gid, minGroup.gid) // Assumes this method updates both heaps correctly
		// log.Print("Finish updating")
		// time.Sleep(time.Duration(2) * time.Second)

	}
	for _, minElement := range *dualHeap.minHeap {
		for _, shard := range minElement.shards {
			config.Shards[shard] = minElement.gid

		}
	}

	log.Printf("CM%v new config: %v", cm.me, config)
}

// func (cm *ConfigModel) re_Balance(config *Config) {
// 	// special judge
// 	if len(config.Groups) == 0 { // if none group, init shards
// 		for i := range config.Shards {
// 			config.Shards[i] = 0
// 		}
// 		return
// 	}
//
// 	// 1 shard - 1 group, 1 group - n shards
// 	group2shard := cm.getGroup2Shards(config)
// 	for {
// 		src := cm.getMaxShards(group2shard)
// 		dst := cm.getMinShards(group2shard)
// 		if src != magicNullGid && len(group2shard[src])-len(group2shard[dst]) <= 1 {
// 			break
// 		}
//
// 		group2shard[dst] = append(group2shard[dst], group2shard[src][0])
// 		group2shard[src] = group2shard[src][1:]
// 	}
//
// 	// reset shard
// 	for gid, shards := range group2shard {
// 		for _, shard := range shards {
// 			config.Shards[shard] = gid
// 		}
// 	}
// }

func (cm *ConfigModel) deepCopy(config *Config) Config {
	ret := Config{
		Num:    config.Num,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for k, v := range config.Groups {
		ret.Groups[k] = v
	}
	for i := range config.Shards {
		ret.Shards[i] = config.Shards[i]
	}
	return ret
}

func (cm *ConfigModel) join(servers map[int][]string) Err {
	newConfig := cm.deepCopy(&cm.configs[len(cm.configs)-1])
	newConfig.Num = len(cm.configs)

	for gid, servers_iter := range servers {
		newServers := make([]string, len(servers_iter))
		copy(newServers, servers_iter)
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = newServers
		} else {
			newConfig.Groups[gid] = append(newConfig.Groups[gid], newServers...)
		}
	}

	cm.reBalance(&newConfig)
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) leave(GIDs []int) Err {
	newConfig := cm.deepCopy(&cm.configs[len(cm.configs)-1])
	newConfig.Num = len(cm.configs)

	group2shard := cm.getGroup2Shards(&newConfig)
	for _, gid := range GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := group2shard[gid]; ok {
			for _, shard := range shards {
				newConfig.Shards[shard] = magicNullGid
			}
		}
	}

	cm.reBalance(&newConfig)
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) move(shard int, gid int) Err {
	newConfig := cm.deepCopy(&cm.configs[len(cm.configs)-1])
	newConfig.Num = len(cm.configs)
	newConfig.Shards[shard] = gid
	cm.configs = append(cm.configs, newConfig)
	return OK
}

func (cm *ConfigModel) query(num int) (Config, Err) {
	if num < 0 || num >= len(cm.configs) {
		return cm.deepCopy(&cm.configs[len(cm.configs)-1]), OK
	}
	return cm.deepCopy(&cm.configs[num]), OK
}

func (cm *ConfigModel) isLegal(opType OpType) bool {
	switch opType {
	case OpJoin:
	case OpLeave:
	case OpMove:
	case OpQuery:
	default:
		return false
	}
	return true
}

func (cm *ConfigModel) Opt(cmd Op) (Config, Err) {
	switch cmd.Op {
	case OpJoin:
		err := cm.join(cmd.Servers)
		return Config{}, err
	case OpLeave:
		err := cm.leave(cmd.GIDs)
		return Config{}, err
	case OpMove:
		err := cm.move(cmd.Shard, cmd.GID)
		return Config{}, err
	case OpQuery:
		config, err := cm.query(cmd.Num)
		return config, err
	default:
		return Config{}, ErrOpt
	}
}
