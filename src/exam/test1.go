package main

import "fmt"

/**
 * Note: 类名、方法名、参数名已经指定，请勿修改
 *
 *
 * 根据价格列表和当前点券数，计算出能买到的最多英雄
 * @param costs int整型 一维数组 英雄点券价格列表
 * @param coins int整型  拥有的点券
 * @return int整型一维数组
 */
func solution(costs []int, coins int) []int {
	remain := coins

	// stack := make([]int, len(costs))
	// sLen := 0
	//fmt.Println("test1")
	hp := NewHeap(len(costs))
	for _, cost := range costs {
		//fmt.Println("test2")
		if remain >= cost {
			remain -= cost
			hp.insert(cost)
		} else {
			topVal := hp.peek()
			if topVal > cost {
				remain += topVal
				remain -= cost
				hp.replaceMax(cost)
			}
		}
	}
	//fmt.Println(hp.arr[:hp.size])
	//fmt.Println(hp.stack[:hp.size])
	return hp.stack[:hp.size]
}

type entry struct {
	val int
	idx int
}
type heap struct {
	arr   []entry
	stack []int
	size  int
}

func NewHeap(n int) *heap {
	return &heap{make([]entry, n), make([]int, n), 0}
}

func (p *heap) insert(val int) {
	p.stack[p.size] = val
	p.arr[p.size] = entry{val, p.size}
	p.size++

	cur := p.size - 1
	for cur > 0 {
		//fmt.Println("test3")
		parent := (cur - 1) / 2
		if p.arr[cur].val > p.arr[parent].val {
			p.arr[cur], p.arr[parent] = p.arr[parent], p.arr[cur]
			cur = parent
			continue
		}
		break
	}
}

func (p *heap) replaceMax(val int) {
	p.stack[p.arr[0].idx] = val
	p.arr[0] = entry{val, p.arr[0].idx}
	p.heapify(0)
}

func (p *heap) peek() int {
	return p.arr[0].val
}

func (p *heap) heapify(idx int) {
	left := 2*idx + 1
	right := 2*idx + 2
	largest := idx
	if left < p.size && p.arr[left].val > p.arr[largest].val {
		largest = left
	}
	if right < p.size && p.arr[right].val > p.arr[largest].val {
		largest = right
	}
	if largest != idx {
		p.arr[idx], p.arr[largest] = p.arr[largest], p.arr[idx]
		p.heapify(largest)
	}
}

func main() {
	costs := []int{15, 10, 11, 10}
	coins := 5
	//solution(costs, coins)
	fmt.Println(solution(costs, coins))
}
