package main

import (
	"fmt"
)

/**
 * Note: 类名、方法名、参数名已经指定，请勿修改
 *
 *
 *
 * @param nums1 int整型 一维数组
 * @param nums1Len int nums1数组长度
 * @param m int整型
 * @param nums2 int整型 一维数组
 * @param nums2Len int nums2数组长度
 * @param n int整型
 * @return 4
 * @return int整型一维数组
 */
func merge(nums1 []int, m int, nums2 []int, n int) []int {
	// write code here
	returnSize := m + n
	result := make([]int, returnSize)
	i, j, k := 0, 0, 0
	for i < m || j < n {
		if i == m {
			for j < n {
				result[k] = nums2[j]
				j++
				k++
			}
			break
		}
		if j == n {
			for i < m {
				result[k] = nums1[i]
				i++
				k++
			}
			break
		}
		if nums1[i] <= nums2[j] {
			result[k] = nums1[i]
			i++
		} else {
			result[k] = nums2[j]
			j++
		}
		k++
	}

	return result
}

func main() {
	nums1 := []int{1, 2, 7, 0, 0, 0}
	nums2 := []int{0, 5, 6}
	m := 3
	n := 3
	fmt.Println(merge(nums1, m, nums2, n))
}
