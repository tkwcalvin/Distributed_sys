package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func main() {

	arr := make([]int, 0)

	var input string
	fmt.Scan(&input)

	numbers := strings.Split(input, ",")
	for _, num := range numbers {
		num, _ := strconv.Atoi(num)
		arr = append(arr, num)
	}

	fmt.Println(arr)

	target := arr[0]

	// sort the remaining array

	sort.Ints(arr[1:])
	fmt.Println(arr)

	result := make([]string, 0)
	for i := 1; i < len(arr); i++ {
		j, k := i+1, len(arr)-1
		for j < k {
			if arr[i]+arr[j]+arr[k] == target {

				result = append(result, fmt.Sprintf("%d %d %d", arr[i], arr[j], arr[k]))
				break
			}
			if arr[i]+arr[j]+arr[k] < target {
				j++
			} else {
				k--
			}
		}
	}
	// print the result, 3 tuple seperate by comma
	// sort the string by alphabetical order
	sort.Strings(result)
	fmt.Println(strings.Join(result, ","))
}
