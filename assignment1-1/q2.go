package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"

	"assign1.1/common"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	sum := 0

	fmt.Println("Starting sum worker")
	for data := range nums {
		sum += data
	}

	fmt.Println("Sum worker finished with result ", sum)
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	f, err := os.Open(fileName)
	common.CheckError(err, "Openning file")

	numbers, err := readInts(f)
	resultCh := make(chan int, 5)
	rang := len(numbers) / num
	result := 0

	for i := 1; i <= num; i++ {
		fmt.Printf("i = %d\n", i)
		ch := make(chan int, 1)
		go sumWorker(ch, resultCh)

		var lastIdx, endIdx int
		if i == 1 {
			lastIdx = -1
		} else {
			lastIdx = (i - 1) * rang
		}
		if i >= num {
			endIdx = len(numbers) - 1
		} else {
			endIdx = i * rang
		}

		go func(numbers []int, lastIdx, endIdx, ith int, ch chan int) {
			fmt.Printf("Starting %dth sending routine (%d - %d)\n", ith, lastIdx+1, endIdx)
			for i := lastIdx + 1; i <= endIdx; i++ {
				ch <- numbers[i]
			}
			fmt.Printf("Finished %dth sending routine\n", ith)
			close(ch)
		}(numbers, lastIdx, endIdx, i, ch)
	}

	fmt.Println("Starting receiving summing result routine")

	for i := 0; i < num; i++ {
		result += <-resultCh
	}
	fmt.Println("Done summing results routine with", result)
	close(resultCh)

	return result
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
