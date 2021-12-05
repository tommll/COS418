package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"assign1.1/common"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
	data, err := ioutil.ReadFile(path)
	common.CheckError(err, "Openning file")

	words := make([]string, 0)
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		words = append(words, strings.Split(line, " ")...)
	}

	memo := make(map[string]int)

	for _, word := range words {
		word = strings.ToLower(preprocess(word))

		if _, ok := memo[word]; ok {
			memo[word]++
			continue
		}

		if len(word) >= charThreshold {
			if _, ok := memo[word]; !ok {
				memo[word] = 1
			}
		}

	}

	result := make([]WordCount, 0)

	for word, count := range memo {
		result = append(result, WordCount{
			Word:  word,
			Count: count})
	}

	sortWordCounts(result)
	topKresult := make([]WordCount, 0)

	for i := 0; i < numWords; i++ {
		topKresult = append(topKresult, result[i])
	}

	return topKresult
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}

func preprocess(word string) string {
	word = strings.TrimSpace(word)
	word = strings.Replace(word, "'", "", 1)
	word = strings.Replace(word, "?", "", 1)
	word = strings.Replace(word, ".", "", 1)
	word = strings.Replace(word, ")", "", 1)
	word = strings.Replace(word, "(", "", 1)
	word = strings.Replace(word, "!", "", 1)
	word = strings.Replace(word, ",", "", 1)
	word = strings.Replace(word, "[", "", 1)
	word = strings.Replace(word, "]", "", 1)
	word = strings.Replace(word, "\\", "", 1)
	word = strings.Replace(word, "{", "", 1)
	word = strings.Replace(word, "}", "", 1)
	word = strings.Replace(word, "<", "", 1)
	word = strings.Replace(word, ">", "", 1)
	word = strings.Replace(word, ":", "", 1)
	word = strings.Replace(word, ";", "", 1)
	return word
}
