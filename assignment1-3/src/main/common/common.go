package common

import (
	"regexp"
	"strings"
)

func PreprocessWord(text string) []string {
	result := make([]string, 0)
	words := strings.Fields(text)
	re := regexp.MustCompile(`[^\\\^\$\.\|\?\*\+\-\[\]\{\}\(\)~!#@%&;:'"><,_=/0123456789]+`)

	for _, word := range words {
		arr := re.FindAllString(word, -1)
		// for _, x := range arr {
		// 	x = strings.ToLower(x)
		// }
		result = append(result, arr...)
	}

	return result
}
