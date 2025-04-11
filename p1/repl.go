package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func print_prompt() {
	fmt.Print("db > ")
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		print_prompt()
		input_w_delim, err := reader.ReadString('\n')
		input := input_w_delim[:len(input_w_delim)-2]
		if err != nil {
			if err == io.EOF {
				break
			}
			break
		}

		if strings.Compare(input, ".exit") == 0 {
			break
		} else {
			fmt.Printf("Unrecognized command '%s'.\n", input)
		}

	}

}
