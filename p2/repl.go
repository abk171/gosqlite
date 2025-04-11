package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type MetaCommandResult int
type PrepareCommandState int
type StatementType int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)
const (
	PREPARE_COMMAND_SUCCESS PrepareCommandState = iota
	PREPARE_COMMAND_UNRECOGNIZED_COMMAND
)
const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

func print_prompt() {
	fmt.Print("db > ")
}

func do_meta_command(input string) MetaCommandResult {
	if strings.Compare(input, ".exit") == 0 {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND

}

func prepare_statement(input string, statement *StatementType) PrepareCommandState {
	if strings.Compare(input[:6], "insert") == 0 {
		*statement = STATEMENT_INSERT
		return PREPARE_COMMAND_SUCCESS
	} else if strings.Compare(input, "select") == 0 {
		*statement = STATEMENT_SELECT
		return PREPARE_COMMAND_SUCCESS
	} else {
		return PREPARE_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func execute_statement(statement StatementType) {
	switch statement {
	case STATEMENT_INSERT:
		fmt.Printf("This is where we would do an insert\n")
	case STATEMENT_SELECT:
		fmt.Printf("This is where we would do a select\n")
	}

}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		print_prompt()
		input_w_delim, err := reader.ReadString('\n')
		input := input_w_delim[:len(input_w_delim)-1]
		if err != nil {
			if err == io.EOF {
				break
			}
			// better error handling
			break
		}

		if input[0] == '.' {
			switch do_meta_command(input) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'.\n", input)
				continue
			}
		}
		var ss StatementType
		switch prepare_statement(input, &ss) {
		case PREPARE_COMMAND_SUCCESS:
			break
		case PREPARE_COMMAND_UNRECOGNIZED_COMMAND:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
			continue
		}

		execute_statement(ss)
		fmt.Printf("Executed\n")

	}

}
