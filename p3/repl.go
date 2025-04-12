package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const COLUMN_USERNAME_SIZE = 32
const COLUMN_EMAIL_SIZE = 256

const PAGE_SIZE = 4096
const TABLE_MAX_PAGES = 100

type MetaCommandResult int
type PrepareCommandState int
type StatementType int
type ExecuteResult int

type Row struct {
	id       uint32
	username [COLUMN_USERNAME_SIZE]byte
	email    [COLUMN_EMAIL_SIZE]byte
}

type Statement struct {
	row_to_insert Row
	st            StatementType
}

type Page struct {
	data [PAGE_SIZE]byte
}

type Table struct {
	num_rows uint32
	pages    [TABLE_MAX_PAGES]*Page
}

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)
const (
	PREPARE_COMMAND_SUCCESS PrepareCommandState = iota
	PREPARE_SYNTAX_ERROR
	PREPARE_COMMAND_UNRECOGNIZED_COMMAND
)
const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)
const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_UNKNOWN
	EXECULTE_TABLE_FULL
)

var (
	ID_SIZE       uint32
	USERNAME_SIZE uint32
	EMAIL_SIZE    uint32
	ROW_SIZE      uint32

	ID_OFFSET       uint32
	USERNAME_OFFSET uint32
	EMAIL_OFFSET    uint32

	ROWS_PER_PAGE  uint32
	TABLE_MAX_ROWS uint32
)

func init() {
	ID_SIZE = uint32(unsafe.Sizeof(Row{}.id))
	USERNAME_SIZE = uint32(unsafe.Sizeof(Row{}.username))
	EMAIL_SIZE = uint32(unsafe.Sizeof(Row{}.email))

	ID_OFFSET = 0
	USERNAME_OFFSET = uint32(unsafe.Offsetof(Row{}.username))
	EMAIL_OFFSET = uint32(unsafe.Offsetof(Row{}.email))

	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

	ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE
}

func serialize_row(row *Row, destination []byte) {
	binary.LittleEndian.PutUint32(destination[ID_OFFSET:ID_OFFSET+ID_SIZE], row.id)
	copy(destination[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE], row.username[:])
	copy(destination[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], row.email[:])
}

func deserialize_row(source []byte, row *Row) {
	row.id = binary.LittleEndian.Uint32(source[ID_OFFSET : ID_OFFSET+ID_SIZE])
	copy(row.username[:], source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])
	copy(row.email[:], source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])
}

func row_slot(table *Table, row_num uint32) []byte {
	page_num := row_num / ROWS_PER_PAGE
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	page := table.pages[page_num]

	if page == nil {
		fmt.Println("Page was nil! allocating")
		page = &Page{} // allocate page
		table.pages[page_num] = page
	}

	return page.data[byte_offset : byte_offset+ROW_SIZE]

}

func print_prompt() {
	fmt.Print("db > ")
}

func do_meta_command(input string) MetaCommandResult {
	if strings.Compare(input, ".exit") == 0 {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND

}

func prepare_statement(input string, statement *Statement) PrepareCommandState {
	if strings.Compare(input[:6], "insert") == 0 {
		statement.st = STATEMENT_INSERT
		splits := strings.SplitN(input, " ", 4)
		if len(splits) != 4 || splits[0] != "insert" {
			panic("Bad input")
		}
		id, err := strconv.Atoi(splits[1])
		if err != nil {
			panic(err)
		}
		statement.row_to_insert.id = uint32(id)
		copy(statement.row_to_insert.username[:], splits[2])
		copy(statement.row_to_insert.email[:], splits[3])

		fmt.Printf("row.id = %d\nrow.username = %s\nrow.email = %s\n", statement.row_to_insert.id, statement.row_to_insert.username, statement.row_to_insert.email)
		return PREPARE_COMMAND_SUCCESS

	} else if strings.Compare(input, "select") == 0 {
		statement.st = STATEMENT_SELECT
		return PREPARE_COMMAND_SUCCESS
	} else {
		return PREPARE_COMMAND_UNRECOGNIZED_COMMAND
	}
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	if table.num_rows >= TABLE_MAX_ROWS {
		return EXECULTE_TABLE_FULL
	}
	serialize_row(&statement.row_to_insert, row_slot(table, table.num_rows))
	table.num_rows += 1
	return EXECUTE_SUCCESS
}

func execute_select(st *Statement, table *Table) ExecuteResult {
	row := &Row{}
	for i := 0; i < int(table.num_rows); i++ {
		deserialize_row(row_slot(table, uint32(i)), row)
		fmt.Printf("(%d %s %s)\n", row.id, string(row.username[:]), string(row.email[:]))
	}
	return EXECUTE_SUCCESS
}

func execute_statement(statement *Statement, table *Table) ExecuteResult {
	switch statement.st {
	case STATEMENT_INSERT:
		return execute_insert(statement, table)
	case STATEMENT_SELECT:
		return execute_select(statement, table)
	}
	return EXECUTE_UNKNOWN
}

func main() {
	table := &Table{}
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
		statement := &Statement{}
		switch prepare_statement(input, statement) {
		case PREPARE_COMMAND_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_COMMAND_UNRECOGNIZED_COMMAND:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
			continue
		}
		// var statement Statement
		// statement.st = ss

		switch execute_statement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Printf("Executed\n")
		case EXECULTE_TABLE_FULL:
			fmt.Println("Error: Table full")
		case EXECUTE_UNKNOWN:
			fmt.Println("Error: Uknown error")
		}

	}

}
