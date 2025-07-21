package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
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
	PREPARE_STRING_TOO_LONG
	PREPARE_NEGATIVE_ID
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)
const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)
const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_UNKNOWN
	EXECUTE_TABLE_FULL
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
		log.Println("INFO: row_slot: Page was nil! allocating")
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
		log.Println("INFO: do_meta_command: .exit:\nExiting the program...")
		os.Exit(0)
	}
	if strings.Compare(input, ".help") == 0 {
		log.Println("INFO: do_meta_command: .help:\nShowing help message...")
		fmt.Println("Available commands:")
		fmt.Println("\t.exit - Exit the program")
		fmt.Println("\t.help - Show this help message")
		fmt.Println("\tinsert <id> <username> <email> - Insert a new row")
		fmt.Println("\tselect - Select all rows")
		return META_COMMAND_SUCCESS
	}
	log.Printf("WARNING: do_meta_command: Unrecognized command %s\n", input)
	return META_COMMAND_UNRECOGNIZED_COMMAND

}

func prepare_statement(input string, statement *Statement) PrepareCommandState {
	if len(input) >= 6 && strings.Compare(input[:6], "insert") == 0 {
		statement.st = STATEMENT_INSERT
		splits := strings.SplitN(input, " ", 4)
		if len(splits) != 4 {
			log.Printf("WARNING: prepare_statement: splits = %v, expected 4 parts", splits)
			return PREPARE_SYNTAX_ERROR
		}
		id, err := strconv.Atoi(splits[1])
		if err != nil {
			log.Printf("WARNING: prepare_statement: id = %v is not numeric", splits[1])
			return PREPARE_SYNTAX_ERROR
		} else if id < 0 {
			log.Printf("WARNING: prepare_statement: id = %d is negative", id)
			return PREPARE_NEGATIVE_ID
		}

		if len(splits[2]) > COLUMN_USERNAME_SIZE {
			log.Printf("WARNING: prepare_statement: username %s is too long, max size is %d", splits[2], COLUMN_USERNAME_SIZE)
			return PREPARE_STRING_TOO_LONG
		} else if len(splits[3]) > COLUMN_EMAIL_SIZE {
			log.Printf("WARNING: prepare_statement: email %s is too long, max size is %d", splits[3], COLUMN_EMAIL_SIZE)
			return PREPARE_STRING_TOO_LONG
		}

		statement.row_to_insert.id = uint32(id)
		copy(statement.row_to_insert.username[:COLUMN_USERNAME_SIZE], splits[2])
		copy(statement.row_to_insert.email[:COLUMN_EMAIL_SIZE], splits[3])

		log.Printf("INFO: prepare_statement: insert statement\n")
		return PREPARE_COMMAND_SUCCESS

	} else if len(input) >= 6 && strings.Compare(input, "select") == 0 {
		statement.st = STATEMENT_SELECT
		log.Println("INFO: prepare_statement: select statement")
		return PREPARE_COMMAND_SUCCESS
	} else {
		log.Printf("WARNING: prepare_statement: Unrecognized command %s\n", input)
		return PREPARE_UNRECOGNIZED_STATEMENT
	}
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	if table.num_rows >= TABLE_MAX_ROWS {
		log.Println("ERROR: execute_insert: Table full")
		return EXECUTE_TABLE_FULL
	}
	serialize_row(&statement.row_to_insert, row_slot(table, table.num_rows))
	table.num_rows += 1

	if table.num_rows > TABLE_MAX_ROWS {
		log.Println("WARNING: execute_insert: Table full after insert")
	}
	log.Printf("INFO: execute_insert: Inserted row id = %d, username = %s, email = %s\n", statement.row_to_insert.id, string(statement.row_to_insert.username[:]), string(statement.row_to_insert.email[:]))
	return EXECUTE_SUCCESS
}

func execute_select(st *Statement, table *Table) ExecuteResult {
	row := &Row{}
	if table.num_rows == 0 {
		log.Println("INFO: execute_select: No rows to select")
		return EXECUTE_SUCCESS
	}
	for i := 0; i < int(table.num_rows); i++ {
		deserialize_row(row_slot(table, uint32(i)), row)
		trimmedUsername := strings.TrimRight(string(row.username[:]), "\x00")
		trimmedEmail := strings.TrimRight(string(row.email[:]), "\x00")
		fmt.Printf("(%d %s %s)\n", row.id, trimmedUsername, trimmedEmail)
	}
	log.Printf("INFO: execute_select: Selected %d rows\n", table.num_rows)
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
	debugPtr := flag.Bool("debug", false, "Enable debug mode")
	flag.Parse()
	if *debugPtr {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(io.Discard) // Disable debug output
	}
	log.Printf("INFO: init: ID_SIZE = %d, USERNAME_SIZE = %d, EMAIL_SIZE = %d\n", ID_SIZE, USERNAME_SIZE, EMAIL_SIZE)
	log.Printf("INFO: init: ID_OFFSET = %d, USERNAME_OFFSET = %d, EMAIL_OFFSET = %d\n", ID_OFFSET, USERNAME_OFFSET, EMAIL_OFFSET)
	log.Printf("INFO: init: ROW_SIZE = %d, ROWS_PER_PAGE = %d, TABLE_MAX_ROWS = %d\n", ROW_SIZE, ROWS_PER_PAGE, TABLE_MAX_ROWS)

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
				fmt.Printf("Unrecognized command %s\n", input)
				continue
			}
		}
		statement := &Statement{}
		switch prepare_statement(input, statement) {
		case PREPARE_COMMAND_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement. Following are the valid commands:")
			do_meta_command(".help")
			continue
		case PREPARE_STRING_TOO_LONG:
			fmt.Printf("String is too long. Maximum size is %d for username and %d for email\n", COLUMN_USERNAME_SIZE, COLUMN_EMAIL_SIZE)
			continue
		case PREPARE_NEGATIVE_ID:
			fmt.Println("ID must be a positive integer")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of %s. following are the valid commands:\n", input)
			do_meta_command(".help")
			continue
		}
		// var statement Statement
		// statement.st = ss

		switch execute_statement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Printf("Executed\n")
			if *debugPtr {
				log.Printf("INFO: execute_statement: Executed. Table now has %d rows\n", table.num_rows)
			}
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full")
			if *debugPtr {
				log.Println("ERROR: execute_statement: Table full")
			}
		case EXECUTE_UNKNOWN:
			fmt.Println("Error: Uknown error")
		}

	}

}
