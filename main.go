package main

/*
#cgo LDFLAGS: -lsqlite3
#include <fcntl.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

sqlite3 *db;

int open_sqlite(char *db_file_name) {
  char *err;

  // Open an in-memory database to use as a handle for loading the memvfs extension
  if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
    fprintf(stderr, "open :memory: %s\n", sqlite3_errmsg(db));
    return 0;
  }

  sqlite3_enable_load_extension(db, 1);
  if (sqlite3_load_extension(db, "./memvfs", NULL, &err) != SQLITE_OK) {
    fprintf(stderr, "load extension: %s\n", err);
    return 0;
  }

  sqlite3_close(db);

  int fd = open(db_file_name, O_RDONLY);
  if (fd < 0) {
    perror("open");
    return 0;
  }
  struct stat s;
  if (fstat(fd, &s) < 0) {
    perror("fstat");
    return 0;
  }
  void *memdb = sqlite3_malloc64(s.st_size);

  if (read(fd, memdb, s.st_size) != s.st_size) {
    perror("read");
    return 0;
  }
  unsigned char ch_val = 1;

  void *position1 = (char *)memdb + 18;
  void *position2 = (char *)memdb + 19;
  memcpy(position1, &ch_val, 1);
  memcpy(position2, &ch_val, 1);

  close(fd);

  char *memuri = sqlite3_mprintf("file:whatever?ptr=0x%p&sz=%lld&freeonclose=1&max=65536",
                                 memdb, (long long)s.st_size);

  printf("Trying to open '%s'\n", memuri);
  if (sqlite3_open_v2(memuri, &db, SQLITE_OPEN_WAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI,
                      "memvfs") != SQLITE_OK) {
    fprintf(stderr, "open memvfs: %s\n", sqlite3_errmsg(db));
    return 0;
  }
  sqlite3_free(memuri);
   return 1;
}
//Send query to sqlite
int query_sqlite(char *query) {

  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) !=
      SQLITE_OK) {
    fprintf(stderr, "prepare: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 0;
  }

  for (int rc = sqlite3_step(stmt); rc == SQLITE_ROW; rc = sqlite3_step(stmt)) {
    printf("%s\n", sqlite3_column_text(stmt, 0));
  }

  sqlite3_finalize(stmt);

  return 1;
}
//Close sqlite connection
void close_sqlite() {
  sqlite3_close(db);
}

*/
import "C"
import (
	"errors"
	"log"
	"unsafe"
)

//Reading sqlite database from file into memory for further reading.
func sqlite_read_to_mem(FileName string) (bool, error) {
	cStrFileName := C.CString(FileName)
	if int(C.open_sqlite(cStrFileName)) != 1 {
		return false, errors.New("Wrong read file")
	}
	return true, nil
}

//Send SQL request to sqlite database.
func sqlite_query(Query string) ([]map[string]interface{}, error) {
	cStrQuery := C.CString(Query)
	// C.query_sqlite(cStrQuery)

	var stmt *C.sqlite3_stmt
	if C.sqlite3_prepare_v2(C.db, cStrQuery, -1, &stmt, nil) != C.SQLITE_OK {
		log.Println(C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_errmsg(C.db)))))
		C.sqlite3_close(C.db)
		return nil, errors.New(C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_errmsg(C.db)))))
	}

	var respone []map[string]interface{}

	for {
		if rc := C.sqlite3_step(stmt); rc == C.SQLITE_ROW {
			count_column := C.sqlite3_data_count(stmt)
			row_map := make(map[string]interface{})
			for i := 0; i < int(count_column); i++ {
				name_column := C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_name(stmt, C.int(i)))))
				switch tmpType := C.sqlite3_column_type(stmt, C.int(i)); tmpType {
				case C.SQLITE_INTEGER:
					row_map[name_column] = C.sqlite3_column_int(stmt, C.int(i))
				case C.SQLITE_FLOAT:
					row_map[name_column] = C.sqlite3_column_double(stmt, C.int(i))
				case C.SQLITE_TEXT:
					row_map[name_column] = C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt, C.int(i)))))
				case C.SQLITE_NULL:
					row_map[name_column] = C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt, C.int(i)))))
				}
			}
			respone = append(respone, row_map)
		} else {
			if rc == C.SQLITE_DONE {
				C.sqlite3_finalize(stmt)
				return respone, nil
			} else {
				return nil, errors.New("Error request")
			}
		}
	}
}

//Close connection sqlite database.
func sqlite_close() {
	C.close_sqlite()
}

func test() {
	sqlite_read_to_mem("foo.db")
	query := "SELECT b FROM test"
	res, err := sqlite_query(query)
	if err != nil {
		log.Println(err)
	}
	log.Println(res)
	log.Println("-----------------------")

	// cquery := C.CString(query)
	// C.query_sqlite(cquery)
}

func main() {
	test()
}
