package main

import (
	"context"
	"crypto/sha512"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/djherbis/times"

	_ "github.com/go-sql-driver/mysql"
)

type filemsg struct {
	host  string
	path  string
	name  string
	ext   string
	size  int64
	atime time.Time
	mtime time.Time
	ctime time.Time
	btime time.Time
	hash  string
}

type foldermsg struct {
	path  string
	name  string
	atime time.Time
	mtime time.Time
	ctime time.Time
	btime time.Time
}

const (
	username = "root"
	password = ""
	hostname = "127.0.0.1:3306"
	dbname   = "dupedb"
)

func gethash(cin chan filemsg, cout chan filemsg) {
	for {
		data := <-cin

		f, err := os.Open(data.path)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		h := sha512.New()
		if _, err := io.Copy(h, f); err != nil {
			log.Fatal(err)
		}

		data.hash = fmt.Sprintf("%x", h.Sum(nil))
		cout <- data
	}
}

func printfile(c chan filemsg, cout chan filemsg, display bool) {
	for {
		data := <-c
		if display {
			fmt.Printf("File : %s\n Name: %s\n Extension: %s\n Size: %v\n Hash: %s\n ATIME: %s\n CTIME: %s\n MTIME: %s\n BTIME: %s\n\n", data.path, data.name, data.ext, data.size, data.hash, data.atime.Format("01-02-2006 15:04:05"), data.ctime.Format("01-02-2006 15:04:05"), data.mtime.Format("01-02-2006 15:04:05"), data.btime.Format("01-02-2006 15:04:05"))
		}
		cout <- data
	}
}

func printfolder(c chan foldermsg, display bool) {
	for {
		data := <-c
		if display {
			fmt.Printf("Folder : %s\n", data.name)
		}
	}
}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return nil, err
	}

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return nil, err
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return nil, err
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", dbname)
	return db, nil
}

func createFileTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS files(ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, host varchar(32) not null, path varchar(1023) not null, name varchar(255) not null, extension varchar(32), hash varchar(255), size BIGINT, created DATETIME, modified DATETIME, accessed DATETIME, birth DATETIME);`

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating files table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
		return err
	}
	log.Printf("Rows affected when creating table: %d", rows)
	return nil
}

func insert(db *sql.DB, fl filemsg) error {
	query := "INSERT INTO files(host, path, name, extension, hash, size, created, modified, accessed, birth) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, fl.host, fl.path, fl.name, fl.ext, fl.hash, fl.size, fl.ctime, fl.mtime, fl.atime, fl.btime)
	if err != nil {
		log.Printf("Error %s when inserting row into files table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d file created: %s", rows, fl.name)
	return nil
}

func savefile(c chan filemsg, save bool, display bool) {

	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()
	log.Printf("Successfully connected to database")

	for {
		data := <-c

		if save {
			err := insert(db, data)
			if err != nil {
				log.Printf("Insert product failed with error %s", err)
				return
			}

		}
		if display {
			fmt.Printf("FileDB : %s\n\n", data.path)
		}
	}
}

func main() {
	fmt.Println("DupeFinder Starting Up")

	hostname, err := os.Hostname()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()
	log.Printf("Successfully connected to database")

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return
	}
	defer db.Close()

	log.Printf("Successfully connected to database")
	err = createFileTable(db)
	if err != nil {
		log.Printf("Create files table failed with error %s", err)
		return
	}

	fmt.Println("DupeFinder Completed Setting Up Database")

	var inputVar string
	var hostnameVar string

	flag.StringVar(&inputVar, "path", ".", "path to scan")
	flag.StringVar(&hostnameVar, "host", hostname, "path to scan")

	flag.Parse()

	folderchann := make(chan foldermsg)
	filechann := make(chan filemsg)
	resultschann := make(chan filemsg)
	hashchann := make(chan filemsg, 3)

	go printfolder(folderchann, true)
	go gethash(hashchann, filechann)
	go gethash(hashchann, filechann)
	go gethash(hashchann, filechann)
	go printfile(filechann, resultschann, false)
	go savefile(resultschann, true, false)

	err = filepath.Walk(inputVar, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err.Error())
			return err
		}

		abs_fname, err := filepath.Abs(path)
		if err != nil {
			log.Fatal(err.Error())
			return err
		}

		t, err := times.Stat(abs_fname)
		if err != nil {
			log.Fatal(err.Error())
			return err
		}

		ext := filepath.Ext(abs_fname)

		atime := t.AccessTime()
		mtime := t.ModTime()
		var ctime time.Time
		var btime time.Time
		if t.HasChangeTime() {
			ctime = t.ChangeTime()
		} else {
			ctime = mtime
		}

		if t.HasBirthTime() {
			btime = t.BirthTime()
		} else {
			btime = mtime
		}

		if info.IsDir() {
			msg := foldermsg{abs_fname, info.Name(), atime, mtime, ctime, btime}
			folderchann <- msg
		} else {
			msg := filemsg{hostnameVar, abs_fname, info.Name(), ext, info.Size(), atime, mtime, ctime, btime, ""}
			hashchann <- msg
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
