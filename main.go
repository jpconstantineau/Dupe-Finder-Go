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

		//data.hash = data.hash
		data.hash = fmt.Sprintf("%x", h.Sum(nil))
		cout <- data
	}
}

func printfile(c chan filemsg) {
	for {
		data := <-c
		fmt.Printf("File : %s\n Name: %s\n Extension: %s\n Size: %v\n Hash: %s\n ATIME: %s\n CTIME: %s\n MTIME: %s\n BTIME: %s\n\n", data.path, data.name, data.ext, data.size, data.hash, data.atime.Format("01-02-2006 15:04:05"), data.ctime.Format("01-02-2006 15:04:05"), data.mtime.Format("01-02-2006 15:04:05"), data.btime.Format("01-02-2006 15:04:05"))
	}
}

func printfolder(c chan foldermsg) {
	for {
		data := <-c
		fmt.Printf("Folder : %s\n", data.name)
	}
}

const (
	username = "root"
	password = ""
	hostname = "127.0.0.1:3306"
	dbname   = "dupedb"
)

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return nil, err
	}
	//defer db.Close()

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
	//defer db.Close()

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
	query := `CREATE TABLE IF NOT EXISTS files(file_id int primary key auto_increment, product_name text, 
        product_price int, created_at datetime default CURRENT_TIMESTAMP, updated_at datetime default CURRENT_TIMESTAMP)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating product table", err)
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

func main() {
	fmt.Println("DupeFinder Starting Up")

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

	flag.StringVar(&inputVar, "path", ".", "path to scan")
	flag.Parse()

	folderchann := make(chan foldermsg)
	filechann := make(chan filemsg)
	hashchann := make(chan filemsg, 3)

	go printfolder(folderchann)
	go gethash(hashchann, filechann)
	go gethash(hashchann, filechann)
	go gethash(hashchann, filechann)
	go printfile(filechann)

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
			msg := filemsg{abs_fname, info.Name(), ext, info.Size(), atime, mtime, ctime, btime, ""}
			hashchann <- msg
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
