package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
  "time"
	"strconv"
	"strings"
  "io/ioutil"
	//"testing"
)

const (
    CONN_HOST = "localhost"
    CONN_PORT = "8080"
    CONN_TYPE = "tcp"
)

type FileInfo struct{
  size,version,lifetime int
}

var m map[string]FileInfo

var filetokens map[string] chan bool

func main(){
  serverMain();
}

func serverMain() {
   m = make(map[string]FileInfo)
   filetokens = make(map[string] chan bool)
    // Listen for incoming connections.
    l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
    if err != nil {
        fmt.Println("Error listening:", err.Error())
        os.Exit(1)
    }
    // Close the listener when the application closes.
    defer l.Close()
    fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
    for {
        // Listen for an incoming connection.
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
        }
        // Handle connections in a new goroutine.
        go handleRequest(conn)
        fmt.Println("sent to handle request")

    }
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
  reader := bufio.NewReader(conn)
  for { // read first line
    b,_,_ := reader.ReadLine()
    resp := string(b) // extract the text from the buffer
    fmt.Println(resp); 
    arr := strings.Split(resp, " ") // split into OK and <version>
    if arr[0]== "write" || arr[0]=="read" || arr[0]== "delete" || arr[0] == "cas"{
      name := arr[1]
      _,ok := m[name]
      if !ok{
        filetokens[name] = make(chan bool)
        go func(){
          filetokens[name]<-true
        }()
        os.Create(name)
        fmt.Sprintln("created %v",name)
      }
      <-filetokens[name]
      if arr[0]=="write"{
        size,err := strconv.Atoi(arr[2])
        contents := readerHelper(reader,size)
        bcont := []byte(contents)
        fmt.Sprintln("size of return read %v",len(bcont))
        err = ioutil.WriteFile(name,bcont,0644)
        sendError(conn,err,4)
        var v int
        if ok{
          v = m[name].version + 1
        } else{
          v = 0
        }
        var fi FileInfo
        fi.size,err = strconv.Atoi(arr[2])
        sendError(conn,err,4)
        fi.version = v
        fi.lifetime,err = strconv.Atoi(arr[3])
        sendError(conn,err,4)
        m[name]=fi
        fmt.Fprintf(conn, "OK %v\r\n", fi.version)
        go killSelf(name,m[name].lifetime,m[name].version)
      } else if arr[0] == "read"{
        if ok{
          dat,err := ioutil.ReadFile(name)
          sendError(conn,err,4)
          fi := m[name]
          fmt.Fprintf(conn, "CONTENTS %v %v %v \r\n%v\r\n", fi.version, fi.size, fi.lifetime,string(dat))
        } else{
           sendError(conn,nil,2)
         }
      } else if arr[0] == "cas"{
        size,_ := strconv.Atoi(arr[2])
        contents := readerHelper(reader,size)
        if ok{
          v,_ := strconv.Atoi(arr[2])
          if m[name].version == v{
            var fi FileInfo
            fi.size,_ = strconv.Atoi(arr[2])
            fi.version = m[name].version + 1
            fi.lifetime,_ = strconv.Atoi(arr[3])
            m[name]=fi
            bcont := []byte(contents)
            err := ioutil.WriteFile(name,bcont,0644) //error here
            sendError(conn,err,4)
            go killSelf(name,m[name].lifetime,m[name].version)
            fmt.Fprintf(conn, "OK %v\r\n", fi.version)
          }else{
            sendError(conn,nil,1)
          }
        }else{
          sendError(conn,nil,2)
        }
      } else if arr[0] == "delete" {
        if ok{
          delete(m,arr[1])
          err := os.Remove(name)
          sendError(conn,err,4)
          fmt.Fprintf(conn, "OK\r\n")
        }else{
          sendError(conn,nil,2)
        }
      }
      go func(){
        filetokens[name]<-true
      }()
    } else{
      sendError(conn,nil,3)
    }
 }  
  // Close the connection when you're done with it.
  conn.Close()
}

func killSelf(name string, lifetime int, version_old int){
  time.Sleep (time.Duration(lifetime) * time.Second)
  if version_old == m[name].version{
     delete(m,name)
     os.Remove(name)
  }
}

func sendError(conn net.Conn, err error, errortype int){
  if err != nil{
    fmt.Fprintf(conn, "ERR_INTERNAL\r\n")
  }
  if errortype == 1{
    fmt.Fprintf(conn, "ERR_VERSION\r\n")
  }else if errortype == 2{
    fmt.Fprintf(conn, "ERR_FILE_NOT_FOUND\r\n")
  }else if errortype == 3{
    fmt.Fprintf(conn, "ERR_CMD_ERR\r\n")
  }
}

func readerHelper(reader *bufio.Reader, size int) []byte{
  var c []byte
  for i:=1;i<=size;i++{
    x,_ := reader.ReadByte()
    c = append(c,x) 
  }
  reader.ReadByte();
  reader.ReadByte();
  return []byte(c)
}