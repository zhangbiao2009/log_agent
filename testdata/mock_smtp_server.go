package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:2525")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Mock SMTP server listening on 127.0.0.1:2525")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept: %v\n", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	send(conn, "220 localhost Mock SMTP ready")

	var from string
	var to []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(line)

		switch {
		case strings.HasPrefix(upper, "EHLO") || strings.HasPrefix(upper, "HELO"):
			send(conn, "250 localhost")
		case strings.HasPrefix(upper, "MAIL FROM:"):
			from = line[len("MAIL FROM:"):]
			send(conn, "250 OK")
		case strings.HasPrefix(upper, "RCPT TO:"):
			to = append(to, line[len("RCPT TO:"):])
			send(conn, "250 OK")
		case upper == "DATA":
			send(conn, "354 Start mail input; end with <CRLF>.<CRLF>")
			body, err := readData(r)
			if err != nil {
				return
			}
			fmt.Println("========== EMAIL RECEIVED ==========")
			fmt.Printf("From: %s\nTo: %v\n", from, to)
			fmt.Println("--- Body ---")
			fmt.Println(body)
			fmt.Println("====================================")
			send(conn, "250 OK: message accepted")
			from = ""
			to = nil
		case upper == "QUIT":
			send(conn, "221 Bye")
			return
		case strings.HasPrefix(upper, "AUTH"):
			send(conn, "235 Authentication successful")
		default:
			send(conn, "250 OK")
		}
	}
}

func readData(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return sb.String(), err
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "." {
			break
		}
		sb.WriteString(line)
	}
	return sb.String(), nil
}

func send(conn net.Conn, msg string) {
	fmt.Fprintf(conn, "%s\r\n", msg)
}
