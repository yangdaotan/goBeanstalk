package goBeanstalk

import (
	"bytes"
	"strconv"
	"strings"
)

func ParseDict(dat []byte) map[string]string {
	if dat == nil {
		return nil
	}
	d := make(map[string]string)
	if bytes.HasPrefix(dat, yamlHead) {
		dat = dat[4:]
	}
	for _, s := range bytes.Split(dat, nl) {
		kv := bytes.SplitN(s, colonSpace, 2)
		if len(kv) != 2 {
			continue
		}
		d[string(kv[0])] = string(kv[1])
	}
	return d
}

func ParseList(dat []byte) []string {
	if dat == nil {
		return nil
	}
	l := []string{}
	if bytes.HasPrefix(dat, yamlHead) {
		dat = dat[4:]
	}
	for _, s := range bytes.Split(dat, nl) {
		if !bytes.HasPrefix(s, minusSpace) {
			continue
		}
		l = append(l, string(s[2:]))
	}
	return l
}

func ParseSize(s string) (string, int, error) {
	i := strings.LastIndex(s, " ")
	if i == -1 { //不存在" "
		return "", 0, FindRespError(s)
	}
	n, err := strconv.Atoi(s[i+1:]) //string转为int，如： "   123", 从1开始取
	if err != nil {
		return "", 0, err
	}
	return s[:i], n, nil //s[0:i]
}
